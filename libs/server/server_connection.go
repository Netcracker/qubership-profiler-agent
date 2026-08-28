package server

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"
)

type (
	ConnectedPod struct {
		Uuid                        common.Uuid
		Namespace, Service, PodName string
		// RestartTimeMs is the pod-restart boundary, stamped by the collector at
		// TCP accept; the agent does not transmit it (01-write-contract.md §1 V4).
		RestartTimeMs int64
	}

	// ConnectionHandler acts as server and receives data from the profiler agent
	ConnectionHandler struct {
		ctx    context.Context
		cancel context.CancelFunc
		opts   ConnectionOpts

		conn       net.Conn
		acceptedAt time.Time
		// closed makes Close idempotent: the handler's deferred Close and a
		// shutdown-driven Service.Stop race for the same socket.
		closed atomic.Bool

		socketReader *io.TcpReader
		socketWriter *io.TcpWriter

		// writeMu serializes every socketWriter access. The read loop and the
		// periodic ack flusher (periodicFlush) run in separate goroutines, and the
		// underlying bufio.Writer is not safe for concurrent use.
		writeMu sync.Mutex
		// ackBuffered marks that at least one RCV_DATA ack byte sits unflushed in
		// the write buffer, awaiting the periodic flush. Guarded by writeMu.
		ackBuffered bool

		pod       *ConnectedPod
		listener  Listener
		commands  uint64
		dataBytes uint64

		namespace, service, podName string
	}
)

func (sc *ConnectionHandler) Handle() {
	// One misbehaving connection must never take the whole collector down. A
	// decoder or handler panic (a nil deref, a bad index) is contained here:
	// log it, then fall through to the deferred Close so only this connection
	// dies while every other agent keeps streaming (№5 backstop).
	defer func() {
		if r := recover(); r != nil {
			log.Error(sc.ctx, fmt.Errorf("panic: %v", r),
				"connection handler recovered from a panic; closing this connection")
		}
	}()
	defer func() {
		_ = sc.Close()
		// The TCP connection IS the pod-restart: once it drops, the agent
		// reconnects as a fresh pod-restart (01-write-contract.md §3.7), so the
		// listener finalizes this one's state.
		if sc.listener != nil && sc.pod != nil {
			sc.listener.PodDisconnected(sc.ctx, sc.pod)
		}
	}()
	log.Debug(sc.ctx, " Got connection from %v ", sc.conn.RemoteAddr())
	sc.socketReader = io.PrepareTcpReader(sc)
	sc.socketWriter = io.PrepareTcpWriter(sc)

	// The read loop below blocks on the next command, so RCV_DATA acks written
	// buffered would sit in the write buffer until the next flushing command. The
	// agent drains all pending acks before every stream rotation (06 §5), so
	// flush them on a fixed cadence instead of waiting for a command to do it.
	stopFlusher := make(chan struct{})
	go sc.periodicFlush(stopFlusher)
	defer close(stopFlusher)

	for {
		err := sc.HandleCommand(sc.ctx)
		if err != nil {
			if err == errAgentClosed || io.IsExpectedDisconnect(err) {
				log.Debug(sc.ctx, "connection handler stopped: %v", err)
			} else {
				log.Error(sc.ctx, err, "connection handler stopped")
			}
			break
		}
	}
}

func (sc *ConnectionHandler) HandleCommand(ctx context.Context) (err error) {
	var read byte
	read, err = sc.socketReader.ReadFixedByte(ctx)
	if err != nil {
		return
	}
	op := model.Command(read)
	sc.commands++

	startTime := time.Now()
	defer func() {
		if sc.listener != nil {
			sc.listener.ReceivedCommand(ctx, op, time.Since(startTime), err)
		}
	}()

	switch op {
	case model.COMMAND_REPORT_COMMAND_RESULT:
		err = sc.CommandReportResult(ctx)
		break
	case model.COMMAND_REQUEST_ACK_FLUSH:
		err = sc.CommandAckFlush(ctx)
		break
	case model.COMMAND_CLOSE:
		log.Debug(ctx, " * command close [%v] ", op)
		err = errAgentClosed
		break
	case model.COMMAND_GET_PROTOCOL_VERSION_V2:
		err = sc.CommandGetProtocolVersion(ctx)
		break

	case model.COMMAND_INIT_STREAM_V2:
		err = sc.CommandInitStream(ctx)
		break

	case model.COMMAND_RCV_DATA:
		err = sc.CommandRcvData(ctx)
		break

	default:
		sc.socketReader.Done()
		pos := sc.socketReader.Pos()
		// Signal the agent to reconnect rather than letting it stall on a missing
		// ack; the stream is unrecoverable once framing is lost (06 §2, §6).
		_ = sc.writeAck(ctx, model.ACK_ERROR_MAGIC, true)
		err = fmt.Errorf("unknown command %02X at pos: %d (%02X) ", op, pos, pos)
		break
	}

	if err != nil && err != errAgentClosed {
		pos := sc.socketReader.Pos()
		if io.IsExpectedDisconnect(err) {
			log.Debug(ctx, "command %02X: connection closed around pos: %d (%02X): %v", op, pos, pos, err)
		} else {
			log.Error(ctx, err, " command %02X failed around pos: %d (%02X) ", op, pos, pos)
		}
	}
	return err
}

func (sc *ConnectionHandler) CommandReportResult(ctx context.Context) (err error) {
	var executedCommandId common.Uuid
	executedCommandId, err = sc.socketReader.ReadUuid(ctx)
	if err != nil {
		return
	}
	var success byte
	success, err = sc.socketReader.ReadFixedByte(ctx)
	if err != nil {
		return
	}
	log.Debug(ctx, "command id [%v], success? %v ", executedCommandId, success)
	return
}

func (sc *ConnectionHandler) CommandGetProtocolVersion(ctx context.Context) (err error) {
	log.Debug(sc.ctx, "Receiving GET_PROTOCOL_VERSION_V2")
	var clProtocol uint64
	clProtocol, err = sc.socketReader.ReadFixedLong(ctx)
	if err != nil {
		return
	}
	var podName, service, namespace string
	podName, err = sc.socketReader.ReadFixedString(ctx)
	if err != nil {
		return
	}
	service, err = sc.socketReader.ReadFixedString(ctx)
	if err != nil {
		return
	}
	namespace, err = sc.socketReader.ReadFixedString(ctx)
	if err != nil {
		return
	}

	// resp — hold writeMu so the periodic flusher never races the bufio writer.
	sc.writeMu.Lock()
	err = sc.socketWriter.WriteFixedLong(ctx, ProtocolVersion)
	if err == nil {
		err = sc.flushWriterLocked() // flush
	}
	sc.writeMu.Unlock()
	if err != nil {
		return
	}

	podUuid, err := common.RandomUuidChecked()
	if err != nil {
		return errors.Wrap(err, "generate pod uuid")
	}
	sc.pod = &ConnectedPod{Uuid: podUuid, Namespace: namespace, Service: service, PodName: podName,
		RestartTimeMs: sc.acceptedAt.UnixMilli()}
	if err = sc.listener.RegisterPod(sc.pod); err != nil {
		// No ack protocol exists at handshake time; closing the socket makes
		// the agent reconnect (06 §6).
		sc.pod = nil
		return errors.Wrap(err, "register pod")
	}
	log.Debug(ctx, "Received GET_PROTOCOL_VERSION_V2 [cli:%v / svr:%v] for %v/%v [%v] ",
		clProtocol, ProtocolVersion, namespace, service, podName)

	return
}

func (sc *ConnectionHandler) CommandInitStream(ctx context.Context) (err error) {
	log.Debug(sc.ctx, "Receiving COMMAND_INIT_STREAM_V2")
	// The handshake sets sc.pod; without it the listener has no pod-restart to
	// register a stream against, and AppendData below would deref a nil pod and
	// crash the whole collector. Refuse the out-of-order command (№5).
	if sc.pod == nil {
		_ = sc.writeAck(ctx, model.ACK_ERROR_MAGIC, true)
		return errors.New("INIT_STREAM_V2 before the GET_PROTOCOL_VERSION handshake")
	}
	// req
	var streamType string
	streamType, err = sc.socketReader.ReadFixedString(ctx)
	if err != nil {
		return
	}
	var requestedRollingSequenceId int
	requestedRollingSequenceId, err = sc.socketReader.ReadFixedInt(ctx)
	if err != nil {
		return
	}
	var resetRequired int
	resetRequired, err = sc.socketReader.ReadFixedInt(ctx)
	if err != nil {
		return
	}

	// An unknown stream gets a null handle and a teardown; the agent reads the
	// null UUID, throws, and reconnects (06 §4, §6).
	if !model.IsKnownStream(streamType) {
		sc.replyNullHandle(ctx)
		return fmt.Errorf("unknown stream %q from %v", streamType, sc.pod)
	}

	// The collector owns the handle and the rotation policy (06 §4). The handle
	// must be non-nil and stable: the agent keys every RCV_DATA by it. A zero
	// handle is read as the null UUID and drives the agent into a reconnect
	// loop, so surface a crypto/rand failure rather than emit one (wire-LOW).
	// The rolling sequence echoes the agent's request; a reset restarts from it.
	handleId, err := common.RandomUuidChecked()
	if err != nil {
		sc.replyNullHandle(ctx)
		return errors.Wrap(err, "generate stream handle")
	}
	rotationPeriod := sc.opts.RotationPeriod
	requiredRotationSize := sc.opts.RequiredRotationSize
	if requiredRotationSize == 0 {
		requiredRotationSize = DefaultRequiredRotationSize
	}
	rollingSequenceId := requestedRollingSequenceId

	if sc.listener != nil {
		if err = sc.listener.RegisterStream(ctx, sc.pod, handleId, streamType, resetRequired,
			requestedRollingSequenceId, rollingSequenceId, rotationPeriod, requiredRotationSize); err != nil {
			// A failing INIT_STREAM_V2 handler answers like an unknown stream:
			// null handle, then teardown (06 §6).
			sc.replyNullHandle(ctx)
			return errors.Wrapf(err, "register stream %q", streamType)
		}
	}
	log.Debug(sc.ctx, "INIT_STREAM_V2 for %v: req  => seqId=%v, reset? %v ",
		streamType, requestedRollingSequenceId, resetRequired)
	log.Debug(sc.ctx, "INIT_STREAM_V2 for %v: resp => handleId=%v, rotation (period: %v, size: %v), seqId=%v ",
		streamType, handleId, rotationPeriod, requiredRotationSize, rollingSequenceId)

	// resp — hold writeMu so the periodic flusher never races the bufio writer.
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	if err = sc.socketWriter.WriteUuid(ctx, handleId); err != nil {
		return
	}
	if err = sc.socketWriter.WriteFixedLong(ctx, rotationPeriod); err != nil {
		return
	}
	if err = sc.socketWriter.WriteFixedLong(ctx, requiredRotationSize); err != nil {
		return
	}
	if err = sc.socketWriter.WriteFixedInt(ctx, rollingSequenceId); err != nil {
		return
	}
	// flush
	return sc.flushWriterLocked()
}

func (sc *ConnectionHandler) CommandRcvData(ctx context.Context) (err error) {
	log.Trace(sc.ctx, "Receiving COMMAND_RCV_DATA")
	// A pre-handshake RCV_DATA has no registered pod-restart; routing it would
	// deref a nil sc.pod inside the listener and crash the collector. Signal the
	// agent to reconnect from a clean handshake instead (№5).
	if sc.pod == nil {
		_ = sc.writeAck(ctx, model.ACK_ERROR_MAGIC, true)
		return errors.New("RCV_DATA before the GET_PROTOCOL_VERSION handshake")
	}
	var handleId common.Uuid
	handleId, err = sc.socketReader.ReadUuid(ctx)
	if err != nil {
		return
	}
	var chunk string
	chunk, err = sc.socketReader.ReadFixedString(ctx)
	if err != nil {
		return
	}

	if sc.listener != nil {
		n, err := sc.listener.AppendData(ctx, sc.pod, handleId, chunk)
		sc.dataBytes += uint64(n)
		if err != nil {
			// A failing RCV_DATA handler signals ACK_ERROR_MAGIC before the
			// teardown so the agent reconnects rather than stalling (06 §6).
			_ = sc.writeAck(ctx, model.ACK_ERROR_MAGIC, true)
			return errors.Wrap(err, "append data")
		}
	}

	// One ack byte per payload (06 §5). Written buffered; the agent's flush
	// cycle sends REQUEST_ACK_FLUSH, which forces these out (see CommandAckFlush).
	return sc.writeAck(ctx, model.ACK_OK, false)
}

// CommandAckFlush answers a REQUEST_ACK_FLUSH with one ack byte and forces a
// flush, draining every buffered RCV_DATA ack in order (06 §5).
func (sc *ConnectionHandler) CommandAckFlush(ctx context.Context) (err error) {
	return sc.writeAck(ctx, model.ACK_OK, true)
}

// writeAck writes a single acknowledgement byte to the agent, optionally
// flushing. value is either ACK_OK (the diagnostic-command count, always 0 in
// the MVP) or ACK_ERROR_MAGIC to force a reconnect (06 §5, §6). A buffered ack
// (flush = false) is drained by periodicFlush within FlushCheckInterval.
func (sc *ConnectionHandler) writeAck(ctx context.Context, value byte, flush bool) error {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	if err := sc.socketWriter.WriteFixedByte(ctx, value); err != nil {
		return err
	}
	if flush {
		return sc.flushWriterLocked()
	}
	sc.ackBuffered = true
	return nil
}

// flushWriterLocked flushes the socket write buffer and clears the pending-ack
// marker, so the next periodic tick has nothing left to drain. The caller must
// hold writeMu.
func (sc *ConnectionHandler) flushWriterLocked() error {
	sc.ackBuffered = false
	return sc.socketWriter.Flush()
}

// replyNullHandle answers INIT_STREAM_V2 with the null UUID and flushes — the
// teardown the agent reads before it throws and reconnects (06 §4, §6). Errors
// are ignored: the connection is being torn down regardless.
func (sc *ConnectionHandler) replyNullHandle(ctx context.Context) {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	_ = sc.socketWriter.WriteUuid(ctx, common.Uuid{})
	_ = sc.flushWriterLocked()
}

// periodicFlush drains buffered RCV_DATA acks on a fixed cadence so the agent's
// pre-rotation ack drain never blocks on an ack the collector has written but
// not yet flushed (06 §5). It runs for the lifetime of the connection and exits
// when Handle closes stop. A flush error means the write side is already gone;
// the read loop observes the same failure and tears the connection down, so the
// flusher just stops.
func (sc *ConnectionHandler) periodicFlush(stop <-chan struct{}) {
	ticker := time.NewTicker(FlushCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := sc.flushPendingAcks(); err != nil {
				log.Debug(sc.ctx, "periodic ack flush stopped: %v", err)
				return
			}
		}
	}
}

// flushPendingAcks flushes the write buffer when an RCV_DATA ack is buffered,
// and is a no-op otherwise so an idle connection never touches the socket.
func (sc *ConnectionHandler) flushPendingAcks() error {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()
	if !sc.ackBuffered {
		return nil
	}
	return sc.flushWriterLocked()
}

// Read wrapper around tcp connection (add metrics, etc.)
func (sc *ConnectionHandler) Read(buf []byte) (n int, err error) {
	startTime := time.Now()
	err = sc.conn.SetReadDeadline(startTime.Add(sc.opts.Timeout.ReadTimeout))
	n, err = sc.conn.Read(buf)
	if err != nil {
		log.Debug(sc.ctx, "READ-ERR: %+v", err.Error())
	}
	if sc.listener != nil {
		sc.listener.Read(sc.ctx, n, time.Since(startTime), err)
	}
	return
}

// Write wrapper around tcp connection (add metrics, etc.)
func (sc *ConnectionHandler) Write(data []byte) (n int, err error) {
	startTime := time.Now()
	err = sc.conn.SetWriteDeadline(startTime.Add(sc.opts.Timeout.WriteTimeout))
	n, err = sc.conn.Write(data)
	if err != nil {
		log.Debug(sc.ctx, "WRITE-ERR: %+v", err.Error())
	}
	if sc.listener != nil {
		sc.listener.Write(sc.ctx, n, time.Since(startTime), err)
	}
	return
}

func (sc *ConnectionHandler) Close() (err error) {
	if sc.closed.Swap(true) {
		return nil // already closed by the other side of the Stop/Handle race
	}
	if sc.conn != nil {
		err = sc.conn.Close()
		if err != nil {
			log.Error(sc.ctx, err, "Error during closing the connection from %v ", sc.conn.RemoteAddr())
		}
	}
	if sc.cancel != nil {
		sc.cancel()
	}
	return err
}
