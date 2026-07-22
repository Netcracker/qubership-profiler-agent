package emulator

import (
	"context"
	"net"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"
)

type (
	// AgentConnection acts as a client (the profiler agent) and sends data to
	// the CDT collector. Its ack semantics mirror DefaultCollectorClient.java:
	// one pending ack per RCV_DATA and per REQUEST_ACK_FLUSH, an opportunistic
	// drain before each write, a synchronous drain at flush and before every
	// stream open (backend/docs/design/virtual-dumper.md §2.4).
	AgentConnection struct {
		podName       string
		ctx           context.Context
		cancel        context.CancelFunc
		fileReader    *io.BlobReader
		socketReader  *io.TcpReader
		socketWriter  *io.TcpWriter
		pendingAcks   int
		serverVersion uint64
		conn          net.Conn
		// probing switches Read to an immediate deadline so DrainAcks(false)
		// can poll readability without blocking (Java's in.available()).
		probing  bool
		Opts     ConnectionOpts
		listener AgentListener
	}
)

func PrepareAgent(ctx context.Context, cancel context.CancelFunc, listener AgentListener,
	podName string) (ac *AgentConnection) {

	return &AgentConnection{
		podName:  podName,
		ctx:      ctx,
		cancel:   cancel,
		listener: listener,
	}
}

func (ac *AgentConnection) Prepare(opts ConnectionOpts) *AgentConnection {
	ac.Opts = opts
	return ac
}

func (ac *AgentConnection) Pass(err error) bool {
	return err == nil
}

func (ac *AgentConnection) Connect() (err error) {
	opts := ac.Opts

	log.Debug(ac.ctx, "Connecting to %v with timeout %v ", opts.ProtocolAddress, opts.Timeout.ConnectTimeout)
	ac.conn, err = net.DialTimeout("tcp", opts.ProtocolAddress, opts.Timeout.ConnectTimeout)
	if err != nil {
		return
	}

	err = ac.conn.SetReadDeadline(time.Now().Add(opts.Timeout.SessionTimeout))
	if err != nil {
		return
	}

	ac.socketReader = io.PrepareTcpReader(ac)
	ac.socketWriter = io.PrepareTcpWriter(ac)

	return
}

func (ac *AgentConnection) InitializeConnection(protocolVersion uint64, namespace, service, pod string) (err error) {
	log.Debug(ac.ctx, "trying to execute GET_PROTOCOL_VERSION_V2 as %v", pod)
	return ac.sendOperation(model.COMMAND_GET_PROTOCOL_VERSION_V2, true, func(ac *AgentConnection) error {
		// req
		err = ac.socketWriter.WriteFixedLong(ac.ctx, protocolVersion)
		if err != nil {
			return errors.Wrapf(err, "could not read")
		}
		err = ac.socketWriter.WriteFixedString(ac.ctx, pod)
		if err != nil {
			return err
		}
		err = ac.socketWriter.WriteFixedString(ac.ctx, service)
		if err != nil {
			return err
		}
		err = ac.socketWriter.WriteFixedString(ac.ctx, namespace)
		if err != nil {
			return err
		}
		// flush
		err = ac.socketWriter.Flush()
		if err != nil {
			return err
		}

		// response
		svrProtocol, err := ac.socketReader.ReadFixedLong(ac.ctx)
		ac.serverVersion = svrProtocol
		log.Debug(ac.ctx, "GET_PROTOCOL_VERSION_V2 protocols [cli:%v-svr:%v] for %v ",
			protocolVersion, svrProtocol, pod)
		return err
	})
}

// ServerVersion returns the protocol version the collector replied with during
// the last handshake. It must be PROTOCOL_VERSION_V2 (06-wire-protocol-server.md §3).
func (ac *AgentConnection) ServerVersion() uint64 {
	return ac.serverVersion
}

// PendingAcks reports how many sent commands still await their ack byte.
func (ac *AgentConnection) PendingAcks() int {
	return ac.pendingAcks
}

// InitStream opens (or rotates) a rolling stream. It first drains every
// pending ack synchronously — the agent never lets stream opens interleave
// with data acks (DefaultCollectorClient.attemptCreateRollingChunk) — then
// sends INIT_STREAM_V2 and reads the reply. A null handle means the collector
// refused the stream and is tearing the connection down (06 §4, §6).
func (ac *AgentConnection) InitStream(streamType string, requestedSeqId int, resetRequired bool) (reply InitStreamReply, err error) {
	if err = ac.DrainAcks(true); err != nil {
		return
	}
	err = ac.sendOperation(model.COMMAND_INIT_STREAM_V2, true, func(ac *AgentConnection) error {
		// req
		if err := ac.socketWriter.WriteFixedString(ac.ctx, streamType); err != nil {
			return err
		}
		if err := ac.socketWriter.WriteFixedInt(ac.ctx, requestedSeqId); err != nil {
			return err
		}
		req := 0
		if resetRequired {
			req = 1
		}
		if err := ac.socketWriter.WriteFixedInt(ac.ctx, req); err != nil {
			return err
		}
		if err := ac.socketWriter.Flush(); err != nil {
			return err
		}
		// resp
		handle, err := ac.socketReader.ReadUuid(ac.ctx)
		if err != nil {
			return err
		}
		if handle.ToBin() == (common.UUID{}) {
			return errors.Errorf("collector refused stream %q: null handle", streamType)
		}
		rotationPeriod, err := ac.socketReader.ReadFixedLong(ac.ctx)
		if err != nil {
			return err
		}
		requiredRotationSize, err := ac.socketReader.ReadFixedLong(ac.ctx)
		if err != nil {
			return err
		}
		rollingSequenceId, err := ac.socketReader.ReadFixedInt(ac.ctx)
		if err != nil {
			return err
		}
		reply = InitStreamReply{
			Handle:               handle,
			RotationPeriodMs:     rotationPeriod,
			RequiredRotationSize: requiredRotationSize,
			SeqId:                rollingSequenceId,
		}
		log.Debug(ac.ctx, "INIT_STREAM_V2 for %v: req  => seqId=%v, reset? %v ",
			streamType, requestedSeqId, resetRequired)
		log.Debug(ac.ctx, "INIT_STREAM_V2 for %v: resp => handleId=%v, rotation (period: %v, size: %v), seqId=%v ",
			streamType, handle, rotationPeriod, requiredRotationSize, rollingSequenceId)
		return nil
	})
	return
}

// CommandInitStream keeps the historical handle-only signature; new code uses
// InitStream for the full reply.
func (ac *AgentConnection) CommandInitStream(streamType string, requestedSeqId int, resetRequired bool) (handleId common.Uuid, err error) {
	reply, err := ac.InitStream(streamType, requestedSeqId, resetRequired)
	return reply.Handle, err
}

func (ac *AgentConnection) CommandRcvStringData(streamType string, handleId common.Uuid, chunk string) (err error) {
	return ac.CommandRcvData(streamType, handleId, []byte(chunk))
}

// CommandRcvData sends one RCV_DATA payload (at most MaxBufSize bytes). It
// counts one pending ack and does not flush — the dumper flushes on its 5 s
// cadence. Acks that already arrived are consumed first so pendingAcks cannot
// grow without bound between flush cycles (validateWriteDataAcks(false)).
func (ac *AgentConnection) CommandRcvData(streamType string, handleId common.Uuid, chunk []byte) (err error) {
	if err = ac.DrainAcks(false); err != nil {
		return err
	}
	return ac.sendOperation(model.COMMAND_RCV_DATA, false, func(ac *AgentConnection) error {
		if err := ac.socketWriter.WriteUuid(ac.ctx, handleId); err != nil {
			return err
		}
		if err := ac.socketWriter.WriteFixedBuf(ac.ctx, chunk); err != nil {
			return err
		}
		ac.pendingAcks++
		return nil
	})
}

// RequestAckFlush queues one REQUEST_ACK_FLUSH command (buffered, not
// flushed) and counts its pending ack, mirroring requestAckFlush(false).
func (ac *AgentConnection) RequestAckFlush() (err error) {
	return ac.sendOperation(model.COMMAND_REQUEST_ACK_FLUSH, false, func(ac *AgentConnection) error {
		ac.pendingAcks++
		return nil
	})
}

func (ac *AgentConnection) CommandClose() (err error) {
	return ac.sendOperation(model.COMMAND_CLOSE, true, func(ac *AgentConnection) error {
		// flush
		err = ac.socketWriter.Flush()
		if err != nil {
			return err
		}
		return err
	})
}

// Flush runs the agent's flush cycle (DefaultCollectorClient.flush): queue one
// REQUEST_ACK_FLUSH, push everything to the socket, then drain every pending
// ack synchronously. The collector force-flushes its buffered acks when the
// REQUEST_ACK_FLUSH arrives, so the drain returns promptly (06 §5).
func (ac *AgentConnection) Flush() error {
	if err := ac.RequestAckFlush(); err != nil {
		return err
	}
	return ac.DrainAcks(true)
}

// WaitForAcks drains every pending ack synchronously, flushing first.
func (ac *AgentConnection) WaitForAcks() (err error) {
	return ac.DrainAcks(true)
}

// DrainAcks consumes acknowledgement bytes. sync=true mirrors
// validateWriteDataAcks(true): flush the socket, then block (under the read
// timeout) until every pending ack has arrived. sync=false mirrors the
// opportunistic drain: consume only acks that are already readable, never
// block.
func (ac *AgentConnection) DrainAcks(sync bool) error {
	if sync {
		if err := ac.socketWriter.Flush(); err != nil {
			return ac.check(err)
		}
	}
	for ac.pendingAcks > 0 {
		if !sync && !ac.ackReadable() {
			return nil
		}
		if err := ac.readOneAck(); err != nil {
			return err
		}
	}
	return nil
}

// ackReadable reports whether at least one byte can be read without blocking —
// the Go stand-in for Java's in.available() > 0. FIONREAD answers without
// touching the read state; platforms without it get a 1 ms read poll (an
// expired deadline is no use — Go fails such reads even when data is ready).
func (ac *AgentConnection) ackReadable() bool {
	if ac.socketReader.Buffered() > 0 {
		return true
	}
	if n := readableBytes(ac.conn); n >= 0 {
		return n > 0
	}
	ac.probing = true
	defer func() { ac.probing = false }()
	_, err := ac.socketReader.Peek(1)
	return err == nil
}

// readOneAck consumes one acknowledgement byte (validateAckSync). A value in
// 0..127 is the count of piggybacked diagnostic commands; ACK_ERROR_MAGIC
// surfaces as ErrAckRefused so callers can tell backpressure from breakage.
func (ac *AgentConnection) readOneAck() error {
	b, err := ac.socketReader.ReadFixedByte(ac.ctx)
	if err != nil {
		return ac.check(errors.Wrap(err, "read ack"))
	}
	switch {
	case b == model.ACK_ERROR_MAGIC:
		return ac.check(ErrAckRefused)
	case b > 0x7F:
		return ac.check(errors.Errorf("invalid ack byte 0x%02X", b))
	}
	if b > 0 {
		if err := ac.dispatchCommands(int(b)); err != nil {
			return err
		}
	}
	ac.pendingAcks--
	return nil
}

// dispatchCommands mirrors DefaultCollectorClient.dispatchCommands: read the
// piggybacked (id, command) pairs and report failure for each — the emulator
// has no diagtools to execute.
func (ac *AgentConnection) dispatchCommands(n int) error {
	for i := 0; i < n; i++ {
		id, err := ac.socketReader.ReadUuid(ac.ctx)
		if err != nil {
			return ac.check(errors.Wrap(err, "read piggybacked command id"))
		}
		if _, err := ac.socketReader.ReadFixedString(ac.ctx); err != nil {
			return ac.check(errors.Wrap(err, "read piggybacked command"))
		}
		if err := ac.reportCommandResult(id, false); err != nil {
			return err
		}
	}
	return ac.socketWriter.Flush()
}

// reportCommandResult answers one piggybacked diagnostic command
// (COMMAND_REPORT_COMMAND_RESULT; 'K' = success, 0xFF = failure).
func (ac *AgentConnection) reportCommandResult(id common.Uuid, success bool) error {
	result := byte(0xFF)
	if success {
		result = 'K'
	}
	return ac.sendOperation(model.COMMAND_REPORT_COMMAND_RESULT, false, func(ac *AgentConnection) error {
		if err := ac.socketWriter.WriteUuid(ac.ctx, id); err != nil {
			return err
		}
		return ac.socketWriter.WriteFixedByte(ac.ctx, result)
	})
}

func (ac *AgentConnection) sendOperation(c model.Command, flush bool, worker func(ac *AgentConnection) error) (err error) {
	startTime := time.Now()
	if alive, err := ac.isAlive(); !alive {
		return err
	}
	defer func() {
		if ac.listener != nil {
			ac.listener.Command(c, time.Since(startTime), err)
		}
	}()

	err = ac.socketWriter.WriteFixedByte(ac.ctx, byte(c))
	if err != nil {
		return err
	}
	err = worker(ac)
	if err != nil {
		return err
	}
	// flush
	if flush {
		err = ac.socketWriter.Flush()
	}
	return err
}

func (ac *AgentConnection) Read(buf []byte) (n int, err error) {
	startTime := time.Now()
	deadline := startTime.Add(ac.Opts.Timeout.ReadTimeout)
	if ac.probing {
		deadline = startTime.Add(time.Millisecond) // short readability poll
	}
	err = ac.conn.SetReadDeadline(deadline)
	n, err = ac.conn.Read(buf)
	if err != nil {
		if ac.probing {
			return // a failed poll is the expected idle case, not an error
		}
		log.Debug(ac.ctx, "READ-ERR: %+v", err.Error())
	}
	if ac.listener != nil {
		ac.listener.Read(n, time.Since(startTime), err)
	}
	return
}

func (ac *AgentConnection) Write(data []byte) (n int, err error) {
	startTime := time.Now()
	err = ac.conn.SetWriteDeadline(startTime.Add(ac.Opts.Timeout.WriteTimeout))
	n, err = ac.conn.Write(data)
	if err != nil {
		log.Debug(ac.ctx, "WRITE-ERR: %+v", err.Error())
	}
	if ac.listener != nil {
		ac.listener.Write(n, time.Since(startTime), err)
	}
	return
}

func (ac *AgentConnection) Close() error {
	if ac.cancel != nil {
		ac.cancel()
	}
	if ac.conn == nil {
		return nil
	}
	return ac.conn.Close()
}

func (ac *AgentConnection) isAlive() (bool, error) {
	if ac == nil || ac.conn == nil {
		return false, ac.check(ErrNotConnected)
	}
	if ac.ctx.Err() != nil {
		return false, nil
	}
	if ac.listener != nil {
		return ac.listener.IsAlive()
	}
	return true, nil
}

func (ac *AgentConnection) check(err error) error {
	if ac == nil || ac.conn == nil {
		return ErrNotConnected
	}
	if ac.listener != nil {
		ac.listener.Error(err)
	}
	return err
}
