package server

import (
	"context"
	"fmt"
	"net"
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
	}

	// ConnectionHandler acts as server and receives data from the profiler agent
	ConnectionHandler struct {
		ctx    context.Context
		cancel context.CancelFunc
		opts   ConnectionOpts

		conn net.Conn

		socketReader *io.TcpReader
		socketWriter *io.TcpWriter
		pendingAcks  int

		pod       *ConnectedPod
		listener  Listener
		commands  uint64
		dataBytes uint64

		namespace, service, podName string
	}
)

func (sc *ConnectionHandler) Handle() {
	log.Debug(sc.ctx, " Got connection from %v ", sc.conn.RemoteAddr())
	sc.socketReader = io.PrepareTcpReader(sc)
	sc.socketWriter = io.PrepareTcpWriter(sc)

	for {
		err := sc.HandleCommand(sc.ctx)
		if err != nil {
			log.Error(sc.ctx, err, "could not")
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
		break // do nothing
	case model.COMMAND_SKIP:
		break // do nothing
	case model.COMMAND_CLOSE:
		log.Debug(ctx, " * command close [%v] ", op)
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
		err = fmt.Errorf("unknown command %02X at pos: %d (%02X) ", op, pos, pos)
		break
		//return nil, errors.Errorf("invalid dump format, unknown command %02X at pos: %d (%02X) ", op, data.Pos(), data.Pos())
		//data.Next()
	}

	if err != nil {
		pos := sc.socketReader.Pos()
		log.Error(ctx, err, " invalid format, error around pos: %d (%02X) ", op, pos, pos)
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

	// resp
	err = sc.socketWriter.WriteFixedLong(ctx, ProtocolVersion)
	if err != nil {
		return
	}
	// flush
	err = sc.socketWriter.Flush()
	if err != nil {
		return
	}

	sc.pod = &ConnectedPod{Uuid: common.RandomUuid(), Namespace: namespace, Service: service, PodName: podName}
	sc.listener.RegisterPod(sc.pod)
	log.Debug(ctx, "Received GET_PROTOCOL_VERSION_V2 [cli:%v / svr:%v] for %v/%v [%v] ",
		clProtocol, ProtocolVersion, namespace, service, podName)

	return
}

func (sc *ConnectionHandler) CommandInitStream(ctx context.Context) (err error) {
	log.Debug(sc.ctx, "Receiving COMMAND_INIT_STREAM_V2")
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

	var handleId common.Uuid
	var rotationPeriod uint64
	var requiredRotationSize uint64
	var rollingSequenceId int
	if sc.listener != nil {
		sc.listener.RegisterStream(ctx, sc.pod, handleId, streamType, resetRequired,
			requestedRollingSequenceId, rollingSequenceId, rotationPeriod, requiredRotationSize)
		log.Debug(sc.ctx, "INIT_STREAM_V2 for %v: req  => seqId=%v, reset? %v ",
			streamType, requestedRollingSequenceId, resetRequired)
		log.Debug(sc.ctx, "INIT_STREAM_V2 for %v: resp => handleId=%v, rotation (period: %v, size: %v), seqId=%v ",
			streamType, handleId, rotationPeriod, requiredRotationSize, rollingSequenceId)
	}

	//// ack?
	//err = sc.waitForAcks() // or send ?
	//if err != nil {
	//	return err
	//}

	// resp
	err = sc.socketWriter.WriteUuid(ctx, handleId)
	if err != nil {
		return
	}
	err = sc.socketWriter.WriteFixedLong(ctx, rotationPeriod)
	if err != nil {
		return
	}
	err = sc.socketWriter.WriteFixedLong(ctx, requiredRotationSize)
	if err != nil {
		return
	}
	err = sc.socketWriter.WriteFixedInt(ctx, rollingSequenceId)
	if err != nil {
		return
	}

	// flush
	err = sc.socketWriter.Flush()
	if err != nil {
		return err
	}

	return
}

func (sc *ConnectionHandler) CommandRcvData(ctx context.Context) (err error) {
	log.Trace(sc.ctx, "Receiving COMMAND_RCV_DATA")
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
	//// flush
	//sc.pendingAcks += 2
	//err = sc.socketWriter.WriteFixedByte(sc.ctx, byte(model.COMMAND_REQUEST_ACK_FLUSH))
	//if err != nil {
	//	return err
	//}
	//err = sc.socketWriter.Flush()
	//sc.check(err)

	if sc.listener != nil {
		sc.dataBytes += uint64(sc.listener.AppendData(ctx, sc.pod, handleId, chunk))
		//log.Trace(sc.ctx, "RCV_DATA for '%s' with %d bytes, [handle: %v] ", streamType, len(chunk), handleId)
	}
	return
}

func (sc *ConnectionHandler) CommandRequestFlush(ctx context.Context) (err error) {
	return sc.sendOperation(ctx, model.COMMAND_REQUEST_ACK_FLUSH, true, func(sc *ConnectionHandler) error {
		// flush
		sc.pendingAcks += 1
		err = sc.socketWriter.Flush()
		if err != nil {
			return err
		}
		return err
	})
}
func (sc *ConnectionHandler) CommandClose(ctx context.Context) (err error) {
	return sc.sendOperation(ctx, model.COMMAND_CLOSE, true, func(sc *ConnectionHandler) error {
		// flush
		err = sc.socketWriter.Flush()
		if err != nil {
			return err
		}
		return err
	})
}

func (sc *ConnectionHandler) WaitForAcks() (err error) {
	return sc.waitForAcks() // for run.go
}

func (sc *ConnectionHandler) waitForAcks() (err error) {
	for sc.pendingAcks > 0 {
		byt, err := sc.socketReader.ReadFixedByte(sc.ctx)
		if sc.check(err) != nil {
			return errors.Wrap(err, "could not get ack of RC data")
		}
		if byt != 0x00 {
			return errors.New("invalid acknowledgement for RCV data")
		}
		sc.pendingAcks--
	}
	return nil
}

func (sc *ConnectionHandler) sendOperation(ctx context.Context,
	c model.Command, flush bool, worker func(sc *ConnectionHandler) error) (err error) {

	if alive, err := sc.isAlive(); !alive {
		return err
	}
	defer func() {
		if sc.listener != nil {
			sc.listener.SentCommand(ctx, c)
		}
	}()

	err = sc.socketWriter.WriteFixedByte(sc.ctx, byte(c))
	if err != nil {
		return err
	}
	err = worker(sc)
	if err != nil {
		return err
	}
	// flush
	if flush {
		err = sc.socketWriter.Flush()
		//sc.check(err)
	}
	return err
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
	if sc.conn != nil {
		err = sc.conn.Close()
		if err != nil {
			log.Error(sc.ctx, err, "Error during closing the connection from %v ", sc.conn.RemoteAddr())
		}
	}
	sc.cancel()
	return err
}

func (sc *ConnectionHandler) isAlive() (bool, error) {
	if sc == nil || sc.conn == nil {
		return false, sc.check(ErrNotConnected)
	}
	if sc.ctx.Err() != nil {
		return false, nil
	}
	if sc.listener != nil {
		return sc.listener.IsAlive(sc.ctx)
	}
	return true, nil
}

func (sc *ConnectionHandler) check(err error) error {
	if sc == nil || sc.conn == nil {
		return ErrNotConnected
	}
	if sc.listener != nil {
		sc.listener.Error(err)
	}
	return err
}
