package emulator

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/streams"
	"net"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"
)

type (
	// AgentConnection acts as a client (the profiler agent) and sends data to the CDT collector
	AgentConnection struct {
		podName      string
		ctx          context.Context
		cancel       context.CancelFunc
		fileReader   *io.BlobReader
		socketReader *io.TcpReader
		socketWriter *io.TcpWriter
		pendingAcks  int
		conn         net.Conn
		Opts         ConnectionOpts
		listener     AgentListener
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
		//if ac.check(err) != nil {
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
		log.Debug(ac.ctx, "GET_PROTOCOL_VERSION_V2 protocols [cli:%v-svr:%v] for %v ",
			protocolVersion, svrProtocol, pod)
		return err
	})
}
func (ac *AgentConnection) CommandInitStream(streamType string, requestedSeqId int, resetRequired bool) (handleId common.Uuid, err error) {
	err = ac.sendOperation(model.COMMAND_INIT_STREAM_V2, true, func(ac *AgentConnection) error {
		// req
		err = ac.socketWriter.WriteFixedString(ac.ctx, streamType)
		if err != nil {
			return err
		}
		err = ac.socketWriter.WriteFixedInt(ac.ctx, requestedSeqId)
		if err != nil {
			return err
		}
		req := 0
		if resetRequired {
			req = 1
		}
		err = ac.socketWriter.WriteFixedInt(ac.ctx, req)
		if err != nil {
			return err
		}
		// flush
		err = ac.socketWriter.Flush()
		if err != nil {
			return err
		}
		// ack?
		err = ac.waitForAcks()
		if err != nil {
			return err
		}
		// resp
		handleId, err = ac.socketReader.ReadUuid(ac.ctx)
		if err != nil {
			return err
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
		log.Debug(ac.ctx, "%s: INIT_STREAM_V2 for %v: req  => seqId=%v, reset? %v ",
			time.Now().Format(time.TimeOnly), streamType, requestedSeqId, resetRequired)
		log.Debug(ac.ctx, "%s: INIT_STREAM_V2 for %v: resp => handleId=%v, rotation (period: %v, size: %v), seqId=%v ",
			time.Now().Format(time.TimeOnly), streamType, handleId, rotationPeriod, requiredRotationSize, rollingSequenceId)
		return err
	})
	return
}
func (ac *AgentConnection) CommandRcvStringData(streamType string, handleId common.Uuid, chunk string) (err error) {
	return ac.CommandRcvData(streamType, handleId, []byte(chunk))
}

func (ac *AgentConnection) CommandRcvData(streamType string, handleId common.Uuid, chunk []byte) (err error) {
	return ac.sendOperation(model.COMMAND_RCV_DATA, false, func(ac *AgentConnection) error {
		//err = ac.waitForAcks()
		//ac.check(err)
		// req
		err = ac.socketWriter.WriteUuid(ac.ctx, handleId)
		if err != nil {
			return err
		}
		err = ac.socketWriter.WriteFixedBuf(ac.ctx, chunk)
		if err != nil {
			return err
		}
		// flush
		ac.pendingAcks += 2
		err = ac.socketWriter.WriteFixedByte(ac.ctx, byte(model.COMMAND_REQUEST_ACK_FLUSH))
		if err != nil {
			return err
		}
		//err = ac.socketWriter.Flush()
		//ac.check(err)

		//utils.LogDebug(ac.ctx, "%s: RCV_DATA for '%s' with %d bytes, [handle: %v] ", time.Now().Format(time.TimeOnly), streamType, len(chunk), handleId)
		return err
	})
}
func (ac *AgentConnection) CommandRequestFlush() (err error) {
	return ac.sendOperation(model.COMMAND_REQUEST_ACK_FLUSH, true, func(ac *AgentConnection) error {
		// flush
		ac.pendingAcks += 1
		err = ac.socketWriter.Flush()
		if err != nil {
			return err
		}
		return err
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

func (ac *AgentConnection) WaitForAcks() (err error) {
	return ac.waitForAcks() // for run.go
}

func (ac *AgentConnection) waitForAcks() (err error) {
	for ac.pendingAcks > 0 {
		byt, err := ac.socketReader.ReadFixedByte(ac.ctx)
		if ac.check(err) != nil {
			return errors.Wrap(err, "could not get ack of RC data")
		}
		if byt != 0x00 {
			return errors.New("invalid acknowledgement for RCV data")
		}
		ac.pendingAcks--
	}
	return nil
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
		//ac.check(err)
	}
	return err
}

func (ac *AgentConnection) SendCallsAsNow(requestedSeqId int, c *model.Chunk, period string) (err error) {
	t, err := time.ParseDuration(period)
	if err != nil {
		t = 0
	}
	return ac.SendCalls(requestedSeqId, c, time.Now().UnixMilli(), t)
}

func (ac *AgentConnection) SendCalls(requestedSeqId int, c *model.Chunk, emulatedTs int64, period time.Duration) (err error) {
	err = c.ReplaceLong(ac.ctx, 8, uint64(emulatedTs))
	operations := len(c.Bytes()) / MaxBufSize
	wait := int(period) / (operations + 1)
	return ac.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), len(c.Bytes()), time.Duration(wait))
}

func (ac *AgentConnection) SendTraces(requestedSeqId int, c *model.Chunk, periodStr string) (err error) {
	period, err := time.ParseDuration(periodStr)
	if err != nil {
		period = 0
	}
	operations := len(c.Bytes()) / MaxBufSize
	wait := int(period) / (operations + 1)
	return ac.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), len(c.Bytes()), time.Duration(wait))
}

func (ac *AgentConnection) SendDictionary(requestedSeqId int, c *model.Chunk, limitPhrases int, limitWords int) (err error) {
	var pos int
	_, pos, err = streams.ReadDictionaryUntil(ac.ctx, c, limitPhrases, limitWords)
	return ac.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), pos, 10*time.Microsecond)
}

func (ac *AgentConnection) SendChunk(requestedSeqId int, c *model.Chunk) (err error) {
	log.Trace(ac.ctx, "sending for pod '%s' chunk#%s[%d] with %d bytes",
		ac.podName, c.StreamType, requestedSeqId, len(c.Bytes()))
	return ac.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), len(c.Bytes()), 0)
}

func (ac *AgentConnection) sendChunkBytes(requestedSeqId int, streamType string, buf []byte, n int, waitTime time.Duration) (err error) {
	handleId, err := ac.CommandInitStream(streamType, requestedSeqId, false)
	if err != nil {
		return err
	}
	log.Debug(ac.ctx, "[%s] received id for chunk#%s[%s]", ac.podName, streamType, handleId.String())

	for i := 0; i < n; i += MaxBufSize {
		var arr []byte
		if i+MaxBufSize < n {
			arr = buf[i : i+MaxBufSize]
		} else {
			arr = buf[i:n]
		}
		if len(arr) > 0 {
			err = ac.CommandRcvData(streamType, handleId, arr)
			if err != nil {
				return err
			}
			if waitTime > 0 {
				time.Sleep(waitTime)
			}
		}
	}

	// flush
	err = ac.socketWriter.Flush()
	log.Debug(ac.ctx, "[%s] flushed chunk#%s[%s]", ac.podName, streamType, handleId.String())
	return ac.check(err)
}

func (ac *AgentConnection) Read(buf []byte) (n int, err error) {
	startTime := time.Now()
	err = ac.conn.SetReadDeadline(startTime.Add(ac.Opts.Timeout.ReadTimeout))
	n, err = ac.conn.Read(buf)
	if err != nil {
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
