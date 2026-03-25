package cdt

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/tools/load-generator/pkg/utils"
	profilerCommon "github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/generator"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser/streams"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"

	"net"
	"time"

	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

const (
	MaxBufSize = 1024
)

var ErrNotConnected = errors.New("not connected")

type CdtAgentConnection struct {
	vu           modules.VU
	metrics      Metrics
	tags         *metrics.TagSet
	podName      string
	ctx          context.Context
	cancel       context.CancelFunc
	fileReader   *io.BlobReader
	socketReader *io.TcpReader
	socketWriter *io.TcpWriter
	pendindAcks  int
	conn         net.Conn
	Opts         generator.Options
}

func (tcp *CdtAgentConnection) Pass(err error) bool {
	return err == nil
}

func (tcp *CdtAgentConnection) Connect(podName string) error {
	var err error
	opts := tcp.Opts

	tcp.podName = podName
	tcp.ctx, tcp.cancel = getContext(tcp.vu, opts.LogLevel)

	// Parse test duration from options
	var testDuration time.Duration
	testDuration, err = time.ParseDuration(tcp.Opts.TestDuration)
	if err != nil {
		//if tcp.check(err) != nil {
		return err
	}

	// Set context timeout
	tcp.ctx, tcp.cancel = context.WithTimeout(tcp.ctx, testDuration)

	utils.LogDebug(tcp.ctx, "Connecting to %v with timeout %v ", opts.ProtocolAddr(), opts.ConnectTimeout())
	tcp.conn, err = net.DialTimeout("tcp", opts.ProtocolAddr(), opts.ConnectTimeout())
	if err != nil {
		//if tcp.check(err) != nil {
		return err
	}

	err = tcp.conn.SetReadDeadline(time.Now().Add(opts.SessionTimeout()))
	if err != nil {
		return err
	}

	tcp.socketReader = io.PrepareTcpReader(tcp)
	tcp.socketWriter = io.PrepareTcpWriter(tcp)

	return err
}

func (tcp *CdtAgentConnection) CommandGetProtocolVersion(protocolVersion uint64, namespace, service, pod string) (err error) {
	utils.LogDebug(tcp.ctx, "trying to execute GET_PROTOCOL_VERSION_V2 as %v", pod)
	return tcp.sendOperation(model.COMMAND_GET_PROTOCOL_VERSION_V2, true, func(tcp *CdtAgentConnection) error {
		// req
		err = tcp.socketWriter.WriteFixedLong(tcp.ctx, protocolVersion)
		if err != nil {
			return err
		}
		err = tcp.socketWriter.WriteFixedString(tcp.ctx, pod)
		if err != nil {
			return err
		}
		err = tcp.socketWriter.WriteFixedString(tcp.ctx, service)
		if err != nil {
			return err
		}
		err = tcp.socketWriter.WriteFixedString(tcp.ctx, namespace)
		if err != nil {
			return err
		}
		// flush
		err = tcp.socketWriter.Flush()
		if err != nil {
			return err
		}
		// resp
		svrProtocol, err := tcp.socketReader.ReadFixedLong(tcp.ctx)
		utils.LogDebug(tcp.ctx, "GET_PROTOCOL_VERSION_V2 protocols [cli:%v-svr:%v] for %v ",
			protocolVersion, svrProtocol, pod)
		return err
	})
}
func (tcp *CdtAgentConnection) CommandInitStream(streamType string, requestedSeqId int, resetRequired bool) (handleId profilerCommon.Uuid, err error) {
	err = tcp.sendOperation(model.COMMAND_INIT_STREAM_V2, true, func(tcp *CdtAgentConnection) error {
		// req
		err = tcp.socketWriter.WriteFixedString(tcp.ctx, streamType)
		if err != nil {
			return err
		}
		err = tcp.socketWriter.WriteFixedInt(tcp.ctx, requestedSeqId)
		if err != nil {
			return err
		}
		req := 0
		if resetRequired {
			req = 1
		}
		err = tcp.socketWriter.WriteFixedInt(tcp.ctx, req)
		if err != nil {
			return err
		}
		// flush
		err = tcp.socketWriter.Flush()
		if err != nil {
			return err
		}
		// ack?
		err = tcp.waitForAcks()
		if err != nil {
			return err
		}
		// resp
		handleId, err = tcp.socketReader.ReadUuid(tcp.ctx)
		if err != nil {
			return err
		}
		rotationPeriod, err := tcp.socketReader.ReadFixedLong(tcp.ctx)
		if err != nil {
			return err
		}
		requiredRotationSize, err := tcp.socketReader.ReadFixedLong(tcp.ctx)
		if err != nil {
			return err
		}
		rollingSequenceId, err := tcp.socketReader.ReadFixedInt(tcp.ctx)
		if err != nil {
			return err
		}
		utils.LogDebug(tcp.ctx, "%s: INIT_STREAM_V2 for %v: req  => seqId=%v, reset? %v ",
			time.Now().Format(time.TimeOnly), streamType, requestedSeqId, resetRequired)
		utils.LogDebug(tcp.ctx, "%s: INIT_STREAM_V2 for %v: resp => handleId=%v, rotation (period: %v, size: %v), seqId=%v ",
			time.Now().Format(time.TimeOnly), streamType, handleId, rotationPeriod, requiredRotationSize, rollingSequenceId)
		return err
	})
	return
}
func (tcp *CdtAgentConnection) CommandRcvStringData(streamType string, handleId profilerCommon.Uuid, chunk string) (err error) {
	return tcp.CommandRcvData(streamType, handleId, []byte(chunk))
}

func (tcp *CdtAgentConnection) CommandRcvData(streamType string, handleId profilerCommon.Uuid, chunk []byte) (err error) {
	return tcp.sendOperation(model.COMMAND_RCV_DATA, false, func(tcp *CdtAgentConnection) error {
		//err = tcp.waitForAcks()
		//tcp.check(err)
		// req
		err = tcp.socketWriter.WriteUuid(tcp.ctx, handleId)
		if err != nil {
			return err
		}
		err = tcp.socketWriter.WriteFixedBuf(tcp.ctx, chunk)
		if err != nil {
			return err
		}
		// flush
		tcp.pendindAcks += 2
		err = tcp.socketWriter.WriteFixedByte(tcp.ctx, byte(model.COMMAND_REQUEST_ACK_FLUSH))
		if err != nil {
			return err
		}
		//err = tcp.socketWriter.Flush()
		//tcp.check(err)

		//utils.LogDebug(tcp.ctx, "%s: RCV_DATA for '%s' with %d bytes, [handle: %v] ", time.Now().Format(time.TimeOnly), streamType, len(chunk), handleId)
		return err
	})
}
func (tcp *CdtAgentConnection) CommandRequestFlush() (err error) {
	utils.LogDebug(tcp.ctx, "Sending COMMAND_REQUEST_ACK_FLUSH")

	return tcp.sendOperation(model.COMMAND_REQUEST_ACK_FLUSH, true, func(tcp *CdtAgentConnection) error {
		// flush
		tcp.pendindAcks += 1
		err = tcp.socketWriter.Flush()
		if err != nil {
			return err
		}
		return err
	})
}
func (tcp *CdtAgentConnection) CommandClose() (err error) {
	utils.LogDebug(tcp.ctx, "Sending COMMAND_CLOSE")

	return tcp.sendOperation(model.COMMAND_CLOSE, true, func(tcp *CdtAgentConnection) error {
		// flush
		err = tcp.socketWriter.Flush()
		if err != nil {
			return err
		}
		return err
	})
}

func (tcp *CdtAgentConnection) WaitForAcks() (err error) {
	return tcp.waitForAcks() // for run.go
}

func (tcp *CdtAgentConnection) waitForAcks() (err error) {
	for tcp.pendindAcks > 0 {
		byt, err := tcp.socketReader.ReadFixedByte(tcp.ctx)
		if tcp.check(err) != nil {
			return errors.Wrap(err, "could not get ack of RC data")
		}
		if byt != 0x00 {
			return errors.New("invalid acknowledgement for RCV data")
		}
		tcp.pendindAcks--
	}
	return nil
}
func (tcp *CdtAgentConnection) sendOperation(c model.Command, flush bool, worker func(tcp *CdtAgentConnection) error) (err error) {
	startedAt := time.Now()
	if alive, err := tcp.isAlive(); !alive {
		return err
	}
	defer func() {
		now := time.Now()
		tcp.reportStats(tcp.metrics.sendMessageTiming, now, metrics.D(now.Sub(startedAt)))
		if err != nil {
			tcp.reportStats(tcp.metrics.sendMessageErrors, now, 1)
		} else {
			tcp.reportStats(tcp.metrics.sendMessage, now, 1)
		}
	}()

	err = tcp.socketWriter.WriteFixedByte(tcp.ctx, byte(c))
	if err != nil {
		return err
	}
	err = worker(tcp)
	if err != nil {
		return err
	}
	// flush
	if flush {
		err = tcp.socketWriter.Flush()
		//tcp.check(err)
	}
	return err
}

func (tcp *CdtAgentConnection) SendCallsAsNow(requestedSeqId int, c *model.Chunk, period string) (err error) {
	t, err := time.ParseDuration(period)
	if err != nil {
		t = 0
	}
	return tcp.SendCalls(requestedSeqId, c, time.Now().UnixMilli(), t)
}

func (tcp *CdtAgentConnection) SendCalls(requestedSeqId int, c *model.Chunk, emulatedTs int64, period time.Duration) (err error) {
	err = c.ReplaceLong(tcp.ctx, 8, uint64(emulatedTs))
	if err != nil {
		return err
	}

	operations := len(c.Bytes()) / MaxBufSize
	wait := int(period) / (operations + 1)
	return tcp.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), len(c.Bytes()), time.Duration(wait))
}

func (tcp *CdtAgentConnection) SendTraces(requestedSeqId int, c *model.Chunk, periodStr string) (err error) {
	period, err := time.ParseDuration(periodStr)
	if err != nil {
		period = 0
	}
	operations := len(c.Bytes()) / MaxBufSize
	wait := int(period) / (operations + 1)
	return tcp.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), len(c.Bytes()), time.Duration(wait))
}

func (tcp *CdtAgentConnection) SendDictionary(requestedSeqId int, c *model.Chunk, limitPhrases int, limitWords int) (err error) {
	_, pos, err := streams.ReadDictionaryUntil(tcp.ctx, c, limitPhrases, limitWords)
	if err != nil {
		return err
	}

	return tcp.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), pos, 10*time.Microsecond)
}

func (tcp *CdtAgentConnection) SendChunk(requestedSeqId int, c *model.Chunk) (err error) {
	utils.LogTrace(tcp.ctx, "sending for pod '%s' chunk#%s[%d] with %d bytes",
		tcp.podName, c.StreamType, requestedSeqId, len(c.Bytes()))
	return tcp.sendChunkBytes(requestedSeqId, c.StreamType, c.Bytes(), len(c.Bytes()), 0)
}

func (tcp *CdtAgentConnection) sendChunkBytes(requestedSeqId int, streamType string, buf []byte, n int, waitTime time.Duration) (err error) {
	handleId, err := tcp.CommandInitStream(streamType, requestedSeqId, false)
	if err != nil {
		return err
	}
	utils.LogDebug(tcp.ctx, "[%s] received id for chunk#%s[%s]", tcp.podName, streamType, handleId.String())

	for i := 0; i < n; i += MaxBufSize {

		select {
		case <-tcp.ctx.Done():
			utils.LogDebug(tcp.ctx, "[%s] sendChunkBytes deadline exceeded: %v", tcp.podName, tcp.ctx.Err())
			return tcp.ctx.Err()
		default:
		}

		var arr []byte
		if i+MaxBufSize < n {
			arr = buf[i : i+MaxBufSize]
		} else {
			arr = buf[i:n]
		}
		if len(arr) > 0 {
			err = tcp.CommandRcvData(streamType, handleId, arr)
			if err != nil {
				return err
			}
			if waitTime > 0 {
				time.Sleep(waitTime)
			}
		}
	}

	// flush
	err = tcp.socketWriter.Flush()
	utils.LogDebug(tcp.ctx, "[%s] flushed chunk#%s[%s]", tcp.podName, streamType, handleId.String())
	return tcp.check(err)
}

func (tcp *CdtAgentConnection) Read(buf []byte) (n int, err error) {

	now := time.Now()
	n, err = tcp.conn.Read(buf)
	//check(err)
	if err != nil {
		tcp.reportStats(tcp.metrics.readMessageErrors, now, 1)
		utils.LogDebug(tcp.ctx, "READ-ERR: %+v", err.Error())
	} else {
		tcp.reportStats(tcp.metrics.readMessage, now, 1)
		tcp.reportStats(tcp.metrics.dataReceived, time.Now(), float64(n))
	}
	return
}

func (tcp *CdtAgentConnection) Write(data []byte) (n int, err error) {
	now := time.Now()
	n, err = tcp.conn.Write(data)
	if err != nil {
		tcp.reportStats(tcp.metrics.sendMessageErrors, now, 1)
		utils.LogDebug(tcp.ctx, "WRITE-ERR:", err.Error())
	} else {
		tcp.reportStats(tcp.metrics.sendMessage, now, 1)
		tcp.reportStats(tcp.metrics.dataSent, time.Now(), float64(n))
	}
	return
}

func (tcp *CdtAgentConnection) Close() error {
	utils.LogDebug(tcp.ctx, "Closing connection")
	tcp.cancel()
	if tcp.conn == nil {
		return nil
	}
	return tcp.conn.Close()
}

func (tcp *CdtAgentConnection) isAlive() (bool, error) {
	if tcp == nil || tcp.conn == nil {
		return false, tcp.check(ErrNotConnected)
	}
	if tcp.ctx.Err() != nil {
		return false, nil
	}
	if tcp.vu != nil && (tcp.vu.Context().Err() != nil || tcp.vu.State() == nil) {
		return false, nil
	}
	return true, nil
}

func (tcp *CdtAgentConnection) check(err error) error {
	if tcp == nil || tcp.conn == nil {
		return ErrNotConnected
	}
	if err != nil {
		if tcp.vu != nil {
			common.Throw(tcp.vu.Runtime(), err)
		} else {
			panic(err)
		}
	}
	return err
}

func (tcp *CdtAgentConnection) reportStats(metric *metrics.Metric, now time.Time, value float64) {
	if metric == nil {
		return
	}

	state := tcp.vu.State()
	ctx := tcp.vu.Context()
	if state == nil || ctx == nil {
		return
	}

	// Add relevant tags (scenario, test-wide tags, etc.) to our custom metrics
	allTags := tcp.vu.State().Tags.GetCurrentValues().Tags
	//allTags := tcp.tags.WithTagsFromMap(state.Options.RunTags)

	metrics.PushIfNotDone(ctx, state.Samples, metrics.Sample{
		Time: now,
		TimeSeries: metrics.TimeSeries{
			Metric: metric,
			Tags:   allTags,
		},
		Value: value,
	})
}
