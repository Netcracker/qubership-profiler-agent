package parser

import (
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	TcpFile struct { // original file with caught data (by WireShark, etc.)
		FileName string // name (for logs)
		FilePath string // full path
	}

	LoadedTcpData struct { // object with parsed data (no methods! -- because of k6 plugin)
		Origin          TcpFile
		Size            int64
		ProtocolVersion uint64
		Namespace       string
		Microservice    string
		PodName         string
		Streams         map[string]*model.Chunk
		StreamTypes     map[model.StreamType]common.Uuid // latest registered by type
	}
)

func ParsePodTcpDump(ctx context.Context, file TcpFile) (*LoadedTcpData, error) {
	receivedTcpData, err := io.OpenFileAsBlob(file.FilePath)
	if err != nil {
		return nil, err
	}
	pod := &LoadedTcpData{
		Origin:          file,
		ProtocolVersion: 100605,
		Namespace:       "unknown",
		Microservice:    "unknown",
		PodName:         "unknown",
		Size:            int64(receivedTcpData.Len),
		Streams:         map[string]*model.Chunk{},
		StreamTypes:     map[string]common.Uuid{},
	}

	dataBytes, err := ParseProtocol(ctx, receivedTcpData, nil, &PodListener{pod, receivedTcpData})
	log.Debug(ctx, "Total %d bytes of stream data from %d bytes total [%d%%]",
		dataBytes, pod.Size, 100*int64(dataBytes)/pod.Size)
	return pod, err
}

func ParseProtocol(ctx context.Context, r io.Reader, wr io.Writer, listener Listener) (uint64, error) {
	var err error
	dataBytes := uint64(0)
	req := 0
	for !r.EOF() {
		var read byte
		read, err = r.ReadFixedByte(ctx)
		if err != nil {
			break
		}
		op := model.Command(read)
		req++

		switch op {
		case model.COMMAND_REPORT_COMMAND_RESULT:
			var executedCommandId common.Uuid
			executedCommandId, err = r.ReadUuid(ctx)
			if err != nil {
				break
			}
			var success byte
			success, err = r.ReadFixedByte(ctx)
			if err != nil {
				break
			}
			if listener != nil {
				listener.PrintDebug(ctx)
				log.Debug(ctx, "command id [%v], success? %v ", executedCommandId, success)
			}
			break
		case model.COMMAND_REQUEST_ACK_FLUSH:
			break // do nothing
		case model.COMMAND_SKIP:
			break // do nothing
		case model.COMMAND_CLOSE:
			//data.next() // do nothing
			if listener != nil {
				listener.PrintDebug(ctx)
				log.Debug(ctx, " * command close [%v] ", op)
			}
			break
		case model.COMMAND_GET_PROTOCOL_VERSION:
			var clProtocol uint64
			clProtocol, err = r.ReadFixedLong(ctx)
			if err != nil {
				break
			}
			if listener != nil {
				listener.PrintDebug(ctx)
				log.Debug(ctx, "client protocol: %v", clProtocol)
			}
			break
		case model.COMMAND_GET_PROTOCOL_VERSION_V2:
			var clProtocol uint64
			clProtocol, err = r.ReadFixedLong(ctx)
			if err != nil {
				break
			}
			var podName, service, namespace string
			podName, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			service, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			namespace, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}

			// resp
			var svrProtocol uint64
			svrProtocol, err = r.ReadFixedLong(ctx)
			if err != nil {
				break
			}
			if listener != nil {
				listener.RegisterPod(clProtocol, namespace, service, podName)
				log.Debug(ctx, "Protocols [cli:%v-svr:%v] for %v/%v [%v] ",
					clProtocol, svrProtocol, namespace, service, podName)
			}
			break

		case model.COMMAND_INIT_STREAM:
			// deprecated
			var podName, service, namespace string
			namespace, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			service, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			podName, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			if listener != nil {
				// In this command, we do not receive the client protocol version,
				// but since it is deprecated, we just pass 0.
				listener.RegisterPod(0, namespace, service, podName)
				log.Debug(ctx, "INIT_STREAM for %v/%v [%v] ", namespace, service, podName)
			}
			break

		case model.COMMAND_INIT_STREAM_V2:
			// req
			var streamType string
			streamType, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			var requestedRollingSequenceId int
			requestedRollingSequenceId, err = r.ReadFixedInt(ctx)
			if err != nil {
				break
			}
			var resetRequired int
			resetRequired, err = r.ReadFixedInt(ctx)
			if err != nil {
				break
			}
			// resp
			var handleId common.Uuid
			handleId, err = r.ReadUuid(ctx)
			if err != nil {
				break
			}
			var rotationPeriod uint64
			rotationPeriod, err = r.ReadFixedLong(ctx)
			if err != nil {
				break
			}
			var requiredRotationSize uint64
			requiredRotationSize, err = r.ReadFixedLong(ctx)
			if err != nil {
				break
			}
			var rollingSequenceId int
			rollingSequenceId, err = r.ReadFixedInt(ctx)
			if err != nil {
				break
			}
			if listener != nil {
				listener.RegisterStream(ctx, handleId, streamType, resetRequired,
					requestedRollingSequenceId, rollingSequenceId, rotationPeriod, requiredRotationSize)
			}
			break

		case model.COMMAND_RCV_DATA:
			var handleId common.Uuid
			handleId, err = r.ReadUuid(ctx)
			if err != nil {
				break
			}
			var chunk string
			chunk, err = r.ReadFixedString(ctx)
			if err != nil {
				break
			}
			if listener != nil {
				dataBytes += uint64(listener.AppendData(ctx, handleId, chunk))
			}
			break

		default:
			r.Done()
			log.Debug(ctx, "invalid dump format, unknown command %02X at pos: %d (%02X) ", op, r.Pos(), r.Pos())
			break
			//return nil, errors.Errorf("invalid dump format, unknown command %02X at pos: %d (%02X) ", op, data.Pos(), data.Pos())
			//data.Next()
		}

		if err != nil {
			log.Error(ctx, err, "invalid dump format, error around pos: %d (%02X) ", op, r.Pos(), r.Pos())
			break
		}
	}

	log.Debug(ctx, "Got %d commands from client and %d bytes of stream data", req, dataBytes)
	return dataBytes, err
}
