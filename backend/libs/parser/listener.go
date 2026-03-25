package parser

import (
	"bytes"
	"context"

	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type (
	Listener interface {
		RegisterPod(protocolVersion uint64, namespace string, service string, podName string)
		AppendData(ctx context.Context, handleId common.Uuid, chunk string) int
		RegisterStream(ctx context.Context, handleId common.Uuid, streamType string, resetRequired int,
			requestedRollingSequenceId int, rollingSequenceId int,
			rotationPeriod uint64, requiredRotationSize uint64)
		Close(ctx context.Context)
		PrintDebug(ctx context.Context)
	}
	PodListener struct {
		data   *LoadedTcpData
		reader *io.BlobReader
	}
)

func (p *PodListener) RegisterPod(protocolVersion uint64, namespace string, service string, podName string) {
	p.data.ProtocolVersion = protocolVersion
	p.data.Namespace = namespace
	p.data.Microservice = service
	p.data.PodName = podName
}

func (p *PodListener) RegisterStream(ctx context.Context, handleId common.Uuid, streamType string, resetRequired int,
	requestedRollingSequenceId int, rollingSequenceId int,
	rotationPeriod uint64, requiredRotationSize uint64) {

	log.Trace(ctx, "INIT_STREAM_V2 for %v: req  => seqId=%v, reset? %v",
		streamType, requestedRollingSequenceId, resetRequired > 0)
	log.Debug(ctx, "INIT_STREAM_V2 for %v: resp => handleId=%v, rotation (period: %v, size: %v), seqId=%v ",
		streamType, handleId, rotationPeriod, requiredRotationSize, rollingSequenceId)

	// store to cache
	chunk := model.NewChunk(handleId, streamType, rollingSequenceId, rotationPeriod, requiredRotationSize)
	chunk.Init(&bytes.Buffer{})
	p.data.Streams[handleId.String()] = chunk
	p.data.StreamTypes[streamType] = handleId
}

func (p *PodListener) AppendData(ctx context.Context, handleId common.Uuid, chunk string) int {
	if c, has := p.data.Streams[handleId.String()]; has {
		//utils.LogDebug(ctx, "RCV_DATA for '%s' with %d bytes, [handle: %v] ", c.streamType, len(chunk), handleId)
		c.Append(chunk)
		return len(chunk)
	} else {
		log.Debug(ctx, "RCV_DATA: unknown handle %v ", handleId)
		return 0
	}
}

func (p *PodListener) PrintDebug(ctx context.Context) {
	p.reader.PrintDebug(ctx)
}

func (p *PodListener) Close(ctx context.Context) {
	// TODO release all chunks to sync.Pool ?
}
