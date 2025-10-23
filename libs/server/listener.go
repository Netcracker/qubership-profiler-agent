package server

import (
	"context"
	"time"

	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

type (
	Listener interface {
		RegisterPod(pod *ConnectedPod)
		AppendData(ctx context.Context, pod *ConnectedPod, handleId common.Uuid, chunk string) int
		RegisterStream(ctx context.Context,
			pod *ConnectedPod, handleId common.Uuid, streamType string,
			resetRequired int, requestedRollingSequenceId int, rollingSequenceId int,
			rotationPeriod uint64, requiredRotationSize uint64)

		SentCommand(ctx context.Context, c model.Command)
		ReceivedCommand(ctx context.Context, c model.Command, latency time.Duration, err error)

		Read(ctx context.Context, bytes int, latency time.Duration, err error)
		Write(ctx context.Context, bytes int, latency time.Duration, err error)
		IsAlive(ctx context.Context) (bool, error)

		Error(err error)
		PrintDebug(ctx context.Context)
		Close(ctx context.Context)
	}
)
