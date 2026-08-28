package server

import (
	"context"
	"time"

	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

type (
	// Listener receives the demultiplexed agent streams. Errors propagate into
	// the wire error path of 06-wire-protocol-server.md §6: a failed
	// RegisterStream yields the null-UUID teardown, a failed AppendData yields
	// ACK_ERROR_MAGIC and a close, so the agent reconnects instead of stalling.
	Listener interface {
		RegisterPod(pod *ConnectedPod) error
		AppendData(ctx context.Context, pod *ConnectedPod, handleId common.Uuid, chunk string) (int, error)
		RegisterStream(ctx context.Context,
			pod *ConnectedPod, handleId common.Uuid, streamType string,
			resetRequired int, requestedRollingSequenceId int, rollingSequenceId int,
			rotationPeriod uint64, requiredRotationSize uint64) error
		// PodDisconnected fires when the pod's TCP connection ends, however it
		// ends; the pod-restart is closed and never resumes (01 §3.7).
		PodDisconnected(ctx context.Context, pod *ConnectedPod)

		ReceivedCommand(ctx context.Context, c model.Command, latency time.Duration, err error)

		Read(ctx context.Context, bytes int, latency time.Duration, err error)
		Write(ctx context.Context, bytes int, latency time.Duration, err error)

		PrintDebug(ctx context.Context)
		Close(ctx context.Context)
	}
)
