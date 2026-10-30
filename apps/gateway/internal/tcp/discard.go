package tcp

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/metric"

	"github.com/Netcracker/qubership-profiler-agent/libs/wire/common"
	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/server"
)

// discardListener is a server.Listener that keeps nothing. The collector
// protocol still runs in full, so the agent gets its handshake reply, its
// stream handles, and every ack, and stays quiet and alive.
type discardListener struct {
	discarded metric.Int64Counter
}

func (discardListener) RegisterPod(*server.ConnectedPod) error { return nil }

func (discardListener) AppendData(_ context.Context, _ *server.ConnectedPod, _ common.Uuid, chunk string) (int, error) {
	return len(chunk), nil
}

func (discardListener) RegisterStream(context.Context, *server.ConnectedPod, common.Uuid, string, int, int, int, uint64, uint64) error {
	return nil
}

func (discardListener) PodDisconnected(context.Context, *server.ConnectedPod)                {}
func (discardListener) ReceivedCommand(context.Context, model.Command, time.Duration, error) {}
func (discardListener) Write(context.Context, int, time.Duration, error)                     {}
func (discardListener) PrintDebug(context.Context)                                           {}
func (discardListener) Close(context.Context)                                                {}

// Read counts every byte the agent sent, as bytes discarded.
func (l discardListener) Read(ctx context.Context, n int, _ time.Duration, _ error) {
	if n > 0 {
		l.discarded.Add(ctx, int64(n), metric.WithAttributes(surfaceAttr))
	}
}
