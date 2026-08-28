package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/health"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// signalActor returns a run.Group actor implementing the 03-lifecycle.md §5.1
// drain: SIGTERM flips readiness to DRAINING, the drain grace lets the
// endpoint set drop this instance while it still serves, then the group tears
// down. A second signal skips the grace.
func signalActor(ctx context.Context, gate *health.Gate, drainGrace time.Duration) (func() error, func(error)) {
	done := make(chan struct{})
	execute := func() error {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sig)
		select {
		case s := <-sig:
			log.Info(ctx, "received %s, draining for %s", s, drainGrace)
			gate.Set(health.StateDraining, "received "+s.String())
			timer := time.NewTimer(drainGrace)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-sig:
				log.Info(ctx, "second signal, skipping the drain grace")
			case <-ctx.Done():
			case <-done:
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	}
	interrupt := func(error) { close(done) }
	return execute, interrupt
}
