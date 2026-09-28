package health

import (
	"context"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

// TrackProgress refreshes the probe details every interval with describe()
// while the gate stays in state, and logs each refreshed string at INFO with
// ctx. A tick that finds the gate in another state leaves the details alone,
// so a concurrent Set wins over a late refresh.
//
// The returned stop ends the refresh and returns once no further describe
// call can happen; calling it again is a no-op.
func (g *Gate) TrackProgress(ctx context.Context, state State, interval time.Duration, describe func() string) (stop func()) {
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				details := describe()
				g.mu.Lock()
				updated := g.state == state
				if updated {
					g.details = details
				}
				g.mu.Unlock()
				if updated {
					log.Info(ctx, "%s", details)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-exited
		})
	}
}
