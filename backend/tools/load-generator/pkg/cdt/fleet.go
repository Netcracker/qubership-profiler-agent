package cdt

import (
	"math/rand"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/vdumper"
	"github.com/pkg/errors"
)

// FleetSummary is what runFleet returns to the script once the VU is asked to
// stop: fleet totals for checks and end-of-test assertions.
type FleetSummary struct {
	Pods        int   `js:"pods"`
	Connects    int64 `js:"connects"`
	Reconnects  int64 `js:"reconnects"`
	Churns      int64 `js:"churns"`
	AckErrors   int64 `js:"ackErrors"`
	Dropped     int64 `js:"dropped"`
	PodFailures int   `js:"podFailures"`
}

// RunFleet drives opts.Pods virtual dumpers until the VU is interrupted (the
// externally-controlled executor scales down, or the test ends), then closes
// them gracefully and reports the fleet totals. Surfaced to JS as runFleet.
func (mi *ModuleInstance) RunFleet(opts FleetOptions) (FleetSummary, error) {
	state := mi.vu.State()
	if state == nil {
		return FleetSummary{}, errors.New("runFleet must be called inside the VU default function")
	}
	opts = opts.withDefaults()
	spread, err := opts.validate()
	if err != nil {
		return FleetSummary{}, err
	}
	workload, err := opts.workload()
	if err != nil {
		return FleetSummary{}, err
	}

	ctx := mi.vu.Context()
	tags := state.Tags.GetCurrentValues().Tags
	adapter := newStatsAdapter(ctx, state.Samples, tags, mi.metrics)

	// Stagger the connects (the feeder does the same) so scaling a step up
	// does not read as a reconnect storm on the collector side.
	jitter := rand.New(rand.NewSource(opts.Seed + int64(state.VUID))) //nolint:gosec // startup spread, not crypto

	var wg sync.WaitGroup
	var mu sync.Mutex
	podFailures := 0
	for i := 0; i < opts.Pods; i++ {
		cfg := opts.podConfig(workload, state.VUID, i, adapter)
		delay := time.Duration(0)
		if spread > 0 {
			delay = time.Duration(jitter.Int63n(int64(spread)))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			if err := vdumper.New(cfg).Run(ctx); err != nil && ctx.Err() == nil {
				// Permanent stops only (blacklisted, protocol version): the
				// reconnect loop swallows transient errors.
				state.Logger.WithError(err).Errorf("pod %s stopped", cfg.PodName)
				mu.Lock()
				podFailures++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return FleetSummary{
		Pods:        opts.Pods,
		Connects:    adapter.connects.Load(),
		Reconnects:  adapter.reconnects.Load(),
		Churns:      adapter.churns.Load(),
		AckErrors:   adapter.ackErrors.Load(),
		Dropped:     adapter.dropped.Load(),
		PodFailures: podFailures,
	}, nil
}
