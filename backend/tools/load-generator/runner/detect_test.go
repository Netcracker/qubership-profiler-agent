package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var t0 = time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)

// series builds points at a fixed 15 s cadence.
func series(values ...float64) []Point {
	ps := make([]Point, len(values))
	for i, v := range values {
		ps[i] = Point{At: t0.Add(time.Duration(i) * 15 * time.Second), Value: v}
	}
	return ps
}

const w = 2 * time.Minute // plateau window: 8 samples at 15 s

func TestIsFlat(t *testing.T) {
	assert.True(t, isFlat(series(100, 101, 99, 100, 100, 101, 100, 99, 100), w, 0.05),
		"noise around a level is flat")
	assert.False(t, isFlat(series(100, 110, 121, 133, 146, 161, 177, 195, 214), w, 0.05),
		"10% growth per sample is not flat")
	assert.False(t, isFlat(series(100, 100), w, 0.05),
		"two samples cannot prove a plateau")
	assert.True(t, isFlat(series(0, 0, 0, 0, 0, 0, 0, 0), w, 0.05),
		"an all-zero series is flat")
}

func TestRelativeSlopeWindowing(t *testing.T) {
	// Growth long ago, flat lately: only the last window is judged.
	ps := series(10, 20, 40, 80, 160, 160, 161, 159, 160, 160, 161, 160, 159, 160)
	assert.True(t, isFlat(ps, w, 0.05), "a series that found its level is flat")
}

func TestMeanOfLastWindow(t *testing.T) {
	ps := series(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100, 100, 100, 100, 100, 100, 100, 100, 100)
	assert.InDelta(t, 100, meanOf(ps, w), 1, "the mean covers only the last window")
}

func TestStickyShare(t *testing.T) {
	d := Detector{Name: "ingest-paused", Kind: "sticky-share", Share: 0.05}
	assert.False(t, detectorFires(d, series(0, 0, 0, 0, 0, 0, 0, 0, 0, 0), w, 0.05, 0))
	assert.False(t, detectorFires(d, series(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
		w, 0.05, 0), "one blip in twenty samples stays under a 5% share")
	assert.True(t, detectorFires(d, series(0, 0, 1, 1, 0, 1, 0, 1, 0, 1), w, 0.05, 0),
		"a sticking gauge fires")
}

// grow builds n points climbing linearly from start by step.
func grow(start, step float64, n int) []Point {
	vals := make([]float64, n)
	for i := range vals {
		vals[i] = start + step*float64(i)
	}
	return series(vals...)
}

// sawtooth builds n points cycling start..start+amplitude with the given
// period (in samples) — the purge-cycle shape of the T5 storm.
func sawtooth(start, amplitude float64, period, n int) []Point {
	vals := make([]float64, n)
	for i := range vals {
		phase := i % period
		vals[i] = start + amplitude*float64(phase)/float64(period-1)
	}
	return series(vals...)
}

func TestMonotonicGrowth(t *testing.T) {
	d := Detector{Name: "pending-parquet", Kind: "monotonic-growth", MinGrowth: 0.10}
	assert.True(t, detectorFires(d, grow(100, 15, 30), w, 0.05, 0),
		"unbounded growth through the hold fires")
	plateaued := append(grow(100, 15, 10), grow(250, 0.1, 20)...)
	for i := range plateaued {
		plateaued[i].At = t0.Add(time.Duration(i) * 15 * time.Second)
	}
	assert.False(t, detectorFires(d, plateaued, w, 0.05, 0),
		"growth that found a plateau is a level shift, not saturation")
	assert.False(t, detectorFires(d, sawtooth(100, 2, 8, 30), w, 0.05, 0),
		"noise under minGrowth does not fire")
	assert.True(t, detectorFires(d, grow(0, 30, 30), w, 0.05, 0),
		"growth from zero fires")
}

func TestMonotonicGrowthNeedsThreeWindows(t *testing.T) {
	// The T5 storm regressions. Attempt a: seconds after the grace expiry a
	// rising sawtooth edge read as first-to-last growth. Attempt b: at
	// exactly ONE plateau window of span the least-squares fit covered a
	// single trough-to-crest arc of a sawtooth whose period exceeded the
	// window. Under three windows of span nothing is judged.
	d := Detector{Name: "pod-restarts-growth", Kind: "monotonic-growth", MinGrowth: 0.10, MinValue: 100}
	assert.False(t, detectorFires(d, series(217, 219, 230, 232, 236, 236), w, 0.05, 0),
		"90 seconds of a rising sawtooth edge is not judgeable growth")
	oneArc := series(208, 211, 215, 219, 222, 226, 230, 235, 241, 247)
	assert.False(t, detectorFires(d, oneArc, w, 0.05, 0),
		"one trough-to-crest arc spanning a single window is not judgeable growth")
}

func TestMonotonicGrowthIgnoresSawtoothPlateau(t *testing.T) {
	// The T5 storm shape at full length: tracked pod-restarts oscillate
	// ~208–247 as purge cycles run, with a period longer than the plateau
	// window. Over three windows the cycles average out and the fit is
	// ~zero — a healthy plateau must not read as growth.
	d := Detector{Name: "pod-restarts-growth", Kind: "monotonic-growth", MinGrowth: 0.10, MinValue: 100}
	assert.False(t, detectorFires(d, sawtooth(208, 39, 10, 40), w, 0.05, 0),
		"a purge-cycle sawtooth around a level is not growth, whatever edge the window ends on")
	assert.True(t, detectorFires(d, grow(208, 4, 40), w, 0.05, 0),
		"a genuine climb of the same magnitude still fires")
}

func TestNonzero(t *testing.T) {
	d := Detector{Name: "refused-bytes", Kind: "nonzero"}
	assert.False(t, detectorFires(d, series(0, 0, 0), w, 0.05, 0))
	assert.True(t, detectorFires(d, series(0, 0, 0.5), w, 0.05, 0))
}

func TestBaselineRatio(t *testing.T) {
	d := Detector{Name: "ack-flush", Kind: "baseline-ratio", Ratio: 5}
	degraded := series(10, 10, 10, 10, 60, 62, 61, 63, 65, 64, 66, 65)
	assert.False(t, detectorFires(d, degraded, w, 0.05, 0),
		"no baseline yet: the detector stays silent")
	assert.True(t, detectorFires(d, degraded, w, 0.05, 10),
		"p95 at 6x the first-step baseline fires")
	assert.False(t, detectorFires(d, series(10, 11, 12, 11, 10, 12, 11, 12), w, 0.05, 10),
		"staying near the baseline does not fire")
}

func TestAfterGrace(t *testing.T) {
	ps := series(0, 0, 300, 500, 480, 510, 495, 505)
	assert.Equal(t, ps, afterGrace(ps, t0, 0), "zero grace keeps everything")
	// A 1 m grace at a 15 s cadence drops the first four samples — the
	// cold-start fill a growth detector must not read as saturation.
	trimmed := afterGrace(ps, t0, time.Minute)
	assert.Equal(t, ps[4:], trimmed)
	assert.Empty(t, afterGrace(ps, t0, time.Hour), "grace beyond the hold keeps nothing")

	d := Detector{Name: "pending-parquet-growth", Kind: "monotonic-growth", MinGrowth: 0.10}
	assert.True(t, detectorFires(d, grow(0, 200, 30), w, 0.05, 0),
		"without grace the cold-start fill from zero fires")
	flatTail := append(grow(0, 250, 3), sawtooth(480, 30, 6, 30)...)
	for i := range flatTail {
		flatTail[i].At = t0.Add(time.Duration(i) * 15 * time.Second)
	}
	assert.False(t, detectorFires(d, afterGrace(flatTail, t0, time.Minute), w, 0.05, 0),
		"after the grace the series is an oscillating level, not growth")
}

func TestMonotonicGrowthMinValue(t *testing.T) {
	// A climb that never reaches the floor: whatever its slope, it is not
	// backlog.
	d := Detector{Name: "pending", Kind: "monotonic-growth", MinGrowth: 0.10}
	assert.True(t, detectorFires(d, grow(0, 250, 30), w, 0.05, 0))
	d.MinValue = 8 << 20
	assert.False(t, detectorFires(d, grow(0, 250, 30), w, 0.05, 0),
		"below the absolute floor a rising edge is not backlog")
	assert.True(t, detectorFires(d, grow(0, 20<<20, 30), w, 0.05, 0),
		"growth past the floor still fires")
}
