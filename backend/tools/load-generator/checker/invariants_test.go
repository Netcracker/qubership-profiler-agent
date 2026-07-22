package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// histAt builds a history whose samples carry one collector target with the
// given series values, spaced a second apart (inside the retention window)
// and past the warm-up.
func histAt(t *testing.T, name string, values []float64) *history {
	t.Helper()
	h := newHistory(0, time.Hour)
	base := time.Now().Add(-time.Duration(len(values)) * time.Second)
	h.started = base // samples are backdated; keep them past the warm-up
	for i, v := range values {
		h.append(sample{
			at:      base.Add(time.Duration(i) * time.Second),
			targets: map[string]metrics{"http://c0:8081/metrics": {name: v}},
		})
	}
	return h
}

// stateOf wraps a metrics history into the state shape invariants take.
func stateOf(h *history) *state { return &state{metrics: h} }

func findInvariant(t *testing.T, cfg invariantConfig, name string) invariant {
	t.Helper()
	for _, inv := range allInvariants(cfg) {
		if inv.name == name {
			return inv
		}
	}
	t.Fatalf("invariant %q is not defined", name)
	return invariant{}
}

func defaultConfig() invariantConfig {
	return invariantConfig{maxHotLag: 15 * time.Minute, goroutineTolerance: 0.10, maxScrapeGap: 3}
}

func TestIngestPausedRatio(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "ingest-paused-not-sticky")

	flat := make([]float64, 200)
	assert.Empty(t, inv.check(stateOf(histAt(t, "profiler_backpressure_ingest_paused", flat))),
		"never paused must pass")

	flat[10] = 1 // one paused sample out of 200 = 0.5% < 1%
	assert.Empty(t, inv.check(stateOf(histAt(t, "profiler_backpressure_ingest_paused", flat))))

	for i := 0; i < 5; i++ { // 6/200 = 3%
		flat[20+i] = 1
	}
	assert.NotEmpty(t, inv.check(stateOf(histAt(t, "profiler_backpressure_ingest_paused", flat))),
		"paused 3%% of the run must violate the 1%% budget")
}

func TestRefusedBytesInvariant(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "no-refused-bytes")
	assert.Empty(t, inv.check(stateOf(histAt(t, "profiler_ingest_refused_bytes_total", []float64{0, 0, 0}))))
	assert.NotEmpty(t, inv.check(stateOf(histAt(t, "profiler_ingest_refused_bytes_total", []float64{0, 0, 4096}))))
}

func TestHotWindowLagInvariant(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "hot-window-lag-bounded")
	assert.Empty(t, inv.check(stateOf(histAt(t, "profiler_hotstore_hot_window_lag_seconds", []float64{30, 120, 300}))))
	assert.NotEmpty(t, inv.check(stateOf(histAt(t, "profiler_hotstore_hot_window_lag_seconds", []float64{30, 1200}))),
		"20 min lag must exceed the 15 min budget")
}

func TestMonotonicGrowth(t *testing.T) {
	assert.NoError(t, monotonicGrowth([]float64{100, 120, 90, 130, 95}),
		"an oscillating series passes")
	assert.NoError(t, monotonicGrowth([]float64{100, 100, 102, 103}),
		"3%% net growth sits inside the tolerance")
	assert.Error(t, monotonicGrowth([]float64{100, 110, 120, 150}),
		"never-decreasing 50%% growth is the leak signal")
	assert.Error(t, monotonicGrowth([]float64{0, 0, 4096}),
		"growth from an empty store still counts")
}

func TestHotStoreGrowthNeedsFullWindow(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "hot-store-not-growing")
	// Growing steadily, but the samples span less than the 1h window: the
	// trend is not judged yet.
	h := histAt(t, "profiler_hotstore_segments_disk_bytes", []float64{100, 200, 300})
	require.Empty(t, inv.check(stateOf(h)))
}

func TestSumSeriesByTarget(t *testing.T) {
	s := sample{at: time.Now(), targets: map[string]metrics{
		"c0": {"a": 1, "b": 10},
		"c1": {"a": 2},
	}}
	sums := sumSeriesByTarget([]sample{s}, "a", "b")
	assert.Equal(t, []float64{11}, sums["c0"])
	assert.Equal(t, []float64{2}, sums["c1"])
}

// histPairsAt builds a full-window history of (connections, goroutines)
// pairs on one target: the samples exactly span the trend window, so the
// append-side pruning keeps them all and windowFull holds.
func histPairsAt(t *testing.T, pairs [][2]float64) *history {
	t.Helper()
	step := 10 * time.Minute
	// The warm-up doubles as pruning slack; started is backdated past it so
	// checked() still judges every sample.
	h := newHistory(step, time.Duration(len(pairs)-1)*step)
	base := time.Now().Add(-time.Duration(len(pairs)-1) * step)
	h.started = base.Add(-step)
	for i, p := range pairs {
		h.append(sample{
			at: base.Add(time.Duration(i) * step),
			targets: map[string]metrics{"c0": {
				"profiler_ingest_active_connections": p[0],
				"go_goroutines":                      p[1],
			}},
		})
	}
	return h
}

func TestGoroutinesTrend(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "goroutines-flat")

	// The phase-4 false positive: a 115–130 oscillation around a flat
	// baseline at 20 constant connections. The old range rule latched it
	// (spread 15 > 12 allowed); the trend rule must not — legal worker and
	// scrape jitter fits to ~zero growth.
	assert.Empty(t, inv.check(stateOf(histPairsAt(t, [][2]float64{
		{20, 116}, {20, 125}, {20, 115}, {20, 130}, {20, 118}, {20, 127},
		{20, 116}, {20, 129}, {20, 117}, {20, 124}, {20, 115}, {20, 128},
	}))), "oscillation around a flat baseline is not a leak")

	// A steady climb of the same magnitude IS the leak signal: fitted growth
	// ~+33 over the window against an allowance of ~13, still climbing at
	// the tail.
	assert.NotEmpty(t, inv.check(stateOf(histPairsAt(t, [][2]float64{
		{20, 116}, {20, 119}, {20, 122}, {20, 125}, {20, 128}, {20, 131},
		{20, 134}, {20, 137}, {20, 140}, {20, 143}, {20, 146}, {20, 149},
	}))), "a sustained climb at constant connections is the leak signal")

	// Growth that found its level passes: the tail quarter is flat, so the
	// process ramped (worker pools, caches) and stopped — not a leak.
	assert.Empty(t, inv.check(stateOf(histPairsAt(t, [][2]float64{
		{20, 116}, {20, 124}, {20, 132}, {20, 140}, {20, 148}, {20, 156},
		{20, 160}, {20, 160}, {20, 160}, {20, 160}, {20, 160}, {20, 160},
	}))), "growth followed by a flat tail found its level")

	assert.Empty(t, inv.check(stateOf(histPairsAt(t, [][2]float64{
		{100, 1200}, {150, 1800}, {200, 2400}, {250, 3000}, {300, 3600}, {350, 4200},
		{400, 4800}, {450, 5400}, {500, 6000}, {550, 6600}, {600, 7200}, {650, 7800},
	}))), "moving connections are not judged")

	// A collector restart inside the window drops the connection count to
	// zero and back: the constant-connections gate keeps the mixed window
	// out of judgment even though goroutines re-ramp from the fresh
	// process's baseline.
	assert.Empty(t, inv.check(stateOf(histPairsAt(t, [][2]float64{
		{20, 120}, {20, 121}, {20, 122}, {0, 35}, {0, 36}, {20, 80},
		{20, 90}, {20, 100}, {20, 110}, {20, 118}, {20, 121}, {20, 123},
	}))), "a restart-spanning window is not judged")

	assert.Empty(t, inv.check(stateOf(histPairsAt(t, [][2]float64{
		{0, 12}, {0, 13}, {0, 14}, {0, 15}, {0, 16}, {0, 17},
		{0, 18}, {0, 19}, {0, 20}, {0, 21}, {0, 21}, {0, 21},
	}))), "near-idle process: fitted growth of ~9 sits inside the absolute floor of 10")
}

func TestGoroutinesTrendNeedsEnoughPoints(t *testing.T) {
	// Steeply climbing, but under minTrendPoints pairs: not judged.
	assert.Empty(t, goroutinesTrending([]tsPoint{
		{at: time.Unix(0, 0), v: 100}, {at: time.Unix(600, 0), v: 200},
		{at: time.Unix(1200, 0), v: 300}, {at: time.Unix(1800, 0), v: 400},
	}, 0.10), "a trend needs at least %d points", minTrendPoints)
}

func TestFittedGrowthUsesTimestampsNotIndexes(t *testing.T) {
	// The same values on an uneven time grid: the fit must weigh the real
	// spacing. A jump concentrated at the end of a long quiet stretch reads
	// as a shallower slope than index-based fitting would claim.
	even := []tsPoint{
		{at: time.Unix(0, 0), v: 100}, {at: time.Unix(100, 0), v: 110},
		{at: time.Unix(200, 0), v: 120}, {at: time.Unix(300, 0), v: 130},
	}
	growth, mean := fittedGrowth(even)
	assert.InDelta(t, 30, growth, 1e-6, "linear series: fitted growth equals the actual rise")
	assert.InDelta(t, 115, mean, 1e-6)

	gapped := []tsPoint{
		{at: time.Unix(0, 0), v: 100}, {at: time.Unix(100, 0), v: 110},
		{at: time.Unix(200, 0), v: 120}, {at: time.Unix(3000, 0), v: 130},
	}
	gappedGrowth, _ := fittedGrowth(gapped)
	assert.Less(t, gappedGrowth, 30.0, "a long scrape gap flattens the fitted slope")
}

func TestRSSUnderLimit(t *testing.T) {
	cfg := defaultConfig()
	cfg.rssLimitBytes = 1 << 30 // 1 GiB
	inv := findInvariant(t, cfg, "rss-under-limit")

	assert.Empty(t, inv.check(stateOf(histAt(t, "process_resident_memory_bytes",
		[]float64{5e8, 6e8, 5.5e8}))), "oscillating RSS under the limit passes")

	assert.NotEmpty(t, inv.check(stateOf(histAt(t, "process_resident_memory_bytes",
		[]float64{5e8, 6e8, 1.2e9}))), "RSS past the pod limit must violate")

	// Monotonic growth needs the full window; histAt spans seconds against a
	// 1h window, so growth alone is not judged there.
	step := 10 * time.Minute
	h := newHistory(step, 3*step) // warm-up doubles as pruning slack
	base := time.Now().Add(-3 * step)
	h.started = base.Add(-step)
	for i, v := range []float64{5e8, 5.5e8, 6e8, 7e8} {
		h.append(sample{at: base.Add(time.Duration(i) * step),
			targets: map[string]metrics{"c0": {"process_resident_memory_bytes": v}}})
	}
	assert.NotEmpty(t, inv.check(stateOf(h)), "40%% monotonic RSS growth over the window is the leak signal")

	cfg.rssLimitBytes = 0
	invOff := findInvariant(t, cfg, "rss-under-limit")
	assert.Empty(t, invOff.check(stateOf(histAt(t, "process_resident_memory_bytes", []float64{5e8, 1.2e9}))),
		"the RSS check stays off without a limit")
}

func TestLatchKeepsMidRunViolation(t *testing.T) {
	l := newLatch()
	inv := invariant{name: "test-inv", plan: "§8.x"}

	at := time.Now()
	_, isNew := l.record(at, inv, finding{subject: "c0", msg: "boom"})
	assert.True(t, isNew)
	rec, isNew := l.record(at.Add(time.Minute), inv, finding{subject: "c0", msg: "boom again"})
	assert.False(t, isNew)
	assert.Equal(t, 2, rec.count)
	assert.Equal(t, at, rec.firstAt)
	assert.Equal(t, at.Add(time.Minute), rec.lastAt)

	// The registry never clears: a healthy final tick records nothing new,
	// and the run still fails.
	assert.Equal(t, 1, l.unexpectedLen())
}

func TestGapTracker(t *testing.T) {
	g := newGapTracker(0)
	g.started = time.Now().Add(-time.Hour) // past warm-up

	for i := 0; i < 3; i++ {
		g.observe("c0", false)
	}
	assert.Empty(t, g.findings(3, nil, nil), "3 consecutive misses sit at the limit")

	g.observe("c0", false)
	fs := g.findings(3, nil, nil)
	require.Len(t, fs, 1, "the 4th consecutive miss latches")
	assert.Equal(t, "c0", fs[0].subject)

	g.observe("c0", true)
	assert.Empty(t, g.findings(3, nil, nil), "a successful poll resets the gap (the latch upstream keeps the record)")
}

func TestGapTrackerSilentDuringWarmup(t *testing.T) {
	g := newGapTracker(time.Hour)
	for i := 0; i < 10; i++ {
		g.observe("c0", false)
	}
	assert.Empty(t, g.findings(3, nil, nil), "warm-up gaps are not judged")
}
