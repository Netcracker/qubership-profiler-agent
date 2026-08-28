package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeFaultLog builds a faults.jsonl from raw lines and returns a loaded
// faultState.
func writeFaultLog(t *testing.T, lines ...string) *faultState {
	t.Helper()
	path := filepath.Join(t.TempDir(), "faults.jsonl")
	body := ""
	for _, l := range lines {
		body += l + "\n"
	}
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	fs := newFaultState(path)
	require.NoError(t, fs.reload())
	return fs
}

func eventLine(faultID, event string, at time.Time, pod string, expects []string, settleSec float64) string {
	expStr := ""
	for i, e := range expects {
		if i > 0 {
			expStr += ","
		}
		expStr += fmt.Sprintf("%q", e)
	}
	return fmt.Sprintf(`{"faultId":%q,"name":%q,"event":%q,"at":%q,"scheduledAt":%q,"action":"x","target":{"pod":%q},"expects":[%s],"settleSec":%g}`,
		faultID, faultID, event, at.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano), pod, expStr, settleSec)
}

var t0 = time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)

func TestAllowanceWindowMatchesObservationTime(t *testing.T) {
	fs := writeFaultLog(t,
		eventLine("s3-outage", "started", t0, "", []string{"refused-bytes"}, 300),
		eventLine("s3-outage", "reverted", t0.Add(10*time.Minute), "", []string{"refused-bytes"}, 300),
	)
	assert.False(t, fs.expected("refused-bytes", t0.Add(-time.Minute)),
		"an observation BEFORE started stays unexpected — the reverse race")
	assert.True(t, fs.expected("refused-bytes", t0.Add(5*time.Minute)))
	assert.True(t, fs.expected("refused-bytes", t0.Add(14*time.Minute)), "the settle tail is expected")
	assert.False(t, fs.expected("refused-bytes", t0.Add(16*time.Minute)))
	assert.False(t, fs.expected("ingest-paused", t0.Add(5*time.Minute)), "undeclared signals expect nothing")
}

func TestOpenWindowExtendsToNow(t *testing.T) {
	fs := writeFaultLog(t,
		eventLine("s3-outage", "started", t0, "", []string{"refused-bytes"}, 300),
	)
	assert.True(t, fs.expected("refused-bytes", t0.Add(3*time.Hour)),
		"an active fault's window is open-ended")
	assert.True(t, fs.hasOpen("refused-bytes"))
}

func TestTornLastLineIsSkipped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "faults.jsonl")
	body := eventLine("kill", "started", t0, "collector-1", []string{"restarts"}, 300) + "\n" +
		`{"faultId":"kill","event":"end` // torn mid-write
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	fs := newFaultState(path)
	require.NoError(t, fs.reload())
	assert.True(t, fs.expected("restarts", t0.Add(time.Minute)), "the complete line is honored")
	assert.True(t, fs.hasOpen("restarts"), "the torn close event is ignored until rewritten")
}

// The forward race of doc/checker.md: a violation scraped BEFORE the
// injection started must stay unexpected even when the started event lands
// between the scrape and the evaluation. The §8.3 predicate matches by the
// sample time, so an increment observed pre-fault is a violation.
func TestRefusedBytesBeforeInjectionStaysUnexpected(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "no-refused-bytes")
	h := histAt(t, "profiler_ingest_refused_bytes_total", []float64{0, 0, 4096})
	scrapeAt := h.samples[len(h.samples)-1].at
	// The fault starts AFTER the scrape that saw the increment.
	st := stateOf(h)
	st.faults = writeFaultLog(t,
		eventLine("s3-outage", "started", scrapeAt.Add(200*time.Millisecond), "", []string{"refused-bytes"}, 300),
	)
	fs := inv.check(st)
	require.Len(t, fs, 1)
	assert.False(t, fs[0].expected, "measured before the injection: unexpected")
}

func TestRefusedBytesInsideWindowIsExpected(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "no-refused-bytes")
	h := histAt(t, "profiler_ingest_refused_bytes_total", []float64{0, 0, 4096, 4096})
	incrementAt := h.samples[2].at
	st := stateOf(h)
	st.faults = writeFaultLog(t,
		eventLine("s3-outage", "started", incrementAt.Add(-time.Second), "", []string{"refused-bytes"}, 300),
	)
	fs := inv.check(st)
	require.Len(t, fs, 1)
	assert.True(t, fs[0].expected)
	assert.Contains(t, fs[0].msg, "4096", "the expected refusal still reports its volume")
}

func TestRefusedBytesStopsLatchingAfterDrain(t *testing.T) {
	// The predicate change: a windowed refusal must not keep the cumulative
	// counter latching forever — flat post-drain samples produce no new
	// unexpected findings.
	inv := findInvariant(t, defaultConfig(), "no-refused-bytes")
	h := histAt(t, "profiler_ingest_refused_bytes_total", []float64{4096, 4096, 4096})
	st := stateOf(h)
	st.faults = writeFaultLog(t,
		eventLine("s3-outage", "started", t0, "", []string{"refused-bytes"}, 300),
		eventLine("s3-outage", "reverted", t0.Add(time.Minute), "", []string{"refused-bytes"}, 300),
	)
	assert.Empty(t, inv.check(st), "a flat counter after the windowed refusal is not a violation")
}

func TestRestartAllowanceScopedToPod(t *testing.T) {
	p := newPodState(0)
	base := time.Now()
	p.observe([]podInfo{
		{Name: "collector-1", UID: "a", Restarts: 0},
		{Name: "profiler-backend-query-x", UID: "q", Restarts: 0},
	}, base)

	faults := writeFaultLog(t,
		eventLine("kill-collector-001", "started", base, "collector-1", []string{"restarts"}, 600),
		eventLine("kill-collector-001", "ended", base, "collector-1", []string{"restarts"}, 600),
	)

	// The killed collector restarts — expected; the query pod restarting at
	// the same moment is NOT covered by the collector's allowance.
	p.observe([]podInfo{
		{Name: "collector-1", UID: "a", Restarts: 1},
		{Name: "profiler-backend-query-x", UID: "q", Restarts: 1},
	}, base.Add(time.Minute))
	fs := p.findings(faults)
	var unexpected, expected []finding
	for _, f := range fs {
		if f.expected {
			expected = append(expected, f)
		} else {
			unexpected = append(unexpected, f)
		}
	}
	require.Len(t, unexpected, 1, "the query restart must stay a violation")
	assert.Contains(t, unexpected[0].msg, "query")
	require.Len(t, expected, 1)
	assert.Contains(t, expected[0].msg, "collector-1")
}

func TestRestartAllowanceHonorsDeclaredBudget(t *testing.T) {
	// The T5.2 measurement: a grace-0 collector kill produces a replacement
	// AND one collector.lock-collision container restart. A fault declaring
	// restartBudget 2 absorbs both; a third event still latches.
	p := newPodState(0)
	base := time.Now()
	p.observe([]podInfo{{Name: "collector-1", UID: "a", Restarts: 0}}, base)

	line := eventLine("kill-001", "started", base, "collector-1", []string{"restarts"}, 600)
	line = line[:len(line)-1] + `,"restartBudget":2}`
	closeLine := eventLine("kill-001", "ended", base, "collector-1", []string{"restarts"}, 600)
	closeLine = closeLine[:len(closeLine)-1] + `,"restartBudget":2}`
	faults := writeFaultLog(t, line, closeLine)

	// Replacement + one lock-collision restart: both expected.
	p.observe([]podInfo{{Name: "collector-1", UID: "b", Restarts: 1}}, base.Add(time.Minute))
	for _, f := range p.findings(faults) {
		assert.True(t, f.expected, "both units fit the declared budget: %s", f.msg)
	}

	// A second container restart exceeds the budget.
	p.observe([]podInfo{{Name: "collector-1", UID: "b", Restarts: 2}}, base.Add(2*time.Minute))
	var unexpected int
	for _, f := range p.findings(faults) {
		if !f.expected && f.subject == "restart-budget" {
			unexpected++
		}
	}
	assert.Equal(t, 1, unexpected, "the third unit latches")
}

func TestRestartAllowanceBudgetIsPerInjection(t *testing.T) {
	p := newPodState(0)
	base := time.Now()
	p.observe([]podInfo{{Name: "collector-1", UID: "a", Restarts: 0}}, base)

	faults := writeFaultLog(t,
		eventLine("kill-001", "started", base, "collector-1", []string{"restarts"}, 600),
		eventLine("kill-001", "ended", base, "collector-1", []string{"restarts"}, 600),
	)

	// Two restarts inside one injection window: one is expected, the excess
	// is a violation — a single kill does not excuse a crashloop.
	p.observe([]podInfo{{Name: "collector-1", UID: "a", Restarts: 2}}, base.Add(time.Minute))
	fs := p.findings(faults)
	var unexpectedN int
	for _, f := range fs {
		if !f.expected && f.subject == "restart-budget" {
			unexpectedN++
			assert.Contains(t, f.msg, "+1", "only the excess unit is unexpected")
		}
	}
	assert.Equal(t, 1, unexpectedN)
}

func TestRestartOutsideWindowIsUnexpected(t *testing.T) {
	p := newPodState(0)
	base := time.Now()
	p.observe([]podInfo{{Name: "collector-1", UID: "a", Restarts: 0}}, base)

	faults := writeFaultLog(t,
		eventLine("kill-001", "started", base.Add(-2*time.Hour), "collector-1", []string{"restarts"}, 60),
		eventLine("kill-001", "ended", base.Add(-2*time.Hour), "collector-1", []string{"restarts"}, 60),
	)
	p.observe([]podInfo{{Name: "collector-1", UID: "a", Restarts: 1}}, base.Add(time.Minute))
	fs := p.findings(faults)
	require.Len(t, fs, 1)
	assert.False(t, fs[0].expected, "the window closed two hours ago")
}

func TestCompactionDeadlineShift(t *testing.T) {
	fs := writeFaultLog(t,
		eventLine("s3-outage", "started", t0, "", []string{"compaction-lag"}, 300),
		eventLine("s3-outage", "reverted", t0.Add(15*time.Minute), "", []string{"compaction-lag"}, 300),
	)
	// Closed window: 15m fault + 5m settle = 20m shift for buckets it may
	// have delayed.
	assert.Equal(t, 20*time.Minute, fs.deadlineShift("compaction-lag", t0.Add(-time.Hour)))
	assert.Equal(t, time.Duration(0), fs.deadlineShift("compaction-lag", t0.Add(time.Hour)),
		"buckets ending after the window closed are not shifted")
	assert.False(t, fs.hasOpen("compaction-lag"))
}

func TestIngestPausedWindowLeavesRatio(t *testing.T) {
	inv := findInvariant(t, defaultConfig(), "ingest-paused-not-sticky")
	values := make([]float64, 200)
	for i := 20; i < 30; i++ { // 10/200 = 5% — way over the 1% budget
		values[i] = 1
	}
	h := histAt(t, "profiler_backpressure_ingest_paused", values)
	st := stateOf(h)
	st.faults = writeFaultLog(t,
		eventLine("s3-outage", "started", h.samples[19].at, "", []string{"ingest-paused"}, 2),
		eventLine("s3-outage", "reverted", h.samples[29].at, "", []string{"ingest-paused"}, 2),
	)
	assert.Empty(t, inv.check(st), "in-window paused samples leave the ratio entirely")

	// The same pauses without a matching window still violate.
	st.faults = nil
	assert.NotEmpty(t, inv.check(stateOf(h)))
}

func TestScrapeGapScopedByTargetMapping(t *testing.T) {
	g := newGapTracker(0)
	g.started = time.Now().Add(-time.Hour)
	for i := 0; i < 5; i++ {
		g.observe("http://localhost:8082/metrics", false)
		g.observe("http://localhost:8083/metrics", false)
	}
	faults := writeFaultLog(t,
		eventLine("kill-001", "started", time.Now().Add(-time.Minute), "collector-1", []string{"scrape-gap"}, 600),
		eventLine("kill-001", "ended", time.Now().Add(-time.Minute), "collector-1", []string{"scrape-gap"}, 600),
	)
	targetPods := map[string]string{"http://localhost:8082/metrics": "collector-1"}
	fs := g.findings(3, faults, targetPods)
	require.Len(t, fs, 2)
	byTarget := map[string]finding{}
	for _, f := range fs {
		byTarget[f.subject] = f
	}
	assert.True(t, byTarget["http://localhost:8082/metrics"].expected,
		"the killed collector's mapped target is expected")
	assert.False(t, byTarget["http://localhost:8083/metrics"].expected,
		"an unmapped target's silence stays a violation")
}

func TestLatchSeparatesExpectedFromUnexpected(t *testing.T) {
	l := newLatch()
	inv := invariant{name: "test-inv", plan: "§8.x"}
	at := time.Now()
	l.record(at, inv, finding{subject: "a", msg: "boom"})
	l.record(at, inv, finding{subject: "b", msg: "planned", expected: true})
	assert.Equal(t, 1, l.unexpectedLen())
	assert.Equal(t, 1, l.expectedLen())
}
