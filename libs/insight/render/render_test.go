package render_test

import (
	"bytes"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight/render"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cliMore is the next-page line the profiler CLI prints.
const cliMore = "More calls: --cursor %s"

var update = flag.Bool("update", false, "rewrite the golden files")

// golden compares text with testdata/<name>.golden; -update rewrites it.
func golden(t *testing.T, name, text string) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden")
	if *update {
		require.NoError(t, os.WriteFile(path, []byte(text), 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, string(want), text)
}

var (
	t0     = time.Date(2026, 10, 14, 11, 0, 0, 0, time.UTC)
	window = insight.ResolvedWindow{From: t0, To: t0.Add(time.Hour)}
)

func TestServicesText(t *testing.T) {
	var b bytes.Buffer
	require.NoError(t, render.Services(&b, insight.ServicesResult{
		Window: window,
		Namespaces: []insight.Namespace{{Name: "prod", Services: []insight.Service{
			{Name: "billing", Pods: []insight.Pod{{Name: "billing-7f8c", Restarts: []insight.Restart{
				{StartedAt: t0.Add(-8 * time.Hour), FirstCall: t0, LastCall: t0.Add(10 * time.Minute)},
				{StartedAt: t0.Add(15 * time.Minute), FirstCall: t0.Add(16 * time.Minute), LastCall: t0.Add(time.Hour)},
			}}}},
			{Name: "orders", Pods: []insight.Pod{{Name: "orders-5d9c", Restarts: []insight.Restart{
				{StartedAt: t0.Add(-24 * time.Hour), FirstCall: t0, LastCall: t0.Add(time.Hour)},
			}}}},
		}}},
		Partial: []string{"s3 list pods/v1/2026/10/14/: timeout"},
	}))
	golden(t, "services", b.String())

	b.Reset()
	require.NoError(t, render.Services(&b, insight.ServicesResult{Window: window}))
	assert.Contains(t, b.String(), "No pods have profiling data in the window.")
}

func sampleCalls() []insight.Call {
	return []insight.Call{
		{
			Ref: "prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error", Start: t0.Add(50 * time.Minute),
			DurationMs: 2470, Method: "void com.example.Billing.charge(long) (Billing.java) [billing.jar]",
			Namespace: "prod", Service: "billing", Pod: "billing-7f8c", CPUMs: 310, WaitMs: 1900, QueueWaitMs: 15,
			SuspendMs: 120, Error: true, RequestID: "abc123", TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			HTTP: "POST http://billing:8080/charges",
		},
		{
			Ref: "prod:billing:billing-7f8c:1760420000000:5:9000:1@1760432000000@short_clean", Start: t0.Add(33 * time.Minute),
			DurationMs: 12, Method: "com.example.Health.check", Namespace: "prod", Service: "billing", Pod: "billing-7f8c",
			CPUMs: 1, Truncated: "buffer_full",
		},
	}
}

func TestCallsText(t *testing.T) {
	var b bytes.Buffer
	require.NoError(t, render.Calls(&b, insight.CallsResult{
		Window: &window, Calls: sampleCalls(), NextCursor: "cursor-page-2",
	}, cliMore))
	golden(t, "calls", b.String())

	b.Reset()
	require.NoError(t, render.Calls(&b, insight.CallsResult{Restarted: true, Window: &window}, cliMore))
	assert.Contains(t, b.String(), "The cursor was stale")
	assert.Contains(t, b.String(), "No calls match.")
}

func TestExplainText(t *testing.T) {
	call := sampleCalls()[0]
	var b bytes.Buffer
	require.NoError(t, render.Explain(&b, insight.ExplainResult{
		Call:      &call,
		Focus:     "com.example.Billing.charge(long)",
		FocusNode: 0,
		HotPath: &insight.PathNode{
			Node: 3, Method: "com.example.Billing.charge(long)", TotalMs: 2470, SelfMs: 20, SuspendMs: 120, Executions: 1, Skipped: 3,
			Hidden: 2, HiddenMs: 40,
			Children: []*insight.PathNode{
				{Node: 4, Method: "com.example.Repo.load(long)", TotalMs: 2000, SelfMs: 50, Executions: 1, Children: []*insight.PathNode{
					{Node: 5, Method: "a.Jdbc.execute", TotalMs: 1950, SelfMs: 1950, Executions: 90},
				}},
				{Node: 12, Method: "com.example.Http.call()", TotalMs: 410, SelfMs: 410, Executions: 2},
			},
		},
		TopSelf: []insight.MethodStat{
			{Method: "a.Jdbc.execute", SelfMs: 1950, Executions: 90},
			{Method: "com.example.Http.call()", SelfMs: 410, Executions: 2},
		},
		TopSQL: []insight.SQLStat{
			{Statement: "select * from orders where id = ?", DurationMs: 1800, Executions: 88},
			{Statement: "(other statements)", DurationMs: 150, Executions: 2},
		},
		Notes: []string{"2 nodes under 5% of 2470 ms are hidden; a lower threshold shows them"},
	}))
	golden(t, "explain", b.String())
}

// shortWriter accepts the first n bytes, then fails like a closed pipe.
type shortWriter struct{ n int }

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > w.n {
		k := w.n
		w.n = 0
		return k, io.ErrClosedPipe
	}
	w.n -= len(p)
	return len(p), nil
}

// TestTextReportsWriteErrors cuts each text output at every byte offset, so a
// write on any path, down to the last line, fails the command instead of
// leaving a truncated result behind a zero exit status.
func TestTextReportsWriteErrors(t *testing.T) {
	call := sampleCalls()[0]
	for name, write := range map[string]func(io.Writer) error{
		"services": func(w io.Writer) error {
			return render.Services(w, insight.ServicesResult{Window: window, Namespaces: []insight.Namespace{{
				Name: "prod", Services: []insight.Service{{Name: "billing", Pods: []insight.Pod{{
					Name: "billing-7f8c", Restarts: []insight.Restart{{StartedAt: t0, FirstCall: t0, LastCall: t0}},
				}}}},
			}}})
		},
		"services, empty": func(w io.Writer) error {
			return render.Services(w, insight.ServicesResult{Window: window, Partial: []string{"timeout"}})
		},
		"calls": func(w io.Writer) error {
			return render.Calls(w, insight.CallsResult{Window: &window, Calls: sampleCalls(), NextCursor: "c2"}, cliMore)
		},
		"calls, empty": func(w io.Writer) error {
			return render.Calls(w, insight.CallsResult{Restarted: true, NextCursor: "c2"}, cliMore)
		},
		"explain": func(w io.Writer) error {
			return render.Explain(w, insight.ExplainResult{
				Call: &call, Focus: "f",
				HotPath: &insight.PathNode{Method: "a", Hidden: 1, Children: []*insight.PathNode{{Method: "b"}}},
				TopSelf: []insight.MethodStat{{Method: "b"}}, TopSQL: []insight.SQLStat{{Statement: "s"}},
				Notes: []string{"n"},
			})
		},
		"hotspots": func(w io.Writer) error { return render.Hotspots(w, sampleHotspots()) },
		"hotspots, empty": func(w io.Writer) error {
			return render.Hotspots(w, insight.HotspotsResult{Window: window, Approximate: true, Notes: []string{"n"}})
		},
		"timeline": func(w io.Writer) error { return render.Timeline(w, sampleTimeline()) },
	} {
		var full bytes.Buffer
		require.NoError(t, write(&full), name)
		for n := 0; n < full.Len(); n++ {
			assert.ErrorIs(t, write(&shortWriter{n: n}), io.ErrClosedPipe, "%s cut at byte %d of %d", name, n, full.Len())
		}
	}
}

func i64(v int64) *int64 { return &v }

func sampleHotspots() insight.HotspotsResult {
	calls := sampleCalls()
	return insight.HotspotsResult{
		Window: window, GroupBy: "method", Order: insight.OrderDurationSumDesc,
		Groups: []insight.StatsRow{
			{Key: "void com.example.Billing.charge(long) (Billing.java) [billing.jar]", StatsValues: insight.StatsValues{
				Count: 120, Errors: 3, DurationMsSum: 96000, P50Ms: i64(700), P95Ms: i64(1900), P99Ms: i64(2400),
				CPUMsSum: 12000, WaitMsSum: 70000, SuspendMsSum: 900,
			}},
			{Key: "com.example.Health.check", StatsValues: insight.StatsValues{
				Count: 1100, DurationMsSum: 2200, P50Ms: i64(2), P95Ms: i64(3), P99Ms: i64(5), CPUMsSum: 1100,
			}},
		},
		Other:    &insight.StatsValues{Count: 20, Errors: 1, DurationMsSum: 1800, P50Ms: i64(80), P95Ms: i64(200), P99Ms: i64(250)},
		Distinct: 7, Totals: insight.StatsTotals{Count: 1240, Errors: 4},
		Partial: []string{"collector 10.0.0.7 stats: timeout"},
		Sample: &insight.HotspotsSample{
			SampleInfo: insight.SampleInfo{
				Conditions: []string{"namespace=prod", "service=billing", `method=~^(?:x|y)$`},
				Requested:  20, Listed: 20, Trees: 19, Of: 1220, Calls: calls[:1],
				Skipped: []string{"1 calls have no tree (buffer_full)"},
			},
			Groups: []string{"void com.example.Billing.charge(long) (Billing.java) [billing.jar]", "com.example.Health.check"},
			Methods: []insight.SampledMethod{
				{MethodStat: insight.MethodStat{Method: "a.Jdbc.execute", SelfMs: 30500, Executions: 1710}, Calls: 19},
				{MethodStat: insight.MethodStat{Method: "com.example.Http.call()", SelfMs: 4100, Executions: 21}, Calls: 7},
			},
			SQL: []insight.SampledSQL{
				{SQLStat: insight.SQLStat{Statement: "select * from orders where id = ?", DurationMs: 28000, Executions: 1650}, Calls: 19},
			},
		},
		Notes: []string{
			"5 more groups with 20 calls are summed as other; a larger limit lists more",
			"the sample reads 20 of 1220 calls; a larger sample reads more",
		},
	}
}

func TestHotspotsText(t *testing.T) {
	var b bytes.Buffer
	require.NoError(t, render.Hotspots(&b, sampleHotspots()))
	golden(t, "hotspots", b.String())
	assert.Contains(t, b.String(), "Sampled, not exact: methods and SQL from the trees of the 20 slowest of 1220 calls in the top 2 groups",
		"the sampled layer says it is a sample and of how many calls")

	b.Reset()
	require.NoError(t, render.Hotspots(&b, insight.HotspotsResult{Window: window, GroupBy: "service", Order: insight.OrderCountDesc}))
	assert.Contains(t, b.String(), "No calls match.")
	assert.NotContains(t, b.String(), "Sampled")
}

func sampleTimeline() insight.TimelineResult {
	return insight.TimelineResult{
		Window: window, BucketMs: 1_200_000, Totals: insight.StatsTotals{Count: 1300, Errors: 9},
		Buckets: []insight.TimelineBucket{
			{Start: t0, End: t0.Add(20 * time.Minute), Count: 400, Errors: 1, P50Ms: i64(110), P95Ms: i64(400), P99Ms: i64(700)},
			{Start: t0.Add(20 * time.Minute), End: t0.Add(40 * time.Minute), Count: 900, Errors: 8, P50Ms: i64(150), P95Ms: i64(2100), P99Ms: i64(3900)},
			{Start: t0.Add(40 * time.Minute), End: t0.Add(time.Hour)},
		},
		Partial: []string{"s3 list: timeout"},
	}
}

func TestTimelineText(t *testing.T) {
	var b bytes.Buffer
	require.NoError(t, render.Timeline(&b, sampleTimeline()))
	golden(t, "timeline", b.String())
}
