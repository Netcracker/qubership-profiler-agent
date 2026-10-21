package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	require.NoError(t, writeServices(&b, insight.ServicesResult{
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
	require.NoError(t, writeServices(&b, insight.ServicesResult{Window: window}))
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
	require.NoError(t, writeCalls(&b, insight.CallsResult{
		Window: &window, Calls: sampleCalls(), NextCursor: "cursor-page-2",
	}))
	golden(t, "calls", b.String())

	b.Reset()
	require.NoError(t, writeCalls(&b, insight.CallsResult{Restarted: true, Window: &window}))
	assert.Contains(t, b.String(), "The cursor was stale")
	assert.Contains(t, b.String(), "No calls match.")
}

func TestExplainText(t *testing.T) {
	call := sampleCalls()[0]
	var b bytes.Buffer
	require.NoError(t, writeExplain(&b, insight.ExplainResult{
		Call:  &call,
		Focus: "com.example.Billing.charge(long)",
		HotPath: &insight.PathNode{
			Method: "com.example.Billing.charge(long)", TotalMs: 2470, SelfMs: 20, SuspendMs: 120, Executions: 1, Skipped: 3,
			Hidden: 2, HiddenMs: 40,
			Children: []*insight.PathNode{
				{Method: "com.example.Repo.load(long)", TotalMs: 2000, SelfMs: 50, Executions: 1, Children: []*insight.PathNode{
					{Method: "a.Jdbc.execute", TotalMs: 1950, SelfMs: 1950, Executions: 90},
				}},
				{Method: "com.example.Http.call()", TotalMs: 410, SelfMs: 410, Executions: 2},
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
			return writeServices(w, insight.ServicesResult{Window: window, Namespaces: []insight.Namespace{{
				Name: "prod", Services: []insight.Service{{Name: "billing", Pods: []insight.Pod{{
					Name: "billing-7f8c", Restarts: []insight.Restart{{StartedAt: t0, FirstCall: t0, LastCall: t0}},
				}}}},
			}}})
		},
		"services, empty": func(w io.Writer) error {
			return writeServices(w, insight.ServicesResult{Window: window, Partial: []string{"timeout"}})
		},
		"calls": func(w io.Writer) error {
			return writeCalls(w, insight.CallsResult{Window: &window, Calls: sampleCalls(), NextCursor: "c2"})
		},
		"calls, empty": func(w io.Writer) error {
			return writeCalls(w, insight.CallsResult{Restarted: true, NextCursor: "c2"})
		},
		"explain": func(w io.Writer) error {
			return writeExplain(w, insight.ExplainResult{
				Call: &call, Focus: "f",
				HotPath: &insight.PathNode{Method: "a", Hidden: 1, Children: []*insight.PathNode{{Method: "b"}}},
				TopSelf: []insight.MethodStat{{Method: "b"}}, TopSQL: []insight.SQLStat{{Statement: "s"}},
				Notes: []string{"n"},
			})
		},
	} {
		var full bytes.Buffer
		require.NoError(t, write(&full), name)
		for n := 0; n < full.Len(); n++ {
			assert.ErrorIs(t, write(&shortWriter{n: n}), io.ErrClosedPipe, "%s cut at byte %d of %d", name, n, full.Len())
		}
	}
}

// serve answers /api/v1/pods and /api/v1/calls from testdata and records the
// last query.
func serve(t *testing.T) (string, *http.Request) {
	t.Helper()
	last := &http.Request{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*last = *r.Clone(context.Background())
		name := map[string]string{"/api/v1/pods": "pods.json", "/api/v1/calls": "calls.json"}[r.URL.Path]
		if name == "" {
			http.NotFound(w, r)
			return
		}
		body, err := os.ReadFile(filepath.Join("testdata", name))
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, last
}

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	root := NewRoot(&out)
	root.SetArgs(args)
	root.SetErr(&bytes.Buffer{})
	err := root.ExecuteContext(context.Background())
	return out.String(), err
}

func TestCallsCommandSendsFlagsAsConditions(t *testing.T) {
	url, last := serve(t)
	out, err := run(t, "calls", "--url", url, "--since", "30m", "--service", "prod/billing",
		"--filter", "request_id=abc123", "--filter", "duration>1s", "--limit", "5")
	require.NoError(t, err)
	q := last.URL.Query()
	assert.Equal(t, []string{"request_id=abc123", "duration>1s", "namespace=prod", "service=billing"}, q["filter"])
	assert.Equal(t, "5", q.Get("limit"))
	assert.Contains(t, out, "request_id=abc123")

	// The printed cursor continues the listing under the same filters, and
	// the window of page 1 stays with the server's cursor.
	_, token, found := strings.Cut(out, "More calls: --cursor ")
	require.True(t, found, out)
	_, err = run(t, "calls", "--url", url, "--cursor", strings.TrimSpace(token))
	require.NoError(t, err)
	q = last.URL.Query()
	assert.Equal(t, "cursor-page-2", q.Get("cursor"))
	assert.ElementsMatch(t, []string{"request_id=abc123", "duration>1s", "namespace=prod", "service=billing"}, q["filter"])
	assert.Empty(t, q.Get("from"))
}

func TestJSONFlagPrintsTheTypedResult(t *testing.T) {
	url, _ := serve(t)
	out, err := run(t, "services", "--url", url, "--json")
	require.NoError(t, err)
	var res insight.ServicesResult
	require.NoError(t, json.Unmarshal([]byte(out), &res))
	assert.Len(t, res.Namespaces, 2)
}

func TestURLComesFromTheEnvironment(t *testing.T) {
	url, _ := serve(t)
	t.Setenv(urlEnv, url)
	_, err := run(t, "services")
	assert.NoError(t, err)

	t.Setenv(urlEnv, "")
	_, err = run(t, "services")
	assert.ErrorContains(t, err, "set --url or PROFILER_URL")
}

func TestCommandErrors(t *testing.T) {
	url, _ := serve(t)
	for _, args := range [][]string{
		{"calls", "--url", url, "--since", "1h", "--from", "1760430000000"},
		{"calls", "--url", url, "--filter", "service"},
		{"explain", "--url", url},
		{"explain", "--url", url, "not-a-ref"},
	} {
		_, err := run(t, args...)
		assert.Error(t, err, "%v", args)
	}
}
