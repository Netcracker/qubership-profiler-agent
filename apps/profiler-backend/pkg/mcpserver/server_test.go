package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	clicmd "github.com/Netcracker/qubership-profiler-backend/tools/profiler-cli/cmd"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const ref = "prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error"

// query is an httptest stand-in for the query service. It records the
// requests it gets, so a test can check what a tool sent.
type query struct {
	mu       sync.Mutex
	requests []*http.Request
}

func (q *query) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q.mu.Lock()
	q.requests = append(q.requests, r.Clone(context.Background()))
	q.mu.Unlock()
	switch {
	case r.URL.Path == "/api/v1/pods":
		pod := func(ns, svc, name string) map[string]any {
			return map[string]any{"namespace": ns, "service": svc, "pod": name, "restart_time_ms": 1760420000000,
				"time_min_ms": 1760430000000, "time_max_ms": 1760433600000}
		}
		writeJSON(w, map[string]any{"pods": []map[string]any{
			pod("prod", "billing", "billing-7f8c"), pod("stage", "orders", "orders-1"),
		}})
	case r.URL.Path == "/api/v1/calls":
		pk, _ := model.ParsePKPath("prod:billing:billing-7f8c:1760420000000:5:12340:0")
		writeJSON(w, map[string]any{"calls": []model.CallJSON{{
			PK: pk, TsMs: 1760433000123, DurationMs: 2470, Method: "a.Main.run", RetentionClass: "any_error",
			CpuTimeMs: 310, WaitTimeMs: 1900, ErrorFlag: true,
			Params: map[string][]string{"x-request-id": {"abc123"}},
		}}, "next_cursor": "page-2"})
	case r.URL.Path == "/api/v1/config":
		writeJSON(w, map[string]any{"filter_capabilities": model.Capabilities()})
	case r.URL.Path == "/api/v1/stats":
		writeJSON(w, statsBody(r.URL.Query()))
	case strings.HasSuffix(r.URL.Path, "/tree"):
		_, _ = w.Write(calltree.Encode(&calltree.Tree{
			Methods: []string{"a.Main.run", "a.Repo.load"},
			Root: &calltree.Node{MethodIdx: 0, DurationMs: 1000, SelfDurationMs: 100, Executions: 1, SelfExecutions: 1,
				Children: []*calltree.Node{
					{MethodIdx: 1, DurationMs: 900, SelfDurationMs: 900, Executions: 3, SelfExecutions: 3},
				}},
		}))
	default:
		http.NotFound(w, r)
	}
}

// targetFromMs starts the target window of the tests; a /stats request
// before it is for the default baseline of a compare, whose calls were
// faster.
const targetFromMs = 1760430000000

// statsBody answers /stats: two time buckets for the time grouping, and one
// group for a field grouping, five times slower in the target window than
// before it.
func statsBody(q url.Values) stats.Response {
	if q.Get("group_by") == stats.GroupByTime {
		p := func(v int64) *int64 { return &v }
		bucket := int64(1_800_000)
		return stats.Response{GroupBy: stats.GroupByTime, BucketMs: &bucket, Distinct: 2,
			Totals: stats.Totals{Count: 10, Errors: 1},
			Rows: []stats.Row{
				{Key: int64(targetFromMs), RowValues: stats.RowValues{Count: 10, Errors: 1, DurationMsSum: 5000,
					P50Ms: p(400), P95Ms: p(900), P99Ms: p(990)}},
				{Key: int64(targetFromMs + 1_800_000), RowValues: stats.RowValues{}},
			}}
	}
	from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
	mean := int64(2470)
	if from < targetFromMs {
		mean = 494
	}
	p95 := mean + 100
	return stats.Response{GroupBy: q.Get("group_by"), Distinct: 1, Totals: stats.Totals{Count: 10, Errors: 1},
		Rows: []stats.Row{{Key: "a.Main.run", RowValues: stats.RowValues{
			Count: 10, Errors: 1, DurationMsSum: 10 * mean, P50Ms: &mean, P95Ms: &p95, P99Ms: &p95,
		}}}}
}

func (q *query) last() *http.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.requests) == 0 {
		return nil
	}
	return q.requests[len(q.requests)-1]
}

// requestsTo returns the query parameters of the requests to path, oldest
// first.
func (q *query) requestsTo(path string) []url.Values {
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []url.Values
	for _, r := range q.requests {
		if r.URL.Path == path {
			out = append(out, r.URL.Query())
		}
	}
	return out
}

func (q *query) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.requests)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// counting records every request the server hands it, and rejects the ones
// reject matches with 401.
type counting struct {
	mu      sync.Mutex
	methods []string
	reject  func(*http.Request) bool
	// credential is put in the context of every accepted request.
	credential string
}

func (a *counting) Authenticate(w http.ResponseWriter, r *http.Request) *http.Request {
	a.mu.Lock()
	a.methods = append(a.methods, r.Method)
	a.mu.Unlock()
	if a.reject != nil && a.reject(r) {
		http.Error(w, "no token", http.StatusUnauthorized)
		return nil
	}
	if a.credential != "" {
		return r.WithContext(insight.WithCredential(r.Context(), a.credential))
	}
	return r
}

func (a *counting) calls() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.methods)
}

type fixture struct {
	query  *query
	client *insight.Client
	auth   *counting
	url    string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	q := &query{}
	qs := httptest.NewServer(q)
	t.Cleanup(qs.Close)
	client, err := insight.New(qs.URL)
	require.NoError(t, err)
	auth := &counting{}
	handler, err := NewHandler(Options{Client: client, Authenticator: auth})
	require.NoError(t, err)
	ms := httptest.NewServer(handler)
	t.Cleanup(ms.Close)
	return &fixture{query: q, client: client, auth: auth, url: ms.URL}
}

func (f *fixture) connect(t *testing.T) *mcp.ClientSession {
	t.Helper()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	session, err := c.Connect(context.Background(), &mcp.StreamableClientTransport{Endpoint: f.url}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func call(t *testing.T, s *mcp.ClientSession, name string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	require.NoError(t, err)
	return res
}

func text(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

// TestToolsCoverTheCLIOperations keeps the two front ends in step: every
// CLI subcommand has a tool, and every tool is a CLI subcommand.
func TestToolsCoverTheCLIOperations(t *testing.T) {
	var cli []string
	for _, c := range clicmd.NewRoot(io.Discard).Commands() {
		if c.Name() != "help" {
			cli = append(cli, c.Name())
		}
	}
	var ours []string
	for _, op := range operations {
		ours = append(ours, op.name)
	}
	assert.ElementsMatch(t, cli, ours)

	f := newFixture(t)
	res, err := f.connect(t).ListTools(context.Background(), nil)
	require.NoError(t, err)
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
		assert.True(t, tool.Annotations.ReadOnlyHint, tool.Name)
	}
	assert.Equal(t, []string{
		"profiler_compare_windows", "profiler_explain_call", "profiler_find_calls", "profiler_hotspots",
		"profiler_list_services", "profiler_timeline",
	}, slices.Sorted(slices.Values(names)))
}

// structured decodes a tool's structured content into the library's type.
func structured[T any](t *testing.T, res *mcp.CallToolResult) T {
	t.Helper()
	require.False(t, res.IsError, text(res))
	raw, err := json.Marshal(res.StructuredContent)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

// TestToolsReturnTheLibraryResult compares each tool's structured content
// with the same operation called on libs/insight directly.
func TestToolsReturnTheLibraryResult(t *testing.T) {
	f := newFixture(t)
	s := f.connect(t)
	ctx := context.Background()
	window := map[string]any{"from": "1760430000000", "to": "1760433600000"}
	libWindow, err := insight.ParseWindow("", "1760430000000", "1760433600000")
	require.NoError(t, err)

	t.Run("services", func(t *testing.T) {
		res := call(t, s, "profiler_list_services", merge(window, map[string]any{
			"filters": []map[string]any{{"field": "namespace", "op": "=", "value": "prod"}},
		}))
		want, err := f.client.Services(ctx, insight.ServicesInput{
			Window: libWindow, Filters: insight.Filters{Conditions: []string{"namespace=prod"}},
		})
		require.NoError(t, err)
		assert.Equal(t, want, structured[insight.ServicesResult](t, res))
		assert.Contains(t, text(res), "billing-7f8c")
		assert.NotContains(t, text(res), "orders-1")
	})

	t.Run("calls", func(t *testing.T) {
		res := call(t, s, "profiler_find_calls", merge(window, map[string]any{
			"service": "prod/billing",
			"filters": []map[string]any{
				{"field": "request_id", "op": "=", "value": "abc123"},
				{"field": "duration", "op": ">", "value": "1s"},
				{"field": "pod", "op": "in", "value": "billing-7f8c,billing-2"},
			},
			"limit": 5, "order": "duration_desc",
		}))
		got := structured[insight.CallsResult](t, res)
		sent := f.query.last().URL.Query()
		assert.Equal(t, "duration_desc", sent.Get("order"))
		assert.Equal(t, []string{"request_id = abc123", "duration > 1s", "pod in billing-7f8c,billing-2",
			"namespace=prod", "service=billing"}, sent["filter"])
		assert.Equal(t, "5", sent.Get("limit"))

		want, err := f.client.Calls(ctx, insight.CallsInput{Window: libWindow, Limit: 5, Order: insight.OrderSlowest, Filters: insight.Filters{
			Service:    "prod/billing",
			Conditions: []string{"request_id = abc123", "duration > 1s", "pod in billing-7f8c,billing-2"},
		}})
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Contains(t, text(res), `More calls: pass "cursor": "`+got.NextCursor+`"`)
		assert.Contains(t, text(res), ref)
	})

	t.Run("explain", func(t *testing.T) {
		res := call(t, s, "profiler_explain_call", map[string]any{"ref": ref, "node": 1, "threshold": 0.01})
		node := 1
		want, err := f.client.Explain(ctx, insight.ExplainInput{Ref: ref, FocusNode: &node, Threshold: 0.01})
		require.NoError(t, err)
		assert.Equal(t, want, structured[insight.ExplainResult](t, res))
		assert.Contains(t, text(res), "Focus: #1 a.Repo.load")
	})

	t.Run("hotspots", func(t *testing.T) {
		res := call(t, s, "profiler_hotspots", merge(window, map[string]any{
			"service": "prod/billing", "group_by": "pod", "limit": 5, "order": "count_desc", "sample": 3,
		}))
		got := structured[insight.HotspotsResult](t, res)
		sent := f.query.requestsTo("/api/v1/stats")[0]
		assert.Equal(t, "pod", sent.Get("group_by"))
		assert.Equal(t, "5", sent.Get("limit"))
		assert.Equal(t, "count_desc", sent.Get("order"))

		want, err := f.client.Hotspots(ctx, insight.HotspotsInput{
			Window: libWindow, Filters: insight.Filters{Service: "prod/billing"},
			GroupBy: "pod", Limit: 5, Order: insight.OrderCountDesc, Sample: 3,
		})
		require.NoError(t, err)
		assert.Equal(t, want, got)
		require.NotNil(t, got.Sample, "the sampled layer reads the call's tree")
		assert.Equal(t, 3, got.Sample.Requested)
		assert.Contains(t, text(res), "Sampled, not exact")
		assert.Contains(t, text(res), "a.Repo.load")
	})

	t.Run("hotspots defaults", func(t *testing.T) {
		res := call(t, s, "profiler_hotspots", window)
		want, err := f.client.Hotspots(ctx, insight.HotspotsInput{Window: libWindow})
		require.NoError(t, err)
		got := structured[insight.HotspotsResult](t, res)
		assert.Equal(t, want, got)
		assert.Equal(t, insight.DefaultGroupBy, got.GroupBy)
		assert.Equal(t, insight.OrderDurationSumDesc, got.Order)
		assert.Equal(t, insight.DefaultSample, got.Sample.Requested)
	})

	t.Run("timeline", func(t *testing.T) {
		res := call(t, s, "profiler_timeline", merge(window, map[string]any{
			"service": "prod/billing", "buckets": 2,
		}))
		sent := f.query.last().URL.Query()
		assert.Equal(t, "time", sent.Get("group_by"))
		assert.Equal(t, "2", sent.Get("buckets"))
		want, err := f.client.Timeline(ctx, insight.TimelineInput{
			Window: libWindow, Filters: insight.Filters{Service: "prod/billing"}, Buckets: 2,
		})
		require.NoError(t, err)
		got := structured[insight.TimelineResult](t, res)
		assert.Equal(t, want, got)
		assert.Len(t, got.Buckets, 2)
	})

	t.Run("compare", func(t *testing.T) {
		res := call(t, s, "profiler_compare_windows", merge(window, map[string]any{
			"service": "prod/billing", "sample": 2,
		}))
		want, err := f.client.Compare(ctx, insight.CompareInput{
			Target: libWindow, Filters: insight.Filters{Service: "prod/billing"}, Sample: 2,
		})
		require.NoError(t, err)
		got := structured[insight.CompareResult](t, res)
		assert.Equal(t, overJSON(t, want), got)
		assert.Equal(t, time.UnixMilli(1760426400000).UTC(), got.Baseline.From,
			"the default baseline is the window of the same length before the target")
		assert.Contains(t, text(res), "[slower]")
	})

	t.Run("compare with a baseline shift", func(t *testing.T) {
		res := call(t, s, "profiler_compare_windows", merge(window, map[string]any{"baseline_shift": "1d"}))
		want, err := f.client.Compare(ctx, insight.CompareInput{Target: libWindow, BaselineShift: 24 * time.Hour})
		require.NoError(t, err)
		got := structured[insight.CompareResult](t, res)
		assert.Equal(t, overJSON(t, want), got)
		assert.Equal(t, libWindow.From.Add(-24*time.Hour), got.Baseline.From)
	})

	t.Run("compare with a baseline window", func(t *testing.T) {
		res := call(t, s, "profiler_compare_windows", merge(window, map[string]any{
			"baseline_from": "1760340000000", "baseline_to": "1760343600000",
		}))
		baseline, err := insight.ParseWindow("", "1760340000000", "1760343600000")
		require.NoError(t, err)
		want, err := f.client.Compare(ctx, insight.CompareInput{Target: libWindow, Baseline: baseline})
		require.NoError(t, err)
		got := structured[insight.CompareResult](t, res)
		assert.Equal(t, overJSON(t, want), got)
		assert.Equal(t, time.UnixMilli(1760340000000).UTC(), got.Baseline.From)
	})
}

// overJSON returns v as a client decodes it from JSON: an empty list
// under omitempty, such as the Partial of a complete compare, arrives as nil.
func overJSON[T any](t *testing.T, v T) T {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	var out T
	require.NoError(t, json.Unmarshal(raw, &out))
	return out
}

func merge(a, b map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func TestAuthenticatorRunsOnEveryRequest(t *testing.T) {
	f := newFixture(t)
	f.auth.credential = "Bearer caller-token"
	s := f.connect(t)
	afterConnect := f.auth.calls()
	require.Positive(t, afterConnect, "initialize passes the authenticator")

	_, err := s.ListTools(context.Background(), nil)
	require.NoError(t, err)
	assert.Greater(t, f.auth.calls(), afterConnect, "tools/list passes the authenticator")

	before := f.auth.calls()
	res := call(t, s, "profiler_list_services", nil)
	require.False(t, res.IsError, text(res))
	assert.Greater(t, f.auth.calls(), before, "tools/call passes the authenticator")
	assert.Equal(t, "Bearer caller-token", f.query.last().Header.Get("Authorization"),
		"the identity the authenticator puts in the context reaches query")
}

func TestAuthenticatorRejectionStopsTheCall(t *testing.T) {
	f := newFixture(t)
	s := f.connect(t)
	f.auth.reject = func(*http.Request) bool { return true }
	_, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: "profiler_list_services"})
	require.Error(t, err)
	assert.Zero(t, f.query.count(), "a rejected request never reaches query")
}

func TestInvalidInputIsReportedClearly(t *testing.T) {
	f := newFixture(t)
	s := f.connect(t)
	for name, tc := range map[string]struct {
		tool string
		args map[string]any
		want []string
	}{
		"missing ref": {"profiler_explain_call", map[string]any{}, []string{"ref"}},
		"wrong type":  {"profiler_find_calls", map[string]any{"limit": "ten"}, []string{"limit"}},
		"unknown argument": {"profiler_find_calls", map[string]any{"service_name": "prod/billing"},
			[]string{"service_name"}},
		"unknown operator": {"profiler_find_calls", map[string]any{
			"filters": []map[string]any{{"field": "duration", "op": "~", "value": "1s"}},
		}, []string{"op"}},
		"condition without a value": {"profiler_find_calls", map[string]any{
			"filters": []map[string]any{{"field": "duration", "op": ">"}},
		}, []string{"value"}},
		"bad window": {"profiler_find_calls", map[string]any{"since": "yesterday"},
			[]string{`since "yesterday" is not a positive duration`}},
		"bad ref": {"profiler_explain_call", map[string]any{"ref": "abc"},
			[]string{"is not a call reference"}},
		"unknown order": {"profiler_hotspots", map[string]any{"order": "p95_desc"}, []string{"order"}},
		"calls order on hotspots": {"profiler_hotspots", map[string]any{"order": "duration_desc"},
			[]string{"order"}},
		"hotspots order on calls": {"profiler_find_calls", map[string]any{"order": "count_desc"}, []string{"order"}},
		"hotspots by time": {"profiler_hotspots", map[string]any{"group_by": "time"},
			[]string{"the timeline operation groups by time"}},
		"too many buckets": {"profiler_timeline", map[string]any{"buckets": 100000},
			[]string{"buckets must be between 1 and"}},
		"both baseline forms": {"profiler_compare_windows", map[string]any{
			"baseline_shift": "1d", "baseline_from": "1760340000000", "baseline_to": "1760343600000",
		}, []string{"baseline_shift or baseline_from and baseline_to, not both"}},
		"half a baseline window": {"profiler_compare_windows", map[string]any{"baseline_from": "1760340000000"},
			[]string{"give both baseline_from and baseline_to"}},
		"bad baseline shift": {"profiler_compare_windows", map[string]any{"baseline_shift": "yesterday"},
			[]string{"baseline shift"}},
		"overlapping baseline": {"profiler_compare_windows", map[string]any{
			"from": "1760430000000", "to": "1760433600000",
			"baseline_from": "1760429000000", "baseline_to": "1760431000000",
		}, []string{"overlaps the target"}},
	} {
		t.Run(name, func(t *testing.T) {
			res := call(t, s, tc.tool, tc.args)
			require.True(t, res.IsError)
			t.Log(text(res))
			for _, w := range tc.want {
				assert.Contains(t, text(res), w)
			}
		})
	}
	assert.Zero(t, f.query.count(), "invalid input never reaches query")
}

func TestConditionText(t *testing.T) {
	for _, tc := range []struct {
		in      Condition
		want    string
		wantErr string
	}{
		{Condition{"duration", ">=", "500ms"}, "duration >= 500ms", ""},
		{Condition{"service", "not in", "a,b"}, "service not in a,b", ""},
		{Condition{"duration", "between", "100ms..2s"}, "duration between 100ms..2s", ""},
		{Condition{"user.tenant", "=", "=x"}, "user.tenant = =x", ""},
		{Condition{"pod", "=~", "billing-.*"}, "pod =~ billing-.*", ""},
		{Condition{"a=b", "=", "x"}, "", "operator character"},
		{Condition{"", "=", "x"}, "", "field name"},
		{Condition{"duration", "gt", "1s"}, "", `op "gt" is not one of`},
	} {
		got, err := tc.in.text()
		if tc.wantErr != "" {
			assert.ErrorContains(t, err, tc.wantErr, "%+v", tc.in)
			continue
		}
		require.NoError(t, err, "%+v", tc.in)
		assert.Equal(t, tc.want, got)
		parsed, err := model.ParseCondition(got)
		require.NoError(t, err)
		assert.Equal(t, tc.in.Op, string(parsed.Op))
	}
}

func TestInstructionsNameEveryTool(t *testing.T) {
	f := newFixture(t)
	s := f.connect(t)
	assert.Equal(t, Instructions, s.InitializeResult().Instructions)
	res, err := s.ListTools(context.Background(), nil)
	require.NoError(t, err)
	for _, tool := range res.Tools {
		assert.Contains(t, Instructions, tool.Name)
	}
}

// TestSchemasOfferWhatTheBackendImplements ties the enums of the tool
// inputs to the query service's own capabilities, so an operator or an
// order the backend gains cannot be missing from the tools.
func TestSchemasOfferWhatTheBackendImplements(t *testing.T) {
	f := newFixture(t)
	res, err := f.connect(t).ListTools(context.Background(), nil)
	require.NoError(t, err)
	schemas := map[string]map[string]any{}
	for _, tool := range res.Tools {
		raw, err := json.Marshal(tool.InputSchema)
		require.NoError(t, err)
		var schema map[string]any
		require.NoError(t, json.Unmarshal(raw, &schema))
		schemas[tool.Name] = schema
	}
	enum := func(tool string, path ...string) []string {
		node := schemas[tool]
		for _, p := range path {
			node = node[p].(map[string]any)
		}
		var out []string
		for _, v := range node["enum"].([]any) {
			out = append(out, v.(string))
		}
		return out
	}

	caps := model.Capabilities()
	assert.Subset(t, enum("profiler_find_calls", "properties", "order"), caps.Orders)

	opEnum := enum("profiler_find_calls", "properties", "filters", "items", "properties", "op")
	for field, fieldOps := range caps.Fields {
		for _, op := range fieldOps {
			assert.Contains(t, opEnum, string(op), "%s on %s", op, field)
		}
	}
	for _, op := range caps.ParamOperators {
		assert.Contains(t, opEnum, string(op), "%s on a param", op)
	}
}
