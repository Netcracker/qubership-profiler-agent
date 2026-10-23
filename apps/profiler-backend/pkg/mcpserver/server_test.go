package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
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

func (q *query) last() *http.Request {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.requests) == 0 {
		return nil
	}
	return q.requests[len(q.requests)-1]
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
	assert.Equal(t, []string{"profiler_explain_call", "profiler_find_calls", "profiler_list_services"},
		slices.Sorted(slices.Values(names)))
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
			"limit": 5,
		}))
		got := structured[insight.CallsResult](t, res)
		sent := f.query.last().URL.Query()
		assert.Equal(t, []string{"request_id = abc123", "duration > 1s", "pod in billing-7f8c,billing-2",
			"namespace=prod", "service=billing"}, sent["filter"])
		assert.Equal(t, "5", sent.Get("limit"))

		want, err := f.client.Calls(ctx, insight.CallsInput{Window: libWindow, Limit: 5, Filters: insight.Filters{
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
