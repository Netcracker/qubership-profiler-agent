package insight

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/stretchr/testify/require"
)

// fakeQuery is an in-memory query service over synthetic calls. It answers
// /config, /stats for field groupings, /calls in duration_desc order without
// paging, and /tree, filtering with the real model.Matcher. The replay
// server serves canned answers in order; operations that send requests at
// once, or whose requests depend on earlier answers, need this one.
type fakeQuery struct {
	t     *testing.T
	calls []fakeCall
	caps  model.FilterCapabilities
	// partial makes a /stats or /calls answer partial; nil means never.
	partial func(path string, q url.Values) bool

	mu   sync.Mutex
	reqs []*url.URL
}

// fakeCall is one synthetic call. A nil tree serves a one-node tree of the
// root method.
type fakeCall struct {
	row  model.CallRow
	tree *calltree.Tree
	// noTree makes /tree answer call_not_found.
	noTree bool
}

func newFake(t *testing.T, calls ...fakeCall) (*Client, *fakeQuery) {
	t.Helper()
	f := &fakeQuery{t: t, calls: calls, caps: model.Capabilities()}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL)
	require.NoError(t, err)
	return c, f
}

// call builds a call of pod prod/<service>/<service>-1 at tsMs. Calls are
// told apart by their record index.
func call(service, method string, tsMs int64, durationMs int32, record int32) fakeCall {
	return fakeCall{row: model.CallRow{
		PK: model.PK{
			PodNamespace: "prod", PodService: service, PodName: service + "-1",
			RestartTimeMs: 1, RecordIndex: record,
		},
		TsMs: tsMs, DurationMs: durationMs, Method: method, RetentionClass: "normal_clean",
	}}
}

func (f *fakeQuery) requests(path string) []url.Values {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []url.Values
	for _, u := range f.reqs {
		if u.Path == path {
			out = append(out, u.Query())
		}
	}
	return out
}

func (f *fakeQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.reqs = append(f.reqs, r.URL)
	f.mu.Unlock()
	q := r.URL.Query()
	switch {
	case r.URL.Path == "/api/v1/config":
		f.json(w, map[string]any{"filter_capabilities": f.caps})
	case r.URL.Path == "/api/v1/stats":
		f.stats(w, q)
	case r.URL.Path == "/api/v1/calls":
		f.list(w, q)
	case strings.HasSuffix(r.URL.Path, "/tree"):
		f.tree(w, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/calls/"), "/tree"))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeQuery) json(w http.ResponseWriter, body any) {
	w.Header().Set("Content-Type", "application/json")
	require.NoError(f.t, json.NewEncoder(w).Encode(body))
}

func (f *fakeQuery) match(q url.Values) []model.CallRow {
	// /calls reads only its own orders; a /stats order is not one.
	q = url.Values{"from": q["from"], "to": q["to"], "filter": q["filter"]}
	query, detail := model.ParseCallsQuery(q)
	require.Empty(f.t, detail)
	m, err := query.Matcher()
	require.NoError(f.t, err)
	var out []model.CallRow
	for _, c := range f.calls {
		if m.Match(&c.row) {
			out = append(out, c.row)
		}
	}
	return out
}

func (f *fakeQuery) isPartial(path string, q url.Values) bool {
	return f.partial != nil && f.partial(path, q)
}

func (f *fakeQuery) stats(w http.ResponseWriter, q url.Values) {
	field := q.Get("group_by")
	require.Contains(f.t, []string{"method", "service", "pod", "namespace"}, field, "the fake groups by an identity field or method")
	rows := f.match(q)
	groups := map[string][]model.CallRow{}
	var totals stats.Totals
	for _, r := range rows {
		key := map[string]string{
			"method": r.Method, "service": r.PK.PodService, "pod": r.PK.PodName, "namespace": r.PK.PodNamespace,
		}[field]
		groups[key] = append(groups[key], r)
		totals.Count++
		if r.ErrorFlag {
			totals.Errors++
		}
	}
	order := stats.Order(cmp.Or(q.Get("order"), string(stats.OrderCountDesc)))
	limit := 20
	if l := q.Get("limit"); l != "" {
		var err error
		limit, err = strconv.Atoi(l)
		require.NoError(f.t, err)
	}
	var out []stats.Row
	for key, rs := range groups {
		out = append(out, stats.Row{Key: key, RowValues: values(rs)})
	}
	metric := func(v stats.RowValues) int64 {
		if order == stats.OrderDurationSumDesc {
			return v.DurationMsSum
		}
		return v.Count
	}
	slices.SortFunc(out, func(a, b stats.Row) int {
		if c := cmp.Compare(metric(b.RowValues), metric(a.RowValues)); c != 0 {
			return c
		}
		return cmp.Compare(a.Key.(string), b.Key.(string))
	})
	resp := stats.Response{GroupBy: field, Distinct: len(out), Totals: totals, Rows: out}
	if len(out) > limit {
		var rest []model.CallRow
		for _, row := range out[limit:] {
			rest = append(rest, groups[row.Key.(string)]...)
		}
		v := values(rest)
		resp.Other, resp.Rows = &v, out[:limit]
	}
	if resp.Rows == nil {
		resp.Rows = []stats.Row{}
	}
	if f.isPartial("/stats", q) {
		resp.Partial, resp.PartialReasons = true, []string{"collector 10.0.0.7 stats: timeout"}
	}
	f.json(w, resp)
}

// values sums a group; the percentiles take the nearest rank, exact.
func values(rs []model.CallRow) stats.RowValues {
	var v stats.RowValues
	durations := make([]int64, 0, len(rs))
	for _, r := range rs {
		v.Count++
		if r.ErrorFlag {
			v.Errors++
		}
		v.DurationMsSum += int64(r.DurationMs)
		durations = append(durations, int64(r.DurationMs))
	}
	if len(durations) == 0 {
		return v
	}
	slices.Sort(durations)
	at := func(p float64) *int64 { d := durations[int(p*float64(len(durations)-1))]; return &d }
	v.P50Ms, v.P95Ms, v.P99Ms = at(0.50), at(0.95), at(0.99)
	return v
}

func (f *fakeQuery) list(w http.ResponseWriter, q url.Values) {
	require.Equal(f.t, "duration_desc", q.Get("order"), "the fake lists the slowest calls only")
	rows := f.match(q)
	slices.SortFunc(rows, func(a, b model.CallRow) int {
		if c := cmp.Compare(b.DurationMs, a.DurationMs); c != 0 {
			return c
		}
		return a.PK.Compare(b.PK)
	})
	limit, err := strconv.Atoi(q.Get("limit"))
	require.NoError(f.t, err)
	rows = capList(rows, limit)
	out := make([]model.CallJSON, len(rows))
	for i, r := range rows {
		out[i] = r.JSON()
	}
	body := map[string]any{"calls": out, "next_cursor": nil}
	if f.isPartial("/calls", q) {
		body["partial"], body["partial_reasons"] = true, []string{"s3 list: timeout"}
	}
	f.json(w, body)
}

func (f *fakeQuery) tree(w http.ResponseWriter, pkPath string) {
	pk, err := model.ParsePKPath(pkPath)
	require.NoError(f.t, err)
	for _, c := range f.calls {
		if c.row.PK != pk || c.noTree {
			continue
		}
		tree := c.tree
		if tree == nil {
			tree = &calltree.Tree{Methods: []string{c.row.Method}, Root: &calltree.Node{
				DurationMs: int64(c.row.DurationMs), SelfDurationMs: int64(c.row.DurationMs), Executions: 1, SelfExecutions: 1,
			}}
		}
		_, _ = w.Write(calltree.Encode(tree))
		return
	}
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write([]byte(`{"title":"Not found","status":404,"code":"call_not_found"}`))
}

// background is a test context.
var background = context.Background()
