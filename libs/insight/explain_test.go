package insight

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// treeBuilder makes call trees in memory. Every node runs once unless the
// test says otherwise; self time is total minus the children's totals, and
// executions count the subtree, as calltree builds them (02 §2.5.3).
type treeBuilder struct{ methods []string }

func (b *treeBuilder) node(method string, totalMs int64, children ...*calltree.Node) *calltree.Node {
	idx := -1
	for i, m := range b.methods {
		if m == method {
			idx = i
		}
	}
	if idx < 0 {
		idx = len(b.methods)
		b.methods = append(b.methods, method)
	}
	self, executions := totalMs, int64(1)
	for _, ch := range children {
		self -= ch.DurationMs
		executions += ch.Executions
	}
	return &calltree.Node{
		MethodIdx: idx, DurationMs: totalMs, SelfDurationMs: self,
		Executions: executions, SelfExecutions: 1, Children: children,
	}
}

// loop marks a node as run n times under its parent; the parents built after
// it count the extra executions.
func loop(n *calltree.Node, times int64) *calltree.Node {
	n.Executions += times - n.SelfExecutions
	n.SelfExecutions = times
	return n
}

// roundTrip encodes and decodes the tree, so the tests read what /tree serves.
func (b *treeBuilder) roundTrip(t *testing.T, root *calltree.Node, params ...string) *calltree.Tree {
	t.Helper()
	tree, _, err := calltree.Decode(calltree.Encode(&calltree.Tree{Methods: b.methods, Params: params, Root: root}))
	require.NoError(t, err)
	return tree
}

func opts(threshold float64, maxNodes int) reduceOptions {
	return reduceOptions{threshold: threshold, maxNodes: maxNodes}
}

func TestReduceCollapsesADeepPassThroughChain(t *testing.T) {
	b := &treeBuilder{}
	leafs := []*calltree.Node{b.node("void a.Repo.load() (Repo.java) [a.jar]", 60), b.node("a.Http.call", 30), b.node("a.Log.write", 2)}
	d := b.node("a.Service.handle", 96, leafs...)
	c := b.node("a.Filter3.doFilter", 97, d)
	bb := b.node("a.Filter2.doFilter", 98, c)
	root := b.node("a.Servlet.service", 100, bb)

	res := reduce(b.roundTrip(t, root), opts(DefaultThreshold, DefaultMaxNodes))
	assert.Equal(t, "a.Servlet.service", res.Focus)
	path := res.HotPath
	assert.Equal(t, "a.Service.handle", path.Method, "three pass-through frames collapse")
	assert.Equal(t, 3, path.Skipped)
	require.Len(t, path.Children, 2)
	assert.Equal(t, "a.Repo.load()", path.Children[0].Method, "slowest first, method shortened")
	assert.Equal(t, int64(60), path.Children[0].TotalMs)
	assert.Equal(t, int64(1), path.Executions, "a node shows its own executions, not its subtree's")
	assert.Equal(t, 1, path.Hidden, "a.Log.write is under 5%")
	assert.Equal(t, int64(2), path.HiddenMs)
	require.Len(t, res.Notes, 1)
	assert.Contains(t, res.Notes[0], "1 nodes under 5% of 100 ms are hidden")
}

func TestReduceDoesNotCollapseAFanOutOrANodeWithParams(t *testing.T) {
	b := &treeBuilder{}
	// The parent loops: the child runs 10 times per parent call, not a pass-through.
	root := b.node("a.Service.handle", 100, loop(b.node("a.Repo.load", 95), 10))
	res := reduce(b.roundTrip(t, root), opts(DefaultThreshold, DefaultMaxNodes))
	assert.Equal(t, "a.Service.handle", res.HotPath.Method)
	assert.Zero(t, res.HotPath.Skipped)

	b = &treeBuilder{}
	root = b.node("a.Service.handle", 100, b.node("a.Repo.load", 95))
	root.Params = []calltree.Param{{ParamIdx: 0, Groups: []calltree.ParamGroup{{Value: "x", DurationMs: 100, Executions: 1}}}}
	res = reduce(b.roundTrip(t, root, "user.tenant"), opts(DefaultThreshold, DefaultMaxNodes))
	assert.Equal(t, "a.Service.handle", res.HotPath.Method, "a node with params stays visible")
}

func TestReduceCutsAWideFanOut(t *testing.T) {
	b := &treeBuilder{}
	var children []*calltree.Node
	for i := 0; i < 100; i++ {
		children = append(children, b.node(fmt.Sprintf("a.Batch.item%02d", i), 10))
	}
	root := b.node("a.Batch.run", 1000, children...)
	tree := b.roundTrip(t, root)

	res := reduce(tree, opts(DefaultThreshold, DefaultMaxNodes))
	assert.Empty(t, res.HotPath.Children, "every item is 1% of the root")
	assert.Equal(t, 100, res.HotPath.Hidden)
	assert.Equal(t, int64(1000), res.HotPath.HiddenMs)

	res = reduce(tree, opts(0.001, DefaultMaxNodes))
	assert.Len(t, res.HotPath.Children, DefaultMaxNodes-1, "the root plus 39 children fill the budget")
	assert.Equal(t, 100-(DefaultMaxNodes-1), res.HotPath.Hidden)
	require.Len(t, res.Notes, 1)
	assert.Contains(t, res.Notes[0], "the node budget of 40 is reached and 61 nodes are hidden")
}

// TestReduceSpendsTheBudgetOnTheSlowestBranch checks the expansion order: a
// small budget goes down the expensive branch first, and every shown node's
// parent is shown.
func TestReduceSpendsTheBudgetOnTheSlowestBranch(t *testing.T) {
	b := &treeBuilder{}
	slow := b.node("a.Slow.run", 700,
		b.node("a.Slow.step1", 400, b.node("a.Slow.leaf", 390), b.node("a.Slow.other", 9)),
		b.node("a.Slow.step2", 290))
	fast := b.node("a.Fast.run", 300, b.node("a.Fast.step", 200), b.node("a.Fast.step2", 99))
	root := b.node("a.Main.run", 1000, slow, fast)

	res := reduce(b.roundTrip(t, root), opts(0.01, 5))
	var shown []string
	var visit func(p *PathNode)
	visit = func(p *PathNode) {
		shown = append(shown, p.Method)
		for _, ch := range p.Children {
			visit(ch)
		}
	}
	visit(res.HotPath)
	// step1 passes 390 of its 400 ms to one child, so it collapses into the
	// leaf; the budget then takes step2 before Fast's children.
	assert.Equal(t, []string{"a.Main.run", "a.Slow.run", "a.Slow.leaf", "a.Slow.step2", "a.Fast.run"}, shown)
	assert.Equal(t, 1, res.HotPath.Children[0].Children[0].Skipped)
	assert.Equal(t, 2, res.HotPath.Children[1].Hidden, "Fast's children are past the budget")
}

func TestReduceRanksSelfTimeAndSQL(t *testing.T) {
	b := &treeBuilder{}
	q1 := b.node("a.Jdbc.execute", 300)
	q1.Params = []calltree.Param{{ParamIdx: 0, Groups: []calltree.ParamGroup{
		{Value: "select * from orders where id = ?", DurationMs: 250, Executions: 50},
		{Value: calltree.OtherGroupValue, DurationMs: 50, Executions: 9},
	}}}
	q2 := b.node("a.Jdbc.execute", 200)
	q2.Params = []calltree.Param{{ParamIdx: 0, Groups: []calltree.ParamGroup{
		{Value: "select  *\n from orders where id = ?", DurationMs: 200, Executions: 40},
		{Value: "sql:3:17", DurationMs: 0, Executions: 1, Unresolved: true},
	}}}
	long := b.node("a.Jdbc.batch", 10)
	long.Params = []calltree.Param{{ParamIdx: 0, Groups: []calltree.ParamGroup{{Value: strings.Repeat("x", 400), DurationMs: 10, Executions: 1}}}}
	root := b.node("a.Service.handle", 600, b.node("a.Repo.load", 500, q1, q2), long)

	res := reduce(b.roundTrip(t, root, "sql"), opts(DefaultThreshold, DefaultMaxNodes))
	require.NotEmpty(t, res.TopSelf)
	assert.Equal(t, MethodStat{Method: "a.Jdbc.execute", SelfMs: 500, Executions: 2}, res.TopSelf[0], "both nodes of a method add up")

	require.Len(t, res.TopSQL, 4)
	assert.Equal(t, SQLStat{Statement: "select * from orders where id = ?", DurationMs: 450, Executions: 90}, res.TopSQL[0],
		"whitespace differences fold into one statement")
	assert.Equal(t, "(other statements)", res.TopSQL[1].Statement)
	assert.Len(t, res.TopSQL[2].Statement, sqlTextLimit+3, "a long statement is truncated")
	assert.Equal(t, "(statement text unavailable: sql:3:17)", res.TopSQL[3].Statement)
}

func TestReduceFocusReRootsAtTheSlowestMatch(t *testing.T) {
	b := &treeBuilder{}
	root := b.node("a.Main.run", 1000,
		b.node("a.Repo.load", 300, b.node("a.Jdbc.execute", 290)),
		b.node("a.Repo.load", 600, b.node("a.Jdbc.execute", 100), b.node("a.Cache.get", 450)))
	tree := b.roundTrip(t, root)

	res := reduce(tree, reduceOptions{focus: "Repo.load", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, "a.Repo.load", res.Focus)
	assert.Equal(t, int64(600), res.HotPath.TotalMs)
	assert.Equal(t, "a.Cache.get", res.TopSelf[0].Method, "top lists cover the focus subtree only")
	require.NotEmpty(t, res.Notes)
	assert.Contains(t, res.Notes[0], "2 nodes match \"Repo.load\"; showing the slowest, with 600 ms of 900 ms")

	res = reduce(tree, reduceOptions{focus: "Nope", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, "a.Main.run", res.Focus)
	assert.Contains(t, res.Notes[0], "no method contains")
}

// TestReduceDefaultOutputStaysSmall pins the output size of the defaults on
// a large tree (risk "output size" of the plan): a wrong default would flood
// a model's context.
func TestReduceDefaultOutputStaysSmall(t *testing.T) {
	b := &treeBuilder{}
	var build func(depth int, total int64) *calltree.Node
	build = func(depth int, total int64) *calltree.Node {
		if depth == 0 {
			return b.node(fmt.Sprintf("a.Leaf.m%d", total%7), total)
		}
		var kids []*calltree.Node
		for i := int64(0); i < 4; i++ {
			kids = append(kids, build(depth-1, total/5))
		}
		return b.node(fmt.Sprintf("a.Level%d.run", depth), total, kids...)
	}
	res := reduce(b.roundTrip(t, build(7, 1_000_000)), opts(DefaultThreshold, DefaultMaxNodes))
	count := 0
	var visit func(p *PathNode)
	visit = func(p *PathNode) {
		count++
		for _, ch := range p.Children {
			visit(ch)
		}
	}
	visit(res.HotPath)
	assert.LessOrEqual(t, count, DefaultMaxNodes)
	assert.LessOrEqual(t, len(res.TopSelf), topListSize)
}

func TestShortMethod(t *testing.T) {
	for full, want := range map[string]string{
		"void com.a.B.m(long) (B.java) [b.jar]":      "com.a.B.m(long)",
		"java.lang.String com.a.B.name() (B.java:1)": "com.a.B.name()",
		"com.example.Service.handle":                 "com.example.Service.handle",
		"com.a.B.<init>(java.util.Map, int)":         "com.a.B.<init>(java.util.Map, int)",
	} {
		assert.Equal(t, want, shortMethod(full), full)
	}
}

func TestExplainFetchesTheTreeAndTheRow(t *testing.T) {
	b := &treeBuilder{}
	root := b.node("void com.example.Billing.charge(long) (Billing.java) [billing.jar]", 2470, b.node("a.Jdbc.execute", 2000))
	body := calltree.Encode(&calltree.Tree{Methods: b.methods, Root: root})

	c, r := newReplay(t, nil, reply{status: http.StatusOK, body: body}, ok("calls.json"))
	ref := "prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error"
	res, err := c.Explain(context.Background(), ExplainInput{Ref: ref})
	require.NoError(t, err)

	require.Len(t, r.reqs, 2)
	assert.Equal(t, "/api/v1/calls/prod:billing:billing-7f8c:1760420000000:5:12340:0/tree", r.reqs[0].URL.Path)
	assert.Equal(t, "1760433000123", r.reqs[0].URL.Query().Get("ts_ms"))
	assert.Equal(t, "any_error", r.reqs[0].URL.Query().Get("retention_class"))
	q := r.reqs[1].URL.Query()
	assert.Equal(t, "prod/billing/billing-7f8c", q.Get("pod"))
	assert.Equal(t, "1760433000124", q.Get("to"), "a one-millisecond window")

	require.NotNil(t, res.Call)
	assert.Equal(t, ref, res.Call.Ref)
	assert.Equal(t, int64(1900), res.Call.WaitMs)
	assert.Equal(t, "com.example.Billing.charge(long)", res.Focus)
	assert.Empty(t, res.Notes)
}

func TestExplainRejectsBadInputBeforeAnyRequest(t *testing.T) {
	c, r := newReplay(t, nil)
	for _, in := range []ExplainInput{
		{Ref: "nope"},
		{Ref: "a:b:c:1:1:1:1@1@any_error", Threshold: 1.5},
		{Ref: "a:b:c:1:1:1:1@1@any_error", MaxNodes: MaxNodesCap + 1},
	} {
		_, err := c.Explain(context.Background(), in)
		assert.Error(t, err, "%+v", in)
	}
	assert.Empty(t, r.reqs)
}

func TestExplainReportsATruncatedCall(t *testing.T) {
	c, _ := newReplay(t, nil, reply{status: http.StatusNotFound, fixture: "problem_trace_unavailable.json"})
	_, err := c.Explain(context.Background(), ExplainInput{Ref: "a:b:c:1:1:1:1@1@any_error"})
	assert.True(t, IsCode(err, CodeTraceUnavailable))
	assert.ErrorContains(t, err, "truncated call")
}
