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

func mustReduce(t *testing.T, tree *calltree.Tree, o reduceOptions) ExplainResult {
	t.Helper()
	res, err := reduce(tree, o)
	require.NoError(t, err)
	return res
}

func focusNode(node int) reduceOptions {
	o := opts(DefaultThreshold, DefaultMaxNodes)
	o.node = &node
	return o
}

// shownNodes lists the hot path's nodes in preorder as "reference method".
func shownNodes(p *PathNode) []string {
	out := []string{fmt.Sprintf("%d %s", p.Node, p.Method)}
	for _, ch := range p.Children {
		out = append(out, shownNodes(ch)...)
	}
	return out
}

func TestReduceCollapsesADeepPassThroughChain(t *testing.T) {
	b := &treeBuilder{}
	leafs := []*calltree.Node{b.node("void a.Repo.load() (Repo.java) [a.jar]", 60), b.node("a.Http.call", 30), b.node("a.Log.write", 2)}
	d := b.node("a.Service.handle", 96, leafs...)
	c := b.node("a.Filter3.doFilter", 97, d)
	bb := b.node("a.Filter2.doFilter", 98, c)
	root := b.node("a.Servlet.service", 100, bb)

	res := mustReduce(t, b.roundTrip(t, root), opts(DefaultThreshold, DefaultMaxNodes))
	assert.Equal(t, "a.Servlet.service", res.Focus)
	path := res.HotPath
	assert.Equal(t, "a.Service.handle", path.Method, "three pass-through frames collapse")
	assert.Equal(t, 3, path.Skipped)
	assert.Zero(t, res.FocusNode)
	assert.Equal(t, 3, path.Node, "the landed node keeps its own reference, not the chain head's")
	require.Len(t, path.Children, 2)
	assert.Equal(t, "a.Repo.load()", path.Children[0].Method, "slowest first, method shortened")
	assert.Equal(t, 4, path.Children[0].Node)
	assert.Equal(t, 5, path.Children[1].Node)
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
	res := mustReduce(t, b.roundTrip(t, root), opts(DefaultThreshold, DefaultMaxNodes))
	assert.Equal(t, "a.Service.handle", res.HotPath.Method)
	assert.Zero(t, res.HotPath.Skipped)

	b = &treeBuilder{}
	root = b.node("a.Service.handle", 100, b.node("a.Repo.load", 95))
	root.Params = []calltree.Param{{ParamIdx: 0, Groups: []calltree.ParamGroup{{Value: "x", DurationMs: 100, Executions: 1}}}}
	res = mustReduce(t, b.roundTrip(t, root, "user.tenant"), opts(DefaultThreshold, DefaultMaxNodes))
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

	res := mustReduce(t, tree, opts(DefaultThreshold, DefaultMaxNodes))
	assert.Empty(t, res.HotPath.Children, "every item is 1% of the root")
	assert.Equal(t, 100, res.HotPath.Hidden)
	assert.Equal(t, int64(1000), res.HotPath.HiddenMs)

	res = mustReduce(t, tree, opts(0.001, DefaultMaxNodes))
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

	res := mustReduce(t, b.roundTrip(t, root), opts(0.01, 5))
	// step1 passes 390 of its 400 ms to one child, so it collapses into the
	// leaf; the budget then takes step2 before Fast's children. Each shown
	// node keeps its preorder reference: step1 is 2, other is 4, and Fast's
	// children, 7 and 8, are past the budget.
	assert.Equal(t, []string{"0 a.Main.run", "1 a.Slow.run", "3 a.Slow.leaf", "5 a.Slow.step2", "6 a.Fast.run"},
		shownNodes(res.HotPath))
	assert.Equal(t, 1, res.HotPath.Children[0].Children[0].Skipped)
	assert.Equal(t, 2, res.HotPath.Children[1].Hidden, "Fast's children are past the budget")
}

// TestReduceChargesTheBudgetWhenANodeIsShown pins the fix for a budget
// spent on a wide level: children used to take slots before the expansion
// order picked a branch, so 39 small siblings hid the expensive grandchildren.
func TestReduceChargesTheBudgetWhenANodeIsShown(t *testing.T) {
	b := &treeBuilder{}
	branch := b.node("a.Slow.run", 700, b.node("a.Slow.query", 400), b.node("a.Slow.render", 300))
	children := []*calltree.Node{branch}
	for i := 0; i < 39; i++ {
		children = append(children, b.node(fmt.Sprintf("a.Small.m%02d", i), 7))
	}
	root := b.node("a.Main.run", 1000, children...)

	res := mustReduce(t, b.roundTrip(t, root), opts(0.001, 40))
	path := res.HotPath
	require.NotEmpty(t, path.Children)
	slow := path.Children[0]
	assert.Equal(t, "a.Slow.run", slow.Method)
	require.Len(t, slow.Children, 2, "the 400 ms and 300 ms nodes take slots before any 7 ms sibling")
	assert.Equal(t, "a.Slow.query", slow.Children[0].Method)
	assert.Equal(t, "a.Slow.render", slow.Children[1].Method)
	assert.Len(t, path.Children, 1+36, "the root, the branch, and its two children leave 36 slots")
	assert.Equal(t, 3, path.Hidden)
	assert.Equal(t, int64(21), path.HiddenMs)
	require.Len(t, res.Notes, 1)
	assert.Contains(t, res.Notes[0], "the node budget of 40 is reached and 3 nodes are hidden")
}

// TestReduceKeepsASiblingAboveTheThreshold pins the fix for a collapse that
// dropped a side child the threshold would show: a 100 ms sibling of a
// 900 ms child is 10% of the root, so the root is not a pass-through.
func TestReduceKeepsASiblingAboveTheThreshold(t *testing.T) {
	b := &treeBuilder{}
	root := b.node("a.Main.run", 1000, b.node("a.Repo.load", 900), b.node("a.Http.call", 100))

	res := mustReduce(t, b.roundTrip(t, root), opts(DefaultThreshold, DefaultMaxNodes))
	path := res.HotPath
	assert.Equal(t, "a.Main.run", path.Method)
	assert.Zero(t, path.Skipped)
	require.Len(t, path.Children, 2)
	assert.Equal(t, "a.Repo.load", path.Children[0].Method)
	assert.Equal(t, "a.Http.call", path.Children[1].Method)
	assert.Zero(t, path.Hidden)
	assert.Empty(t, res.Notes)

	// A side child under the threshold still collapses, and the landed node
	// reports it as hidden.
	b = &treeBuilder{}
	root = b.node("a.Main.run", 1000, b.node("a.Repo.load", 960), b.node("a.Http.call", 40))
	res = mustReduce(t, b.roundTrip(t, root), opts(DefaultThreshold, DefaultMaxNodes))
	path = res.HotPath
	assert.Equal(t, "a.Repo.load", path.Method)
	assert.Equal(t, 1, path.Skipped)
	assert.Equal(t, 1, path.Hidden)
	assert.Equal(t, int64(40), path.HiddenMs)
	require.Len(t, res.Notes, 1)
	assert.Contains(t, res.Notes[0], "1 nodes under 5% of 1000 ms are hidden")

	// A lower threshold shows that child again.
	res = mustReduce(t, b.roundTrip(t, root), opts(0.01, DefaultMaxNodes))
	assert.Equal(t, "a.Main.run", res.HotPath.Method)
	assert.Len(t, res.HotPath.Children, 2)
}

func sqlNode(b *treeBuilder, method string, groups ...calltree.ParamGroup) *calltree.Node {
	var total int64
	for _, g := range groups {
		total += g.DurationMs
	}
	n := b.node(method, total)
	n.Params = []calltree.Param{{ParamIdx: 0, Groups: groups}}
	return n
}

// TestReduceGroupsSQLBySignature pins the fix for keying top SQL by the
// truncated text: statements sharing a long prefix stay apart, and literal
// variants that two nodes keep as different values fold together.
func TestReduceGroupsSQLBySignature(t *testing.T) {
	b := &treeBuilder{}
	cols := "select " + strings.Repeat("customer_name, ", 25) + "id"
	require.Greater(t, len(cols), sqlTextLimit)
	root := b.node("a.Service.handle", 1000,
		sqlNode(b, "a.Jdbc.orders", calltree.ParamGroup{Value: cols + " from orders", DurationMs: 60, Executions: 1}),
		sqlNode(b, "a.Jdbc.payments", calltree.ParamGroup{Value: cols + " from payments", DurationMs: 40, Executions: 1}),
		sqlNode(b, "a.Jdbc.first", calltree.ParamGroup{Value: "select * from t where id = 1", DurationMs: 30, Executions: 3}),
		sqlNode(b, "a.Jdbc.second", calltree.ParamGroup{Value: "select * from t where id = 22", DurationMs: 50, Executions: 5}),
		sqlNode(b, "a.Jdbc.third",
			calltree.ParamGroup{Value: "sql:3:17", DurationMs: 8, Executions: 1, Unresolved: true},
			calltree.ParamGroup{Value: "sql:3:18", DurationMs: 7, Executions: 1, Unresolved: true}),
	)

	res := mustReduce(t, b.roundTrip(t, root, "sql"), opts(DefaultThreshold, DefaultMaxNodes))
	require.Len(t, res.TopSQL, 5)
	assert.Equal(t, SQLStat{Statement: "select * from t where id = 22", DurationMs: 80, Executions: 8}, res.TopSQL[0],
		"literal variants from two nodes fold, shown by the slower one's text")
	assert.Equal(t, int64(60), res.TopSQL[1].DurationMs, "a shared prefix does not merge orders into payments")
	assert.Equal(t, int64(40), res.TopSQL[2].DurationMs)
	assert.Len(t, res.TopSQL[1].Statement, sqlTextLimit+3, "only the shown text is truncated")
	assert.Equal(t, "(statement text unavailable: sql:3:17)", res.TopSQL[3].Statement)
	assert.Equal(t, "(statement text unavailable: sql:3:18)", res.TopSQL[4].Statement,
		"two unresolved references stay apart")
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

	res := mustReduce(t, b.roundTrip(t, root, "sql"), opts(DefaultThreshold, DefaultMaxNodes))
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

	res := mustReduce(t, tree, reduceOptions{focus: "Repo.load", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, "a.Repo.load", res.Focus)
	assert.Equal(t, int64(600), res.HotPath.TotalMs)
	assert.Equal(t, "a.Cache.get", res.TopSelf[0].Method, "top lists cover the focus subtree only")
	require.NotEmpty(t, res.Notes)
	assert.Contains(t, res.Notes[0], "2 nodes match \"Repo.load\"; showing the slowest, node 3, with 600 ms of 900 ms")
	assert.Contains(t, res.Notes[0], "focus on its node: 1 (300 ms)")
	assert.Equal(t, 3, res.FocusNode)

	res = mustReduce(t, tree, reduceOptions{focus: "Nope", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, "a.Main.run", res.Focus)
	assert.Zero(t, res.FocusNode)
	assert.Contains(t, res.Notes[0], "no method contains")
}

// repeatedMethods is the UI's localHotspots fixture (transforms.test.ts),
// with the second Query.run made slower: the same method runs under two
// parents, and Util.recurse calls itself. The comments give each node's
// preorder reference.
func repeatedMethods(t *testing.T) *calltree.Tree {
	b := &treeBuilder{}
	root := b.node("void e.Entry.handle() (Entry.java:1) [app.jar]", 1000, // 0
		b.node("s.ServiceA.doWork", 500, // 1
			loop(b.node("db.Query.run", 250), 30), // 2
			b.node("u.Util.recurse", 150, // 3
				b.node("u.Util.recurse", 50))), // 4
		b.node("s.ServiceB.other", 400, // 5
			b.node("db.Query.run", 350, // 6
				b.node("db.Jdbc.fetch", 200)))) // 7
	return b.roundTrip(t, root)
}

func TestReduceNumbersNodesInWirePreorder(t *testing.T) {
	res := mustReduce(t, repeatedMethods(t), opts(0.001, DefaultMaxNodes))
	// No node is a pass-through and every node is above the threshold, so
	// the hot path shows the whole tree, slowest sibling first.
	assert.ElementsMatch(t, []string{
		"0 e.Entry.handle()", "1 s.ServiceA.doWork", "2 db.Query.run", "3 u.Util.recurse",
		"4 u.Util.recurse", "5 s.ServiceB.other", "6 db.Query.run", "7 db.Jdbc.fetch",
	}, shownNodes(res.HotPath))
}

func TestReduceFocusNodePicksOneOfARepeatedMethod(t *testing.T) {
	tree := repeatedMethods(t)

	res := mustReduce(t, tree, focusNode(2))
	assert.Equal(t, "db.Query.run", res.Focus)
	assert.Equal(t, 2, res.FocusNode)
	assert.Equal(t, 2, res.HotPath.Node)
	assert.Equal(t, int64(250), res.HotPath.TotalMs, "the node under ServiceA, not the slower one under ServiceB")
	assert.Equal(t, int64(30), res.HotPath.Executions)
	assert.Empty(t, res.HotPath.Children)
	assert.Equal(t, []MethodStat{{Method: "db.Query.run", SelfMs: 250, Executions: 30}}, res.TopSelf,
		"top lists cover the selected node only, not every node of the method")
	assert.Empty(t, res.Notes)

	res = mustReduce(t, tree, focusNode(6))
	assert.Equal(t, 6, res.FocusNode)
	assert.Equal(t, int64(350), res.HotPath.TotalMs)
	require.Len(t, res.HotPath.Children, 1)
	assert.Equal(t, 7, res.HotPath.Children[0].Node)

	// The text focus takes the slowest match and names the other by node.
	res = mustReduce(t, tree, reduceOptions{focus: "Query.run", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, 6, res.FocusNode)
	require.Len(t, res.Notes, 1)
	assert.Equal(t, "2 nodes match \"Query.run\"; showing the slowest, node 6, with 350 ms of 600 ms over all matches. "+
		"To explain another, focus on its node: 2 (250 ms)", res.Notes[0])
}

func TestReduceFocusNodePicksTheInnerRecursion(t *testing.T) {
	tree := repeatedMethods(t)

	res := mustReduce(t, tree, focusNode(4))
	assert.Equal(t, "u.Util.recurse", res.Focus)
	assert.Equal(t, 4, res.FocusNode)
	assert.Equal(t, int64(50), res.HotPath.TotalMs)
	assert.Equal(t, []MethodStat{{Method: "u.Util.recurse", SelfMs: 50, Executions: 1}}, res.TopSelf,
		"the inner occurrence keeps only its own time")

	res = mustReduce(t, tree, focusNode(3))
	assert.Equal(t, []MethodStat{{Method: "u.Util.recurse", SelfMs: 150, Executions: 2}}, res.TopSelf,
		"the outer occurrence folds the recursion under it")

	res = mustReduce(t, tree, reduceOptions{focus: "Util.recurse", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, 3, res.FocusNode, "the text focus takes the outer, slower occurrence")
	require.Len(t, res.Notes, 1)
	assert.Contains(t, res.Notes[0], "focus on its node: 4 (50 ms)")
}

func TestReduceFocusNodeOnACollapsedChain(t *testing.T) {
	b := &treeBuilder{}
	root := b.node("a.Main.run", 1000, // 0
		b.node("a.Filter.doFilter", 990, // 1
			b.node("a.Service.handle", 985, // 2
				b.node("a.Repo.load", 600), b.node("a.Http.call", 385)))) // 3, 4

	res := mustReduce(t, b.roundTrip(t, root), focusNode(1))
	assert.Equal(t, 1, res.FocusNode, "the result names the node asked for")
	assert.Equal(t, "a.Filter.doFilter", res.Focus)
	assert.Equal(t, 2, res.HotPath.Node, "the shown node is the one the chain lands on")
	assert.Equal(t, 1, res.HotPath.Skipped)
	assert.Equal(t, []string{"2 a.Service.handle", "3 a.Repo.load", "4 a.Http.call"}, shownNodes(res.HotPath))
}

func TestReduceListsAtMostFiveOtherMatches(t *testing.T) {
	b := &treeBuilder{}
	var children []*calltree.Node
	for i := 0; i < 8; i++ {
		children = append(children, b.node("a.Repo.load", int64(100+i)))
	}
	res := mustReduce(t, b.roundTrip(t, b.node("a.Main.run", 1000, children...)),
		reduceOptions{focus: "Repo.load", threshold: DefaultThreshold, maxNodes: DefaultMaxNodes})
	assert.Equal(t, 8, res.FocusNode)
	require.NotEmpty(t, res.Notes)
	assert.Contains(t, res.Notes[0], "focus on its node: 7 (106 ms), 6 (105 ms), 5 (104 ms), 4 (103 ms), 3 (102 ms), and 2 more")
}

func TestReduceRejectsAFocusNodeOutsideTheTree(t *testing.T) {
	_, err := reduce(repeatedMethods(t), focusNode(8))
	var notFound *NodeNotFoundError
	require.ErrorAs(t, err, &notFound)
	assert.Equal(t, NodeNotFoundError{Node: 8, Count: 8}, *notFound)
	assert.EqualError(t, err, "the call tree has no node 8: its 8 nodes are numbered 0 to 7; "+
		"take a node reference from an earlier explain result of the same call")
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
	res := mustReduce(t, b.roundTrip(t, build(7, 1_000_000)), opts(DefaultThreshold, DefaultMaxNodes))
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
		assert.Equal(t, want, ShortMethod(full), full)
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
	negative, zero := -1, 0
	for _, in := range []ExplainInput{
		{Ref: "nope"},
		{Ref: "a:b:c:1:1:1:1@1@any_error", Threshold: 1.5},
		{Ref: "a:b:c:1:1:1:1@1@any_error", MaxNodes: MaxNodesCap + 1},
		{Ref: "a:b:c:1:1:1:1@1@any_error", FocusNode: &negative},
		{Ref: "a:b:c:1:1:1:1@1@any_error", FocusNode: &zero, Focus: "Repo.load"},
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

func TestExplainRejectsAFocusNodeOutsideTheTree(t *testing.T) {
	b := &treeBuilder{}
	root := b.node("a.Main.run", 10, b.node("a.Repo.load", 5))
	body := calltree.Encode(&calltree.Tree{Methods: b.methods, Root: root})
	c, r := newReplay(t, nil, reply{status: http.StatusOK, body: body})
	node := 2
	_, err := c.Explain(context.Background(), ExplainInput{Ref: "a:b:c:1:1:1:1@1@any_error", FocusNode: &node})
	var notFound *NodeNotFoundError
	require.ErrorAs(t, err, &notFound)
	assert.Equal(t, 2, notFound.Count)
	assert.Len(t, r.reqs, 1, "the call row is not fetched for a rejected node")
}

// TestExplainFollowsTheCursorToTheRow puts the call's row on the third page,
// behind a page of other calls and an empty page, as a busy millisecond and
// the contract's mid-listing empty pages allow.
func TestExplainFollowsTheCursorToTheRow(t *testing.T) {
	b := &treeBuilder{}
	root := b.node("void com.example.Billing.charge(long) (Billing.java) [billing.jar]", 2470)
	tree := calltree.Encode(&calltree.Tree{Methods: b.methods, Root: root})
	row := func(offset int) string {
		return fmt.Sprintf(`{"pk": {"pod_namespace": "prod", "pod_service": "billing", "pod_name": "billing-7f8c",
			"restart_time_ms": 1760420000000, "trace_file_index": 5, "buffer_offset": %d, "record_index": 0},
			"ts_ms": 1760433000123, "duration_ms": 2470, "method": "m", "cpu_time_ms": 310, "wait_time_ms": 1900,
			"retention_class": "any_error", "params": {}}`, offset)
	}
	page := func(rows, cursor string) reply {
		return reply{status: http.StatusOK, body: []byte(`{"calls": [` + rows + `], "next_cursor": ` + cursor + `}`)}
	}
	ref := "prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error"

	c, r := newReplay(t, nil, reply{status: http.StatusOK, body: tree},
		page(row(1)+","+row(2), `"c2"`), page("", `"c3"`), page(row(12340), "null"))
	res, err := c.Explain(context.Background(), ExplainInput{Ref: ref})
	require.NoError(t, err)
	require.Len(t, r.reqs, 4)
	assert.Equal(t, "c2", r.reqs[2].URL.Query().Get("cursor"))
	assert.Empty(t, r.reqs[2].URL.Query().Get("from"), "a cursor page re-sends only the cursor")
	assert.Equal(t, "c3", r.reqs[3].URL.Query().Get("cursor"))
	require.NotNil(t, res.Call)
	assert.Equal(t, int64(1900), res.Call.WaitMs)
	assert.Empty(t, res.Notes)

	// A server that hands back a cursor it already gave ends the search.
	c, r = newReplay(t, nil, reply{status: http.StatusOK, body: tree},
		page(row(1), `"c2"`), page("", `"c2"`))
	res, err = c.Explain(context.Background(), ExplainInput{Ref: ref})
	require.NoError(t, err)
	require.Len(t, r.reqs, 3)
	assert.Nil(t, res.Call)
	assert.Contains(t, res.Notes, "the call's row was not found, so CPU and wait times are missing")
}
