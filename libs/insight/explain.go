package insight

import (
	"container/heap"
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/pkg/errors"
)

// Explain defaults and caps. Every result states when a cap cut it and which
// input widens it, so a wrong default cannot silently hide the answer.
const (
	DefaultThreshold = 0.05
	DefaultMaxNodes  = 40
	MaxNodesCap      = 200
	topListSize      = 10
	// focusNoteMatches caps the other matches a text focus lists by node.
	focusNoteMatches = 5
	sqlTextLimit     = 300
	// passThroughShare is the 08-ui-backend-requirements §5 rule: a node
	// whose time is at most 10% unexplained by its dominant child collapses
	// into that child.
	passThroughShare = 0.10
)

// ExplainInput selects the call and how much of its tree to show.
type ExplainInput struct {
	// Ref is a call reference from the calls operation.
	Ref string
	// Focus re-roots the reduction at the slowest node whose method contains
	// this text. Empty means the root. A method that runs at several places
	// in the tree matches several nodes; FocusNode picks one of them.
	Focus string
	// FocusNode re-roots the reduction at exactly this node; nil means none.
	// A node reference is the node's preorder index in the call tree as
	// /calls/{pk}/tree serves it: the root is 0 and children count in wire
	// order, before any sorting. The UI numbers its TreeNode.id the same way.
	// Every node of an explain result carries its reference. FocusNode and
	// Focus exclude each other.
	FocusNode *int
	// Threshold drops children below this share of the (focus) root's
	// duration; zero means DefaultThreshold.
	Threshold float64
	// MaxNodes caps the hot path; zero means DefaultMaxNodes.
	MaxNodes int
}

// ExplainResult is a call reduced to what a model can read.
type ExplainResult struct {
	// Call is the call's row from /calls; nil when the row was not found,
	// which leaves the tree without CPU and wait figures.
	Call *Call `json:"call,omitempty"`
	// Focus is the method the reduction is rooted at.
	Focus string `json:"focus"`
	// FocusNode is the node reference of the node the reduction is rooted
	// at; 0 is the call's root.
	FocusNode int `json:"focus_node"`
	// HotPath is the reduced tree under Focus.
	HotPath *PathNode `json:"hot_path"`
	// TopSelf ranks methods by self time under Focus.
	TopSelf []MethodStat `json:"top_self"`
	// TopSQL ranks SQL statements by time under Focus.
	TopSQL []SQLStat `json:"top_sql"`
	// Notes say what was cut and which input shows more.
	Notes []string `json:"notes,omitempty"`
}

// PathNode is one node of the reduced tree. Times are in milliseconds.
type PathNode struct {
	// Node is the node's reference (see ExplainInput.FocusNode). A node a
	// pass-through chain collapsed into keeps its own reference.
	Node      int    `json:"node"`
	Method    string `json:"method"`
	TotalMs   int64  `json:"total_ms"`
	SelfMs    int64  `json:"self_ms"`
	SuspendMs int64  `json:"suspend_ms,omitempty"`
	// Executions is how often the method ran at this place in the tree.
	Executions int64 `json:"executions"`
	// Skipped counts the pass-through frames collapsed above this node.
	Skipped int `json:"skipped,omitempty"`
	// Hidden counts the children cut by the threshold or the node budget,
	// and HiddenMs their total time. When frames collapsed into this node,
	// their side children, all below the threshold, count here too.
	Hidden   int         `json:"hidden,omitempty"`
	HiddenMs int64       `json:"hidden_ms,omitempty"`
	Children []*PathNode `json:"children,omitempty"`
}

// MethodStat is one method's time summed over its nodes.
type MethodStat struct {
	Method     string `json:"method"`
	SelfMs     int64  `json:"self_ms"`
	Executions int64  `json:"executions"`
	SuspendMs  int64  `json:"suspend_ms,omitempty"`
}

// SQLStat is one SQL statement group, summed over the nodes that ran it.
type SQLStat struct {
	Statement  string `json:"statement"`
	DurationMs int64  `json:"duration_ms"`
	Executions int64  `json:"executions"`
}

// Explain fetches a call's tree and reduces it: a hot path with pass-through
// chains collapsed and small children cut, the methods with the most self
// time, and the slowest SQL.
func (c *Client) Explain(ctx context.Context, in ExplainInput) (ExplainResult, error) {
	ref, err := ParseCallRef(in.Ref)
	if err != nil {
		return ExplainResult{}, err
	}
	opts, err := in.reduceOptions()
	if err != nil {
		return ExplainResult{}, err
	}
	if err := c.authorize(ctx, []string{ref.PK.PodNamespace}); err != nil {
		return ExplainResult{}, err
	}
	hints := url.Values{
		"ts_ms":           {strconv.FormatInt(ref.TsMs, 10)},
		"retention_class": {ref.RetentionClass},
	}
	body, err := c.getBytes(ctx, "/calls/"+url.PathEscape(ref.PK.PathString())+"/tree", hints)
	if err != nil {
		return ExplainResult{}, err
	}
	tree, _, err := calltree.Decode(body)
	if err != nil {
		return ExplainResult{}, errors.Wrap(err, "decode the call tree")
	}
	res, err := reduce(tree, opts)
	if err != nil {
		return ExplainResult{}, err
	}
	call, err := c.callRow(ctx, ref)
	if err != nil {
		return ExplainResult{}, err
	}
	if call == nil {
		res.Notes = append(res.Notes, "the call's row was not found, so CPU and wait times are missing")
	}
	res.Call = call
	return res, nil
}

// callRow finds the call's /calls row: one millisecond of one pod, which no
// guard rejects and every tier answers cheaply. More than a page of calls can
// share that millisecond, and a page can be empty mid-listing (02 §2.3.1), so
// it follows next_cursor until the row turns up or the listing ends.
func (c *Client) callRow(ctx context.Context, ref CallRef) (*Call, error) {
	pk := ref.PK
	q := url.Values{
		"from":  {strconv.FormatInt(ref.TsMs, 10)},
		"to":    {strconv.FormatInt(ref.TsMs+1, 10)},
		"pod":   {pk.PodNamespace + "/" + pk.PodService + "/" + pk.PodName},
		"limit": {strconv.Itoa(MaxCallsLimit)},
	}
	seen := map[string]bool{}
	for {
		var resp callsResponse
		if err := c.getJSON(ctx, "/calls", q, &resp); err != nil {
			return nil, err
		}
		for _, row := range resp.Calls {
			if row.PK == pk {
				call := toCall(row)
				return &call, nil
			}
		}
		// A cursor the server already returned would loop forever.
		if resp.NextCursor == nil || seen[*resp.NextCursor] {
			return nil, nil
		}
		seen[*resp.NextCursor] = true
		q = url.Values{"cursor": {*resp.NextCursor}, "limit": {strconv.Itoa(MaxCallsLimit)}}
	}
}

// NodeNotFoundError rejects a focus node the call tree does not have.
type NodeNotFoundError struct {
	// Node is the requested node reference.
	Node int
	// Count is the number of nodes in the tree; references run from 0 to
	// Count-1.
	Count int
}

// Error names the valid range and where a reference comes from.
func (e *NodeNotFoundError) Error() string {
	return fmt.Sprintf("the call tree has no node %d: its %d nodes are numbered 0 to %d; "+
		"take a node reference from an earlier explain result of the same call", e.Node, e.Count, e.Count-1)
}

type reduceOptions struct {
	focus     string
	node      *int
	threshold float64
	maxNodes  int
}

func (in ExplainInput) reduceOptions() (reduceOptions, error) {
	o := reduceOptions{focus: strings.TrimSpace(in.Focus), threshold: in.Threshold, maxNodes: in.MaxNodes}
	if in.FocusNode != nil {
		if o.focus != "" {
			return o, errors.Errorf("set a focus method or a focus node, not both: the node already names one place in the tree")
		}
		if *in.FocusNode < 0 {
			return o, errors.Errorf("focus node must be 0 or greater: the call's root is node 0")
		}
		node := *in.FocusNode
		o.node = &node
	}
	if o.threshold == 0 {
		o.threshold = DefaultThreshold
	}
	if o.threshold < 0 || o.threshold >= 1 {
		return o, errors.Errorf("threshold must be a share between 0 and 1, such as 0.05")
	}
	if o.maxNodes == 0 {
		o.maxNodes = DefaultMaxNodes
	}
	if o.maxNodes < 1 || o.maxNodes > MaxNodesCap {
		return o, errors.Errorf("max nodes must be between 1 and %d", MaxNodesCap)
	}
	return o, nil
}

// reduce is the presentation of a tree for a model. It is pure, so tests run
// it on trees built in memory. It fails only on a focus node the tree does
// not have.
func reduce(tree *calltree.Tree, o reduceOptions) (ExplainResult, error) {
	method := func(n *calltree.Node) string {
		if n.MethodIdx >= 0 && n.MethodIdx < len(tree.Methods) {
			return shortMethod(tree.Methods[n.MethodIdx])
		}
		return "#" + strconv.Itoa(n.MethodIdx)
	}
	var res ExplainResult
	// walk visits in preorder and Decode keeps the wire order of children,
	// so a node's index here is its node reference.
	nodes := findNodes(tree.Root, func(*calltree.Node) bool { return true })
	ids := make(map[*calltree.Node]int, len(nodes))
	for i, n := range nodes {
		ids[n] = i
	}

	root := tree.Root
	switch {
	case o.node != nil:
		if *o.node >= len(nodes) {
			return ExplainResult{}, &NodeNotFoundError{Node: *o.node, Count: len(nodes)}
		}
		root = nodes[*o.node]
	case o.focus != "":
		matches := findNodes(tree.Root, func(n *calltree.Node) bool {
			return n.MethodIdx >= 0 && n.MethodIdx < len(tree.Methods) && strings.Contains(tree.Methods[n.MethodIdx], o.focus)
		})
		if len(matches) == 0 {
			res.Notes = append(res.Notes, fmt.Sprintf("no method contains %q; showing the whole call", o.focus))
		} else {
			sort.SliceStable(matches, func(i, j int) bool { return matches[i].DurationMs > matches[j].DurationMs })
			root = matches[0]
			if len(matches) > 1 {
				res.Notes = append(res.Notes, focusMatchesNote(o.focus, matches, ids))
			}
		}
	}
	res.Focus = method(root)
	res.FocusNode = ids[root]

	res.HotPath = hotPath(root, method, ids, o, &res.Notes)
	res.TopSelf = topSelf(root, method)
	res.TopSQL = topSQL(root, tree.Params)
	return res, nil
}

// focusMatchesNote says which of several matches a text focus picked and
// names the others by node reference, slowest first, so the caller can
// focus on another one by node.
func focusMatchesNote(focus string, matches []*calltree.Node, ids map[*calltree.Node]int) string {
	others := make([]string, 0, focusNoteMatches)
	for _, n := range matches[1:] {
		if len(others) == focusNoteMatches {
			break
		}
		others = append(others, fmt.Sprintf("%d (%d ms)", ids[n], n.DurationMs))
	}
	list := strings.Join(others, ", ")
	if rest := len(matches) - 1 - len(others); rest > 0 {
		list += fmt.Sprintf(", and %d more", rest)
	}
	return fmt.Sprintf("%d nodes match %q; showing the slowest, node %d, with %d ms of %d ms over all matches. "+
		"To explain another, focus on its node: %s",
		len(matches), focus, ids[matches[0]], matches[0].DurationMs, sumDuration(matches), list)
}

// passThrough returns the child a node collapses into, or nil: one child
// carries at least 90% of the node's time with the same execution count, the
// node has no params of its own (08 §5), and every other child is below
// minMs. The last rule keeps a branching node whose side child the threshold
// would show: collapsing it would drop that child where no threshold or
// budget can bring it back.
func passThrough(n *calltree.Node, minMs int64) *calltree.Node {
	if len(n.Params) > 0 || len(n.Children) == 0 || n.DurationMs <= 0 {
		return nil
	}
	dominant := n.Children[0]
	for _, ch := range n.Children[1:] {
		if ch.DurationMs > dominant.DurationMs {
			dominant = ch
		}
	}
	// Executions counts the whole subtree (02 §2.5.3); the per-node count is
	// what tells a pass-through from a loop.
	if dominant.SelfExecutions != n.SelfExecutions {
		return nil
	}
	if float64(n.DurationMs-dominant.DurationMs) > passThroughShare*float64(n.DurationMs) {
		return nil
	}
	for _, ch := range n.Children {
		if ch != dominant && !belowThreshold(ch, minMs) {
			return nil
		}
	}
	return dominant
}

// belowThreshold reports whether the hot path hides n as too small to show.
func belowThreshold(n *calltree.Node, minMs int64) bool {
	return n.DurationMs < minMs || n.DurationMs == 0
}

// collapsed is where a pass-through chain lands: the node shown, the count of
// frames skipped above it, and the side children of those frames, which are
// all below the threshold and are reported as hidden on the landed node.
type collapsed struct {
	node      *calltree.Node
	skipped   int
	dropped   int
	droppedMs int64
}

// collapse follows pass-through chains from n.
func collapse(n *calltree.Node, minMs int64) collapsed {
	c := collapsed{node: n}
	for next := passThrough(c.node, minMs); next != nil; next = passThrough(c.node, minMs) {
		for _, ch := range c.node.Children {
			if ch != next {
				c.dropped++
				c.droppedMs += ch.DurationMs
			}
		}
		c.node, c.skipped = next, c.skipped+1
	}
	return c
}

// pathItem is a frontier entry of the hot-path expansion: a child not yet
// shown, the shown node it hangs under, and its place in push order.
type pathItem struct {
	child  *calltree.Node
	parent *PathNode
	seq    int
}

// pathQueue pops the slowest child first; push order breaks ties, so equal
// siblings keep the tree's order.
type pathQueue []pathItem

func (q pathQueue) Len() int { return len(q) }
func (q pathQueue) Less(i, j int) bool {
	if q[i].child.DurationMs != q[j].child.DurationMs {
		return q[i].child.DurationMs > q[j].child.DurationMs
	}
	return q[i].seq < q[j].seq
}
func (q pathQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *pathQueue) Push(x any)   { *q = append(*q, x.(pathItem)) }
func (q *pathQueue) Pop() any     { old := *q; it := old[len(old)-1]; *q = old[:len(old)-1]; return it }

// hotPath expands the tree most-expensive-first. A node takes a budget slot
// only when it leaves the frontier, so the budget goes to the slowest nodes
// at any depth rather than to the first wide level, and every shown node's
// ancestors are shown before it. Every child of a shown node ends up shown or
// counted in its parent's Hidden and HiddenMs.
func hotPath(root *calltree.Node, method func(*calltree.Node) string, ids map[*calltree.Node]int, o reduceOptions, notes *[]string) *PathNode {
	minMs := int64(o.threshold * float64(root.DurationMs))
	queue := &pathQueue{}
	seq, thresholdCut, budgetCut := 0, 0, 0
	// show adds the node a chain from n lands on and queues its children.
	show := func(n *calltree.Node) *PathNode {
		c := collapse(n, minMs)
		p := &PathNode{
			Node: ids[c.node], Method: method(c.node), TotalMs: c.node.DurationMs, SelfMs: c.node.SelfDurationMs,
			SuspendMs: c.node.SuspensionMs, Executions: c.node.SelfExecutions, Skipped: c.skipped,
			Hidden: c.dropped, HiddenMs: c.droppedMs,
		}
		thresholdCut += c.dropped
		for _, ch := range c.node.Children {
			if belowThreshold(ch, minMs) {
				p.Hidden++
				p.HiddenMs += ch.DurationMs
				thresholdCut++
				continue
			}
			heap.Push(queue, pathItem{child: ch, parent: p, seq: seq})
			seq++
		}
		return p
	}
	out := show(root)
	shown := 1
	for queue.Len() > 0 {
		it := heap.Pop(queue).(pathItem)
		if shown >= o.maxNodes {
			it.parent.Hidden++
			it.parent.HiddenMs += it.child.DurationMs
			budgetCut++
			continue
		}
		it.parent.Children = append(it.parent.Children, show(it.child))
		shown++
	}
	if thresholdCut > 0 {
		*notes = append(*notes, fmt.Sprintf("%d nodes under %.0f%% of %d ms are hidden; a lower threshold shows them",
			thresholdCut, o.threshold*100, root.DurationMs))
	}
	if budgetCut > 0 {
		*notes = append(*notes, fmt.Sprintf("the node budget of %d is reached and %d nodes are hidden; a larger budget or a focus node deeper in the tree shows more",
			o.maxNodes, budgetCut))
	}
	return out
}

func topSelf(root *calltree.Node, method func(*calltree.Node) string) []MethodStat {
	byMethod := map[string]*MethodStat{}
	walk(root, func(n *calltree.Node) {
		name := method(n)
		s := byMethod[name]
		if s == nil {
			s = &MethodStat{Method: name}
			byMethod[name] = s
		}
		s.SelfMs += n.SelfDurationMs
		s.Executions += n.SelfExecutions
		s.SuspendMs += n.SelfSuspensionMs
	})
	out := make([]MethodStat, 0, len(byMethod))
	for _, s := range byMethod {
		if s.SelfMs > 0 {
			out = append(out, *s)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SelfMs != out[j].SelfMs {
			return out[i].SelfMs > out[j].SelfMs
		}
		return out[i].Method < out[j].Method
	})
	if len(out) > topListSize {
		out = out[:topListSize]
	}
	return out
}

// topSQL sums the groups of every `sql` param under root. Each node already
// folded its statements by calltree.SQLSignature, but it keeps the first
// value it saw, so two nodes can hold the same statement with different
// literals; topSQL keys by the same signature to fold them again (as the
// UI's params-summary.ts does). The ::other bucket and each unresolved
// reference keep identities of their own. Only the emitted text is
// truncated, so a shared long prefix cannot merge two statements.
func topSQL(root *calltree.Node, params []string) []SQLStat {
	type group struct {
		stat SQLStat
		// slowestMs picks the representative text: the value of the
		// slowest group folded in.
		slowestMs int64
	}
	byKey := map[string]*group{}
	var order []string
	walk(root, func(n *calltree.Node) {
		for _, p := range n.Params {
			if p.ParamIdx < 0 || p.ParamIdx >= len(params) || params[p.ParamIdx] != "sql" {
				continue
			}
			for _, g := range p.Groups {
				var key, text string
				switch {
				case g.Value == calltree.OtherGroupValue:
					key, text = "other", "(other statements)"
				case g.Unresolved:
					key, text = "unresolved:"+g.Value, "(statement text unavailable: "+g.Value+")"
				default:
					key, text = "sql:"+calltree.SQLSignature(g.Value), strings.Join(strings.Fields(g.Value), " ")
				}
				s := byKey[key]
				if s == nil {
					s = &group{stat: SQLStat{Statement: text}, slowestMs: g.DurationMs}
					byKey[key] = s
					order = append(order, key)
				} else if g.DurationMs > s.slowestMs {
					s.stat.Statement, s.slowestMs = text, g.DurationMs
				}
				s.stat.DurationMs += g.DurationMs
				s.stat.Executions += g.Executions
			}
		}
	})
	out := make([]SQLStat, 0, len(byKey))
	for _, key := range order {
		s := byKey[key].stat
		s.Statement = truncate(s.Statement, sqlTextLimit)
		out = append(out, s)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].DurationMs != out[j].DurationMs {
			return out[i].DurationMs > out[j].DurationMs
		}
		return out[i].Statement < out[j].Statement
	})
	if len(out) > topListSize {
		out = out[:topListSize]
	}
	return out
}

// shortMethod drops the source file, the jar, and the return type the agent
// appends: "void com.a.B.m(long) (B.java) [b.jar]" becomes "com.a.B.m(long)".
func shortMethod(full string) string {
	s := full
	if i := strings.Index(s, ") ("); i >= 0 {
		s = s[:i+1]
	}
	open := strings.IndexByte(s, '(')
	head := s
	if open >= 0 {
		head = s[:open]
	}
	if sp := strings.LastIndexByte(head, ' '); sp >= 0 {
		s = s[sp+1:]
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func walk(n *calltree.Node, visit func(*calltree.Node)) {
	visit(n)
	for _, ch := range n.Children {
		walk(ch, visit)
	}
}

func findNodes(n *calltree.Node, match func(*calltree.Node) bool) []*calltree.Node {
	var out []*calltree.Node
	walk(n, func(x *calltree.Node) {
		if match(x) {
			out = append(out, x)
		}
	})
	return out
}

func sumDuration(ns []*calltree.Node) int64 {
	var sum int64
	for _, n := range ns {
		sum += n.DurationMs
	}
	return sum
}
