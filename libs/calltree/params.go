package calltree

// Param aggregation (02-read-contract.md §2.5.3, 08-ui-backend-requirements.md
// R11), ported from the Java parsers/ Hotspot + TreeBuilderTrace semantics: a
// node's values fold into groups keyed by the normalised SQL signature for
// SQL-shaped params and by the exact value otherwise; each invocation adds
// its total duration to every distinct group its values fall into; a
// container holds at most maxGroups groups, evicting the smallest into the
// param's ::other bucket; binds nest under their invocation's SQL group.

import (
	"container/heap"
	"regexp"
	"sort"
	"strings"
)

// DefaultMaxParamGroups is the contract's per-container group cap — the Java
// Hotspot.MAX_PARAMS default.
const DefaultMaxParamGroups = 256

// OtherGroupValue marks the overflow bucket (02 §2.5.3).
const OtherGroupValue = "::other"

type (
	// container aggregates the groups of one scope — a node's top-level
	// params, or one group's nested params — under the shared cap.
	container struct {
		params []*paramAgg
		byIdx  map[int]*paramAgg
		live   int       // groups counting against the cap (::other exempt)
		minima groupHeap // lazy min-heap by durationMs; entries go stale as groups grow
	}

	paramAgg struct {
		paramIdx int
		byKey    map[string]*groupAgg
		order    []*groupAgg
		other    *groupAgg
	}

	groupAgg struct {
		param      *paramAgg
		value      string
		durationMs int64
		executions int64
		unresolved bool
		nested     *container
		heapDur    int64 // priority at the last heap push; != durationMs means stale
		evicted    bool
	}

	groupHeap []*groupAgg
)

func (h groupHeap) Len() int           { return len(h) }
func (h groupHeap) Less(i, j int) bool { return h[i].heapDur < h[j].heapDur }
func (h groupHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *groupHeap) Push(x any)        { *h = append(*h, x.(*groupAgg)) }
func (h *groupHeap) Pop() any          { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
func (h groupHeap) peek() *groupAgg    { return h[0] }

// foldTags folds one exited invocation's tags into the node's aggregation.
// Attribution is per invocation: a group gets the invocation's duration and
// one execution at most once, however many of the invocation's values fall
// into it (the Java per-invocation tag set did the same).
func (b *builder) foldTags(node *Node, tags []invocationTag, durationMs int64) {
	if len(tags) == 0 {
		return
	}
	root := b.containers[node]
	if root == nil {
		root = newContainer()
		b.containers[node] = root
	}
	seen := map[*groupAgg]bool{}
	var lastSQL *groupAgg
	for _, tag := range tags {
		scope := root
		if tag.isBinds && lastSQL != nil && !lastSQL.evicted {
			// Binds bind the most recent SQL of their own invocation.
			if lastSQL.nested == nil {
				lastSQL.nested = newContainer()
			}
			scope = lastSQL.nested
		}
		g := b.groupOf(scope, tag, durationMs)
		if g == nil {
			continue // folded straight into ::other under cap pressure
		}
		if !seen[g] {
			seen[g] = true
			g.durationMs += durationMs
			g.executions++
		}
		if tag.isSQL {
			lastSQL = g
		}
	}
}

// groupOf finds or creates the tag's group in the scope. Under cap pressure
// it mirrors the Java Hotspot.addTag: an incoming group that would stay the
// smallest folds straight into ::other (nil return); otherwise the current
// smallest group is evicted to make room.
func (b *builder) groupOf(scope *container, tag invocationTag, durationMs int64) *groupAgg {
	p := scope.byIdx[tag.paramIdx]
	if p == nil {
		p = &paramAgg{paramIdx: tag.paramIdx, byKey: map[string]*groupAgg{}}
		scope.byIdx[tag.paramIdx] = p
		scope.params = append(scope.params, p)
	}
	key := tag.value
	if (tag.isSQL || tag.isBinds) && !tag.unresolved {
		key = sqlSignature(tag.value)
	}
	if g := p.byKey[key]; g != nil {
		return g
	}
	if scope.live >= b.maxGroups {
		smallest := scope.currentSmallest()
		if smallest == nil || durationMs <= smallest.durationMs {
			p.foldIntoOther(durationMs, 1)
			return nil
		}
		scope.evict(smallest)
	}
	g := &groupAgg{param: p, value: tag.value, unresolved: tag.unresolved}
	p.byKey[key] = g
	p.order = append(p.order, g)
	scope.live++
	g.heapDur = g.durationMs
	heap.Push(&scope.minima, g)
	return g
}

func newContainer() *container {
	return &container{byIdx: map[int]*paramAgg{}}
}

// currentSmallest settles the lazy heap: entries whose group grew since the
// push re-enter with their current duration until the top is truthful.
func (c *container) currentSmallest() *groupAgg {
	for c.minima.Len() > 0 {
		top := c.minima.peek()
		if top.evicted {
			heap.Pop(&c.minima)
			continue
		}
		if top.heapDur != top.durationMs {
			top.heapDur = top.durationMs
			heap.Fix(&c.minima, 0)
			continue
		}
		return top
	}
	return nil
}

// evict folds a group into its param's ::other bucket. Its key is freed — a
// re-appearing value starts a fresh group, as in the Java aggregation — and
// its nested params are folded away: ::other keeps totals only (02 §2.5.3).
func (c *container) evict(g *groupAgg) {
	g.evicted = true
	c.live--
	key := ""
	for k, v := range g.param.byKey {
		if v == g {
			key = k
			break
		}
	}
	delete(g.param.byKey, key)
	g.param.foldIntoOther(g.durationMs, g.executions)
}

func (p *paramAgg) foldIntoOther(durationMs, executions int64) {
	if p.other == nil {
		p.other = &groupAgg{param: p, value: OtherGroupValue}
	}
	p.other.durationMs += durationMs
	p.other.executions += executions
}

// materializeParams renders every node's aggregation into the wire shape:
// params in first-seen order, groups by durationMs descending with ::other
// last (02 §2.5.3).
func (b *builder) materializeParams(n *Node) {
	if scope := b.containers[n]; scope != nil {
		n.Params = scope.materialize()
	}
	for _, child := range n.Children {
		b.materializeParams(child)
	}
}

func (c *container) materialize() []Param {
	out := make([]Param, 0, len(c.params))
	for _, p := range c.params {
		param := Param{ParamIdx: p.paramIdx}
		for _, g := range p.order {
			if g.evicted {
				continue
			}
			group := ParamGroup{
				Value:      g.value,
				DurationMs: g.durationMs,
				Executions: g.executions,
				Unresolved: g.unresolved,
			}
			if g.nested != nil {
				group.Params = g.nested.materialize()
			}
			param.Groups = append(param.Groups, group)
		}
		sort.SliceStable(param.Groups, func(i, j int) bool {
			return param.Groups[i].DurationMs > param.Groups[j].DurationMs
		})
		if p.other != nil {
			param.Groups = append(param.Groups, ParamGroup{
				Value:      p.other.value,
				DurationMs: p.other.durationMs,
				Executions: p.other.executions,
			})
		}
		out = append(out, param)
	}
	return out
}

// literalRe strips single-quoted SQL literals, ” escapes included.
var literalRe = regexp.MustCompile(`'(?:''|[^'])*'`)

// sqlSignature is the old UI's similarity key (profiler.mjs:3469,
// signatures.sql): drop commas, strip quoted literals and digits, abbreviate
// every word to its first character, drop whitespace. Two SQL texts differing
// only in literals and identifiers' tails share a signature and fold into one
// group.
func sqlSignature(sql string) string {
	s := strings.ReplaceAll(sql, ",", "")
	s = literalRe.ReplaceAllString(s, "")
	var out strings.Builder
	out.Grow(len(s) / 4)
	inWord := false
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
			// Digits vanish without ending the word: the JS chain strips
			// \d+ before abbreviating, so "ab1cd" is one word, "a".
		case r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z':
			if !inWord {
				out.WriteRune(r)
				inWord = true
			}
		case r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '\f' || r == '\v':
			inWord = false
		default:
			out.WriteRune(r)
			inWord = false
		}
	}
	return out.String()
}
