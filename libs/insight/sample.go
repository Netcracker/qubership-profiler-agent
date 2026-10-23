package insight

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// Sample sizes. A sampled layer decodes one call tree per sampled call, which
// the read memory budget rules out for every call of a window (section 4.3 of
// the agent-access plan), so the sample is small and its size is an input.
const (
	DefaultSample = 20
	MaxSample     = 100
	// treeFetchers is how many trees a sample fetches at once.
	treeFetchers = 4
	// maxSamplePages bounds the pages a sample lists.
	maxSamplePages = 10
	// sampleCallsShown caps the sampled calls a result lists for explain.
	sampleCallsShown = 5
)

// SampleInfo says which calls a sampled layer read. Its figures are a sample:
// the slowest calls over-represent outliers, and the inner methods and SQL of
// the other calls are not read.
type SampleInfo struct {
	// Conditions are the /calls conditions the sample was listed under, so
	// the calls operation lists the same calls with OrderSlowest.
	Conditions []string `json:"conditions"`
	// Requested is the sample size asked for.
	Requested int `json:"requested"`
	// Listed is how many calls the listing returned; fewer than Requested
	// when fewer calls match.
	Listed int `json:"listed"`
	// Trees is how many call trees were merged.
	Trees int `json:"trees"`
	// Of is how many calls the sample was drawn from, from the exact layer.
	Of int64 `json:"of"`
	// Calls are the slowest sampled calls, for the explain operation.
	Calls []Call `json:"calls,omitempty"`
	// Skipped says why listed calls were left out of the merge.
	Skipped []string `json:"skipped,omitempty"`
	// Partial names the sources that failed while listing the sample.
	Partial []string `json:"partial,omitempty"`
}

// SampledMethod is a method's self time summed over the sampled trees.
type SampledMethod struct {
	MethodStat
	// Calls is how many sampled calls ran the method.
	Calls int `json:"calls"`
}

// SampledSQL is a statement group summed over the sampled trees.
type SampledSQL struct {
	SQLStat
	// Calls is how many sampled calls ran the statement.
	Calls int `json:"calls"`
}

// sampleAgg merges call trees: methods by short name, statements by
// signature.
type sampleAgg struct {
	trees       int
	methods     map[string]*MethodStat
	methodCalls map[string]int
	sql         sqlTotals
	sqlCalls    map[string]int
}

func newSampleAgg() *sampleAgg {
	return &sampleAgg{
		methods: map[string]*MethodStat{}, methodCalls: map[string]int{},
		sql: sqlTotals{}, sqlCalls: map[string]int{},
	}
}

// add merges one tree. An unresolved statement reference names an entry of
// its own call's dictionary, so the same reference in two calls can be two
// statements: the merge folds every unresolved reference into one entry
// instead of keying by the reference.
func (a *sampleAgg) add(tree *calltree.Tree) {
	a.trees++
	method := func(n *calltree.Node) string {
		if n.MethodIdx >= 0 && n.MethodIdx < len(tree.Methods) {
			return ShortMethod(tree.Methods[n.MethodIdx])
		}
		return "#unknown"
	}
	for name, s := range methodTotals(tree.Root, method) {
		m := a.methods[name]
		if m == nil {
			m = &MethodStat{Method: name}
			a.methods[name] = m
		}
		m.SelfMs += s.SelfMs
		m.Executions += s.Executions
		m.SuspendMs += s.SuspendMs
		a.methodCalls[name]++
	}
	seen := map[string]bool{}
	for key, g := range collectSQL(tree.Root, tree.Params) {
		text := g.stat.Statement
		if strings.HasPrefix(key, "unresolved:") {
			key, text = "unresolved", "(statement text unavailable)"
		}
		a.sql.add(key, text, g.stat.DurationMs, g.stat.Executions)
		if !seen[key] {
			seen[key] = true
			a.sqlCalls[key]++
		}
	}
}

// rankedMethods lists the merged methods with self time, the most first.
func (a *sampleAgg) rankedMethods() []SampledMethod {
	ranked := rankMethods(a.methods)
	out := make([]SampledMethod, len(ranked))
	for i, m := range ranked {
		out[i] = SampledMethod{MethodStat: m, Calls: a.methodCalls[m.Method]}
	}
	return out
}

// rankedSQL lists the merged statements, the slowest first.
func (a *sampleAgg) rankedSQL() []SampledSQL {
	out := make([]SampledSQL, 0, len(a.sql))
	for key, g := range a.sql {
		s := g.stat
		s.Statement = truncate(s.Statement, sqlTextLimit)
		out = append(out, SampledSQL{SQLStat: s, Calls: a.sqlCalls[key]})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DurationMs != out[j].DurationMs {
			return out[i].DurationMs > out[j].DurationMs
		}
		return out[i].Statement < out[j].Statement
	})
	return out
}

// sample lists the k slowest calls under the conditions in the window and
// merges their trees. A call without a tree, or whose tree fails to load, is
// skipped and named in Skipped; only a canceled context or a failed listing
// fails the sample.
func (c *Client) sample(ctx context.Context, window ResolvedWindow, conditions []string, k int) (SampleInfo, *sampleAgg, error) {
	info := SampleInfo{Conditions: conditions, Requested: k}
	page, err := c.Calls(ctx, CallsInput{
		Window:  Window{From: window.From, To: window.To},
		Filters: Filters{Conditions: conditions},
		Order:   OrderSlowest,
		Limit:   k,
	})
	if err != nil {
		return info, nil, err
	}
	calls, partial := page.Calls, page.Partial
	// A duration_desc page can stop short of the limit, even empty, without
	// ending the listing (02 §2.3.5), so the sample follows the cursor until
	// it has k calls. Each page opens a shorter duration band or returns a
	// row, so the bound only stops a server that misbehaves.
	for pages := 1; len(calls) < k && page.NextCursor != "" && pages < maxSamplePages; pages++ {
		page, err = c.Calls(ctx, CallsInput{Cursor: page.NextCursor, Order: OrderSlowest, Limit: k - len(calls)})
		if err != nil {
			return info, nil, err
		}
		if page.Restarted {
			break
		}
		fresh := 0
		for _, call := range page.Calls {
			if !slices.ContainsFunc(calls, func(c Call) bool { return c.Ref == call.Ref }) {
				calls = append(calls, call)
				fresh++
			}
		}
		partial = append(partial, page.Partial...)
		if fresh == 0 && len(page.Calls) > 0 {
			break // a page of repeats: the server does not advance
		}
	}
	info.Partial = partial
	info.Listed = len(calls)
	info.Calls = capList(calls, sampleCallsShown)

	var refs []CallRef
	noTree := map[string]int{}
	for _, call := range calls {
		if call.Truncated != "" {
			noTree[call.Truncated]++
			continue
		}
		ref, err := ParseCallRef(call.Ref)
		if err != nil {
			return info, nil, err
		}
		refs = append(refs, ref)
	}
	for _, reason := range sortedKeys(noTree) {
		info.Skipped = append(info.Skipped, fmt.Sprintf("%d calls have no tree (%s)", noTree[reason], reason))
	}

	trees, failed, err := c.fetchTrees(ctx, refs)
	if err != nil {
		return info, nil, err
	}
	for _, reason := range sortedKeys(failed) {
		info.Skipped = append(info.Skipped, fmt.Sprintf("%d trees failed to load: %s", failed[reason], reason))
	}
	agg := newSampleAgg()
	for _, tree := range trees {
		if tree != nil {
			agg.add(tree)
		}
	}
	info.Trees = agg.trees
	return info, agg, nil
}

// fetchTrees loads the trees of refs, a few at a time. A tree that fails to
// load is nil in the result and counted by error text in failed.
func (c *Client) fetchTrees(ctx context.Context, refs []CallRef) ([]*calltree.Tree, map[string]int, error) {
	namespaces := make([]string, 0, len(refs))
	for _, ref := range refs {
		if !slices.Contains(namespaces, ref.PK.PodNamespace) {
			namespaces = append(namespaces, ref.PK.PodNamespace)
		}
	}
	if len(refs) > 0 {
		if err := c.authorize(ctx, namespaces); err != nil {
			return nil, nil, err
		}
	}
	trees := make([]*calltree.Tree, len(refs))
	errs := make([]error, len(refs))
	next := make(chan int)
	var wg sync.WaitGroup
	for range min(treeFetchers, len(refs)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				trees[i], errs[i] = c.tree(ctx, refs[i])
			}
		}()
	}
	for i := range refs {
		next <- i
	}
	close(next)
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	failed := map[string]int{}
	for _, err := range errs {
		if err != nil {
			failed[err.Error()]++
		}
	}
	return trees, failed, nil
}

// groupCondition returns one condition that selects the calls of the given
// groups, and the keys it covers. It prefers `<field> in <keys>`. method has
// no `in` in the filter model (02 §2.3.3), so for a field with `=~` only it
// matches the whole value against an alternation of the quoted keys, which
// selects exactly those groups in one scan. With `=` only it covers the first
// key. It returns an empty condition when the build has none of these
// operators on the field.
func groupCondition(caps model.FilterCapabilities, field string, keys []string) (string, []string) {
	if len(keys) == 0 {
		return "", nil
	}
	ops := caps.Fields[field]
	if !model.IsField(field) {
		ops = caps.ParamOperators
	}
	switch {
	case slices.Contains(ops, model.OpIn):
		return model.Condition{Field: field, Op: model.OpIn, Values: keys}.String(), keys
	case slices.Contains(ops, model.OpMatch):
		quoted := make([]string, len(keys))
		for i, k := range keys {
			quoted[i] = regexp.QuoteMeta(k)
		}
		pattern := "^(?:" + strings.Join(quoted, "|") + ")$"
		return model.Condition{Field: field, Op: model.OpMatch, Values: []string{pattern}}.String(), keys
	case slices.Contains(ops, model.OpEq):
		return model.Condition{Field: field, Op: model.OpEq, Values: keys[:1]}.String(), keys[:1]
	}
	return "", nil
}

// configResponse is the part of /config the operations read.
type configResponse struct {
	FilterCapabilities model.FilterCapabilities `json:"filter_capabilities"`
}

// capabilities reads what the build implements of the filter model
// (02 §2.3.4). A build older than the filter model has none, which leaves
// every field without a set condition.
func (c *Client) capabilities(ctx context.Context) (model.FilterCapabilities, error) {
	var resp configResponse
	if err := c.getJSON(ctx, "/config", nil, &resp); err != nil {
		return model.FilterCapabilities{}, errors.Wrap(err, "read the filter capabilities")
	}
	return resp.FilterCapabilities, nil
}

func checkSample(k int) (int, error) {
	if k == 0 {
		return DefaultSample, nil
	}
	if k < 0 || k > MaxSample {
		return 0, errors.Errorf("sample must be between 1 and %d calls", MaxSample)
	}
	return k, nil
}
