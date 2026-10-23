package insight

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/clock"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/pkg/errors"
)

// Compare defaults and thresholds.
const (
	// DefaultCompareSample is smaller than DefaultSample because compare
	// samples both windows.
	DefaultCompareSample = 10
	// slowerRatio marks a group as slower: its mean or p95 duration in the
	// target is at least this multiple of the baseline's.
	slowerRatio = 1.2
	// compareSampleGroups caps the changed groups the sample reads.
	compareSampleGroups = 3
	// shortCleanTTL is the default retention of short_clean calls
	// (01-write-contract §6.4); a baseline older than that has lost its fast
	// calls, which skews every per-call figure.
	shortCleanTTL = 48 * time.Hour
)

// The status of a group in a comparison (section 4.3 of the agent-access
// plan).
const (
	// StatusCompared: the group has figures in both windows.
	StatusCompared = "compared"
	// StatusNew: the group has no call in the baseline, on complete
	// evidence.
	StatusNew = "new"
	// StatusVanished: the group has no call in the target, on complete
	// evidence.
	StatusVanished = "vanished"
	// StatusOutsideTop: the group is below the top N of one window, and the
	// build cannot look it up there, so it has no figures for that window.
	StatusOutsideTop = "outside_top"
	// StatusUnknown: the window that lacks the group answered partially, so
	// a failed source may hold its calls, or answered approximately even
	// when asked for that group alone, so a source may have dropped it.
	StatusUnknown = "unknown"
)

// The windows of a comparison, as CompareGroup.Missing names them.
const (
	WindowBaseline = "baseline"
	WindowTarget   = "target"
)

// CompareInput selects two windows and the calls to compare in them.
type CompareInput struct {
	// Target is the window under investigation, such as the hour after a
	// deployment.
	Target Window
	// Baseline is the window to compare with. The zero Baseline is the
	// window of the same length that ends where Target starts, unless
	// BaselineShift is set.
	Baseline Window
	// BaselineShift places the baseline at Target moved back by this much,
	// such as 24h for the same hours a day before. It excludes Baseline.
	BaselineShift time.Duration
	Filters       Filters
	// GroupBy is a field of /stats; empty means DefaultGroupBy.
	GroupBy string
	// Limit is how many top groups each window lists; zero means
	// DefaultHotspotLimit.
	Limit int
	// Order ranks the groups in each window; empty means
	// OrderDurationSumDesc.
	Order stats.Order
	// Sample is how many of the slowest calls of the changed groups to read
	// trees from in each window; zero means DefaultCompareSample.
	Sample int
}

// CompareResult joins the top groups of two windows. Groups and the totals
// are exact; Sample is not.
type CompareResult struct {
	Baseline ResolvedWindow `json:"baseline"`
	Target   ResolvedWindow `json:"target"`
	GroupBy  string         `json:"group_by"`
	Order    stats.Order    `json:"order"`
	// BaselineTotals and TargetTotals count every call under the filters.
	BaselineTotals StatsTotals `json:"baseline_totals"`
	TargetTotals   StatsTotals `json:"target_totals"`
	// Groups are the groups in the top N of either window: the compared
	// ones by the size of their change, then the new, the vanished, the
	// outside-top, and the unknown ones.
	Groups []CompareGroup `json:"groups"`
	// Approximate reports that a source kept only its heaviest groups, so
	// a group's ranking figure may be overcounted.
	Approximate bool `json:"approximate,omitempty"`
	// Partial names the sources that failed, prefixed with the window.
	Partial []string `json:"partial,omitempty"`
	// Sample is the sampled layer; nil when no group got slower or is new.
	Sample *CompareSample `json:"sample,omitempty"`
	// Notes say what was cut and which input shows more.
	Notes []string `json:"notes,omitempty"`
}

// CompareGroup is one group in both windows.
type CompareGroup struct {
	Key    string `json:"key"`
	Status string `json:"status"`
	// Missing names the window without figures, WindowBaseline or
	// WindowTarget, for StatusOutsideTop and StatusUnknown.
	Missing  string       `json:"missing,omitempty"`
	Baseline *CompareSide `json:"baseline,omitempty"`
	Target   *CompareSide `json:"target,omitempty"`
	// TimeMsPerMinDelta is the change of the group's total duration per
	// minute of window, target minus baseline. A new group counts its
	// target alone and a vanished one its baseline alone; the outside-top
	// and unknown ones have none.
	TimeMsPerMinDelta float64 `json:"time_ms_per_min_delta"`
	// Slower marks a compared group whose mean or p95 duration rose by 20%
	// or more.
	Slower bool `json:"slower,omitempty"`
}

// CompareSide is a group's figures in one window, with the rates that make
// windows of different lengths comparable.
type CompareSide struct {
	StatsValues
	CallsPerMin  float64 `json:"calls_per_min"`
	TimeMsPerMin float64 `json:"time_ms_per_min"`
	MeanMs       float64 `json:"mean_ms"`
	// ErrorRate is the share of the group's calls that failed, 0 to 1.
	ErrorRate float64 `json:"error_rate"`
}

// CompareSample reads the trees of the slowest calls of the changed groups
// in each window and compares their inner methods and SQL per sampled call.
type CompareSample struct {
	// Groups are the changed groups the sample reads.
	Groups []string `json:"groups"`
	// Baseline is nil when no sampled group has baseline calls.
	Baseline *SampleInfo    `json:"baseline,omitempty"`
	Target   SampleInfo     `json:"target"`
	Methods  []MethodChange `json:"methods"`
	SQL      []SQLChange    `json:"sql"`
}

// MethodChange compares a method's self time per sampled call.
type MethodChange struct {
	Method                string  `json:"method"`
	BaselineSelfMsPerCall float64 `json:"baseline_self_ms_per_call"`
	TargetSelfMsPerCall   float64 `json:"target_self_ms_per_call"`
	DeltaMsPerCall        float64 `json:"delta_ms_per_call"`
	// BaselineCalls and TargetCalls count the sampled calls that ran it.
	BaselineCalls int `json:"baseline_calls"`
	TargetCalls   int `json:"target_calls"`
}

// SQLChange compares a statement's time per sampled call.
type SQLChange struct {
	Statement         string  `json:"statement"`
	BaselineMsPerCall float64 `json:"baseline_ms_per_call"`
	TargetMsPerCall   float64 `json:"target_ms_per_call"`
	DeltaMsPerCall    float64 `json:"delta_ms_per_call"`
	BaselineCalls     int     `json:"baseline_calls"`
	TargetCalls       int     `json:"target_calls"`
}

// Compare joins the top groups of two windows and ranks what changed. A
// /stats response holds only the top N groups, so a group listed in one
// window and not the other is looked up in the other window before it is
// called new or vanished; only complete evidence makes it so. The groups
// that got slower, or are new, are then sampled in both windows.
func (c *Client) Compare(ctx context.Context, in CompareInput) (CompareResult, error) {
	in, err := in.withDefaults()
	if err != nil {
		return CompareResult{}, err
	}
	target, baseline, err := in.windows()
	if err != nil {
		return CompareResult{}, err
	}
	conditions, err := in.Filters.conditions()
	if err != nil {
		return CompareResult{}, err
	}
	statsIn := func(w ResolvedWindow, extra string, limit int) StatsInput {
		cs := conditions
		if extra != "" {
			cs = append(slices.Clone(conditions), extra)
		}
		return StatsInput{
			Window: Window{From: w.From, To: w.To}, Filters: Filters{Conditions: cs},
			GroupBy: in.GroupBy, Limit: limit, Order: in.Order,
		}
	}

	var stBase, stTarget StatsResult
	var errBase, errTarget error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); stBase, errBase = c.Stats(ctx, statsIn(baseline, "", in.Limit)) }()
	go func() { defer wg.Done(); stTarget, errTarget = c.Stats(ctx, statsIn(target, "", in.Limit)) }()
	wg.Wait()
	if errBase != nil {
		return CompareResult{}, errors.Wrap(errBase, "baseline")
	}
	if errTarget != nil {
		return CompareResult{}, errors.Wrap(errTarget, "target")
	}

	res := CompareResult{
		Baseline: baseline, Target: target, GroupBy: in.GroupBy, Order: in.Order,
		BaselineTotals: stBase.Totals, TargetTotals: stTarget.Totals,
		Approximate: stBase.Approximate || stTarget.Approximate,
	}
	res.Partial = append(prefixed(WindowBaseline, stBase.Partial), prefixed(WindowTarget, stTarget.Partial)...)

	j := &join{
		base: rowsByKey(stBase), target: rowsByKey(stTarget),
		baseStatus: map[string]string{}, targetStatus: map[string]string{},
	}
	missingInBase := missingKeys(stTarget, j.base)
	missingInTarget := missingKeys(stBase, j.target)
	lk := &lookup{c: c, field: in.GroupBy, statsIn: statsIn, res: &res}
	if err := lk.resolve(ctx, WindowBaseline, baseline, stBase, missingInBase, j.base, j.baseStatus); err != nil {
		return CompareResult{}, err
	}
	if err := lk.resolve(ctx, WindowTarget, target, stTarget, missingInTarget, j.target, j.targetStatus); err != nil {
		return CompareResult{}, err
	}

	keys := append(keysOf(stTarget), missingInTarget...)
	res.Groups = j.groups(keys, baseline, target)
	if stBase.Other != nil || stTarget.Other != nil {
		res.Notes = append(res.Notes, fmt.Sprintf("only groups in the top %d of either window are compared; a larger limit compares more", in.Limit))
	}
	if clock.Now().Sub(baseline.From) > shortCleanTTL {
		res.Notes = append(res.Notes, "the baseline starts more than two days ago, when fast successful calls may have aged out; "+
			"add duration>=100ms to compare like with like")
	}

	if err := c.compareSample(ctx, in, conditions, lk, &res); err != nil {
		return CompareResult{}, err
	}
	return res, nil
}

// join holds each window's figures by key and the status of each key a
// window does not have figures for.
type join struct {
	base, target             map[string]StatsValues
	baseStatus, targetStatus map[string]string
}

// groups builds the comparison rows for keys and ranks them.
func (j *join) groups(keys []string, baseline, target ResolvedWindow) []CompareGroup {
	baseMin := baseline.To.Sub(baseline.From).Minutes()
	targetMin := target.To.Sub(target.From).Minutes()
	out := make([]CompareGroup, 0, len(keys))
	for _, key := range keys {
		g := CompareGroup{Key: key}
		if v, ok := j.base[key]; ok {
			g.Baseline = side(v, baseMin)
		}
		if v, ok := j.target[key]; ok {
			g.Target = side(v, targetMin)
		}
		switch {
		case g.Baseline != nil && g.Target != nil:
			g.Status = StatusCompared
			g.TimeMsPerMinDelta = g.Target.TimeMsPerMin - g.Baseline.TimeMsPerMin
			g.Slower = rose(g.Baseline.MeanMs, g.Target.MeanMs) || rose(ptrFloat(g.Baseline.P95Ms), ptrFloat(g.Target.P95Ms))
		case g.Target != nil:
			g.Status, g.Missing = j.baseStatus[key], WindowBaseline
			if g.Status == StatusNew {
				g.Missing, g.TimeMsPerMinDelta = "", g.Target.TimeMsPerMin
			}
		default:
			g.Status, g.Missing = j.targetStatus[key], WindowTarget
			if g.Status == StatusVanished {
				g.Missing, g.TimeMsPerMinDelta = "", -g.Baseline.TimeMsPerMin
			}
		}
		out = append(out, g)
	}
	rank := map[string]int{StatusCompared: 0, StatusNew: 1, StatusVanished: 2, StatusOutsideTop: 3, StatusUnknown: 4}
	sort.SliceStable(out, func(a, b int) bool {
		ga, gb := out[a], out[b]
		if rank[ga.Status] != rank[gb.Status] {
			return rank[ga.Status] < rank[gb.Status]
		}
		if da, db := math.Abs(ga.TimeMsPerMinDelta), math.Abs(gb.TimeMsPerMinDelta); da != db {
			return da > db
		}
		if ta, tb := knownTime(ga), knownTime(gb); ta != tb {
			return ta > tb
		}
		return ga.Key < gb.Key
	})
	return out
}

// knownTime ranks the groups without a delta by the time of the window
// they have figures for.
func knownTime(g CompareGroup) float64 {
	if g.Target != nil {
		return g.Target.TimeMsPerMin
	}
	if g.Baseline != nil {
		return g.Baseline.TimeMsPerMin
	}
	return 0
}

// lookup completes one window's figures for the keys the other window
// listed.
type lookup struct {
	c       *Client
	field   string
	statsIn func(w ResolvedWindow, extra string, limit int) StatsInput
	res     *CompareResult
	caps    *model.FilterCapabilities
}

func (l *lookup) capabilities(ctx context.Context) (model.FilterCapabilities, error) {
	if l.caps == nil {
		caps, err := l.c.capabilities(ctx)
		if err != nil {
			return caps, err
		}
		l.caps = &caps
	}
	return *l.caps, nil
}

// resolve decides, for each key the window's response does not list,
// whether the window has the group (its figures go into found), has no call
// of it (absent: new or vanished), or cannot tell. The evidence rules are
// section 4.3 of the agent-access plan:
//
//   - A response that lists every group (other is null, approximate is
//     false) and is not partial proves a missing key absent; no lookup runs.
//   - Otherwise one /stats query restricted to the missing keys looks them
//     up. A key it returns gets its figures. A key it does not return is
//     absent only if the lookup is exact: a partial lookup leaves the key
//     unknown, and an approximate one may have dropped the key, so the
//     omitted keys are looked up again in halves (see lookUp).
//   - A key no single condition can select (the build has no `in`, `=~`, or
//     `=` on the field, or `=` covers only one key) is not looked up: it is
//     unknown when the window's response is partial and outside the top N
//     otherwise.
func (l *lookup) resolve(ctx context.Context, name string, w ResolvedWindow, st StatsResult, keys []string,
	found map[string]StatsValues, status map[string]string) error {
	if len(keys) == 0 {
		return nil
	}
	absent := StatusVanished
	if name == WindowBaseline {
		absent = StatusNew
	}
	if st.Other == nil && !st.Approximate && len(st.Partial) == 0 {
		for _, k := range keys {
			status[k] = absent
		}
		return nil
	}
	caps, err := l.capabilities(ctx)
	if err != nil {
		return err
	}
	cond, covered := groupCondition(caps, l.field, keys)
	for _, k := range keys {
		if !slices.Contains(covered, k) {
			status[k] = StatusOutsideTop
			if len(st.Partial) > 0 {
				status[k] = StatusUnknown
			}
		}
	}
	if cond == "" {
		return nil
	}
	return l.lookUp(ctx, name, w, cond, covered, absent, found, status)
}

// lookUp runs one /stats query restricted to keys by cond and classifies
// the keys it does not return. Under a multi-valued param grouping the
// restricted calls also fall into the rows of their other values (02 §2.9),
// which can outrank a requested key, so the query asks for the largest
// limit rather than one row per key. A response that still cuts groups into
// other, or is approximate (a source kept fewer groups than it saw, as
// Space-Saving does), is no evidence that an omitted key is absent, so the
// omitted keys are looked up again in two halves until a response is exact.
// A single key that still comes back cut or approximate, or any key a
// partial response omits, is unknown. The halving keeps the queries under
// twice the number of keys.
//
// A lookup of one key ranks that key first or tied for first: every call it
// selects carries the key, so the key's row holds all of them. It can miss
// the response only when more than stats.MaxLimit co-occurring values tie
// with it and sort before it.
func (l *lookup) lookUp(ctx context.Context, name string, w ResolvedWindow, cond string, keys []string,
	absent string, found map[string]StatsValues, status map[string]string) error {
	got, err := l.c.Stats(ctx, l.statsIn(w, cond, stats.MaxLimit))
	if err != nil {
		return errors.Wrapf(err, "look up %d groups in the %s", len(keys), name)
	}
	l.res.Partial = append(l.res.Partial, prefixed(name+" lookup", got.Partial)...)
	l.res.Approximate = l.res.Approximate || got.Approximate
	for _, row := range got.Rows {
		if slices.Contains(keys, row.Key) {
			found[row.Key] = row.StatsValues
		}
	}
	var omitted []string
	for _, k := range keys {
		if _, ok := found[k]; !ok {
			omitted = append(omitted, k)
		}
	}
	exact := !got.Approximate && got.Other == nil
	switch {
	case len(omitted) == 0:
		return nil
	case len(got.Partial) > 0 || (!exact && len(keys) == 1):
		for _, k := range omitted {
			status[k] = StatusUnknown
		}
		return nil
	case exact:
		for _, k := range omitted {
			status[k] = absent
		}
		return nil
	}
	caps, err := l.capabilities(ctx)
	if err != nil {
		return err
	}
	half := (len(omitted) + 1) / 2
	for _, part := range [][]string{omitted[:half], omitted[half:]} {
		if len(part) == 0 {
			continue
		}
		cond, covered := groupCondition(caps, l.field, part)
		for _, k := range part {
			if !slices.Contains(covered, k) {
				status[k] = StatusUnknown
			}
		}
		if cond == "" {
			continue
		}
		if err := l.lookUp(ctx, name, w, cond, covered, absent, found, status); err != nil {
			return err
		}
	}
	return nil
}

// compareSample samples the groups that got slower or are new, in both
// windows, and compares their inner methods and SQL per sampled call. A
// group that got faster is not sampled: the question compare answers is
// what got slower.
func (c *Client) compareSample(ctx context.Context, in CompareInput, conditions []string, lk *lookup, res *CompareResult) error {
	var changed []CompareGroup
	for _, g := range res.Groups {
		if g.Slower || g.Status == StatusNew {
			changed = append(changed, g)
		}
	}
	if len(changed) == 0 {
		res.Notes = append(res.Notes, "no group got slower by 20% or more and none is new, so no call trees were sampled")
		return nil
	}
	sort.SliceStable(changed, func(a, b int) bool { return changed[a].TimeMsPerMinDelta > changed[b].TimeMsPerMinDelta })
	if len(changed) > compareSampleGroups {
		res.Notes = append(res.Notes, fmt.Sprintf("the sample reads the %d changed groups that added the most time, of %d",
			compareSampleGroups, len(changed)))
		changed = changed[:compareSampleGroups]
	}
	keys := make([]string, len(changed))
	for i, g := range changed {
		keys[i] = g.Key
	}
	caps, err := lk.capabilities(ctx)
	if err != nil {
		return err
	}
	cond, covered := groupCondition(caps, in.GroupBy, keys)
	if cond == "" {
		res.Notes = append(res.Notes, fmt.Sprintf("no call trees were sampled: the server cannot select %s values in one condition", in.GroupBy))
		return nil
	}
	if len(covered) < len(keys) {
		res.Notes = append(res.Notes, fmt.Sprintf("the sample reads %d of the %d changed groups: the server selects one %s value per condition",
			len(covered), len(keys), in.GroupBy))
	}
	sampleConds := append(slices.Clone(conditions), cond)
	sample := &CompareSample{Groups: covered}

	pickTarget := func(g CompareGroup) *CompareSide { return g.Target }
	pickBase := func(g CompareGroup) *CompareSide { return g.Baseline }
	targetOf, targetPartial, err := c.compareSampleOf(ctx, in.GroupBy, res, WindowTarget, res.Target, sampleConds, covered, pickTarget)
	if err != nil {
		return err
	}
	baseOf, basePartial, err := c.compareSampleOf(ctx, in.GroupBy, res, WindowBaseline, res.Baseline, sampleConds, covered, pickBase)
	if err != nil {
		return err
	}

	targetInfo, targetAgg, err := c.sample(ctx, res.Target, sampleConds, in.Sample)
	if err != nil {
		return sampleFailed(ctx, err, res)
	}
	targetInfo.Of, targetInfo.Partial = targetOf, append(targetInfo.Partial, targetPartial...)
	sample.Target = targetInfo
	baseAgg := newSampleAgg()
	// Without a count, any baseline figure of a sampled group says the
	// baseline has calls to sample.
	hasBase := groupCount(res.Groups, covered, pickBase) > 0
	if baseOf != nil {
		hasBase = *baseOf > 0
	}
	if hasBase {
		info, agg, err := c.sample(ctx, res.Baseline, sampleConds, in.Sample)
		if err != nil {
			return sampleFailed(ctx, err, res)
		}
		info.Of, info.Partial = baseOf, append(info.Partial, basePartial...)
		sample.Baseline, baseAgg = &info, agg
	}
	sample.Methods, sample.SQL = diffSamples(baseAgg, targetAgg)
	if n := len(sample.Methods); n > topListSize {
		sample.Methods = sample.Methods[:topListSize]
		res.Notes = append(res.Notes, fmt.Sprintf("the sampled methods list the %d largest changes of %d; explain a sampled call to see the rest", topListSize, n))
	}
	if n := len(sample.SQL); n > topListSize {
		sample.SQL = sample.SQL[:topListSize]
		res.Notes = append(res.Notes, fmt.Sprintf("the sampled SQL lists the %d largest changes of %d statements", topListSize, n))
	}
	res.Sample = sample
	return nil
}

// compareSampleOf returns how many calls of the sampled groups one window
// has, each counted once. The sum of the groups' counts is that number only
// for a single-valued grouping whose figures are exact and present for every
// sampled group in the window; otherwise the calls are counted with one
// more /stats query, and a failed count is nil with a note.
func (c *Client) compareSampleOf(ctx context.Context, field string, res *CompareResult, name string, w ResolvedWindow,
	conds, keys []string, pick func(CompareGroup) *CompareSide) (*int64, []string, error) {
	exact := model.IsSingleValued(field) && !res.Approximate
	for _, g := range res.Groups {
		// A group without figures for this window, and without proof that
		// it has no calls there, leaves the sum short.
		if slices.Contains(keys, g.Key) && pick(g) == nil && g.Status != StatusNew && g.Status != StatusVanished {
			exact = false
			break
		}
	}
	if exact {
		n := groupCount(res.Groups, keys, pick)
		return &n, nil, nil
	}
	n, partial, note, err := c.callCount(ctx, w, conds)
	if err != nil {
		return nil, nil, err
	}
	if note != "" {
		res.Notes = append(res.Notes, name+": "+note)
	}
	return n, partial, nil
}

// sampleFailed keeps the exact layer when the sample cannot be listed.
func sampleFailed(ctx context.Context, err error, res *CompareResult) error {
	if ctx.Err() != nil {
		return err
	}
	res.Notes = append(res.Notes, "the sampled layer failed: "+err.Error())
	return nil
}

// diffSamples compares per sampled call, so samples of different sizes
// compare, and ranks by the size of the change.
func diffSamples(base, target *sampleAgg) ([]MethodChange, []SQLChange) {
	perCall := func(v int64, trees int) float64 {
		if trees == 0 {
			return 0
		}
		return float64(v) / float64(trees)
	}
	var methods []MethodChange
	for _, name := range unionKeys(base.methods, target.methods) {
		m := MethodChange{Method: name, BaselineCalls: base.methodCalls[name], TargetCalls: target.methodCalls[name]}
		if s := base.methods[name]; s != nil {
			m.BaselineSelfMsPerCall = perCall(s.SelfMs, base.trees)
		}
		if s := target.methods[name]; s != nil {
			m.TargetSelfMsPerCall = perCall(s.SelfMs, target.trees)
		}
		m.DeltaMsPerCall = m.TargetSelfMsPerCall - m.BaselineSelfMsPerCall
		if m.DeltaMsPerCall != 0 {
			methods = append(methods, m)
		}
	}
	sort.SliceStable(methods, func(a, b int) bool {
		return math.Abs(methods[a].DeltaMsPerCall) > math.Abs(methods[b].DeltaMsPerCall)
	})
	var sql []SQLChange
	for _, key := range unionKeys(base.sql, target.sql) {
		s := SQLChange{BaselineCalls: base.sqlCalls[key], TargetCalls: target.sqlCalls[key]}
		if g := base.sql[key]; g != nil {
			s.Statement, s.BaselineMsPerCall = g.stat.Statement, perCall(g.stat.DurationMs, base.trees)
		}
		if g := target.sql[key]; g != nil {
			s.Statement, s.TargetMsPerCall = g.stat.Statement, perCall(g.stat.DurationMs, target.trees)
		}
		s.Statement = truncate(s.Statement, sqlTextLimit)
		s.DeltaMsPerCall = s.TargetMsPerCall - s.BaselineMsPerCall
		if s.DeltaMsPerCall != 0 {
			sql = append(sql, s)
		}
	}
	sort.SliceStable(sql, func(a, b int) bool { return math.Abs(sql[a].DeltaMsPerCall) > math.Abs(sql[b].DeltaMsPerCall) })
	return methods, sql
}

// unionKeys lists the keys of both maps, sorted, so ties rank by key.
func unionKeys[V any](a, b map[string]V) []string {
	keys := sortedKeys(a)
	for _, k := range sortedKeys(b) {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

// groupCount sums one window's call count over the given keys.
func groupCount(groups []CompareGroup, keys []string, pick func(CompareGroup) *CompareSide) int64 {
	var n int64
	for _, g := range groups {
		if s := pick(g); s != nil && slices.Contains(keys, g.Key) {
			n += s.Count
		}
	}
	return n
}

func side(v StatsValues, minutes float64) *CompareSide {
	s := &CompareSide{StatsValues: v}
	if minutes > 0 {
		s.CallsPerMin = float64(v.Count) / minutes
		s.TimeMsPerMin = float64(v.DurationMsSum) / minutes
	}
	if v.Count > 0 {
		s.MeanMs = float64(v.DurationMsSum) / float64(v.Count)
		s.ErrorRate = float64(v.Errors) / float64(v.Count)
	}
	return s
}

// rose reports whether a figure rose by slowerRatio or more. A zero
// baseline (a group of 0 ms calls) rises only to a figure of 1 ms or more.
func rose(before, after float64) bool {
	if before <= 0 {
		return after >= 1
	}
	return after >= before*slowerRatio
}

func ptrFloat(v *int64) float64 {
	if v == nil {
		return 0
	}
	return float64(*v)
}

func rowsByKey(st StatsResult) map[string]StatsValues {
	out := make(map[string]StatsValues, len(st.Rows))
	for _, r := range st.Rows {
		out[r.Key] = r.StatsValues
	}
	return out
}

func keysOf(st StatsResult) []string {
	out := make([]string, len(st.Rows))
	for i, r := range st.Rows {
		out[i] = r.Key
	}
	return out
}

// missingKeys lists the keys of st that have no figures in other.
func missingKeys(st StatsResult, other map[string]StatsValues) []string {
	var out []string
	for _, r := range st.Rows {
		if _, ok := other[r.Key]; !ok {
			out = append(out, r.Key)
		}
	}
	return out
}

func prefixed(prefix string, reasons []string) []string {
	out := make([]string, len(reasons))
	for i, r := range reasons {
		out[i] = prefix + ": " + r
	}
	return out
}

func (in CompareInput) withDefaults() (CompareInput, error) {
	h, err := HotspotsInput{GroupBy: in.GroupBy, Limit: in.Limit, Order: in.Order, Sample: 1}.withDefaults()
	if err != nil {
		return in, err
	}
	in.GroupBy, in.Limit, in.Order = h.GroupBy, h.Limit, h.Order
	if in.Sample == 0 {
		in.Sample = DefaultCompareSample
	}
	if _, err := checkSample(in.Sample); err != nil {
		return in, err
	}
	if in.BaselineShift < 0 {
		return in, errors.New("baseline shift must be positive: it moves the target window back in time")
	}
	if in.BaselineShift > 0 && in.Baseline != (Window{}) {
		return in, errors.New("give a baseline window or a baseline shift, not both")
	}
	return in, nil
}

// windows resolves the target and the baseline. The baseline may not
// overlap the target: a call in both would count on both sides.
func (in CompareInput) windows() (target, baseline ResolvedWindow, err error) {
	target, err = in.Target.Resolve()
	if err != nil {
		return target, baseline, errors.Wrap(err, "target")
	}
	if in.Baseline == (Window{}) {
		shift := in.BaselineShift
		if shift == 0 {
			shift = target.To.Sub(target.From)
		}
		baseline = ResolvedWindow{From: target.From.Add(-shift), To: target.To.Add(-shift)}
	} else if baseline, err = in.Baseline.Resolve(); err != nil {
		return target, baseline, errors.Wrap(err, "baseline")
	}
	if baseline.From.Before(target.To) && target.From.Before(baseline.To) {
		return target, baseline, errors.Errorf("the baseline (%s to %s) overlaps the target (%s to %s); compare windows that do not overlap",
			baseline.From.Format(time.RFC3339), baseline.To.Format(time.RFC3339),
			target.From.Format(time.RFC3339), target.To.Format(time.RFC3339))
	}
	return target, baseline, nil
}
