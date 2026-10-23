package stats

import (
	"container/heap"
	"slices"
	"strconv"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// Grouping places a call into groups. A time grouping keys a call by the
// start of its bucket in Unix ms, written in decimal; a field grouping keys
// it by the field's value as 02 §2.3.3 defines it, so `<field>=<key>`
// selects the group's calls; param_name keys it by the names of the params
// it carries (02 §2.9).
type Grouping struct {
	// Field is GroupByTime, model.GroupByParamName, or a field of 02 §2.3.3.
	Field string
	// OriginMs and BucketMs are the bucket grid of a time grouping.
	OriginMs int64
	BucketMs int64
}

// Keys appends the groups of a row to buf[:0] and returns it. A row has one
// key, none when it lacks the field, and one per distinct value under a
// multi-valued param or param_name. An empty value is no key: `<field>=`
// is not a condition.
func (g Grouping) Keys(r *model.CallRow, buf []string) []string {
	buf = buf[:0]
	switch g.Field {
	case GroupByTime:
		i := (r.TsMs - g.OriginMs) / g.BucketMs
		return append(buf, strconv.FormatInt(g.OriginMs+i*g.BucketMs, 10))
	case model.GroupByParamName:
		for name, vs := range r.Params {
			if len(vs) > 0 {
				buf = append(buf, name)
			}
		}
		return buf
	}
	values, ok := model.FieldValues(g.Field, r)
	if !ok {
		return buf
	}
	for _, v := range values {
		if v != "" && !slices.Contains(buf, v) {
			buf = append(buf, v)
		}
	}
	return buf
}

// Values are the aggregates of one group: the members of a /stats row
// before the percentiles are read from the sketch.
type Values struct {
	Count          int64
	Errors         int64
	DurationMsSum  int64
	CPUTimeMsSum   int64
	WaitTimeMsSum  int64
	QueueWaitMsSum int64
	SuspendMsSum   int64
	Sketch         Sketch
}

// add records one call and reports whether its sketch opened a bin.
func (v *Values) add(r *model.CallRow) bool {
	v.Count++
	if r.ErrorFlag {
		v.Errors++
	}
	v.DurationMsSum += int64(r.DurationMs)
	v.CPUTimeMsSum += r.CpuTimeMs
	v.WaitTimeMsSum += r.WaitTimeMs
	v.QueueWaitMsSum += int64(r.QueueWaitMs)
	v.SuspendMsSum += int64(r.SuspendMs)
	return v.Sketch.Add(int64(r.DurationMs))
}

// merge adds o to v and returns how many sketch bins it opened.
func (v *Values) merge(o *Values) int {
	v.Count += o.Count
	v.Errors += o.Errors
	v.DurationMsSum += o.DurationMsSum
	v.CPUTimeMsSum += o.CPUTimeMsSum
	v.WaitTimeMsSum += o.WaitTimeMsSum
	v.QueueWaitMsSum += o.QueueWaitMsSum
	v.SuspendMsSum += o.SuspendMsSum
	return v.Sketch.Merge(&o.Sketch)
}

// metric is the value order ranks by.
func (v *Values) metric(o Order) int64 {
	if o == OrderDurationSumDesc {
		return v.DurationMsSum
	}
	return v.Count
}

func (v *Values) setMetric(o Order, m int64) {
	if o == OrderDurationSumDesc {
		v.DurationMsSum = m
	} else {
		v.Count = m
	}
}

// Totals counts the calls and errors under the conditions, whether or not a
// call falls into a group.
type Totals struct {
	Count  int64 `json:"count"`
	Errors int64 `json:"errors"`
}

// The accounting model of the read memory budget for groups (02 §2.9, §7.5):
// a fixed cost per group plus its key, and a cost per sketch bin, roughly the
// map entry. Estimates, like every charge of §7.5.
const (
	GroupBytes = 256
	BinBytes   = 48
)

// ChargeFunc reserves n more bytes of the read memory budget; a non-nil
// error aborts the aggregation. A nil ChargeFunc charges nothing.
type ChargeFunc func(n int64) error

// Group is one group of a source, with its key.
type Group struct {
	Key string
	Values
}

// Aggregator is one source's /stats aggregation: every matching call goes
// through Add. It keeps at most maxGroups groups of a field grouping; past
// that it runs Space-Saving weighted by the order metric (02 §2.9): the
// lightest group gives its slot to the new value, which inherits that
// group's metric, and the result is approximate. A time grouping is never
// bounded: it holds at most MaxBuckets groups.
type Aggregator struct {
	grouping    Grouping
	order       Order
	maxGroups   int
	charge      ChargeFunc
	groups      map[string]*slot
	heap        slotHeap
	totals      Totals
	approximate bool
	keys        []string // reused by Add
}

type slot struct {
	Group
	index int // position in the heap
	order Order
}

// NewAggregator returns an empty aggregator. maxGroups <= 0 selects
// DefaultMaxGroups.
func NewAggregator(g Grouping, order Order, maxGroups int, charge ChargeFunc) *Aggregator {
	if maxGroups <= 0 {
		maxGroups = DefaultMaxGroups
	}
	if order == "" {
		order = OrderCountDesc
	}
	return &Aggregator{grouping: g, order: order, maxGroups: maxGroups, charge: charge, groups: map[string]*slot{}}
}

func (a *Aggregator) reserve(n int64) error {
	if a.charge == nil || n == 0 {
		return nil
	}
	return a.charge(n)
}

// Add records one call that matched the conditions: once in the totals and
// once in each of its groups. The row may be reused by the caller after Add
// returns: the aggregator copies the keys.
func (a *Aggregator) Add(r *model.CallRow) error {
	a.totals.Count++
	if r.ErrorFlag {
		a.totals.Errors++
	}
	a.keys = a.grouping.Keys(r, a.keys)
	for _, key := range a.keys {
		s, ok := a.groups[key]
		if !ok {
			var err error
			if s, err = a.open(key); err != nil {
				return err
			}
		}
		if s.add(r) {
			if err := a.reserve(BinBytes); err != nil {
				return err
			}
		}
		heap.Fix(&a.heap, s.index)
	}
	return nil
}

// open makes a slot for a new key: a fresh one below the bound, the
// lightest one's past it.
func (a *Aggregator) open(key string) (*slot, error) {
	key = strings.Clone(key)
	if a.grouping.Field == GroupByTime || len(a.groups) < a.maxGroups {
		if err := a.reserve(GroupBytes + int64(len(key))); err != nil {
			return nil, err
		}
		s := &slot{Group: Group{Key: key}, order: a.order}
		a.groups[key] = s
		heap.Push(&a.heap, s)
		return s, nil
	}
	if err := a.reserve(int64(len(key))); err != nil {
		return nil, err
	}
	a.approximate = true
	s := a.heap[0]
	delete(a.groups, s.Key)
	inherited := s.metric(a.order)
	s.Group = Group{Key: key}
	s.setMetric(a.order, inherited)
	a.groups[key] = s
	return s, nil
}

// Groups returns the groups in no particular order.
func (a *Aggregator) Groups() []Group {
	out := make([]Group, 0, len(a.groups))
	for _, s := range a.groups {
		out = append(out, s.Group)
	}
	return out
}

// Totals returns the call and error counts of every added call.
func (a *Aggregator) Totals() Totals { return a.totals }

// Approximate reports whether Space-Saving replaced a group.
func (a *Aggregator) Approximate() bool { return a.approximate }

// slotHeap is a min-heap by the order metric, so the lightest group is at
// the root. Metrics only grow, so Add fixes the touched slot downward.
type slotHeap []*slot

func (h slotHeap) Len() int { return len(h) }
func (h slotHeap) Less(i, j int) bool {
	a, b := h[i].metric(h[i].order), h[j].metric(h[j].order)
	if a != b {
		return a < b
	}
	return h[i].Key > h[j].Key // the key that would rank last goes first
}
func (h slotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *slotHeap) Push(x any) {
	s := x.(*slot)
	s.index = len(*h)
	*h = append(*h, s)
}
func (h *slotHeap) Pop() any {
	old := *h
	n := len(old)
	s := old[n-1]
	*h = old[:n-1]
	return s
}
