package stats

import (
	"sort"
	"strconv"
	"strings"
)

// Merger combines the groups of every source of one /stats request: the hot
// replicas and the cold scan. Sketches merge by adding bins, so the merged
// percentiles are those of one sketch over every call (02 §2.9). The sources
// are bounded already, so the merger keeps every group it receives.
type Merger struct {
	groups      map[string]*Values
	totals      Totals
	approximate bool
	charge      ChargeFunc
}

// NewMerger returns an empty merger that charges the groups and bins it
// opens to charge (nil charges nothing).
func NewMerger(charge ChargeFunc) *Merger {
	return &Merger{groups: map[string]*Values{}, charge: charge}
}

// AddSource folds one source's result into the merger.
func (m *Merger) AddSource(groups []Group, totals Totals, approximate bool) error {
	m.totals.Count += totals.Count
	m.totals.Errors += totals.Errors
	m.approximate = m.approximate || approximate
	for i := range groups {
		g := &groups[i]
		v, ok := m.groups[g.Key]
		if !ok {
			if err := m.reserve(GroupBytes + int64(len(g.Key))); err != nil {
				return err
			}
			v = &Values{}
			m.groups[strings.Clone(g.Key)] = v
		}
		if bins := v.merge(&g.Values); bins > 0 {
			if err := m.reserve(int64(bins) * BinBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Merger) reserve(n int64) error {
	if m.charge == nil {
		return nil
	}
	return m.charge(n)
}

// Response ranks and cuts the merged groups into the external body. The
// partial members are left for the caller.
func (m *Merger) Response(r Request) Response {
	out := Response{GroupBy: r.GroupBy, Totals: m.totals, Approximate: m.approximate, PartialReasons: []string{}}
	if r.GroupBy == GroupByTime {
		m.timeRows(r, &out)
		return out
	}
	keys := make([]string, 0, len(m.groups))
	for k := range m.groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := m.groups[keys[i]].metric(r.Order), m.groups[keys[j]].metric(r.Order)
		if a != b {
			return a > b
		}
		return keys[i] < keys[j]
	})
	out.Distinct = len(keys)
	n := min(r.Limit, len(keys))
	out.Rows = make([]Row, 0, n)
	for _, k := range keys[:n] {
		out.Rows = append(out.Rows, Row{Key: k, RowValues: rowValues(m.groups[k])})
	}
	if len(keys) > n {
		var other Values
		for _, k := range keys[n:] {
			other.merge(m.groups[k])
		}
		rv := rowValues(&other)
		out.Other = &rv
	}
	return out
}

// timeRows emits one row per bucket, in time order, empty buckets included.
func (m *Merger) timeRows(r Request, out *Response) {
	g := r.Grouping()
	bucket := g.BucketMs
	out.BucketMs = &bucket
	span := uint64(r.Query.ToMs) - uint64(r.Query.FromMs)
	n := int(span / uint64(bucket))
	if span%uint64(bucket) != 0 {
		n++
	}
	out.Distinct = n
	out.Rows = make([]Row, 0, n)
	for i := 0; i < n; i++ {
		start := g.OriginMs + int64(i)*bucket
		v := m.groups[strconv.FormatInt(start, 10)]
		if v == nil {
			v = &Values{}
		}
		out.Rows = append(out.Rows, Row{Key: start, RowValues: rowValues(v)})
	}
}

func rowValues(v *Values) RowValues {
	return RowValues{
		Count: v.Count, Errors: v.Errors, DurationMsSum: v.DurationMsSum,
		P50Ms: v.Sketch.QuantileMs(0.50), P95Ms: v.Sketch.QuantileMs(0.95), P99Ms: v.Sketch.QuantileMs(0.99),
		CPUTimeMsSum: v.CPUTimeMsSum, WaitTimeMsSum: v.WaitTimeMsSum,
		QueueWaitMsSum: v.QueueWaitMsSum, SuspendMsSum: v.SuspendMsSum,
	}
}
