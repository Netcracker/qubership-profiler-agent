package insight

import (
	"context"
	"fmt"
	"slices"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/pkg/errors"
)

// Hotspots defaults and caps.
const (
	// DefaultGroupBy groups by the root method, the only call-level field
	// that names code.
	DefaultGroupBy      = "method"
	DefaultHotspotLimit = 10
	MaxHotspotLimit     = 50
	// sampleGroupsMax caps the groups a sample is restricted to, which keeps
	// the restricting condition short enough for a URL.
	sampleGroupsMax = 10
)

// HotspotsInput selects the calls to rank and how.
type HotspotsInput struct {
	Window  Window
	Filters Filters
	// GroupBy is a field of /stats; empty means DefaultGroupBy.
	GroupBy string
	// Limit is how many groups to list; zero means DefaultHotspotLimit.
	Limit int
	// Order ranks the groups; empty means OrderDurationSumDesc, where the
	// time goes.
	Order stats.Order
	// Sample is how many of the slowest calls to read trees from; zero means
	// DefaultSample.
	Sample int
}

// HotspotsResult has two layers. Groups, Other, and Totals are exact: they
// cover every call under the filters. Sample is a sample of the slowest
// calls, and its methods and SQL are not exact.
type HotspotsResult struct {
	Window  ResolvedWindow `json:"window"`
	GroupBy string         `json:"group_by"`
	Order   stats.Order    `json:"order"`
	// Groups are the top groups by Order.
	Groups []StatsRow `json:"groups"`
	// Other sums the groups past the limit; nil when every group is listed.
	Other *StatsValues `json:"other,omitempty"`
	// Distinct is the number of groups; a lower bound when Approximate.
	Distinct int         `json:"distinct"`
	Totals   StatsTotals `json:"totals"`
	// Approximate reports that a source kept only its heaviest groups, so
	// a group's ranking figure may be overcounted.
	Approximate bool `json:"approximate,omitempty"`
	// Partial names the sources that failed; the figures may miss calls.
	Partial []string `json:"partial,omitempty"`
	// Sample is the sampled layer; nil when no call matched.
	Sample *HotspotsSample `json:"sample,omitempty"`
	// Notes say what was cut and which input shows more.
	Notes []string `json:"notes,omitempty"`
}

// HotspotsSample is the sampled layer: methods by self time and SQL,
// summed over the trees of the slowest calls.
type HotspotsSample struct {
	SampleInfo
	// Groups are the keys the sample was restricted to; empty means it was
	// drawn from every call under the filters.
	Groups  []string        `json:"groups,omitempty"`
	Methods []SampledMethod `json:"methods"`
	SQL     []SampledSQL    `json:"sql"`
}

// Hotspots ranks where the time goes. The exact layer groups every call
// under the filters by a field (the root method by default) with counts,
// errors, percentiles, and time sums. The sampled layer reads the trees of
// the slowest calls of the top groups and ranks the methods inside them by
// self time, and their SQL.
func (c *Client) Hotspots(ctx context.Context, in HotspotsInput) (HotspotsResult, error) {
	in, err := in.withDefaults()
	if err != nil {
		return HotspotsResult{}, err
	}
	window, err := in.Window.Resolve()
	if err != nil {
		return HotspotsResult{}, err
	}
	conditions, err := in.Filters.conditions()
	if err != nil {
		return HotspotsResult{}, err
	}
	st, err := c.Stats(ctx, StatsInput{
		Window: Window{From: window.From, To: window.To}, Filters: Filters{Conditions: conditions},
		GroupBy: in.GroupBy, Limit: in.Limit, Order: in.Order,
	})
	if err != nil {
		return HotspotsResult{}, err
	}
	res := HotspotsResult{
		Window: window, GroupBy: in.GroupBy, Order: in.Order, Groups: st.Rows, Other: st.Other,
		Distinct: st.Distinct, Totals: st.Totals, Approximate: st.Approximate, Partial: st.Partial,
	}
	if st.Other != nil {
		res.Notes = append(res.Notes, fmt.Sprintf("%d more groups with %d calls are summed as other; a larger limit lists more",
			st.Distinct-len(st.Rows), st.Other.Count))
	}
	if len(st.Rows) == 0 {
		return res, nil
	}

	sampleConditions, groups, of, err := c.sampleScope(ctx, st, in.GroupBy, conditions, &res.Notes)
	if err != nil {
		return HotspotsResult{}, err
	}
	info, agg, err := c.sample(ctx, window, sampleConditions, in.Sample)
	if err != nil {
		if ctx.Err() != nil {
			return HotspotsResult{}, err
		}
		// The exact layer stands on its own; a sample that cannot be listed
		// leaves it without inner methods rather than failing it.
		res.Notes = append(res.Notes, "the sampled layer failed: "+err.Error())
		return res, nil
	}
	info.Of = of
	sample := &HotspotsSample{
		SampleInfo: info, Groups: groups,
		Methods: agg.rankedMethods(), SQL: agg.rankedSQL(),
	}
	if n := len(sample.Methods); n > topListSize {
		sample.Methods = sample.Methods[:topListSize]
		res.Notes = append(res.Notes, fmt.Sprintf("the sampled methods list the top %d of %d; explain a sampled call to see the rest", topListSize, n))
	}
	if n := len(sample.SQL); n > topListSize {
		sample.SQL = sample.SQL[:topListSize]
		res.Notes = append(res.Notes, fmt.Sprintf("the sampled SQL lists the top %d of %d statements; explain a sampled call to see the rest", topListSize, n))
	}
	if int64(in.Sample) < of {
		res.Notes = append(res.Notes, fmt.Sprintf("the sample reads %d of %d calls; a larger sample reads more", in.Sample, of))
	}
	res.Sample = sample
	return res, nil
}

// sampleScope picks the calls the sample is drawn from. When every group is
// listed, that is every call under the filters. When groups were cut, the
// sample is restricted to the top listed groups, so it explains the rows
// shown rather than a group past the cut; it returns the keys and their
// exact call count. A build that cannot select the groups in one condition
// leaves the sample unrestricted, with a note.
func (c *Client) sampleScope(ctx context.Context, st StatsResult, field string, conditions []string, notes *[]string) ([]string, []string, int64, error) {
	if st.Other == nil {
		return conditions, nil, st.Totals.Count, nil
	}
	keys := make([]string, 0, min(len(st.Rows), sampleGroupsMax))
	for _, row := range capList(st.Rows, sampleGroupsMax) {
		keys = append(keys, row.Key)
	}
	caps, err := c.capabilities(ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	cond, covered := groupCondition(caps, field, keys)
	if cond == "" {
		*notes = append(*notes, fmt.Sprintf("the sample is drawn from every call under the filters, not only the listed groups: "+
			"the server cannot select %s values in one condition", field))
		return conditions, nil, st.Totals.Count, nil
	}
	if len(covered) < len(st.Rows) {
		*notes = append(*notes, fmt.Sprintf("the sample is drawn from the top %d of the %d listed groups", len(covered), len(st.Rows)))
	}
	var of int64
	for _, row := range st.Rows {
		if slices.Contains(covered, row.Key) {
			of += row.Count
		}
	}
	return append(slices.Clone(conditions), cond), covered, of, nil
}

func (in HotspotsInput) withDefaults() (HotspotsInput, error) {
	if in.GroupBy == "" {
		in.GroupBy = DefaultGroupBy
	}
	if in.GroupBy == GroupByTime {
		return in, errors.New("hotspots groups by a field such as method or service; the timeline operation groups by time")
	}
	if in.Limit == 0 {
		in.Limit = DefaultHotspotLimit
	}
	if in.Limit < 0 || in.Limit > MaxHotspotLimit {
		return in, errors.Errorf("limit must be between 1 and %d", MaxHotspotLimit)
	}
	if in.Order == "" {
		in.Order = OrderDurationSumDesc
	}
	if in.Order != OrderDurationSumDesc && in.Order != OrderCountDesc {
		return in, errors.Errorf("order must be %s or %s, not %q", OrderDurationSumDesc, OrderCountDesc, in.Order)
	}
	var err error
	in.Sample, err = checkSample(in.Sample)
	return in, err
}
