package insight

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/pkg/errors"
)

// The groupings and orders of /stats (02 §2.9). A field grouping takes any
// field the build lists in filter_capabilities.group_by.
const (
	GroupByTime          = stats.GroupByTime
	OrderCountDesc       = stats.OrderCountDesc
	OrderDurationSumDesc = stats.OrderDurationSumDesc
)

// StatsInput selects the calls to aggregate and how to group them.
type StatsInput struct {
	Window  Window
	Filters Filters
	// GroupBy is GroupByTime or a field such as method or service.
	GroupBy string
	// Buckets is the bucket count of a time grouping; zero means the
	// server's default of 60.
	Buckets int
	// Limit is how many groups a field grouping returns; zero means the
	// server's default of 20. The rest are summed into Other.
	Limit int
	// Order ranks a field grouping; empty means OrderCountDesc.
	Order stats.Order
}

// StatsResult is the aggregate of every call that matches the filters in
// the window.
type StatsResult struct {
	Window  ResolvedWindow `json:"window"`
	GroupBy string         `json:"group_by"`
	// BucketMs is the bucket width of a time grouping.
	BucketMs int64 `json:"bucket_ms,omitempty"`
	// Rows are the time buckets in time order, empty ones included, or the
	// top groups of a field grouping.
	Rows []StatsRow `json:"rows"`
	// Other sums the groups past the limit; nil when every group is listed.
	Other *StatsValues `json:"other,omitempty"`
	// Distinct is the number of groups; a lower bound when Approximate.
	Distinct int         `json:"distinct"`
	Totals   StatsTotals `json:"totals"`
	// Approximate reports that a source had more groups than it keeps, so
	// the ranking metric of a row may be overcounted.
	Approximate bool `json:"approximate,omitempty"`
	// Partial names the sources that failed; the figures may miss calls.
	Partial []string `json:"partial,omitempty"`
}

// StatsRow is one group. A time bucket has Start and End; a field group has
// Key, the field's value, so "<field>=<key>" selects its calls.
type StatsRow struct {
	Key   string     `json:"key,omitempty"`
	Start *time.Time `json:"start,omitempty"`
	End   *time.Time `json:"end,omitempty"`
	StatsValues
}

// StatsValues are the figures of a group, durations in milliseconds. The
// percentiles are within 1% of the exact value and nil for an empty group.
type StatsValues struct {
	Count          int64  `json:"count"`
	Errors         int64  `json:"errors"`
	DurationMsSum  int64  `json:"duration_ms_sum"`
	P50Ms          *int64 `json:"p50_ms,omitempty"`
	P95Ms          *int64 `json:"p95_ms,omitempty"`
	P99Ms          *int64 `json:"p99_ms,omitempty"`
	CPUMsSum       int64  `json:"cpu_ms_sum"`
	WaitMsSum      int64  `json:"wait_ms_sum"`
	QueueWaitMsSum int64  `json:"queue_wait_ms_sum"`
	SuspendMsSum   int64  `json:"suspend_ms_sum"`
}

// StatsTotals counts every call and error under the filters.
type StatsTotals struct {
	Count  int64 `json:"count"`
	Errors int64 `json:"errors"`
}

// statsResponse is the wire body of /stats. The key is kept raw because it
// is a number for a time grouping and a string otherwise.
type statsResponse struct {
	GroupBy  string `json:"group_by"`
	BucketMs *int64 `json:"bucket_ms"`
	Rows     []struct {
		Key json.RawMessage `json:"key"`
		stats.RowValues
	} `json:"rows"`
	Other          *stats.RowValues `json:"other"`
	Distinct       int              `json:"distinct"`
	Totals         stats.Totals     `json:"totals"`
	Approximate    bool             `json:"approximate"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// Stats aggregates the calls that match the filters in the window, per time
// bucket or per value of a field: counts, errors, duration sums and
// percentiles, and the CPU, wait, queue, and suspension sums. The hotspots
// and timeline operations build on it.
func (c *Client) Stats(ctx context.Context, in StatsInput) (StatsResult, error) {
	window, err := in.Window.Resolve()
	if err != nil {
		return StatsResult{}, err
	}
	conditions, err := in.Filters.conditions()
	if err != nil {
		return StatsResult{}, err
	}
	query, err := in.params()
	if err != nil {
		return StatsResult{}, err
	}
	if err := c.authorize(ctx, namespaces(conditions)); err != nil {
		return StatsResult{}, err
	}
	for k, v := range window.values() {
		query[k] = v
	}
	if len(conditions) > 0 {
		query["filter"] = conditions
	}
	var resp statsResponse
	if err := c.getJSON(ctx, "/stats", query, &resp); err != nil {
		return StatsResult{}, err
	}
	return resp.result(window)
}

// params checks the grouping inputs and renders them; the server checks the
// rest.
func (in StatsInput) params() (url.Values, error) {
	v := url.Values{}
	if in.GroupBy == "" {
		return nil, errors.New("group by is required: time or a field such as method or service")
	}
	v.Set("group_by", in.GroupBy)
	if in.GroupBy == GroupByTime {
		if in.Limit != 0 || in.Order != "" {
			return nil, errors.New("limit and order apply to a field grouping, not to time")
		}
		if in.Buckets < 0 || in.Buckets > stats.MaxBuckets {
			return nil, errors.Errorf("buckets must be between 1 and %d", stats.MaxBuckets)
		}
		if in.Buckets > 0 {
			v.Set("buckets", strconv.Itoa(in.Buckets))
		}
		return v, nil
	}
	if in.Buckets != 0 {
		return nil, errors.New("buckets apply to the time grouping only")
	}
	if in.Limit < 0 || in.Limit > stats.MaxLimit {
		return nil, errors.Errorf("limit must be between 1 and %d", stats.MaxLimit)
	}
	if in.Limit > 0 {
		v.Set("limit", strconv.Itoa(in.Limit))
	}
	switch in.Order {
	case "":
	case OrderCountDesc, OrderDurationSumDesc:
		v.Set("order", string(in.Order))
	default:
		return nil, errors.Errorf("order %q is not %s or %s", in.Order, OrderCountDesc, OrderDurationSumDesc)
	}
	return v, nil
}

func (r statsResponse) result(window ResolvedWindow) (StatsResult, error) {
	out := StatsResult{
		Window: window, GroupBy: r.GroupBy, Distinct: r.Distinct, Approximate: r.Approximate,
		Totals: StatsTotals{Count: r.Totals.Count, Errors: r.Totals.Errors},
		Rows:   make([]StatsRow, 0, len(r.Rows)),
	}
	if r.BucketMs != nil {
		out.BucketMs = *r.BucketMs
	}
	if r.Partial {
		out.Partial = r.PartialReasons
	}
	if r.Other != nil {
		v := statsValues(*r.Other)
		out.Other = &v
	}
	for _, row := range r.Rows {
		sr := StatsRow{StatsValues: statsValues(row.RowValues)}
		if r.GroupBy == GroupByTime {
			var ms int64
			if err := json.Unmarshal(row.Key, &ms); err != nil {
				return StatsResult{}, errors.Wrap(err, "decode a time bucket of /stats")
			}
			start := time.UnixMilli(ms).UTC()
			end := start.Add(time.Duration(out.BucketMs) * time.Millisecond)
			if end.After(window.To) {
				end = window.To
			}
			sr.Start, sr.End = &start, &end
		} else if err := json.Unmarshal(row.Key, &sr.Key); err != nil {
			return StatsResult{}, errors.Wrap(err, "decode a group key of /stats")
		}
		out.Rows = append(out.Rows, sr)
	}
	return out, nil
}

func statsValues(v stats.RowValues) StatsValues {
	return StatsValues{
		Count: v.Count, Errors: v.Errors, DurationMsSum: v.DurationMsSum,
		P50Ms: v.P50Ms, P95Ms: v.P95Ms, P99Ms: v.P99Ms,
		CPUMsSum: v.CPUTimeMsSum, WaitMsSum: v.WaitTimeMsSum,
		QueueWaitMsSum: v.QueueWaitMsSum, SuspendMsSum: v.SuspendMsSum,
	}
}
