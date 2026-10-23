package insight

import (
	"context"
	"time"

	"github.com/pkg/errors"
)

// DefaultTimelineBuckets splits a window into 30 buckets: 2 minutes each over
// the default hour, fine enough to place a change and short enough for a
// model's context. The server's own default of 60 serves a histogram.
const DefaultTimelineBuckets = 30

// TimelineInput selects the calls to count over time.
type TimelineInput struct {
	Window  Window
	Filters Filters
	// Buckets is the bucket count; zero means DefaultTimelineBuckets.
	Buckets int
}

// TimelineResult counts the calls under the filters per time bucket. Every
// figure is exact.
type TimelineResult struct {
	Window   ResolvedWindow `json:"window"`
	BucketMs int64          `json:"bucket_ms"`
	// Buckets are in time order, empty ones included.
	Buckets []TimelineBucket `json:"buckets"`
	Totals  StatsTotals      `json:"totals"`
	// Partial names the sources that failed; the counts may miss calls.
	Partial []string `json:"partial,omitempty"`
}

// TimelineBucket is one time bucket. The percentiles are nil for an empty
// bucket.
type TimelineBucket struct {
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
	Count  int64     `json:"count"`
	Errors int64     `json:"errors"`
	P50Ms  *int64    `json:"p50_ms,omitempty"`
	P95Ms  *int64    `json:"p95_ms,omitempty"`
	P99Ms  *int64    `json:"p99_ms,omitempty"`
}

// Timeline counts the calls, the errors, and the duration percentiles per
// time bucket, to find when a change started.
func (c *Client) Timeline(ctx context.Context, in TimelineInput) (TimelineResult, error) {
	buckets := in.Buckets
	if buckets == 0 {
		buckets = DefaultTimelineBuckets
	}
	st, err := c.Stats(ctx, StatsInput{Window: in.Window, Filters: in.Filters, GroupBy: GroupByTime, Buckets: buckets})
	if err != nil {
		return TimelineResult{}, err
	}
	res := TimelineResult{
		Window: st.Window, BucketMs: st.BucketMs, Totals: st.Totals, Partial: st.Partial,
		Buckets: make([]TimelineBucket, 0, len(st.Rows)),
	}
	for _, row := range st.Rows {
		if row.Start == nil || row.End == nil {
			return TimelineResult{}, errors.New("a /stats time bucket has no start")
		}
		res.Buckets = append(res.Buckets, TimelineBucket{
			Start: *row.Start, End: *row.End, Count: row.Count, Errors: row.Errors,
			P50Ms: row.P50Ms, P95Ms: row.P95Ms, P99Ms: row.P99Ms,
		})
	}
	return res, nil
}
