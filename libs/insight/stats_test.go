package insight

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatsByFieldSendsTheGroupingAndMapsRows(t *testing.T) {
	c, r := newReplay(t, nil, ok("stats_service.json"))
	res, err := c.Stats(context.Background(), StatsInput{
		Window:  Window{Since: time.Hour},
		Filters: Filters{Conditions: []string{"duration>100ms"}, Service: "prod/billing"},
		GroupBy: "service", Limit: 1, Order: OrderDurationSumDesc,
	})
	require.NoError(t, err)

	q := r.reqs[0].URL.Query()
	assert.Equal(t, "/api/v1/stats", r.reqs[0].URL.Path)
	assert.Equal(t, "service", q.Get("group_by"))
	assert.Equal(t, "1", q.Get("limit"))
	assert.Equal(t, "duration_sum_desc", q.Get("order"))
	assert.False(t, q.Has("buckets"))
	assert.Equal(t, []string{"duration>100ms", "namespace=prod", "service=billing"}, q["filter"])
	assert.NotEmpty(t, q.Get("from"))

	assert.Equal(t, time.Hour, res.Window.To.Sub(res.Window.From))
	require.Len(t, res.Rows, 1)
	row := res.Rows[0]
	assert.Equal(t, "billing", row.Key)
	assert.Nil(t, row.Start)
	assert.Equal(t, int64(1200), row.Count)
	assert.Equal(t, int64(201000), row.CPUMsSum)
	assert.Equal(t, int64(950), *row.P95Ms)
	require.NotNil(t, res.Other)
	assert.Equal(t, int64(40), res.Other.Count)
	assert.Equal(t, 3, res.Distinct)
	assert.Equal(t, StatsTotals{Count: 1240, Errors: 14}, res.Totals)
	assert.Equal(t, []string{"collector 10.0.0.7 stats: timeout"}, res.Partial)
}

func TestStatsByTimeMapsBuckets(t *testing.T) {
	c, r := newReplay(t, nil, ok("stats_time.json"))
	to := time.UnixMilli(1760433300000).UTC()
	res, err := c.Stats(context.Background(), StatsInput{
		Window:  Window{From: to.Add(-15 * time.Minute), To: to},
		GroupBy: GroupByTime, Buckets: 2,
	})
	require.NoError(t, err)
	q := r.reqs[0].URL.Query()
	assert.Equal(t, "2", q.Get("buckets"))
	assert.False(t, q.Has("limit"))
	assert.False(t, q.Has("order"))

	assert.Equal(t, int64(600_000), res.BucketMs)
	require.Len(t, res.Rows, 2)
	assert.Empty(t, res.Rows[0].Key)
	assert.Equal(t, time.UnixMilli(1760432400000).UTC(), *res.Rows[0].Start)
	assert.Equal(t, time.UnixMilli(1760433000000).UTC(), *res.Rows[0].End)
	assert.Equal(t, to, *res.Rows[1].End, "the last bucket ends at the window's end")
	assert.Nil(t, res.Rows[1].P50Ms, "an empty bucket has no percentiles")
	assert.Nil(t, res.Other)
	assert.Nil(t, res.Partial)
}

func TestStatsChecksTheGroupingBeforeAnyRequest(t *testing.T) {
	c, r := newReplay(t, nil)
	for _, in := range []StatsInput{
		{},
		{GroupBy: GroupByTime, Limit: 5},
		{GroupBy: GroupByTime, Order: OrderCountDesc},
		{GroupBy: GroupByTime, Buckets: 1001},
		{GroupBy: "service", Buckets: 5},
		{GroupBy: "service", Limit: 1001},
		{GroupBy: "service", Order: "start_desc"},
		{GroupBy: "service", Filters: Filters{Conditions: []string{"no operator"}}},
	} {
		_, err := c.Stats(context.Background(), in)
		assert.Error(t, err, "%+v", in)
	}
	assert.Empty(t, r.reqs)
}

func TestStatsMapsProblems(t *testing.T) {
	c, _ := newReplay(t, nil, reply{status: 400, fixture: "problem_query_too_wide.json"})
	_, err := c.Stats(context.Background(), StatsInput{GroupBy: "method"})
	assert.True(t, IsCode(err, CodeQueryTooWide))
	assert.ErrorContains(t, err, "Add one of these conditions")
}
