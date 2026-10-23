package model

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOrder(t *testing.T) {
	for raw, want := range map[string]Order{
		"":              OrderStartDesc,
		"start_desc":    OrderStartDesc,
		"duration_desc": OrderDurationDesc,
		"duration_asc":  OrderDurationAsc,
		"start_asc":     "start_asc",
		"cpu_desc":      "cpu_desc",
		"queue_asc":     "queue_asc",
		"calls_desc":    "calls_desc",
		"net_asc":       "net_asc",
	} {
		got, err := ParseOrder(raw)
		require.NoError(t, err, raw)
		assert.Equal(t, want, got, raw)
	}
	for raw, detail := range map[string]string{
		"start":      "unknown order start",
		"size_desc":  "unknown order size_desc",
		"start_down": "unknown order start_down",
	} {
		_, err := ParseOrder(raw)
		assert.ErrorContains(t, err, detail, raw)
	}
	assert.Len(t, ImplementedOrders, 12, "every field in both directions")

	q, detail := ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "order": {"net_asc"}})
	require.Empty(t, detail)
	assert.Equal(t, "net_asc", q.Values().Get("order"), "the order reaches the collector")
	q.Order = OrderStartDesc
	assert.False(t, q.Values().Has("order"), "the default order adds no parameter an older collector rejects")
}

func TestOrdersCompareTheirKeyThenPK(t *testing.T) {
	low := CallRow{PK: pk("b", 1, 0, 0, 0), TsMs: 1, DurationMs: 1, CpuTimeMs: 1, QueueWaitMs: 1, ChildCalls: 1, NetRead: 1}
	high := CallRow{PK: pk("a", 1, 0, 0, 0), TsMs: 9, DurationMs: 9, CpuTimeMs: 9, QueueWaitMs: 9, ChildCalls: 9, NetWritten: 9}
	for _, o := range ImplementedOrders {
		assert.Equal(t, o.Desc(), o.Before(high.Position(), low.Position()), o)
		assert.Equal(t, !o.Desc(), o.Before(low.Position(), high.Position()), o)
		tie := high
		tie.PK = pk("c", 1, 0, 0, 0)
		assert.True(t, o.Before(high.Position(), tie.Position()), "%s: a tie falls back to pk ASC", o)

		bound := o.PositionAt(o.Key(high.Position()))
		assert.False(t, o.Past(high.Position(), bound), "%s: a position with no PK does not pass its own key", o)
		assert.Equal(t, !o.Desc(), o.Past(Position{TsMs: 10, DurationMs: 10, CpuTimeMs: 10, QueueWaitMs: 10,
			ChildCalls: 10, NetBytes: 10}, bound), o)
	}
	assert.EqualValues(t, 10, OrderStartDesc.Key(CallRow{NetRead: 4, NetWritten: 6, TsMs: 10}.Position()))
	assert.EqualValues(t, 10, Order("net_desc").Key(CallRow{NetRead: 4, NetWritten: 6}.Position()),
		"net sorts by net_read + net_written")
}

func TestParseAfter(t *testing.T) {
	pkText := "ns:svc:pod:1:2:3:4"
	for _, tc := range []struct {
		order  Order
		params url.Values
		want   *Position
		detail string
	}{
		{OrderStartDesc, url.Values{}, nil, ""},
		{"start_asc", url.Values{"after_ts_ms": {"5"}, "after_pk": {pkText}},
			&Position{TsMs: 5, PK: PK{"ns", "svc", "pod", 1, 2, 3, 4}}, ""},
		{OrderStartDesc, url.Values{"after_ts_ms": {"5"}}, nil, "after_ts_ms and after_pk must be sent together"},
		{OrderDurationAsc, url.Values{"after_duration_ms": {"7"}}, &Position{DurationMs: 7, NoPK: true}, ""},
		{OrderDurationDesc, url.Values{"after_duration_ms": {"-1"}}, nil, "must be a duration"},
		{"cpu_desc", url.Values{"after_key": {"9"}, "after_pk": {pkText}},
			&Position{CpuTimeMs: 9, PK: PK{"ns", "svc", "pod", 1, 2, 3, 4}}, ""},
		{"net_asc", url.Values{"after_key": {"9"}}, &Position{NetBytes: 9, NoPK: true}, ""},
		{"calls_asc", url.Values{"after_key": {"99999999999"}}, nil, "out of range"},
		{"queue_desc", url.Values{"after_pk": {pkText}}, nil, "after_pk needs after_key"},
		{"queue_desc", url.Values{"after_key": {"x"}}, nil, "after_key must be an integer"},
	} {
		got, detail := ParseAfter(tc.order, tc.params)
		if tc.detail != "" {
			assert.Contains(t, detail, tc.detail, "%s %v", tc.order, tc.params)
			continue
		}
		assert.Empty(t, detail)
		assert.Equal(t, tc.want, got, "%s %v", tc.order, tc.params)
	}
}

func TestDurationOrderComparesDurationThenPK(t *testing.T) {
	o := OrderDurationDesc
	long := Position{TsMs: 1, DurationMs: 500, PK: pk("b", 1, 0, 0, 0)}
	short := Position{TsMs: 9, DurationMs: 100, PK: pk("a", 1, 0, 0, 0)}
	assert.True(t, o.Before(long, short), "the longer call sorts first whatever its start")
	tieA := Position{DurationMs: 100, PK: pk("a", 1, 0, 0, 0)}
	tieB := Position{DurationMs: 100, PK: pk("b", 1, 0, 0, 0)}
	assert.True(t, o.Before(tieA, tieB), "equal durations fall back to pk ASC")
	assert.True(t, o.Past(tieB, tieA))
	assert.False(t, o.Past(tieA, tieA), "the cursor row itself is not past the cursor")

	bound := Position{DurationMs: 100, NoPK: true}
	assert.False(t, o.Past(tieA, bound), "a position with no PK does not pass its own duration")
	assert.True(t, o.Past(Position{DurationMs: 99, PK: pk("a", 1, 0, 0, 0)}, bound))
	assert.True(t, o.Before(tieB, bound))
}

func TestMergeRunsByDuration(t *testing.T) {
	r := func(d int32, pod string, tier Tier) CallRow {
		return CallRow{PK: pk(pod, 1, 0, 0, 0), TsMs: int64(1000 - d), DurationMs: d, Tier: tier}
	}
	hot := []CallRow{r(900, "a", TierHot), r(300, "b", TierHot), r(300, "c", TierHot)}
	cold := []CallRow{r(500, "d", TierCold), r(300, "b", TierCold), r(10, "e", TierCold)}
	merged, more := MergeRunsBy(OrderDurationDesc, [][]CallRow{hot, cold}, 4)
	require.Len(t, merged, 4)
	assert.True(t, more)
	var got []string
	for _, row := range merged {
		got = append(got, row.PK.PodName)
	}
	assert.Equal(t, []string{"a", "d", "b", "c"}, got)
	assert.Equal(t, TierCold, merged[2].Tier, "the duplicate keeps its cold copy (02 §6.3)")
}

func TestDurationBands(t *testing.T) {
	bands := DurationBands(nil)
	assert.Equal(t, []DurationBand{
		{Class: RetentionHugeClean, LowerMs: 10_000, UpperMs: bands[0].UpperMs},
		{Class: RetentionLongClean, LowerMs: 1000, UpperMs: 10_000},
		{Class: RetentionNormalClean, LowerMs: 100, UpperMs: 1000},
		{Class: RetentionShortClean, LowerMs: 0, UpperMs: 100},
	}, bands)

	assert.Equal(t, RetentionLongClean, BandFor(OrderDurationDesc, bands, Position{DurationMs: 1000}))
	assert.Equal(t, RetentionNormalClean, BandFor(OrderDurationDesc, bands, Position{DurationMs: 1000, NoPK: true}),
		"a band stop resumes at the band below the bound")
	assert.Equal(t, RetentionHugeClean, BandFor(OrderDurationDesc, bands, Position{DurationMs: 50_000}))
	assert.Equal(t, RetentionShortClean, BandFor(OrderDurationDesc, bands, Position{DurationMs: 0}))
	assert.Equal(t, RetentionShortClean, BandFor(OrderDurationDesc, bands, Position{DurationMs: 0, NoPK: true}))

	asc := OrderDurationAsc.WalkBands(bands)
	assert.Equal(t, RetentionShortClean, asc[0].Class, "duration_asc reads the shortest band first")
	assert.Equal(t, RetentionLongClean, BandFor(OrderDurationAsc, asc, Position{DurationMs: 1000}))
	assert.Equal(t, RetentionLongClean, BandFor(OrderDurationAsc, asc, Position{DurationMs: 999, NoPK: true}),
		"a band stop resumes at the band above the bound")
	assert.EqualValues(t, 999, OrderDurationAsc.Frontier(asc[1]))
	assert.EqualValues(t, 100, OrderDurationDesc.Frontier(bands[2]))
	assert.True(t, OrderDurationAsc.Reaches(999, 999))
	assert.False(t, OrderDurationAsc.Reaches(1000, 999))
	assert.True(t, OrderDurationDesc.Reaches(100, 100))
	assert.False(t, OrderDurationDesc.Reaches(99, 100))
}
