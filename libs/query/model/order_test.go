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
	} {
		got, err := ParseOrder(raw)
		require.NoError(t, err, raw)
		assert.Equal(t, want, got, raw)
	}
	for raw, detail := range map[string]string{
		"duration_asc": "order duration_asc is not implemented",
		"cpu_desc":     "order cpu_desc is not implemented",
		"start":        "unknown order start",
		"size_desc":    "unknown order size_desc",
		"start_down":   "unknown order start_down",
	} {
		_, err := ParseOrder(raw)
		assert.ErrorContains(t, err, detail, raw)
	}

	_, detail := ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "order": {"net_asc"}})
	assert.Equal(t, "order net_asc is not implemented", detail, "the 400 detail names the order")
	q, detail := ParseCallsQuery(url.Values{"from": {"1"}, "to": {"2"}, "order": {"duration_desc"}})
	require.Empty(t, detail)
	assert.Equal(t, "duration_desc", q.Values().Get("order"), "the order reaches the collector")
	q.Order = OrderStartDesc
	assert.False(t, q.Values().Has("order"), "the default order adds no parameter an older collector rejects")
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

	assert.Equal(t, RetentionLongClean, BandFor(bands, Position{DurationMs: 1000}))
	assert.Equal(t, RetentionNormalClean, BandFor(bands, Position{DurationMs: 1000, NoPK: true}),
		"a band stop resumes at the band below the bound")
	assert.Equal(t, RetentionHugeClean, BandFor(bands, Position{DurationMs: 50_000}))
	assert.Equal(t, RetentionShortClean, BandFor(bands, Position{DurationMs: 0}))
}
