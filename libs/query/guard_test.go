package query

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGuardSpan(t *testing.T) {
	s := &Service{cold: &cold.Source{}}
	hour := int64(time.Hour / time.Millisecond)
	podLimit := 366 * 24 * time.Hour
	wide := model.CallsQuery{FromMs: 0, ToMs: 7 * hour}

	rej := s.guardSpan(wide, 6*time.Hour, podLimit)
	require.NotNil(t, rej, "a 7h window with no pruning filter is rejected")
	assert.False(t, rej.HasEstimate, "the span layer fires before the LIST, so no estimate")
	assert.Equal(t, []string{"pod", "namespace", "service", "retention_class", "duration_min_ms", "error_only"}, rej.SuggestedFilters)

	assert.Nil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 6 * hour}, 6*time.Hour, podLimit), "at the limit passes")

	for name, q := range map[string]model.CallsQuery{
		"pod":             {FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.PodTuplesCondition("ns/svc/pod")}},
		"retention_class": {FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.RetentionClassCondition(model.RetentionAnyError)}},
		"duration_min_ms": {FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.DurationCondition(model.OpGe, 1000)}},
		"error_only":      {FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.ErrorCondition(true)}},
	} {
		assert.Nil(t, s.guardSpan(q, 6*time.Hour, podLimit), "%s is a narrowing filter (02 §2.3.2)", name)
	}
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.MethodContainsCondition("x")}}, 6*time.Hour, podLimit),
		"method filters rows, not files, and does not exempt")

	// №28: a filter exempts only if it actually prunes the discovery LIST.
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.DurationCondition(model.OpGe, int64(1))}}, 6*time.Hour, podLimit),
		"duration_min_ms below the first tier bound prunes no class and must not exempt")
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.RetentionClassCondition(model.RetentionClasses...)}}, 6*time.Hour, podLimit),
		"a retention_class filter naming every class prunes nothing and must not exempt")
}

// TestGuardSpanPodCap covers the outer bound on a pod-filtered /calls query
// (PR 708 review #1): a `pod` filter exempts the query from WIDE_RANGE_LIMIT,
// but the window stays capped at PROFILER_MAX_PODS_RANGE so a pod filter cannot
// force cold discovery across an arbitrarily large interval.
func TestGuardSpanPodCap(t *testing.T) {
	s := &Service{cold: &cold.Source{}}
	hour := int64(time.Hour / time.Millisecond)
	day := 24 * hour
	wide := 6 * time.Hour
	podLimit := 366 * 24 * time.Hour
	pod := []string{"ns/svc/pod"}

	// The pod filter still lifts the 6h wide-range limit within the pods range.
	assert.Nil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, Filters: []model.Condition{model.PodTuplesCondition(pod...)}}, wide, podLimit),
		"a 7h pod query clears WIDE_RANGE_LIMIT and stays under the pods range")
	assert.Nil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 366 * day, Filters: []model.Condition{model.PodTuplesCondition(pod...)}}, wide, podLimit),
		"a pod query at PROFILER_MAX_PODS_RANGE passes")

	// Past the pods range the pod query is rejected before cold discovery fans
	// out; reverting guardSpan's pod cap flips this back to nil.
	rej := s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 400 * day, Filters: []model.Condition{model.PodTuplesCondition(pod...)}}, wide, podLimit)
	require.NotNil(t, rej, "a pod query wider than PROFILER_MAX_PODS_RANGE is rejected")
	assert.Equal(t, guardLayerSpan, rej.Layer)
	assert.False(t, rej.HasEstimate, "the span layer fires before the cold LIST")

	// The cap holds when a class-axis filter rides along with the pod filter.
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 400 * day, Filters: []model.Condition{model.PodTuplesCondition(pod...), model.ErrorCondition(true)}},
		wide, podLimit), "a pod + error_only query past the pods range is still rejected")
}

// TestGuardSpanFilterConditions covers the filter conditions of 02 §2.3.3: an
// identity condition with = or in counts as a pod filter, with the same
// PROFILER_MAX_PODS_RANGE cap; a param, a negation, or an upper duration bound
// prunes no file and exempts nothing.
func TestGuardSpanFilterConditions(t *testing.T) {
	s := &Service{cold: &cold.Source{}}
	hour := int64(time.Hour / time.Millisecond)
	day := 24 * hour
	wide, podLimit := 6*time.Hour, 366*24*time.Hour
	query := func(toMs int64, texts ...string) model.CallsQuery {
		q := model.CallsQuery{FromMs: 0, ToMs: toMs}
		for _, text := range texts {
			c, err := model.ParseCondition(text)
			require.NoError(t, err)
			q.Filters = append(q.Filters, c)
		}
		return q
	}

	for _, text := range []string{"namespace=prod", "service in billing,orders", "pod=billing-1"} {
		assert.Nil(t, s.guardSpan(query(7*hour, text), wide, podLimit), "%s narrows like pod", text)
		assert.NotNil(t, s.guardSpan(query(400*day, text), wide, podLimit), "%s is capped like pod", text)
	}
	for _, text := range []string{"request_id=abc", "user.tenant=acme", "service!=billing", "duration<1s", "error=false"} {
		rej := s.guardSpan(query(7*hour, text), wide, podLimit)
		assert.NotNil(t, rej, "%s prunes no file", text)
	}
	assert.Nil(t, s.guardSpan(query(7*hour, "duration>2s"), wide, podLimit), "a lower duration bound prunes classes")

	rej := s.guardSpan(query(7*hour, "request_id=abc"), wide, podLimit)
	require.NotNil(t, rej)
	assert.Contains(t, rej.SuggestedFilters, "service")
	rej = s.guardSpan(query(7*hour, "service!=billing"), wide, podLimit)
	require.NotNil(t, rej)
	assert.Contains(t, rej.SuggestedFilters, "namespace", "a negation is not an identity filter")
}

func TestGuardSpanOverflow(t *testing.T) {
	s := &Service{cold: &cold.Source{}}
	podLimit := 366 * 24 * time.Hour
	// A far-future `to` whose millisecond span multiplied to nanoseconds
	// overflows int64 (PR 708 review #1): the pre-fix guard computed the span
	// as a wrapped-negative time.Duration and let it through.
	wide := model.CallsQuery{FromMs: 1, ToMs: 9999999999999}
	rej := s.guardSpan(wide, 6*time.Hour, podLimit)
	require.NotNil(t, rej, "an overflowing far-future window must still be rejected")
	assert.Equal(t, guardLayerSpan, rej.Layer)

	// A class-axis filter still exempts an overflowing window from the span
	// guard: it prunes the discovery LIST classes, and no pod cap applies.
	assert.Nil(t, s.guardSpan(model.CallsQuery{FromMs: 1, ToMs: 9999999999999, Filters: []model.Condition{model.RetentionClassCondition(model.RetentionAnyError)}}, 6*time.Hour, podLimit),
		"a class filter exempts even an overflowing span")

	// A pod filter no longer waves an overflowing far-future window into cold
	// discovery: the pods-range cap rejects it (PR 708 review #1).
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 1, ToMs: 9999999999999, Filters: []model.Condition{model.PodTuplesCondition("ns/svc/pod")}}, 6*time.Hour, podLimit),
		"a pod filter is capped at PROFILER_MAX_PODS_RANGE and rejects an overflowing span")

	// A reversed window (only reachable through a forged cursor, where
	// ParseWindow's to > from no longer holds) is treated as unbounded, not
	// waved through as a negative span.
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 100, ToMs: 1}, 6*time.Hour, podLimit),
		"a reversed window fails closed")
}

func TestGuardPodsSpan(t *testing.T) {
	day := int64(24 * time.Hour / time.Millisecond)
	limit := 366 * 24 * time.Hour

	assert.Nil(t, guardPodsSpan(0, 30*day, limit), "a 30-day window is within the /pods limit")
	assert.Nil(t, guardPodsSpan(0, 366*day, limit), "at the limit passes")

	rej := guardPodsSpan(0, 400*day, limit)
	require.NotNil(t, rej, "a window past the /pods limit is rejected")
	assert.Equal(t, guardLayerSpan, rej.Layer)
	assert.False(t, rej.HasEstimate, "the /pods span layer fires before any LIST")

	// The year-2100 fan-out from the review (~47000 UTC days of S3 LISTs) is
	// rejected before the first LIST (PR 708 review #3).
	assert.NotNil(t, guardPodsSpan(0, 4102444800000, limit), "the year-2100 /pods window is rejected")
}

func TestGuardCost(t *testing.T) {
	files := []cold.FileRef{
		{Class: model.RetentionShortClean, Size: 700},
		{Class: model.RetentionShortClean, Size: 200},
		{Class: model.RetentionLongClean, Size: 100},
	}
	q := model.CallsQuery{FromMs: 0, ToMs: 1000}

	assert.Nil(t, guardCost(q, files, 3, 1000), "at both limits passes")

	rej := guardCost(q, files, 2, 1000)
	require.NotNil(t, rej, "file count over PROFILER_MAX_SCAN_FILES rejects")
	assert.True(t, rej.HasEstimate)
	assert.Equal(t, 3, rej.EstimatedFiles)
	assert.EqualValues(t, 1000, rej.EstimatedBytes)
	assert.Equal(t, map[string]int64{model.RetentionShortClean: 900, model.RetentionLongClean: 100},
		rej.ByClass, "the per-class split points at the dominant class")

	assert.NotNil(t, guardCost(q, files, 3, 999), "byte total over PROFILER_MAX_SCAN_BYTES rejects")

	partial := model.CallsQuery{FromMs: 0, ToMs: 1000, Filters: []model.Condition{model.ErrorCondition(true)}}
	rej = guardCost(partial, files, 2, 1000)
	require.NotNil(t, rej)
	assert.NotContains(t, rej.SuggestedFilters, "error_only", "filters already present are not re-suggested")
}
