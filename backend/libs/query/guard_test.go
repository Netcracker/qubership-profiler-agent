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
	wide := model.CallsQuery{FromMs: 0, ToMs: 7 * hour}

	rej := s.guardSpan(wide, 6*time.Hour)
	require.NotNil(t, rej, "a 7h window with no pruning filter is rejected")
	assert.False(t, rej.HasEstimate, "the span layer fires before the LIST, so no estimate")
	assert.Equal(t, []string{"pod", "retention_class", "duration_min_ms", "error_only"}, rej.SuggestedFilters)

	assert.Nil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 6 * hour}, 6*time.Hour), "at the limit passes")

	for name, q := range map[string]model.CallsQuery{
		"pod":             {FromMs: 0, ToMs: 7 * hour, Pods: []string{"ns/svc/pod"}},
		"retention_class": {FromMs: 0, ToMs: 7 * hour, RetentionClasses: []string{model.RetentionAnyError}},
		"duration_min_ms": {FromMs: 0, ToMs: 7 * hour, DurationMinMs: 1000},
		"error_only":      {FromMs: 0, ToMs: 7 * hour, ErrorOnly: true},
	} {
		assert.Nil(t, s.guardSpan(q, 6*time.Hour), "%s is a narrowing filter (02 §2.3.2)", name)
	}
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, Method: "x"}, 6*time.Hour),
		"method filters rows, not files, and does not exempt")

	// №28: a filter exempts only if it actually prunes the discovery LIST.
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, DurationMinMs: 1}, 6*time.Hour),
		"duration_min_ms below the first tier bound prunes no class and must not exempt")
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour,
		RetentionClasses: model.RetentionClasses}, 6*time.Hour),
		"a retention_class filter naming every class prunes nothing and must not exempt")
}

func TestGuardSpanOverflow(t *testing.T) {
	s := &Service{cold: &cold.Source{}}
	// A far-future `to` whose millisecond span multiplied to nanoseconds
	// overflows int64 (PR 708 review #1): the pre-fix guard computed the span
	// as a wrapped-negative time.Duration and let it through.
	wide := model.CallsQuery{FromMs: 1, ToMs: 9999999999999}
	rej := s.guardSpan(wide, 6*time.Hour)
	require.NotNil(t, rej, "an overflowing far-future window must still be rejected")
	assert.Equal(t, guardLayerSpan, rej.Layer)

	// The exemption still holds for an overflowing window with a pruning filter.
	assert.Nil(t, s.guardSpan(model.CallsQuery{FromMs: 1, ToMs: 9999999999999,
		Pods: []string{"ns/svc/pod"}}, 6*time.Hour), "a pod filter exempts even an overflowing span")

	// A reversed window (only reachable through a forged cursor, where
	// ParseWindow's to > from no longer holds) is treated as unbounded, not
	// waved through as a negative span.
	assert.NotNil(t, s.guardSpan(model.CallsQuery{FromMs: 100, ToMs: 1}, 6*time.Hour),
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

	partial := model.CallsQuery{FromMs: 0, ToMs: 1000, ErrorOnly: true}
	rej = guardCost(partial, files, 2, 1000)
	require.NotNil(t, rej)
	assert.NotContains(t, rej.SuggestedFilters, "error_only", "filters already present are not re-suggested")
}
