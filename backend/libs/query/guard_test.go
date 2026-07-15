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
	hour := int64(time.Hour / time.Millisecond)
	wide := model.CallsQuery{FromMs: 0, ToMs: 7 * hour}

	rej := guardSpan(wide, 6*time.Hour)
	require.NotNil(t, rej, "a 7h window with no pruning filter is rejected")
	assert.False(t, rej.HasEstimate, "the span layer fires before the LIST, so no estimate")
	assert.Equal(t, []string{"pod", "retention_class", "duration_min_ms", "error_only"}, rej.SuggestedFilters)

	assert.Nil(t, guardSpan(model.CallsQuery{FromMs: 0, ToMs: 6 * hour}, 6*time.Hour), "at the limit passes")

	for name, q := range map[string]model.CallsQuery{
		"pod":             {FromMs: 0, ToMs: 7 * hour, Pods: []string{"ns/svc/pod"}},
		"retention_class": {FromMs: 0, ToMs: 7 * hour, RetentionClasses: []string{model.RetentionAnyError}},
		"duration_min_ms": {FromMs: 0, ToMs: 7 * hour, DurationMinMs: 1000},
		"error_only":      {FromMs: 0, ToMs: 7 * hour, ErrorOnly: true},
	} {
		assert.Nil(t, guardSpan(q, 6*time.Hour), "%s is a narrowing filter (02 §2.3.2)", name)
	}
	assert.NotNil(t, guardSpan(model.CallsQuery{FromMs: 0, ToMs: 7 * hour, Method: "x"}, 6*time.Hour),
		"method filters rows, not files, and does not exempt")
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
