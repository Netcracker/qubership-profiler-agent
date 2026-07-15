package cold

import (
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseKey(t *testing.T) {
	// A real seal-pass key: the replica token itself contains dashes.
	key := "parquet/v1/normal_clean/2026/04/23/14/collector-2-a7f3b2c1-20260423T140000Z-20260423T140003Z-20260423T140457Z-3.parquet"
	ref, ok := ParseKey(key, 1234)
	require.True(t, ok)
	assert.Equal(t, "normal_clean", ref.Class)
	assert.EqualValues(t, 1234, ref.Size)
	assert.Equal(t, "collector-2", ref.Replica, "the replica keeps its own dashes")
	assert.Equal(t, "a7f3b2c1", ref.Hash)
	wantMin := time.Date(2026, 4, 23, 14, 0, 3, 0, time.UTC).UnixMilli()
	wantMax := time.Date(2026, 4, 23, 14, 4, 57, 0, time.UTC).UnixMilli() + 999
	assert.Equal(t, wantMin, ref.TimeMinMs)
	assert.Equal(t, wantMax, ref.TimeMaxMs, "timeMax widens to the end of its truncated second")

	// A compacted object keys the reserved MaintainReplica token; its hash
	// covers the compaction's inputs, so the point-fetch path reads it whole.
	compacted := "parquet/v1/normal_clean/2026/04/23/14/maintain-9c2e-20260423T140000Z-20260423T140003Z-20260423T140457Z-0.parquet"
	ref, ok = ParseKey(compacted, 1)
	require.True(t, ok)
	assert.Equal(t, MaintainReplica, ref.Replica)
	assert.Equal(t, "9c2e", ref.Hash)

	for _, bad := range []string{
		"parquet/v1/normal_clean/2026/04/23/14/garbage.parquet",
		"parquet/v1/normal_clean/2026/04/23/14/collector-0-a7f3-20260423T140000Z-20260423T140003Z-20260423T140457Z-3.txt",
		"parquet/v1/unknown_class/2026/04/23/14/collector-0-a7f3-20260423T140000Z-20260423T140003Z-20260423T140457Z-3.parquet",
		"parquet/v2/normal_clean/2026/04/23/14/collector-0-a7f3-20260423T140000Z-20260423T140003Z-20260423T140457Z-3.parquet",
		"parquet/v1/normal_clean/2026/04/23/collector-0-a7f3-20260423T140000Z-20260423T140003Z-20260423T140457Z-3.parquet",
		"parquet/v1/normal_clean/2026/04/23/14/collector-0-a7f3-notastamp-20260423T140003Z-20260423T140457Z-3.parquet",
		"parquet/v1/normal_clean/2026/04/23/14/collector-0-a7f3-20260423T140000Z-20260423T140003Z-20260423T140457Z-x.parquet",
	} {
		_, ok := ParseKey(bad, 1)
		assert.False(t, ok, "must reject %s", bad)
	}
}

func TestHourWalk(t *testing.T) {
	h := func(hh, mm int) int64 {
		return time.Date(2026, 4, 23, hh, mm, 0, 0, time.UTC).UnixMilli()
	}
	// Mid-hour bounds cover both partial hours.
	hours := hourWalk(h(14, 30), h(16, 30))
	require.Len(t, hours, 3)
	assert.Equal(t, 14, hours[0].Hour())
	assert.Equal(t, 16, hours[2].Hour())

	// An exclusive `to` on the exact hour boundary does not list that hour.
	hours = hourWalk(h(14, 0), h(15, 0))
	require.Len(t, hours, 1)
	assert.Equal(t, 14, hours[0].Hour())

	assert.Empty(t, hourWalk(h(14, 0), h(14, 0)))
}

func TestClassesForPrunes(t *testing.T) {
	all := model.RetentionClasses
	assert.Equal(t, all, ClassesFor(model.CallsQuery{}), "no filter lists all five classes")

	assert.Equal(t, []string{model.RetentionAnyError, model.RetentionCorrupted},
		ClassesFor(model.CallsQuery{ErrorOnly: true}), "error_only keeps the error classes")

	assert.Equal(t, []string{model.RetentionLongClean, model.RetentionAnyError, model.RetentionCorrupted},
		ClassesFor(model.CallsQuery{DurationMinMs: 1000}),
		"duration_min_ms >= 1000 drops short_clean and normal_clean, error classes carry any duration")

	assert.Equal(t, []string{model.RetentionNormalClean, model.RetentionLongClean, model.RetentionAnyError, model.RetentionCorrupted},
		ClassesFor(model.CallsQuery{DurationMinMs: 100}), "the 100ms threshold drops only short_clean")

	assert.Equal(t, all, ClassesFor(model.CallsQuery{DurationMinMs: 99}),
		"a threshold below 100ms prunes nothing: short_clean can hold such calls")

	assert.Equal(t, []string{model.RetentionShortClean},
		ClassesFor(model.CallsQuery{RetentionClasses: []string{model.RetentionShortClean}}),
		"an explicit class filter selects prefixes verbatim")

	assert.Equal(t, []string{model.RetentionAnyError},
		ClassesFor(model.CallsQuery{RetentionClasses: []string{model.RetentionShortClean, model.RetentionAnyError}, ErrorOnly: true}),
		"filters intersect")
}
