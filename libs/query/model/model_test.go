package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func pk(pod string, restart int64, file, off, rec int32) PK {
	return PK{PodNamespace: "ns", PodService: "svc", PodName: pod,
		RestartTimeMs: restart, TraceFileIndex: file, BufferOffset: off, RecordIndex: rec}
}

func TestPKCompareIsBinaryAndComponentOrdered(t *testing.T) {
	// Byte-wise collation: "Z" (0x5A) sorts before "a" (0x61), unlike any
	// case-insensitive locale ordering (02 §2.3.1).
	assert.Negative(t, pk("Zebra", 1, 0, 0, 0).Compare(pk("apple", 1, 0, 0, 0)))
	// Component order: pod_name decides before restart_time_ms.
	assert.Negative(t, pk("a", 9, 9, 9, 9).Compare(pk("b", 1, 0, 0, 0)))
	// The INT32 tail compares numerically, component by component.
	assert.Negative(t, pk("a", 1, 1, 5, 5).Compare(pk("a", 1, 2, 0, 0)))
	assert.Negative(t, pk("a", 1, 1, 5, 5).Compare(pk("a", 1, 1, 6, 0)))
	assert.Negative(t, pk("a", 1, 1, 5, 5).Compare(pk("a", 1, 1, 5, 6)))
	assert.Zero(t, pk("a", 1, 1, 5, 5).Compare(pk("a", 1, 1, 5, 5)))
}

func TestPositionOrderTsDescPkAsc(t *testing.T) {
	newer := Position{TsMs: 200, PK: pk("b", 1, 0, 0, 0)}
	older := Position{TsMs: 100, PK: pk("a", 1, 0, 0, 0)}
	assert.True(t, newer.Before(older), "higher ts_ms sorts first")
	tieA := Position{TsMs: 100, PK: pk("a", 1, 0, 0, 0)}
	tieB := Position{TsMs: 100, PK: pk("b", 1, 0, 0, 0)}
	assert.True(t, tieA.Before(tieB), "equal ts_ms falls back to pk ASC")
	assert.True(t, tieB.After(tieA), "seek predicate is the inverse")
}

func row(ts int64, pod string, tier Tier) CallRow {
	return CallRow{PK: pk(pod, 1, 0, 0, 0), TsMs: ts, Tier: tier}
}

func TestMergeRunsOrdersDedupsAndTruncates(t *testing.T) {
	runA := []CallRow{row(300, "a", TierCold), row(100, "a", TierCold)}
	runB := []CallRow{row(200, "b", TierCold), row(100, "b", TierCold)}
	// runC duplicates runA's rows from a hot source (§6.2 overlap window).
	runC := []CallRow{row(300, "a", TierHot), row(100, "a", TierHot)}

	rows, more := MergeRuns([][]CallRow{runA, runB, runC}, 10)
	assert.False(t, more)
	got := make([]int64, 0, len(rows))
	for _, r := range rows {
		got = append(got, r.TsMs)
	}
	assert.Equal(t, []int64{300, 200, 100, 100}, got, "duplicates collapse, order is (ts DESC, pk ASC)")
	assert.Equal(t, TierCold, rows[0].Tier, "the cold copy of a duplicate PK wins (§6.3)")
	assert.Equal(t, "a", rows[2].PK.PodName, "ts tie breaks by pk ASC")

	page, morePages := MergeRuns([][]CallRow{runA, runB, runC}, 2)
	assert.True(t, morePages, "rows past the limit signal a next page")
	assert.Len(t, page, 2)
	assert.Equal(t, int64(300), page[0].TsMs)
	assert.Equal(t, int64(200), page[1].TsMs)
}

func TestMatchAppliesRowPredicates(t *testing.T) {
	r := CallRow{
		PK: pk("pod-1", 1, 0, 0, 0), TsMs: 150, DurationMs: 500,
		Method: "com.example.Db.query", ErrorFlag: false, RetentionClass: RetentionNormalClean,
	}
	base := CallsQuery{FromMs: 100, ToMs: 200}
	assert.True(t, base.Match(r))
	assert.False(t, CallsQuery{FromMs: 151, ToMs: 200}.Match(r), "ts below from")
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 150}.Match(r), "to is exclusive")
	assert.True(t, CallsQuery{FromMs: 100, ToMs: 200, Method: "Db.query"}.Match(r), "substring match")
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 200, Method: "Http"}.Match(r))
	assert.True(t, CallsQuery{FromMs: 100, ToMs: 200, Pods: []string{"ns/svc/pod-1"}}.Match(r))
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 200, Pods: []string{"ns/svc/other"}}.Match(r))
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 200, DurationMinMs: 501}.Match(r))
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 200, DurationMaxMs: 499}.Match(r))
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 200, ErrorOnly: true}.Match(r))
	assert.False(t, CallsQuery{FromMs: 100, ToMs: 200, RetentionClasses: []string{RetentionAnyError}}.Match(r))
}
