// Package model holds the tier-independent read-path types of
// 02-read-contract.md: the Call primary key and its binary collation
// (§2.2, §2.3.1), the frozen /calls query, the merged row shape, and the
// k-way merge that every tier's sorted runs feed into (§6). The cold source
// (libs/query/cold) and the future hot fan-out share these so pagination and
// dedup cannot diverge between tiers.
package model

import (
	"container/heap"
	"strings"
)

// Retention classes (01-write-contract.md §6.4). The values double as S3 key
// segments, so they must match what the seal pass writes. Their bounds and
// TTLs live in RetentionTiers (tiers.go) — the single tier table everything
// else derives from.
const (
	RetentionShortClean  = "short_clean"
	RetentionNormalClean = "normal_clean"
	RetentionLongClean   = "long_clean"
	RetentionHugeClean   = "huge_clean"
	RetentionAnyError    = "any_error"
	RetentionCorrupted   = "corrupted"
)

// RetentionClasses lists every class in tier-table order.
var RetentionClasses = func() []string {
	out := make([]string, len(RetentionTiers))
	for i, t := range RetentionTiers {
		out[i] = t.Class
	}
	return out
}()

// IsRetentionClass reports whether s names a known retention class.
func IsRetentionClass(s string) bool {
	for _, c := range RetentionClasses {
		if c == s {
			return true
		}
	}
	return false
}

type (
	// PK is the 7-component Call primary key (02 §2.2).
	PK struct {
		PodNamespace   string `json:"pod_namespace"`
		PodService     string `json:"pod_service"`
		PodName        string `json:"pod_name"`
		RestartTimeMs  int64  `json:"restart_time_ms"`
		TraceFileIndex int32  `json:"trace_file_index"`
		BufferOffset   int32  `json:"buffer_offset"`
		RecordIndex    int32  `json:"record_index"`
	}

	// Position is one point in the (ts_ms DESC, pk ASC) total order — the
	// keyset a cursor seeks past (02 §2.3.1).
	Position struct {
		TsMs int64 `json:"ts_ms"`
		PK   PK    `json:"pk"`
	}

	// Tier tags a row with the source that produced it, for the §6.3
	// duplicate tiebreak: cold wins over hot deterministically.
	Tier int

	// CallRow is one merged /calls row (02 §2.3). trace_blob is never read on
	// the list path, so the row carries no blob and no blob size.
	CallRow struct {
		PK              PK
		TsMs            int64
		DurationMs      int32
		Method          string
		ThreadName      string
		CpuTimeMs       int64
		WaitTimeMs      int64
		MemoryUsed      int64
		QueueWaitMs     int32
		SuspendMs       int32
		ChildCalls      int32
		Transactions    int32
		LogsGenerated   int64
		LogsWritten     int64
		FileRead        int64
		FileWritten     int64
		NetRead         int64
		NetWritten      int64
		ErrorFlag       bool
		RetentionClass  string
		Params          map[string][]string
		TruncatedReason string // empty when the blob survived
		Tier            Tier
	}

	// CallsQuery is the frozen /calls filter set (02 §2.3, §2.3.1). It rides
	// inside the cursor verbatim, so every field must be JSON-comparable.
	CallsQuery struct {
		FromMs           int64    `json:"from"`
		ToMs             int64    `json:"to"`
		Pods             []string `json:"pod,omitempty"` // "<ns>/<service>/<pod>"
		Method           string   `json:"method,omitempty"`
		DurationMinMs    int32    `json:"duration_min_ms,omitempty"`
		DurationMaxMs    int32    `json:"duration_max_ms,omitempty"` // 0 = unset
		ErrorOnly        bool     `json:"error_only,omitempty"`
		RetentionClasses []string `json:"retention_class,omitempty"`
	}

	// PodTuple is one /pods identity tuple (02 §2.7).
	PodTuple struct {
		Namespace     string `json:"namespace"`
		Service       string `json:"service"`
		Pod           string `json:"pod"`
		RestartTimeMs int64  `json:"restart_time_ms"`
	}
)

// Tiers, ordered by the §6.3 preference: on a duplicate PK the merge keeps
// the lowest value.
const (
	TierCold Tier = iota
	TierHot
)

// Compare orders PKs component by component. Go string comparison is
// byte-wise, which is exactly the binary collation §2.3.1 pins for the string
// components — no locale is ever involved.
func (p PK) Compare(o PK) int {
	if c := compareString(p.PodNamespace, o.PodNamespace); c != 0 {
		return c
	}
	if c := compareString(p.PodService, o.PodService); c != 0 {
		return c
	}
	if c := compareString(p.PodName, o.PodName); c != 0 {
		return c
	}
	if c := compareInt64(p.RestartTimeMs, o.RestartTimeMs); c != 0 {
		return c
	}
	if c := compareInt64(int64(p.TraceFileIndex), int64(o.TraceFileIndex)); c != 0 {
		return c
	}
	if c := compareInt64(int64(p.BufferOffset), int64(o.BufferOffset)); c != 0 {
		return c
	}
	return compareInt64(int64(p.RecordIndex), int64(o.RecordIndex))
}

func compareString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func compareInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// Before reports whether p sorts before o in the (ts_ms DESC, pk ASC) total
// order of §2.3.1.
func (p Position) Before(o Position) bool {
	if p.TsMs != o.TsMs {
		return p.TsMs > o.TsMs
	}
	return p.PK.Compare(o.PK) < 0
}

// After reports whether the row position sits strictly past the cursor
// position — the keyset seek predicate WHERE (ts_ms, pk) < cursor (§2.3.1).
func (p Position) After(cursor Position) bool {
	return cursor.Before(p)
}

// Position returns the row's point in the pagination order.
func (r CallRow) Position() Position {
	return Position{TsMs: r.TsMs, PK: r.PK}
}

// Match applies the row-level predicates of §2.3 (the ts window and the
// filters that do not prune files). Method is a substring match — it covers
// the contract's "substring/prefix" without a second operator.
func (q CallsQuery) Match(r CallRow) bool {
	if r.TsMs < q.FromMs || r.TsMs >= q.ToMs {
		return false
	}
	if len(q.Pods) > 0 {
		podId := r.PK.PodNamespace + "/" + r.PK.PodService + "/" + r.PK.PodName
		found := false
		for _, p := range q.Pods {
			if p == podId {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if q.Method != "" && !strings.Contains(r.Method, q.Method) {
		return false
	}
	if q.DurationMinMs > 0 && r.DurationMs < q.DurationMinMs {
		return false
	}
	if q.DurationMaxMs > 0 && r.DurationMs > q.DurationMaxMs {
		return false
	}
	if q.ErrorOnly && !r.ErrorFlag {
		return false
	}
	if len(q.RetentionClasses) > 0 {
		found := false
		for _, c := range q.RetentionClasses {
			if c == r.RetentionClass {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// mergeHeap orders run heads by (ts_ms DESC, pk ASC), then by Tier so a
// duplicate PK surfaces its cold copy first (§6.3).
type mergeHeap []mergeRun

type mergeRun struct {
	rows []CallRow
	pos  int
}

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	a, b := h[i].rows[h[i].pos], h[j].rows[h[j].pos]
	ap, bp := a.Position(), b.Position()
	if ap.Before(bp) {
		return true
	}
	if bp.Before(ap) {
		return false
	}
	return a.Tier < b.Tier
}
func (h mergeHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)        { *h = append(*h, x.(mergeRun)) }
func (h *mergeHeap) Pop() any          { old := *h; n := len(old); x := old[n-1]; *h = old[:n-1]; return x }
func (h mergeHeap) head(i int) CallRow { return h[i].rows[h[i].pos] }

// MergeRuns k-way merges already-sorted runs into the (ts_ms DESC, pk ASC)
// order, deduplicates by full PK (§6: dedup runs before the truncation and
// before the cursor is computed), and truncates to limit. The second return
// reports whether rows remained past the truncation — the caller's
// next_cursor signal. Runs must each be sorted in the total order; one run
// per parquet file or per fan-out source.
func MergeRuns(runs [][]CallRow, limit int) ([]CallRow, bool) {
	h := make(mergeHeap, 0, len(runs))
	for _, run := range runs {
		if len(run) > 0 {
			h = append(h, mergeRun{rows: run})
		}
	}
	heap.Init(&h)

	var out []CallRow
	var last Position
	for h.Len() > 0 {
		row := h.head(0)
		if h[0].pos++; h[0].pos == len(h[0].rows) {
			heap.Pop(&h)
		} else {
			heap.Fix(&h, 0)
		}
		if len(out) > 0 {
			p := row.Position()
			if !last.Before(p) {
				continue // same (ts_ms, pk): a §6.2 duplicate; the preferred copy came first
			}
		}
		if len(out) == limit {
			return out, true
		}
		out = append(out, row)
		last = row.Position()
	}
	return out, false
}
