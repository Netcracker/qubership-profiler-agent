// Package model holds the tier-independent read-path types of
// 02-read-contract.md: the Call primary key and its binary collation
// (§2.2, §2.3.1), the frozen /calls query, the merged row shape, and the
// k-way merge that every tier's sorted runs feed into (§6). The cold source
// (libs/query/cold) and the future hot fan-out share these so pagination and
// dedup cannot diverge between tiers.
package model

import (
	"container/heap"
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

	// Position is one point in a /calls total order — the keyset a cursor
	// seeks past (02 §2.3.1, §2.3.5). It carries the sort key of every
	// implemented order, and Order picks the one it compares.
	Position struct {
		TsMs        int64 `json:"ts_ms"`
		DurationMs  int32 `json:"duration_ms,omitempty"`
		CpuTimeMs   int64 `json:"cpu_time_ms,omitempty"`
		QueueWaitMs int32 `json:"queue_wait_ms,omitempty"`
		ChildCalls  int32 `json:"child_calls,omitempty"`
		// NetBytes is net_read + net_written, the sort key of the net order.
		NetBytes int64 `json:"net_bytes,omitempty"`
		PK       PK    `json:"pk"`
		// NoPK marks a position that ends a key without naming a row: the
		// lower bound of a duration band a page stopped at (§2.3.5). It
		// passes only strictly lower keys.
		NoPK bool `json:"no_pk,omitempty"`
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

	// CallsQuery is the frozen /calls query (02 §2.3, §2.3.1): the window and
	// the filter conditions (§2.3.3), with the legacy parameters already
	// turned into conditions. It rides inside the cursor verbatim.
	CallsQuery struct {
		FromMs  int64       `json:"from"`
		ToMs    int64       `json:"to"`
		Filters []Condition `json:"filters,omitempty"`
		Order   Order       `json:"order,omitempty"`
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
	return OrderStartDesc.Before(p, o)
}

// After reports whether the row position sits strictly past the cursor
// position — the keyset seek predicate WHERE (ts_ms, pk) < cursor (§2.3.1).
func (p Position) After(cursor Position) bool {
	return cursor.Before(p)
}

// Position returns the row's point in the pagination order.
func (r CallRow) Position() Position {
	return Position{
		TsMs: r.TsMs, DurationMs: r.DurationMs, CpuTimeMs: r.CpuTimeMs, QueueWaitMs: r.QueueWaitMs,
		ChildCalls: r.ChildCalls, NetBytes: r.NetRead + r.NetWritten, PK: r.PK,
	}
}

// mergeHeap orders run heads by the order, then by Tier so a duplicate PK
// surfaces its cold copy first (§6.3).
type mergeHeap struct {
	order Order
	runs  []mergeRun
}

type mergeRun struct {
	rows []CallRow
	pos  int
}

func (h *mergeHeap) Len() int { return len(h.runs) }
func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.head(i), h.head(j)
	ap, bp := a.Position(), b.Position()
	if h.order.Before(ap, bp) {
		return true
	}
	if h.order.Before(bp, ap) {
		return false
	}
	return a.Tier < b.Tier
}
func (h *mergeHeap) Swap(i, j int) { h.runs[i], h.runs[j] = h.runs[j], h.runs[i] }
func (h *mergeHeap) Push(x any)    { h.runs = append(h.runs, x.(mergeRun)) }
func (h *mergeHeap) Pop() any {
	n := len(h.runs)
	x := h.runs[n-1]
	h.runs = h.runs[:n-1]
	return x
}
func (h *mergeHeap) head(i int) CallRow { return h.runs[i].rows[h.runs[i].pos] }

// MergeRuns k-way merges already-sorted runs into the (ts_ms DESC, pk ASC)
// order, deduplicates by full PK (§6: dedup runs before the truncation and
// before the cursor is computed), and truncates to limit. The second return
// reports whether rows remained past the truncation — the caller's
// next_cursor signal. Runs must each be sorted in the total order; one run
// per parquet file or per fan-out source.
func MergeRuns(runs [][]CallRow, limit int) ([]CallRow, bool) {
	return MergeRunsBy(OrderStartDesc, runs, limit)
}

// MergeRunsBy is MergeRuns for any order; every run must be sorted in it.
func MergeRunsBy(order Order, runs [][]CallRow, limit int) ([]CallRow, bool) {
	h := &mergeHeap{order: order, runs: make([]mergeRun, 0, len(runs))}
	for _, run := range runs {
		if len(run) > 0 {
			h.runs = append(h.runs, mergeRun{rows: run})
		}
	}
	heap.Init(h)

	var out []CallRow
	var last Position
	for h.Len() > 0 {
		row := h.head(0)
		if h.runs[0].pos++; h.runs[0].pos == len(h.runs[0].rows) {
			heap.Pop(h)
		} else {
			heap.Fix(h, 0)
		}
		if len(out) > 0 {
			p := row.Position()
			if !order.Before(last, p) {
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
