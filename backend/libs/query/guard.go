package query

import (
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
)

// The two §2.3.2 guard layers, as the `layer` label of the rejection counter.
const (
	guardLayerSpan = "span"
	guardLayerCost = "cost"
)

// guardRejection is a §2.3.2 fail-closed verdict; its fields extend the
// RFC 7807 body so the client can render a "narrow your query" prompt
// instead of a bare error (02 §8). The span layer carries no estimate — it
// fires before the LIST.
type guardRejection struct {
	Layer            string
	Detail           string
	SuggestedFilters []string
	EstimatedFiles   int
	EstimatedBytes   int64
	ByClass          map[string]int64
	HasEstimate      bool
}

// hasNarrowingFilter reports whether the query carries a filter that prunes
// the set of files discovered (02 §2.3.2): pod, retention_class,
// duration_min_ms, or error_only. method and params filter rows inside
// already-listed files, so they do not exempt.
func hasNarrowingFilter(q model.CallsQuery) bool {
	return len(q.Pods) > 0 || len(q.RetentionClasses) > 0 || q.DurationMinMs > 0 || q.ErrorOnly
}

// suggestedFilters lists the narrowing filters the query does not use yet.
func suggestedFilters(q model.CallsQuery) []string {
	var out []string
	if len(q.Pods) == 0 {
		out = append(out, "pod")
	}
	if len(q.RetentionClasses) == 0 {
		out = append(out, "retention_class")
	}
	if q.DurationMinMs <= 0 {
		out = append(out, "duration_min_ms")
	}
	if !q.ErrorOnly {
		out = append(out, "error_only")
	}
	return out
}

// guardSpan is layer 1 (02 §2.3.2): a window wider than the limit with no
// file-pruning filter is rejected before any I/O — the discovery LIST for
// such a query is itself the cost being avoided.
func guardSpan(q model.CallsQuery, limit time.Duration) *guardRejection {
	span := time.Duration(q.ToMs-q.FromMs) * time.Millisecond
	if span <= limit || hasNarrowingFilter(q) {
		return nil
	}
	return &guardRejection{
		Layer: guardLayerSpan,
		Detail: fmt.Sprintf("time span %s exceeds PROFILER_WIDE_RANGE_LIMIT %s and no file-pruning filter is present",
			span, limit),
		SuggestedFilters: suggestedFilters(q),
	}
}

// guardCost is layer 2 (02 §2.3.2): the discovery LIST already carries every
// candidate's size and key-encoded class, so the scan estimate costs no extra
// request and no file is opened before the verdict.
func guardCost(q model.CallsQuery, files []cold.FileRef, maxFiles int, maxBytes int64) *guardRejection {
	var totalBytes int64
	byClass := map[string]int64{}
	for _, f := range files {
		totalBytes += f.Size
		byClass[f.Class] += f.Size
	}
	if len(files) <= maxFiles && totalBytes <= maxBytes {
		return nil
	}
	return &guardRejection{
		Layer: guardLayerCost,
		Detail: fmt.Sprintf("estimated scan of %d files / %d bytes exceeds PROFILER_MAX_SCAN_FILES %d / PROFILER_MAX_SCAN_BYTES %d",
			len(files), totalBytes, maxFiles, maxBytes),
		SuggestedFilters: suggestedFilters(q),
		EstimatedFiles:   len(files),
		EstimatedBytes:   totalBytes,
		ByClass:          byClass,
		HasEstimate:      true,
	}
}
