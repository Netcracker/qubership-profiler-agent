package query

import (
	"fmt"
	"math"
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
// the set of files discovered (02 §2.3.2): a pod filter, or any class-axis
// filter (retention_class, duration_min_ms, error_only) that actually drops
// a class from the discovery LIST. The class check runs the same ClassesFor
// derivation discovery uses, so a filter that prunes nothing — a
// duration_min_ms below the first tier bound, or a retention_class list
// naming every class — no longer buys a guard exemption (№28). method and
// params filter rows inside already-listed files, so they do not exempt.
func (s *Service) hasNarrowingFilter(q model.CallsQuery) bool {
	return len(q.Pods) > 0 || len(s.cold.ClassesFor(q)) < len(model.RetentionClasses)
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

// windowSpanMs returns the window width in milliseconds and whether the int64
// subtraction wrapped. Comparing in milliseconds — instead of the original
// time.Duration(to-from)*time.Millisecond — is what keeps the guard sound:
// the nanosecond multiply overflowed int64 for a far-future `to`, wrapping
// the span negative so a wide window slipped past the guard (PR 708 review
// #1). ParseWindow guarantees to > from, so a real span is positive; a
// non-positive result means the difference itself overflowed and must count
// as unboundedly wide.
func windowSpanMs(fromMs, toMs int64) (span int64, overflow bool) {
	span = toMs - fromMs
	return span, span <= 0
}

// spanText renders a millisecond span for a guard message without the
// time.Duration overflow a far-future window triggers on the ms→ns multiply.
func spanText(span int64, overflow bool) string {
	const maxDurationMs = int64(math.MaxInt64) / int64(time.Millisecond)
	if overflow || span < 0 || span > maxDurationMs {
		return fmt.Sprintf("%dms", span)
	}
	return (time.Duration(span) * time.Millisecond).String()
}

// guardSpan is layer 1 (02 §2.3.2): a window wider than the limit with no
// file-pruning filter is rejected before any I/O — the discovery LIST for
// such a query is itself the cost being avoided.
func (s *Service) guardSpan(q model.CallsQuery, limit time.Duration) *guardRejection {
	span, overflow := windowSpanMs(q.FromMs, q.ToMs)
	if (!overflow && span <= limit.Milliseconds()) || s.hasNarrowingFilter(q) {
		return nil
	}
	return &guardRejection{
		Layer: guardLayerSpan,
		Detail: fmt.Sprintf("time span %s exceeds PROFILER_WIDE_RANGE_LIMIT %s and no file-pruning filter is present",
			spanText(span, overflow), limit),
		SuggestedFilters: suggestedFilters(q),
	}
}

// guardPodsSpan is the /pods analogue of the span layer: /pods walks one UTC
// day per day in the window and issues one S3 LIST per day, so an unbounded
// window fans out into unbounded LIST work (PR 708 review #3). /pods carries
// no file-pruning filter, so the window width is the only bound.
func guardPodsSpan(fromMs, toMs int64, limit time.Duration) *guardRejection {
	span, overflow := windowSpanMs(fromMs, toMs)
	if !overflow && span <= limit.Milliseconds() {
		return nil
	}
	return &guardRejection{
		Layer: guardLayerSpan,
		Detail: fmt.Sprintf("time span %s exceeds PROFILER_MAX_PODS_RANGE %s",
			spanText(span, overflow), limit),
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
