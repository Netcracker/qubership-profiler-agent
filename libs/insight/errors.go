package insight

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/httpproblem"
	"github.com/pkg/errors"
)

// The problem codes of 02-read-contract §8 the operations react to.
const (
	CodeInvalidRequest      = httpproblem.CodeInvalidRequest
	CodeCursorRejected      = httpproblem.CodeCursorRejected
	CodeQueryTooWide        = httpproblem.CodeQueryTooWide
	CodeReadBudgetExhausted = httpproblem.CodeReadBudgetExhausted
	CodeCallNotFound        = httpproblem.CodeCallNotFound
	CodeTraceUnavailable    = httpproblem.CodeTraceUnavailable
)

// APIError is a problem response of the query service (02 §8), with the
// wide-query guard's extension members.
type APIError struct {
	Status           int              `json:"status"`
	Title            string           `json:"title"`
	Detail           string           `json:"detail"`
	Code             string           `json:"code"`
	SuggestedFilters []string         `json:"suggested_filters"`
	EstimatedFiles   *int             `json:"estimated_files"`
	EstimatedBytes   *int64           `json:"estimated_bytes"`
	ByClass          map[string]int64 `json:"by_class"`
	// RetryAfter is the Retry-After header of a 503, zero when absent.
	RetryAfter time.Duration `json:"-"`
}

// Error renders the problem for an agent: what failed and what to do next.
func (e *APIError) Error() string {
	switch e.Code {
	case CodeQueryTooWide:
		return e.tooWideMessage()
	case CodeReadBudgetExhausted:
		return "the profiler is busy: narrow the query or retry later (" + e.Detail + ")"
	case CodeCallNotFound:
		return "no call with this reference: it aged out of retention, or the reference is wrong"
	case CodeTraceUnavailable:
		return "the call has no call tree: the agent dropped its trace (a truncated call)"
	}
	text := e.Detail
	if text == "" {
		text = e.Title
	}
	if e.Code != "" {
		return fmt.Sprintf("%s (%s, HTTP %d)", text, e.Code, e.Status)
	}
	return fmt.Sprintf("%s (HTTP %d)", text, e.Status)
}

// suggestionText phrases each narrowing filter the guard suggests as the
// condition to add (02 §2.3.2).
var suggestionText = map[string]string{
	"pod":             "pod=<pod>",
	"namespace":       "namespace=<namespace>",
	"service":         "service=<service>",
	"retention_class": "retention_class=any_error",
	"duration_min_ms": "duration>1s",
	"error_only":      "error=true",
}

func (e *APIError) tooWideMessage() string {
	var b strings.Builder
	b.WriteString("the query is too wide to scan: ")
	b.WriteString(e.Detail)
	if len(e.SuggestedFilters) > 0 {
		var adds []string
		for _, f := range e.SuggestedFilters {
			if text, ok := suggestionText[f]; ok {
				adds = append(adds, text)
			} else {
				adds = append(adds, f)
			}
		}
		b.WriteString(". Add one of these conditions or shorten the window: ")
		b.WriteString(strings.Join(adds, ", "))
	}
	if len(e.ByClass) > 0 {
		classes := make([]string, 0, len(e.ByClass))
		for c := range e.ByClass {
			classes = append(classes, c)
		}
		sort.Slice(classes, func(i, j int) bool { return e.ByClass[classes[i]] > e.ByClass[classes[j]] })
		parts := make([]string, len(classes))
		for i, c := range classes {
			parts[i] = fmt.Sprintf("%s %d MB", c, e.ByClass[c]>>20)
		}
		b.WriteString(". Estimated scan by class: ")
		b.WriteString(strings.Join(parts, ", "))
	}
	return b.String()
}

// IsCode reports whether err is an APIError with the given code.
func IsCode(err error, code string) bool {
	var e *APIError
	return errors.As(err, &e) && e.Code == code
}
