package insight

import (
	"context"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/pkg/errors"
)

// Calls page sizes. The default keeps a page small enough for a model's
// context; MaxCallsLimit is the query service's own page cap (02 §2.3).
const (
	DefaultCallsLimit = 20
	MaxCallsLimit     = 1000
)

// CallsInput selects calls, newest first.
type CallsInput struct {
	Window  Window
	Filters Filters
	// Limit is the page size; zero means DefaultCallsLimit.
	Limit int
	// Cursor continues from a previous result's NextCursor. The window and
	// filters of the first page apply; the ones given here must match them.
	Cursor string
}

// CallsResult is one page of calls.
type CallsResult struct {
	// Window is the window the page ran over. It is nil on a cursor page,
	// which runs over the window of page 1.
	Window *ResolvedWindow `json:"window,omitempty"`
	Calls  []Call          `json:"calls"`
	// NextCursor continues the listing; empty on the last page.
	NextCursor string `json:"next_cursor,omitempty"`
	// Partial names the sources that failed; the page may miss calls.
	Partial []string `json:"partial,omitempty"`
	// Restarted reports that the server rejected the cursor (expired or
	// stale) and this page is page 1 again.
	Restarted bool `json:"restarted,omitempty"`
}

// Call is one profiled call, with what an agent needs to pick the call to
// explain. Times are in milliseconds.
type Call struct {
	// Ref identifies the call for the explain operation.
	Ref            string    `json:"ref"`
	Start          time.Time `json:"start"`
	DurationMs     int32     `json:"duration_ms"`
	Method         string    `json:"method"`
	Namespace      string    `json:"namespace"`
	Service        string    `json:"service"`
	Pod            string    `json:"pod"`
	CPUMs          int64     `json:"cpu_ms"`
	WaitMs         int64     `json:"wait_ms"`
	QueueWaitMs    int32     `json:"queue_wait_ms"`
	SuspendMs      int32     `json:"suspend_ms"`
	ChildCalls     int32     `json:"child_calls"`
	Error          bool      `json:"error"`
	RetentionClass string    `json:"retention_class"`
	TraceID        string    `json:"trace_id,omitempty"`
	RequestID      string    `json:"request_id,omitempty"`
	// HTTP is "<method> <url>" for a call that served an HTTP request.
	HTTP string `json:"http,omitempty"`
	// Truncated is why the agent dropped the call tree; explain then fails.
	Truncated string `json:"truncated,omitempty"`
}

type callsResponse struct {
	Calls          []model.CallJSON `json:"calls"`
	NextCursor     *string          `json:"next_cursor"`
	Partial        bool             `json:"partial"`
	PartialReasons []string         `json:"partial_reasons"`
}

// Calls lists the calls that match the filters in the window, newest first.
// A request or trace ID condition finds one request's calls; a service and a
// duration bound find its slow calls.
func (c *Client) Calls(ctx context.Context, in CallsInput) (CallsResult, error) {
	window, err := in.Window.Resolve()
	if err != nil {
		return CallsResult{}, err
	}
	conditions, err := in.Filters.conditions()
	if err != nil {
		return CallsResult{}, err
	}
	limit := in.Limit
	if limit == 0 {
		limit = DefaultCallsLimit
	}
	if limit < 0 || limit > MaxCallsLimit {
		return CallsResult{}, errors.Errorf("limit must be between 1 and %d", MaxCallsLimit)
	}
	if err := c.authorize(ctx, namespaces(conditions)); err != nil {
		return CallsResult{}, err
	}

	query := url.Values(window.values())
	query["filter"] = conditions
	query.Set("limit", strconv.Itoa(limit))
	var out CallsResult
	var resp callsResponse
	if in.Cursor != "" {
		// A cursor page re-sends only the cursor: the frozen query wins, and a
		// window resolved again at a later now would contradict it.
		err = c.getJSON(ctx, "/calls", url.Values{"cursor": {in.Cursor}, "limit": {strconv.Itoa(limit)}}, &resp)
		if IsCode(err, CodeCursorRejected) {
			out.Restarted, out.Window = true, &window
			err = c.getJSON(ctx, "/calls", query, &resp)
		}
	} else {
		out.Window = &window
		err = c.getJSON(ctx, "/calls", query, &resp)
	}
	if err != nil {
		return CallsResult{}, err
	}

	out.Calls = make([]Call, 0, len(resp.Calls))
	for _, row := range resp.Calls {
		out.Calls = append(out.Calls, toCall(row))
	}
	if resp.NextCursor != nil {
		out.NextCursor = *resp.NextCursor
	}
	if resp.Partial {
		out.Partial = resp.PartialReasons
	}
	return out, nil
}

func toCall(row model.CallJSON) Call {
	call := Call{
		Ref:            CallRef{PK: row.PK, TsMs: row.TsMs, RetentionClass: row.RetentionClass}.String(),
		Start:          time.UnixMilli(row.TsMs).UTC(),
		DurationMs:     row.DurationMs,
		Method:         row.Method,
		Namespace:      row.PK.PodNamespace,
		Service:        row.PK.PodService,
		Pod:            row.PK.PodName,
		CPUMs:          row.CpuTimeMs,
		WaitMs:         row.WaitTimeMs,
		QueueWaitMs:    row.QueueWaitMs,
		SuspendMs:      row.SuspendMs,
		ChildCalls:     row.ChildCalls,
		Error:          row.ErrorFlag,
		RetentionClass: row.RetentionClass,
		TraceID:        firstParam(row.Params, "trace.id"),
		RequestID:      firstParam(row.Params, "x-request-id"),
	}
	if u := firstParam(row.Params, "web.url"); u != "" {
		call.HTTP = strings.TrimSpace(firstParam(row.Params, "web.method") + " " + u)
	}
	if row.TruncatedReason != nil {
		call.Truncated = *row.TruncatedReason
	}
	return call
}

func firstParam(params map[string][]string, name string) string {
	if vs := params[name]; len(vs) > 0 {
		return vs[0]
	}
	return ""
}
