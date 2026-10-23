package insight

import (
	"context"
	"net/url"
	"slices"
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

// Call orders (02-read-contract §2.3.5). An order is
// `<field>_<asc|desc>`; these two are the ones the operations use.
const (
	// OrderNewest lists the newest calls first; it is the default.
	OrderNewest = "start_desc"
	// OrderSlowest lists the longest calls first.
	OrderSlowest = "duration_desc"
)

// CallOrderFields are the sort fields of 02 §2.3.5.
var CallOrderFields = []string{
	model.OrderFieldStart, model.OrderFieldDuration, model.OrderFieldCPU,
	model.OrderFieldQueue, model.OrderFieldCalls, model.OrderFieldNet,
}

// CallOrders lists every order the contract defines, each field descending
// then ascending. A build may implement a subset; the query service names
// an order it does not implement.
var CallOrders = func() []string {
	out := make([]string, 0, 2*len(CallOrderFields))
	for _, f := range CallOrderFields {
		out = append(out, f+"_desc", f+"_asc")
	}
	return out
}()

// CallsInput selects calls, newest first unless Order says otherwise.
type CallsInput struct {
	Window  Window
	Filters Filters
	// Order is one of CallOrders; empty means OrderNewest. On a
	// cursor page it must be the order of page 1, or be left empty: another
	// order restarts the listing at page 1 in that order.
	Order string
	// Limit is the page size; zero means DefaultCallsLimit.
	Limit int
	// Cursor continues from a previous result's NextCursor. The window of
	// the first page applies, and so do its filters: pass them again or
	// pass none. Other filters fail with *CursorMismatchError.
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
	// Restarted reports that the cursor was rejected (expired, stale, or
	// damaged) and this page is page 1 again.
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

// Calls lists the calls that match the filters in the window, newest first
// unless Order says otherwise. A request or trace ID condition finds one
// request's calls; a service with OrderSlowest finds its slowest calls.
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
	// Only the form is checked here: which orders work is the server's to
	// say, and it names an order it does not implement.
	if in.Order != "" && !slices.Contains(CallOrders, in.Order) {
		return CallsResult{}, errors.Errorf("order must be <field>_asc or <field>_desc with a field of %s, not %q",
			strings.Join(CallOrderFields, ", "), in.Order)
	}
	// The conditions a cursor page runs under are the cursor's own: authorize
	// those, not the ones passed beside it (see callsCursor).
	var serverCursor string
	if in.Cursor != "" {
		if cur, err := decodeCallsCursor(in.Cursor); err == nil {
			if len(conditions) > 0 && !sameConditions(conditions, cur.Conditions) {
				return CallsResult{}, &CursorMismatchError{CursorFilters: cur.Conditions, Filters: conditions}
			}
			conditions, serverCursor = cur.Conditions, cur.Server
		}
		// A cursor that does not decode restarts from page 1, as one the
		// server rejects does.
	}
	if err := c.authorize(ctx, namespaces(conditions)); err != nil {
		return CallsResult{}, err
	}

	query := url.Values(window.values())
	query["filter"] = conditions
	query.Set("limit", strconv.Itoa(limit))
	if in.Order != "" {
		query.Set("order", in.Order)
	}
	var out CallsResult
	var resp callsResponse
	if serverCursor != "" {
		// The window is not re-sent: the server keeps page 1's, and a window
		// resolved again at a later now would contradict it. The conditions
		// are re-sent, so the server rejects a frozen query that has others.
		page := url.Values{"cursor": {serverCursor}, "limit": {strconv.Itoa(limit)}}
		if len(conditions) > 0 {
			page["filter"] = conditions
		}
		// The server freezes the order of page 1 and rejects another one,
		// which restarts the listing below.
		if in.Order != "" {
			page.Set("order", in.Order)
		}
		err = c.getJSON(ctx, "/calls", page, &resp)
		if IsCode(err, CodeCursorRejected) {
			out.Restarted, out.Window = true, &window
			err = c.getJSON(ctx, "/calls", query, &resp)
		}
	} else {
		out.Restarted, out.Window = in.Cursor != "", &window
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
		out.NextCursor = encodeCallsCursor(*resp.NextCursor, conditions)
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
		TraceID:        firstParam(row.Params, model.TraceIDParams...),
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

// firstParam returns the first non-empty value of the first name that has
// one.
func firstParam(params map[string][]string, names ...string) string {
	for _, name := range names {
		for _, v := range params[name] {
			if v != "" {
				return v
			}
		}
	}
	return ""
}
