// Package httpproblem renders every HTTP API failure as the RFC 7807 problem
// envelope of 02-read-contract.md §8, so a client parses one error shape from
// /api/v1 and /internal/v1 alike.
//
// A producer builds its body with New and hands it to Send (inside echo) or
// Write (outside it); ErrorHandler covers everything no producer reached — a
// handler that returned a raw error, an unmatched route, a wrong method.
//
// The envelope carries a machine-readable code alongside the prose title and
// detail. Clients branch on the code: type stays "about:blank", and the title
// and detail are free to be reworded.
package httpproblem

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"

	"github.com/labstack/echo/v4"
)

// ContentType is the RFC 7807 media type every problem body is served with.
const ContentType = "application/problem+json"

// The §8 error codes. A code names the condition a client can act on; the
// title and detail beside it are prose and may be reworded without notice.
const (
	// CodeInvalidRequest is a request the caller has to fix: a malformed
	// parameter, an unsupported version, a value out of range.
	CodeInvalidRequest = "invalid_request"
	// CodeCursorRejected is a pagination cursor the server will not accept —
	// expired, malformed, or contradicted by re-sent filters (02 §2.3.1). The
	// client's answer is to restart from page 1.
	CodeCursorRejected = "cursor_rejected"
	// CodeQueryTooWide is a wide-query guard rejection (02 §2.3.2).
	CodeQueryTooWide = "query_too_wide"
	// CodeReadBudgetExhausted is a read memory budget denial (02 §7.5).
	CodeReadBudgetExhausted = "read_budget_exhausted"
	// CodeCallNotFound means no tier holds the requested call.
	CodeCallNotFound = "call_not_found"
	// CodePodRestartNotFound means the addressed replica hosts no such
	// pod-restart.
	CodePodRestartNotFound = "pod_restart_not_found"
	// CodeTraceUnavailable means the call exists but its trace blob does not.
	CodeTraceUnavailable = "trace_unavailable"
	// CodeNoSourceAvailable means every attempted source failed, so the
	// request has no data at all (02 §8).
	CodeNoSourceAvailable = "no_source_available"
	// CodeNotReady means the service has not mounted its API yet
	// (03-lifecycle.md §2).
	CodeNotReady = "not_ready"
	// CodeNotFound is a path no route matches.
	CodeNotFound = "not_found"
	// CodeMethodNotAllowed is a path that matches no route for this method.
	CodeMethodNotAllowed = "method_not_allowed"
	// CodeInternalError is an unexpected server-side failure; the cause is in
	// the server log, never in the response.
	CodeInternalError = "internal_error"
)

// internalDetail is the detail of every 500. The cause stays in the log:
// bucket names, object keys, and driver messages are not the client's
// business.
const internalDetail = "the request failed inside the server; the cause is in the server log"

// Problem is the RFC 7807 body (02 §8). Build it with New — a hand-written
// literal ships an empty type, which is neither the RFC default nor the
// non-optional member the clients expect. A producer with extension members
// embeds it, so they marshal flat beside these.
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
	Code   string `json:"code,omitempty"`
}

// StatusCode reports the status the envelope is served with, satisfying Body.
func (p Problem) StatusCode() int { return p.Status }

// Body is a problem body that knows its own status. Taking the status from a
// method rather than from a separate argument keeps the JSON status member and
// the response status the same number by construction.
type Body interface {
	StatusCode() int
}

// New builds a problem body. It is the only constructor: it sets the
// "about:blank" type RFC 7807 defaults to, which no producer otherwise
// remembers. code is one of the Code constants, title and detail are prose for
// a human reading the response.
func New(status int, code, title, detail string) Problem {
	return Problem{Type: "about:blank", Title: title, Status: status, Detail: detail, Code: code}
}

// Write serves body as a problem response on w. Producers outside echo — the
// readiness gate — call it directly; inside echo, Send does the unwrapping.
func Write(w http.ResponseWriter, body Body) error {
	w.Header().Set(echo.HeaderContentType, ContentType)
	w.WriteHeader(body.StatusCode())
	return json.NewEncoder(w).Encode(body)
}

// Send serves body as the response to c.
func Send(c echo.Context, body Body) error {
	return Write(c.Response(), body)
}

// ErrorHandler renders as the §8 envelope every failure that reached no
// producer: a handler that returned a raw error, an unmatched route, a wrong
// method. Install it as echo.Echo.HTTPErrorHandler.
//
// An *echo.HTTPError below 500 keeps its status and message; anything else
// answers 500 with internalDetail, so err never reaches the client. A response
// that is already committed is left alone: /trace streams through
// http.ServeContent, and a late error must not corrupt the bytes it wrote.
func ErrorHandler(err error, c echo.Context) {
	if c.Response().Committed {
		return
	}
	status := http.StatusInternalServerError
	detail := internalDetail
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) && httpErr.Code >= 400 && httpErr.Code < 500 {
		status = httpErr.Code
		detail = fmt.Sprintf("%v", httpErr.Message)
	}
	if c.Request().Method == http.MethodHead {
		// Mirrors echo.DefaultHTTPErrorHandler: the status and the headers a
		// GET would carry, no body.
		c.Response().Header().Set(echo.HeaderContentType, ContentType)
		_ = c.NoContent(status)
		return
	}
	_ = Send(c, New(status, codeForStatus(status), strings.ToLower(http.StatusText(status)), detail))
}

// codeForStatus maps a status the boundary saw to its §8 code. Every status
// gets one: an empty code on the member clients branch on would be worse than
// a coarse one.
func codeForStatus(status int) string {
	switch {
	case status == http.StatusNotFound:
		return CodeNotFound
	case status == http.StatusMethodNotAllowed:
		return CodeMethodNotAllowed
	case status >= 400 && status < 500:
		return CodeInvalidRequest
	default:
		return CodeInternalError
	}
}

// IsClientSide reports a request outcome the client caused, not the server: a
// routine 4xx (not-found, bad request), a canceled or timed-out request
// context, or the caller closing its socket mid-response. None of these need
// operator attention, unlike a real 5xx or an unexpected error (PR 708 review
// #23).
func IsClientSide(err error) bool {
	var httpErr *echo.HTTPError
	if errors.As(err, &httpErr) && httpErr.Code >= 400 && httpErr.Code < 500 {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return isBrokenPipeOrReset(err)
}

// isBrokenPipeOrReset detects a client that closed its connection while the
// server was still writing the response.
func isBrokenPipeOrReset(err error) bool {
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		var sysErr *os.SyscallError
		if errors.As(netErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.EPIPE) || errors.Is(sysErr.Err, syscall.ECONNRESET)
		}
	}
	// Some write paths surface only a formatted string with no syscall
	// wrapper to match against — fall back to the message.
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset by peer")
}
