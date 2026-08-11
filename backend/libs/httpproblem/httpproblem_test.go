package httpproblem_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"syscall"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/httpproblem"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// envelope decodes the wire body by its JSON member names rather than through
// httpproblem.Problem, so a renamed member fails the test instead of riding
// along with it.
type envelope struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
	Code   string `json:"code"`
}

// serve runs one handler behind the boundary mapper and returns the response
// with its body.
func serve(t *testing.T, method string, handler echo.HandlerFunc) (*http.Response, string) {
	t.Helper()
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.HTTPErrorHandler = httpproblem.ErrorHandler
	e.Add(method, "/probe", handler)

	server := httptest.NewServer(e)
	defer server.Close()
	req, err := http.NewRequest(method, server.URL+"/probe", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp, string(body)
}

// Every failure that reaches no producer still leaves as the §8 envelope, with
// a code the client can branch on and no server-side detail leaking into a
// 500. A failing case means a handler's error shape changed: fix the mapper,
// not the assertion — a client that has to parse two error schemas is the
// defect this pins.
func TestErrorHandlerEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
		detail string
	}{
		{
			name:   "an unexpected error keeps its cause out of the response",
			err:    errors.New("boom: s3://bucket/key"),
			status: http.StatusInternalServerError,
			code:   httpproblem.CodeInternalError,
		},
		{
			name:   "an unmatched route",
			err:    echo.NewHTTPError(http.StatusNotFound),
			status: http.StatusNotFound,
			code:   httpproblem.CodeNotFound,
			detail: "Not Found",
		},
		{
			name:   "a method no route serves",
			err:    echo.NewHTTPError(http.StatusMethodNotAllowed),
			status: http.StatusMethodNotAllowed,
			code:   httpproblem.CodeMethodNotAllowed,
			detail: "Method Not Allowed",
		},
		{
			name:   "a 4xx with no code of its own still carries one",
			err:    echo.NewHTTPError(http.StatusRequestEntityTooLarge, "body too large"),
			status: http.StatusRequestEntityTooLarge,
			code:   httpproblem.CodeInvalidRequest,
			detail: "body too large",
		},
		{
			name:   "a 5xx HTTPError is an internal failure like any other",
			err:    echo.NewHTTPError(http.StatusInternalServerError, "ui assets lack index.html"),
			status: http.StatusInternalServerError,
			code:   httpproblem.CodeInternalError,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, body := serve(t, http.MethodGet, func(echo.Context) error { return tc.err })

			assert.Equal(t, tc.status, resp.StatusCode)
			assert.Equal(t, httpproblem.ContentType, resp.Header.Get(echo.HeaderContentType))
			var got envelope
			require.NoError(t, json.Unmarshal([]byte(body), &got), "body: %s", body)
			assert.Equal(t, "about:blank", got.Type, "New is the only constructor, so type is never empty")
			assert.Equal(t, tc.status, got.Status, "the JSON status must match the response status")
			assert.Equal(t, tc.code, got.Code)
			assert.NotEmpty(t, got.Title)
			if tc.detail != "" {
				assert.Equal(t, tc.detail, got.Detail)
			}
			if tc.status >= 500 {
				assert.NotContains(t, body, "boom", "a 500 detail must not carry the cause")
				assert.NotContains(t, body, "bucket", "a 500 detail must not carry storage coordinates")
				assert.NotContains(t, body, "index.html")
				assert.NotEmpty(t, got.Detail, "a generic detail is still a detail")
			}
		})
	}
}

// A response the handler already started writing is left alone: /trace streams
// through http.ServeContent, and a problem body appended to it would corrupt
// the bytes the client is reading.
func TestErrorHandlerLeavesACommittedResponseAlone(t *testing.T) {
	resp, body := serve(t, http.MethodGet, func(c echo.Context) error {
		if err := c.String(http.StatusOK, "partial payload"); err != nil {
			return err
		}
		return errors.New("failed after the first bytes went out")
	})

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "partial payload", body)
}

// A HEAD request answers the status and content type its GET would, with no
// body — the same split echo.DefaultHTTPErrorHandler makes.
func TestErrorHandlerHeadHasNoBody(t *testing.T) {
	resp, body := serve(t, http.MethodHead, func(echo.Context) error {
		return echo.NewHTTPError(http.StatusNotFound)
	})

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, httpproblem.ContentType, resp.Header.Get(echo.HeaderContentType))
	assert.Empty(t, body)
}

func TestIsClientSide(t *testing.T) {
	brokenPipe := &net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}
	connReset := &net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}

	cases := []struct {
		name       string
		err        error
		clientSide bool
	}{
		{"404 not found", echo.NewHTTPError(404, "Not Found"), true},
		{"400 bad request", echo.NewHTTPError(400), true},
		{"context canceled", fmt.Errorf("wrap: %w", context.Canceled), true},
		{"context canceled wrapped in an HTTPError.Internal", echo.NewHTTPError(500).WithInternal(context.Canceled), true},
		{"deadline exceeded", context.DeadlineExceeded, true},
		{"broken pipe", brokenPipe, true},
		{"connection reset", connReset, true},
		{"broken pipe as a plain formatted error", errors.New("write tcp 127.0.0.1:8080: broken pipe"), true},
		{"500 internal error", echo.NewHTTPError(500, "boom"), false},
		{"generic unexpected error", errors.New("unexpected nil pointer"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.clientSide, httpproblem.IsClientSide(c.err))
		})
	}
}
