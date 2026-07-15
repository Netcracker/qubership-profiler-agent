package query

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

func TestIsClientSideError(t *testing.T) {
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
			assert.Equal(t, c.clientSide, isClientSideError(c.err))
		})
	}
}
