package io

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsExpectedDisconnect(t *testing.T) {
	connReset := &net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}}
	brokenPipe := &net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}

	cases := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"clean EOF", io.EOF, true},
		{"mid-message EOF", io.ErrUnexpectedEOF, true},
		{"connection reset", connReset, true},
		{"broken pipe", brokenPipe, true},
		{"connection reset as a plain formatted error", errors.New("read tcp 127.0.0.1:8080: connection reset by peer"), true},
		{"use of closed network connection", errors.New("read tcp 127.0.0.1:8080: use of closed network connection"), true},
		{"malformed protocol data", errors.New("fixed-string length 999999 exceeds max 1024 at pos 42"), false},
		{"unrelated error", errors.New("disk full"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, IsExpectedDisconnect(c.err))
		})
	}
}
