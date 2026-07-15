package io

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"syscall"
)

// IsExpectedDisconnect reports a normal remote close: EOF (clean or
// mid-message), a TCP reset, or a broken pipe — the agent's own process
// restarting or redeploying, or it reconnecting for its own reasons. None of
// these are a protocol bug or an internal collector failure, so a caller
// logging one should not log it as an ERROR (PR 708 review #26).
func IsExpectedDisconnect(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		var sysErr *os.SyscallError
		if errors.As(netErr.Err, &sysErr) {
			return errors.Is(sysErr.Err, syscall.ECONNRESET) || errors.Is(sysErr.Err, syscall.EPIPE)
		}
	}
	// Some write/read paths surface only a formatted string with no syscall
	// wrapper to match against — fall back to the message.
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "use of closed network connection")
}
