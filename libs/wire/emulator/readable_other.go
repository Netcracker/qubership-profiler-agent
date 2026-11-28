//go:build !linux && !darwin

package emulator

import "net"

// readableBytes has no FIONREAD equivalent wired up on this platform; -1 makes
// the caller fall back to a short read poll.
func readableBytes(conn net.Conn) int {
	return -1
}
