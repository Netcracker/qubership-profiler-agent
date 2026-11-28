//go:build linux || darwin

package emulator

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// readableBytes reports how many bytes the OS has buffered for reading on the
// connection — the Go equivalent of Java's InputStream.available(), which the
// agent polls before every RCV_DATA (validateWriteDataAcks(false)). Returns -1
// when the connection does not expose a raw descriptor (the caller falls back
// to a short read poll).
func readableBytes(conn net.Conn) int {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1
	}
	n := -1
	if cerr := raw.Control(func(fd uintptr) {
		if v, ierr := unix.IoctlGetInt(int(fd), fionread); ierr == nil {
			n = v
		}
	}); cerr != nil {
		return -1
	}
	return n
}
