//go:build linux

package emulator

import "golang.org/x/sys/unix"

// TIOCINQ is Linux's spelling of FIONREAD (0x541B); x/sys/unix does not
// export the FIONREAD alias for linux.
const fionread = unix.TIOCINQ
