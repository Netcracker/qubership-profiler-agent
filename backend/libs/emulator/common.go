package emulator

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/pkg/errors"
)

const (
	MaxBufSize = 1024
)

var ErrNotConnected = errors.New("not connected")

type (
	ConnectionOpts struct {
		ProtocolAddress string
		Timeout         io.TcpTimeout
	}
)
