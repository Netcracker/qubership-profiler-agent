package server

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/pkg/errors"
)

const (
	ProtocolVersion = 10 // from server perspective
)

var ErrNotConnected = errors.New("not connected")

type (
	ConnectionOpts struct {
		ProtocolPort int
		Timeout      io.TcpTimeout
	}
)
