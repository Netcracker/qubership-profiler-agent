package server

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/pkg/errors"
)

const (
	// ProtocolVersion is the reply to the agent's GET_PROTOCOL_VERSION_V2
	// handshake. It MUST be PROTOCOL_VERSION_V2: replying V3 switches the agent
	// to the posDictionary stream the collector cannot demux
	// (06-wire-protocol-server.md §3).
	ProtocolVersion = model.PROTOCOL_VERSION_V2

	// DefaultRequiredRotationSize is the segment size the collector asks the
	// agent to rotate its stream files at, when ConnectionOpts leaves it unset
	// (06-wire-protocol-server.md §4; PROFILER_SEGMENT_ROTATION_SIZE default).
	DefaultRequiredRotationSize uint64 = 4 * 1024 * 1024
)

var (
	ErrNotConnected = errors.New("not connected")
	// errAgentClosed is returned when the agent sends COMMAND_CLOSE, so the
	// handler loop stops without logging it as a failure.
	errAgentClosed = errors.New("agent requested close")
)

type (
	ConnectionOpts struct {
		ProtocolPort int
		Timeout      io.TcpTimeout
		// RotationPeriod and RequiredRotationSize are echoed to the agent in the
		// INIT_STREAM_V2 reply (06 §4). A zero RequiredRotationSize falls back to
		// DefaultRequiredRotationSize.
		RotationPeriod       uint64
		RequiredRotationSize uint64
	}
)
