package emulator

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/pkg/errors"
)

const (
	MaxBufSize = 1024
)

var ErrNotConnected = errors.New("not connected")

// ErrAckRefused is ACK_ERROR_MAGIC from the collector: it cannot accept data
// (backpressure or a fatal stream error) and expects the agent to reconnect
// (06-wire-protocol-server.md §6). The virtual dumper maps it to the
// drop-window + reconnect path instead of a generic failure.
var ErrAckRefused = errors.New("collector refused data (ACK_ERROR_MAGIC)")

type (
	ConnectionOpts struct {
		ProtocolAddress string
		Timeout         io.TcpTimeout
	}

	// InitStreamReply carries the collector's INIT_STREAM_V2 response fields
	// the virtual dumper needs for its rotation policy (06 §4).
	InitStreamReply struct {
		Handle common.Uuid
		// RotationPeriodMs asks the agent to rotate the stream this often;
		// 0 disables time-based rotation.
		RotationPeriodMs uint64
		// RequiredRotationSize asks the agent to rotate once the stream file
		// grows past this many bytes; 0 disables size-based rotation.
		RequiredRotationSize uint64
		// SeqId is the server-side rolling sequence id; the agent adopts
		// SeqId+1 as its file index (RemoteAndLocalOutputStream).
		SeqId int
	}
)
