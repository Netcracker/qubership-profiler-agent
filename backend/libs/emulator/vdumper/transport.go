package vdumper

import (
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
)

// Transport is the protocol-level connection the virtual dumper drives — the
// same surface DefaultCollectorClient offers the Java dumper.
// *emulator.AgentConnection implements it; phase 3 reuses it for the k6
// module.
type Transport interface {
	Connect() error
	InitializeConnection(protocolVersion uint64, namespace, service, pod string) error
	ServerVersion() uint64
	InitStream(streamType string, requestedSeqId int, resetRequired bool) (emulator.InitStreamReply, error)
	// CommandRcvData sends one payload of at most emulator.MaxBufSize bytes:
	// +1 pending ack, no flush.
	CommandRcvData(streamType string, handleId common.Uuid, chunk []byte) error
	// Flush runs the agent flush cycle: REQUEST_ACK_FLUSH, socket flush, and a
	// synchronous drain of every pending ack.
	Flush() error
	CommandClose() error
	Close() error
}

var _ Transport = (*emulator.AgentConnection)(nil)
