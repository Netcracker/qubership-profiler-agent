package emulator_test

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator/emutest"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin the transport half of the virtual-dumper contract
// (backend/docs/design/virtual-dumper.md §2.4): ack accounting, the
// opportunistic vs synchronous drains, the typed backpressure error, and the
// piggybacked-command dialog — each traced to DefaultCollectorClient.java.

func dial(t *testing.T, col *emutest.Collector) *emulator.AgentConnection {
	t.Helper()
	ac := emulator.PrepareAgent(context.Background(), nil, nil, "pod-1")
	err := ac.Prepare(emulator.ConnectionOpts{
		ProtocolAddress: col.Addr(),
		Timeout: profio.TcpTimeout{
			ConnectTimeout: 2 * time.Second,
			SessionTimeout: time.Minute,
			ReadTimeout:    2 * time.Second,
			WriteTimeout:   2 * time.Second,
		},
	}).Connect()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })
	require.NoError(t, ac.InitializeConnection(model.PROTOCOL_VERSION_V3, "ns", "svc", "pod-1"))
	require.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())
	return ac
}

// TestRcvDataAckAccounting: each RCV_DATA counts exactly one pending ack and
// triggers no flush; the flush cycle sends exactly one REQUEST_ACK_FLUSH and
// drains everything (G6 — the old emulator appended REQUEST_ACK_FLUSH to every
// payload and counted two acks for it).
func TestRcvDataAckAccounting(t *testing.T) {
	col := emutest.Start(t)
	col.BufferAcks = true // mirror a collector that only force-flushes acks
	ac := dial(t, col)

	hs := col.EventsOf(model.COMMAND_GET_PROTOCOL_VERSION_V2)
	require.Len(t, hs, 1)
	assert.Equal(t, "pod-1", hs[0].Pod)
	assert.Equal(t, "svc", hs[0].Service)
	assert.Equal(t, "ns", hs[0].Namespace)

	reply, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)

	payload := []byte("0123456789abcdef")
	for i := 0; i < 3; i++ {
		require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, payload))
	}
	assert.Equal(t, 3, ac.PendingAcks(), "one pending ack per RCV_DATA, no extras")
	assert.Empty(t, col.EventsOf(model.COMMAND_REQUEST_ACK_FLUSH),
		"no REQUEST_ACK_FLUSH may be sent between flush cycles")

	require.NoError(t, ac.Flush())
	assert.Equal(t, 0, ac.PendingAcks(), "the flush cycle drains every ack")
	assert.Len(t, col.EventsOf(model.COMMAND_REQUEST_ACK_FLUSH), 1,
		"the flush cycle sends exactly one REQUEST_ACK_FLUSH")
	assert.Len(t, col.EventsOf(model.COMMAND_RCV_DATA), 3)
	assert.Equal(t, append(append(append([]byte{}, payload...), payload...), payload...),
		col.StreamData(0, model.StreamTrace))
}

// TestOpportunisticDrain: acks that already arrived are consumed before the
// next RCV_DATA, so pendingAcks does not grow without bound between flush
// cycles (validateWriteDataAcks(false)).
func TestOpportunisticDrain(t *testing.T) {
	col := emutest.Start(t) // default: the collector flushes each ack promptly
	ac := dial(t, col)

	reply, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)

	// Push enough full payloads through the 8 KB client buffer that most reach
	// the collector and get acked while we sleep.
	payload := make([]byte, emulator.MaxBufSize)
	for i := 0; i < 9; i++ {
		require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, payload))
	}
	time.Sleep(100 * time.Millisecond) // let the collector ack what it received

	require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, payload))
	assert.Less(t, ac.PendingAcks(), 10,
		"readable acks must be drained opportunistically before a write")

	require.NoError(t, ac.Flush())
	assert.Equal(t, 0, ac.PendingAcks())
}

// TestAckErrorIsTyped: ACK_ERROR_MAGIC surfaces as ErrAckRefused, so the
// virtual dumper can tell collector backpressure from a broken connection (G3).
func TestAckErrorIsTyped(t *testing.T) {
	col := emutest.Start(t)
	col.AckOf = func(conn, ackSeq int) emutest.Ack {
		return emutest.Ack{Refuse: ackSeq == 0}
	}
	ac := dial(t, col)

	reply, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)
	require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, []byte("x")))

	err = ac.Flush()
	require.Error(t, err)
	assert.ErrorIs(t, err, emulator.ErrAckRefused)
}

// TestPiggybackedCommands: an ack byte > 0 carries that many (id, command)
// pairs; the client reads them and reports failure for each — it has no
// diagtools (validateAckSync → dispatchCommands).
func TestPiggybackedCommands(t *testing.T) {
	id1, id2 := common.RandomUuid(), common.RandomUuid()
	col := emutest.Start(t)
	col.AckOf = func(conn, ackSeq int) emutest.Ack {
		if ackSeq == 0 {
			return emutest.Ack{Commands: []emutest.Piggyback{{Id: id1, Text: "td"}, {Id: id2, Text: "heap"}}}
		}
		return emutest.Ack{}
	}
	ac := dial(t, col)

	reply, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)
	require.NoError(t, ac.CommandRcvData(model.StreamTrace, reply.Handle, []byte("x")))
	require.NoError(t, ac.Flush())
	assert.Equal(t, 0, ac.PendingAcks())

	require.Eventually(t, func() bool {
		return len(col.EventsOf(model.COMMAND_REPORT_COMMAND_RESULT)) == 2
	}, 2*time.Second, 10*time.Millisecond, "both piggybacked commands must be answered")
	reports := col.EventsOf(model.COMMAND_REPORT_COMMAND_RESULT)
	assert.Equal(t, id1.ToBin(), reports[0].CommandId.ToBin())
	assert.Equal(t, id2.ToBin(), reports[1].CommandId.ToBin())
	for _, r := range reports {
		assert.False(t, r.Success, "the emulator reports failure: it has no diagtools")
	}
}

// TestInitStreamDrainsPendingAcks: a stream open never interleaves with data
// acks — everything pending is drained synchronously first
// (attemptCreateRollingChunk) — and the reply carries the collector's rotation
// policy.
func TestInitStreamDrainsPendingAcks(t *testing.T) {
	col := emutest.Start(t)
	col.InitReplyOf = func(conn int, stream string) emutest.InitReply {
		return emutest.InitReply{RotationPeriodMs: 60_000, RequiredRotationSize: 12_345}
	}
	ac := dial(t, col)

	trace, err := ac.InitStream(model.StreamTrace, 0, false)
	require.NoError(t, err)
	require.NoError(t, ac.CommandRcvData(model.StreamTrace, trace.Handle, []byte("a")))
	require.NoError(t, ac.CommandRcvData(model.StreamTrace, trace.Handle, []byte("b")))

	calls, err := ac.InitStream(model.StreamCalls, 4, false)
	require.NoError(t, err)
	assert.Equal(t, 0, ac.PendingAcks(), "InitStream must drain pending acks before opening")
	assert.Equal(t, uint64(60_000), calls.RotationPeriodMs)
	assert.Equal(t, uint64(12_345), calls.RequiredRotationSize)
	assert.Equal(t, 4, calls.SeqId, "the double echoes the requested sequence id")

	var kinds []model.Command
	for _, e := range col.Events() {
		if e.Command != model.COMMAND_GET_PROTOCOL_VERSION_V2 {
			kinds = append(kinds, e.Command)
		}
	}
	assert.Equal(t, []model.Command{
		model.COMMAND_INIT_STREAM_V2,
		model.COMMAND_RCV_DATA,
		model.COMMAND_RCV_DATA,
		model.COMMAND_INIT_STREAM_V2,
	}, kinds, "both payloads must precede the second stream open on the wire")
}

// TestInitStreamNullHandle: a null-UUID reply is a refusal, not a handle.
func TestInitStreamNullHandle(t *testing.T) {
	col := emutest.Start(t)
	col.InitReplyOf = func(conn int, stream string) emutest.InitReply {
		return emutest.InitReply{NullHandle: true}
	}
	ac := dial(t, col)

	_, err := ac.InitStream(model.StreamTrace, 0, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "null handle")
}

// TestInitStreamCarriesResetFlag: the resetRequired flag and the requested
// sequence id cross the wire as sent — the dictionary resend after a reconnect
// depends on them (G5).
func TestInitStreamCarriesResetFlag(t *testing.T) {
	col := emutest.Start(t)
	ac := dial(t, col)

	_, err := ac.InitStream(model.StreamDictionary, 3, true)
	require.NoError(t, err)

	inits := col.EventsOf(model.COMMAND_INIT_STREAM_V2)
	require.Len(t, inits, 1)
	assert.Equal(t, model.StreamDictionary, inits[0].Stream)
	assert.Equal(t, 3, inits[0].SeqId)
	assert.True(t, inits[0].Reset)
}
