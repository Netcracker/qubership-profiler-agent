package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	"github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmulator drives the Go agent emulator against the live TCP server and
// checks the server-side wire contract (06-wire-protocol-server.md): the
// handshake reply, the ack cycle, and unknown-stream teardown.
func TestEmulator(t *testing.T) {
	ctx := log.SetLevel(context.Background(), log.DEBUG)
	const ns, svc = "test_namespace", "test_service"

	prepareServer(t, ctx)

	// §3: the collector must reply PROTOCOL_VERSION_V2, never V3 — otherwise the
	// agent switches to the posDictionary stream the collector cannot demux.
	t.Run("handshake replies PROTOCOL_VERSION_V2", func(t *testing.T) {
		ac, err := prepareAgent(t, ctx)
		require.NoError(t, err)
		err = ac.InitializeConnection(model.PROTOCOL_VERSION_V3, ns, svc, "pod-handshake")
		require.NoError(t, err)
		assert.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())
	})

	// §5: every RCV_DATA and REQUEST_ACK_FLUSH is acknowledged with one byte, so
	// the agent's flush cycle drains without stalling into a reconnect.
	t.Run("flush cycle drains without reconnect", func(t *testing.T) {
		ac, err := prepareAgent(t, ctx)
		require.NoError(t, err)
		err = ac.InitializeConnection(model.PROTOCOL_VERSION_V3, ns, svc, "pod-flush")
		require.NoError(t, err)

		handle, err := ac.CommandInitStream(model.StreamDictionary, 0, false)
		require.NoError(t, err)
		assert.NotEqual(t, [16]byte{}, handle.ToBin(), "collector must return a non-nil stream handle")

		for i := 0; i < 5; i++ {
			require.NoError(t, ac.CommandRcvData(model.StreamDictionary, handle, []byte("word")))
		}
		require.NoError(t, ac.Flush())
		require.NoError(t, ac.WaitForAcks(), "every RCV_DATA / REQUEST_ACK_FLUSH must be acked")
	})

	// §5 "Flush timing": the collector must flush a buffered RCV_DATA ack on its
	// own 500 ms cadence, without waiting for a REQUEST_ACK_FLUSH. The agent
	// drains all pending acks before every stream rotation, so an ack that only
	// flushes on the next command would stall the rotation until the 30 s
	// ack-read timeout. This is the regression guard for the missing periodic
	// flush.
	t.Run("buffered ack flushes on the periodic cadence", func(t *testing.T) {
		ac, err := prepareAgent(t, ctx)
		require.NoError(t, err)
		err = ac.InitializeConnection(model.PROTOCOL_VERSION_V3, ns, svc, "pod-periodic-flush")
		require.NoError(t, err)

		handle, err := ac.CommandInitStream(model.StreamDictionary, 0, false)
		require.NoError(t, err)

		// One RCV_DATA, counted as one pending ack. CommandRcvData does not flush,
		// so DrainAcks(true) below only pushes the buffered RCV_DATA to the socket
		// and then blocks reading the ack — it never sends REQUEST_ACK_FLUSH (only
		// Flush() does). The only thing that can deliver the ack is therefore the
		// collector's periodic flush.
		require.NoError(t, ac.CommandRcvData(model.StreamDictionary, handle, []byte("word")))
		require.Equal(t, 1, ac.PendingAcks(), "the ack is still outstanding right after RCV_DATA")

		// FlushCheckInterval is 500 ms, so the ack must land well under the client
		// read timeout (2 s). Without the periodic flush it would block until that
		// timeout and DrainAcks would fail.
		start := time.Now()
		err = ac.DrainAcks(true)
		elapsed := time.Since(start)
		require.NoError(t, err, "the buffered ack must arrive without a REQUEST_ACK_FLUSH")
		assert.Equal(t, 0, ac.PendingAcks(), "the periodic flush must have delivered the ack")
		assert.Less(t, elapsed, 1*time.Second,
			"the collector must flush the buffered ack on its ~500 ms cadence, not stall on the ack-read timeout")
	})

	// §6: an unknown stream gets a null handle and a teardown, so the agent
	// cannot proceed with a bogus stream.
	t.Run("unknown stream is refused", func(t *testing.T) {
		ac, err := prepareAgent(t, ctx)
		require.NoError(t, err)
		err = ac.InitializeConnection(model.PROTOCOL_VERSION_V3, ns, svc, "pod-bogus")
		require.NoError(t, err)

		_, err = ac.CommandInitStream("bogus", 0, false)
		assert.Error(t, err, "unknown stream must not yield a valid handle")
	})

	// Pre-v3.1.4 agents register a "gc" stream unconditionally when streaming
	// remotely; refusing it used to tear down the whole connection, so no
	// other stream's data ever landed either. The collector now accepts and
	// discards it instead of refusing it.
	t.Run("gc stream is accepted and discarded", func(t *testing.T) {
		ac, err := prepareAgent(t, ctx)
		require.NoError(t, err)
		err = ac.InitializeConnection(model.PROTOCOL_VERSION_V3, ns, svc, "pod-gc")
		require.NoError(t, err)

		handle, err := ac.CommandInitStream(model.StreamGc, 0, false)
		require.NoError(t, err, "gc must not be refused like an unknown stream")
		assert.NotEqual(t, [16]byte{}, handle.ToBin(), "collector must return a non-nil stream handle")

		require.NoError(t, ac.CommandRcvData(model.StreamGc, handle, []byte("gc log bytes")))
		require.NoError(t, ac.Flush())
		require.NoError(t, ac.WaitForAcks(), "the gc stream must not desync the ack cycle")
	})

	// №5: a data command before the handshake used to deref a nil sc.pod and
	// crash the whole collector. The server must reject it (ACK_ERROR_MAGIC +
	// close) and stay up for every other agent.
	t.Run("pre-handshake data does not crash the server", func(t *testing.T) {
		t.Run("RCV_DATA before handshake is refused", func(t *testing.T) {
			ac, err := prepareAgent(t, ctx)
			require.NoError(t, err)
			// No InitializeConnection: send RCV_DATA straight away. The ack for
			// RCV_DATA drains via WaitForAcks, which reads ACK_ERROR_MAGIC.
			sendErr := ac.CommandRcvStringData(model.StreamDictionary, common.RandomUuid(), "word")
			ackErr := ac.WaitForAcks()
			assert.True(t, sendErr != nil || ackErr != nil,
				"the server signals ACK_ERROR_MAGIC and closes (send err %v, ack err %v)", sendErr, ackErr)
			_ = ac.Close()
		})

		t.Run("INIT_STREAM before handshake is refused", func(t *testing.T) {
			ac, err := prepareAgent(t, ctx)
			require.NoError(t, err)
			_, err = ac.CommandInitStream(model.StreamDictionary, 0, false)
			assert.Error(t, err, "the server signals ACK_ERROR_MAGIC and closes")
			_ = ac.Close()
		})

		// The server survived both pre-handshake attacks: a fresh connection
		// still completes a normal handshake and flush cycle.
		t.Run("a healthy connection still works afterwards", func(t *testing.T) {
			ac, err := prepareAgent(t, ctx)
			require.NoError(t, err)
			err = ac.InitializeConnection(model.PROTOCOL_VERSION_V3, ns, svc, "pod-after-attack")
			require.NoError(t, err, "the server must still accept a handshake")
			assert.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())

			handle, err := ac.CommandInitStream(model.StreamDictionary, 0, false)
			require.NoError(t, err)
			require.NoError(t, ac.CommandRcvData(model.StreamDictionary, handle, []byte("word")))
			require.NoError(t, ac.Flush())
			require.NoError(t, ac.WaitForAcks())
		})
	})
}

func prepareServer(t *testing.T, ctx context.Context) {
	serverOpts := server.ConnectionOpts{
		ProtocolPort: 1715,
		Timeout: io.TcpTimeout{
			ConnectTimeout: 10 * time.Second,
			SessionTimeout: 60 * time.Second,
			ReadTimeout:    40 * time.Second,
			WriteTimeout:   2 * time.Second,
		},
	}

	serverListener := CreateMockServerListener()
	sc := server.PrepareServer(ctx, serverOpts, serverListener)
	assert.NotNil(t, sc)
	go func() {
		err := sc.Start(ctx)
		assert.Nil(t, err)
	}()
	time.Sleep(100 * time.Millisecond) // wait for the mock server to bind
}

func prepareAgent(t *testing.T, ctx context.Context) (*emulator.AgentConnection, error) {
	clientOpts := emulator.ConnectionOpts{
		ProtocolAddress: "localhost:1715",
		Timeout: io.TcpTimeout{
			ConnectTimeout: 10 * time.Second,
			SessionTimeout: 20 * time.Second,
			ReadTimeout:    2 * time.Second,
			WriteTimeout:   2 * time.Second,
		},
	}
	agentListener := CreateMockAgentListener()
	ac := emulator.PrepareAgent(ctx, nil, agentListener, "testPod")
	assert.NotNil(t, ac)

	err := ac.Prepare(clientOpts).Connect()
	assert.Nil(t, err)
	return ac, err
}
