package integration

import (
	"context"
	"testing"
	"time"

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
