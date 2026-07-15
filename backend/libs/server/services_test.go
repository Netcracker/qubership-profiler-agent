package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubListener is the minimal Listener a connection handler needs.
type stubListener struct{}

func (stubListener) RegisterPod(*ConnectedPod) error { return nil }
func (stubListener) AppendData(context.Context, *ConnectedPod, common.Uuid, string) (int, error) {
	return 0, nil
}
func (stubListener) RegisterStream(context.Context, *ConnectedPod, common.Uuid, string, int, int, int, uint64, uint64) error {
	return nil
}
func (stubListener) PodDisconnected(context.Context, *ConnectedPod)                       {}
func (stubListener) SentCommand(context.Context, model.Command)                           {}
func (stubListener) ReceivedCommand(context.Context, model.Command, time.Duration, error) {}
func (stubListener) Read(context.Context, int, time.Duration, error)                      {}
func (stubListener) Write(context.Context, int, time.Duration, error)                     {}
func (stubListener) IsAlive(context.Context) (bool, error)                                { return true, nil }
func (stubListener) Error(error)                                                          {}
func (stubListener) PrintDebug(context.Context)                                           {}
func (stubListener) Close(context.Context)                                                {}

// TestStopClosesActiveConnections pins the 03 §5.4 drain budget: Stop must
// close active agent connections and return promptly. Before the fix it only
// closed the listener and waited for the handlers, so one idle-but-healthy
// agent held shutdown hostage for the whole read deadline.
func TestStopClosesActiveConnections(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ss := PrepareServer(ctx, ConnectionOpts{
		ProtocolPort: 0, // an ephemeral port; Addr reports the bound one
		Timeout: profio.TcpTimeout{
			// Long enough that a drain-by-deadline would visibly hang the test.
			ReadTimeout:  30 * time.Second,
			WriteTimeout: time.Second,
		},
	}, stubListener{})
	serveDone := make(chan error, 1)
	go func() { serveDone <- ss.Start(ctx) }()
	require.Eventually(t, func() bool { return ss.Addr() != nil },
		5*time.Second, 10*time.Millisecond, "the listener must bind")

	conn, err := net.Dial("tcp", ss.Addr().String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	require.Eventually(t, func() bool {
		ss.mu.Lock()
		defer ss.mu.Unlock()
		return len(ss.active) == 1
	}, 5*time.Second, 10*time.Millisecond, "the connection must reach its handler")

	start := time.Now()
	ss.Stop()
	assert.Less(t, time.Since(start), 5*time.Second,
		"Stop must close active connections, not wait out their read deadline")

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	assert.Error(t, err, "the agent side observes the close and reconnects elsewhere")
	require.NoError(t, <-serveDone, "a stopped listener is not a failure")
}
