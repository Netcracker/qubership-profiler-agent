package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-agent/libs/wire/common"
	"github.com/Netcracker/qubership-profiler-agent/libs/wire/emulator"
	profio "github.com/Netcracker/qubership-profiler-agent/libs/wire/io"
	model "github.com/Netcracker/qubership-profiler-agent/libs/wire/protocol"
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
func (stubListener) ReceivedCommand(context.Context, model.Command, time.Duration, error) {}
func (stubListener) Read(context.Context, int, time.Duration, error)                      {}
func (stubListener) Write(context.Context, int, time.Duration, error)                     {}
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

// recordingPods is a stubListener that keeps the pods it registered.
type recordingPods struct {
	stubListener
	pods chan ConnectedPod
}

func (r recordingPods) RegisterPod(pod *ConnectedPod) error {
	r.pods <- *pod
	return nil
}

// TestServeConnHandlesAConnectionAcceptedElsewhere: the gateway accepts the
// socket and reads the handshake itself, then hands the connection over.
func TestServeConnHandlesAConnectionAcceptedElsewhere(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l := recordingPods{pods: make(chan ConnectedPod, 1)}
	ss := PrepareServer(ctx, ConnectionOpts{Timeout: profio.TcpTimeout{
		ReadTimeout: 5 * time.Second, WriteTimeout: time.Second,
	}}, l)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	served := make(chan struct{})
	go func() {
		defer close(served)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		ss.ServeConn(ctx, c)
	}()

	ac := emulator.PrepareAgent(ctx, nil, nil, "pod-1")
	require.NoError(t, ac.Prepare(emulator.ConnectionOpts{
		ProtocolAddress: ln.Addr().String(),
		Timeout:         profio.TcpTimeout{ConnectTimeout: time.Second, SessionTimeout: time.Minute, ReadTimeout: 5 * time.Second, WriteTimeout: time.Second},
	}).Connect())
	defer ac.Close()
	require.NoError(t, ac.InitializeConnection(model.PROTOCOL_VERSION_V3, "ns", "svc", "pod-1"))
	assert.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())
	pod := <-l.pods
	assert.Equal(t, "svc", pod.Service)

	ss.Stop()
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop must end a connection handed over with ServeConn")
	}
}

func TestServeConnAfterStopClosesTheConnection(t *testing.T) {
	ss := PrepareServer(context.Background(), ConnectionOpts{}, stubListener{})
	ss.Stop()
	server, client := net.Pipe()
	defer client.Close()
	ss.ServeConn(context.Background(), server)
	_, err := client.Read(make([]byte, 1))
	assert.Error(t, err)
}
