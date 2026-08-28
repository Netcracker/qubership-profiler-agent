package ingest

// Connection-lifecycle series for the T3 connection-ceiling runs
// (load-testing-plan.md §6.4): the gauge and the counters share one lock, so
// connects − disconnects = active must hold through every path — normal
// close, a connection that never passed the handshake, and collector
// shutdown.

import (
	"context"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func lifecycleOf(l *Listener) (connects, disconnects uint64, active int) {
	s := l.IngestStatsSnapshot()
	return s.ConnectsTotal, s.DisconnectsTotal, s.ActiveConnections
}

func TestLifecycleNormalClose(t *testing.T) {
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)

	podA, podB := testPod(t), testPod(t)
	podB.PodName = "pod-b"
	require.NoError(t, l.RegisterPod(podA))
	require.NoError(t, l.RegisterPod(podB))

	connects, disconnects, active := lifecycleOf(l)
	assert.EqualValues(t, 2, connects)
	assert.EqualValues(t, 0, disconnects)
	assert.Equal(t, 2, active)

	l.PodDisconnected(context.Background(), podA)
	connects, disconnects, active = lifecycleOf(l)
	assert.EqualValues(t, 2, connects)
	assert.EqualValues(t, 1, disconnects)
	assert.Equal(t, 1, active)
}

// TestLifecycleUnregisteredDisconnect: PodDisconnected fires for every ended
// connection, including ones that never reached RegisterPod (handshake
// failure). Those must move neither counter, or disconnects would overtake
// connects.
func TestLifecycleUnregisteredDisconnect(t *testing.T) {
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)

	l.PodDisconnected(context.Background(), testPod(t))
	connects, disconnects, active := lifecycleOf(l)
	assert.EqualValues(t, 0, connects)
	assert.EqualValues(t, 0, disconnects)
	assert.Equal(t, 0, active)

	// A repeated disconnect of an already-closed registered pod is also a
	// no-op on the second call.
	pod := testPod(t)
	require.NoError(t, l.RegisterPod(pod))
	l.PodDisconnected(context.Background(), pod)
	l.PodDisconnected(context.Background(), pod)
	connects, disconnects, active = lifecycleOf(l)
	assert.EqualValues(t, 1, connects)
	assert.EqualValues(t, 1, disconnects)
	assert.Equal(t, 0, active)
}

// TestLifecycleShutdown: Listener.Close (collector stop) settles the
// accounting for connections that never saw a per-connection
// PodDisconnected — the gauge returns to 0 and the counters balance.
func TestLifecycleShutdown(t *testing.T) {
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)

	for i, pod := range []struct{ name string }{{"pod-a"}, {"pod-b"}, {"pod-c"}} {
		p := testPod(t)
		p.PodName = pod.name
		p.RestartTimeMs += int64(i)
		require.NoError(t, l.RegisterPod(p))
	}
	l.Close(context.Background())

	connects, disconnects, active := lifecycleOf(l)
	assert.EqualValues(t, 3, connects)
	assert.EqualValues(t, 3, disconnects)
	assert.Equal(t, 0, active)
}
