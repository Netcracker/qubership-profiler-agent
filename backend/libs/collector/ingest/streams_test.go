package ingest

// №2 at the ingest edge: a failed AppendCall must fail the calls stream (the
// server then answers ACK_ERROR and the agent re-sends after reconnect), and
// the backpressure gate must refuse RCV_DATA before writing anything.

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testPod(t *testing.T) *server.ConnectedPod {
	t.Helper()
	uuid, err := common.RandomUuidChecked()
	require.NoError(t, err)
	return &server.ConnectedPod{
		Uuid: uuid, Namespace: "ns", Service: "svc", PodName: "pod-i",
		RestartTimeMs: 1_700_000_000_000,
	}
}

// TestCallsDecoderEscalatesAppendErrors pins the №2 loss fix: when
// AppendCall cannot persist (calls.wal rejects the write — ENOSPC being the
// production case), the decoder fails the stream, so a later AppendData
// surfaces the error instead of the calls silently vanishing.
func TestCallsDecoderEscalatesAppendErrors(t *testing.T) {
	ctx := context.Background()
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)
	pod := testPod(t)
	require.NoError(t, l.RegisterPod(pod))

	handle, err := common.RandomUuidChecked()
	require.NoError(t, err)
	require.NoError(t, l.RegisterStream(ctx, pod, handle, model.StreamCalls, 0, 0, 0, 0, 0))

	// Sabotage the WAL underneath: a closed pod-restart rejects appends the
	// same way a full PV does.
	pr, ok := store.PodRestart(hotstore.PodRestartKey{
		Namespace: pod.Namespace, Service: pod.Service,
		PodName: pod.PodName, RestartTimeMs: pod.RestartTimeMs,
	})
	require.True(t, ok)
	require.NoError(t, pr.Close())

	payload := string(wire.CallsStream(1_700_000_000_000, []int64{0}))
	// The pipe hands the payload to the decoder asynchronously; the failure
	// lands on a subsequent write once the decoder has rejected the stream.
	require.Eventually(t, func() bool {
		_, err := l.AppendData(ctx, pod, handle, payload)
		return err != nil
	}, 2*time.Second, 10*time.Millisecond,
		"a failed AppendCall must fail the stream, not be swallowed")
}

// TestAppendDataRefusedUnderBackpressure pins the №2 accept stop: with the
// pending budget exceeded, AppendData refuses before writing, so the server
// answers ACK_ERROR and the payload stays on the agent.
func TestAppendDataRefusedUnderBackpressure(t *testing.T) {
	ctx := context.Background()
	// Any live partition outweighs the 16-byte budget; the gate engages on
	// the first janitor pass.
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir(), PendingUploadMaxBytes: 16})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)
	pod := testPod(t)
	require.NoError(t, l.RegisterPod(pod))

	pr, ok := store.PodRestart(hotstore.PodRestartKey{
		Namespace: pod.Namespace, Service: pod.Service,
		PodName: pod.PodName, RestartTimeMs: pod.RestartTimeMs,
	})
	require.True(t, ok)
	require.NoError(t, pr.AppendCall(1_700_000_000_000, data.Call{
		Method: 0, Duration: 1, ThreadName: "main",
		TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
	}))
	_, err = store.JanitorPass(ctx, time.Now().UnixMilli())
	require.NoError(t, err)
	require.True(t, store.IngestPaused())

	handle, err := common.RandomUuidChecked()
	require.NoError(t, err)
	require.NoError(t, l.RegisterStream(ctx, pod, handle, model.StreamSql, 0, 0, 0, 0, 0))
	_, err = l.AppendData(ctx, pod, handle, "payload")
	require.Error(t, err, "the gate refuses before writing")
	assert.Zero(t, l.IngestStatsSnapshot().BytesRead, "nothing was persisted for the refused payload")
}

// TestGcStreamIsAcceptedAndDiscarded pins the pre-v3.1.4 compatibility fix:
// registering the legacy "gc" stream must not error (the agent opens it
// unconditionally, regardless of GC-log harvesting), and its payload is
// counted as read but never persisted anywhere.
func TestGcStreamIsAcceptedAndDiscarded(t *testing.T) {
	ctx := context.Background()
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)
	pod := testPod(t)
	require.NoError(t, l.RegisterPod(pod))

	handle, err := common.RandomUuidChecked()
	require.NoError(t, err)
	require.NoError(t, l.RegisterStream(ctx, pod, handle, model.StreamGc, 0, 0, 0, 0, 0),
		"the collector must register the gc stream instead of refusing it")

	n, err := l.AppendData(ctx, pod, handle, "gc log bytes nobody wants")
	require.NoError(t, err, "gc payload must be discarded, not rejected")
	assert.Equal(t, len("gc log bytes nobody wants"), n)
	assert.EqualValues(t, len("gc log bytes nobody wants"), l.IngestStatsSnapshot().BytesRead,
		"the bytes are counted as read even though nothing is stored")
}

// TestGcStreamDoesNotBreakSiblingStreams reproduces the real-world failure:
// a pre-v3.1.4 agent opens gc alongside its real data streams on the same
// connection. Before the fix, registering an unknown stream tore down the
// whole pod-restart, so the sibling stream never got a chance either.
func TestGcStreamDoesNotBreakSiblingStreams(t *testing.T) {
	ctx := context.Background()
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()
	l := NewListener(store)
	pod := testPod(t)
	require.NoError(t, l.RegisterPod(pod))

	gcHandle, err := common.RandomUuidChecked()
	require.NoError(t, err)
	require.NoError(t, l.RegisterStream(ctx, pod, gcHandle, model.StreamGc, 0, 0, 0, 0, 0))

	callsHandle, err := common.RandomUuidChecked()
	require.NoError(t, err)
	require.NoError(t, l.RegisterStream(ctx, pod, callsHandle, model.StreamCalls, 0, 0, 0, 0, 0))

	payload := string(wire.CallsStream(1_700_000_000_000, []int64{0}))
	_, err = l.AppendData(ctx, pod, callsHandle, payload)
	require.NoError(t, err, "a sibling stream must keep working once gc is registered")

	// Finalize synchronously (mirrors a real disconnect) so the calls
	// decoder goroutine has drained before TempDir's cleanup runs.
	l.PodDisconnected(ctx, pod)
}
