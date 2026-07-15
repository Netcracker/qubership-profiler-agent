package hotstore

import (
	"context"
	"os"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuspendOverlapMs(t *testing.T) {
	pauses := []SuspendPause{
		{TimeMs: 150, DurationMs: 30}, // [150, 180)
		{TimeMs: 300, DurationMs: 20}, // [300, 320)
	}
	for _, tc := range []struct {
		name       string
		tsMs       int64
		durationMs int
		want       int
	}{
		{"no overlap before", 10, 50, 0},
		{"call covers both pauses", 100, 400, 50},
		{"partial head", 160, 10, 10},
		{"partial tail", 100, 60, 10},
		{"between pauses", 200, 50, 0},
		{"call inside a pause", 155, 10, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, suspendOverlapMs(pauses, tc.tsMs, tc.durationMs))
		})
	}
}

func TestBlobBufferSpill(t *testing.T) {
	dir := t.TempDir()
	prefix := []byte{1, 2, 3, 4, 5, 6, 7, 8}

	t.Run("stays in RAM under the limit", func(t *testing.T) {
		b := newBlobBuffer(dir, 64, prefix)
		require.NoError(t, b.Append([]byte("abc")))
		assert.False(t, b.Spilled())
		got, err := b.Bytes()
		require.NoError(t, err)
		assert.Equal(t, append(append([]byte(nil), prefix...), "abc"...), got)
		b.Free()
	})

	t.Run("spills past the limit and reads back", func(t *testing.T) {
		b := newBlobBuffer(dir, 10, prefix)
		require.NoError(t, b.Append([]byte("hello")))
		require.True(t, b.Spilled(), "8-byte prefix + 5 bytes exceed the 10-byte limit")
		require.NoError(t, b.Append([]byte(" world")))
		got, err := b.Bytes()
		require.NoError(t, err)
		assert.Equal(t, append(append([]byte(nil), prefix...), "hello world"...), got)

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		assert.Len(t, entries, 1, "the spill file lives under the scratch dir")
		b.Free()
		entries, err = os.ReadDir(dir)
		require.NoError(t, err)
		assert.Empty(t, entries, "Free removes the spill file")
		b.Free() // idempotent
	})
}

func TestParsePodRestartKey(t *testing.T) {
	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod-1", RestartTimeMs: 1_700_000_000_000}
	parsed, err := ParsePodRestartKey(key.String())
	require.NoError(t, err)
	assert.Equal(t, key, parsed)

	_, err = ParsePodRestartKey("ns/svc/pod-1")
	assert.Error(t, err)
	_, err = ParsePodRestartKey("ns/svc/pod-1/not-a-number")
	assert.Error(t, err)
}

func TestParseChunk(t *testing.T) {
	stream, offsets := wire.TraceStream(500, []wire.TraceChunk{
		{ThreadId: 42, StartMs: 1_000, Events: []wire.TraceEvent{
			wire.Enter(0, 7), wire.Tag(1, 9, "value"), wire.Enter(40, 8), wire.Exit(1), wire.Exit(2),
		}},
		{ThreadId: 43, StartMs: 2_000, Events: []wire.TraceEvent{wire.Enter(0, 7), wire.Exit(1)}},
	})
	chunk := stream[offsets[0]:offsets[1]]

	var events []TraceEvent
	threadId, consumed, err := ParseChunk(chunk, func(index int, ev TraceEvent) bool {
		require.Equal(t, len(events), index, "event indexes are sequential")
		events = append(events, ev)
		return true
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), threadId)
	assert.Equal(t, len(chunk), consumed, "a complete parse consumes the chunk including the finish marker")
	require.Len(t, events, 5)
	assert.Equal(t, TraceEvent{Kind: TraceEnter, TagId: 7}, events[0])
	assert.Equal(t, TraceEvent{Kind: TraceTag, TagId: 9, Value: "value"}, events[1])
	assert.Equal(t, TraceEvent{Kind: TraceEnter, TagId: 8}, events[2],
		"a delta with a varint continuation must not shift the payload")
	assert.Equal(t, TraceEvent{Kind: TraceExit}, events[3])
	assert.Equal(t, TraceEvent{Kind: TraceExit}, events[4])

	t.Run("stops early when visit returns false", func(t *testing.T) {
		count := 0
		_, _, err := ParseChunk(chunk, func(int, TraceEvent) bool {
			count++
			return count < 2
		})
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("truncation is an error", func(t *testing.T) {
		_, _, err := ParseChunk(chunk[:len(chunk)-1], func(int, TraceEvent) bool { return true })
		assert.Error(t, err, "a chunk without EVENT_FINISH_RECORD must not parse")
	})
}

// TestSealLoopDueAndLateData drives SealDue over a store fed through the
// public API, without a TCP server: the first pass seals the due bucket, a
// late Call re-triggers it into a patch file with the next <seq>
// (01-write-contract.md §6.1, §6.6).
func TestSealLoopDueAndLateData(t *testing.T) {
	ctx := context.Background()
	store, err := Open(Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer func() { _ = store.Close() }()

	key := PodRestartKey{Namespace: "ns", Service: "svc", PodName: "pod", RestartTimeMs: 1_000}
	pr, err := store.OpenPodRestart(key)
	require.NoError(t, err)

	baseMs := int64(1_700_000_000_000)
	call := func(tsMs int64, bufferOffset int) {
		require.NoError(t, pr.AppendCall(tsMs, data.Call{
			Method: 1, Duration: 10, ThreadName: "main",
			TraceFileIndex: 1, BufferOffset: bufferOffset, RecordIndex: 0,
		}))
	}
	call(baseMs, 0)
	call(baseMs+1, 100)

	cfg := store.Config()
	bucket := cfg.Bucket(baseMs)
	graceMs := cfg.TimeBucketGrace.Milliseconds()
	bucketEndMs := cfg.BucketStartMs(bucket) + cfg.TimeBucket.Milliseconds()

	sealed, err := store.SealDue(ctx, bucketEndMs+graceMs-1)
	require.NoError(t, err)
	assert.Zero(t, sealed, "the grace has not elapsed yet")

	sealed, err = store.SealDue(ctx, bucketEndMs+graceMs)
	require.NoError(t, err)
	assert.Equal(t, 1, sealed)
	files, err := store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, 2, files[0].RowCount)
	assert.Equal(t, 0, files[0].Seq)
	// No trace segments exist, so the rows sealed with NULL blobs.
	assert.Equal(t, int64(2), store.SealCountersSnapshot().Truncated[TruncDiskBudget])

	sealed, err = store.SealDue(ctx, bucketEndMs+graceMs)
	require.NoError(t, err)
	assert.Zero(t, sealed, "the watermark covers every indexed call")

	// A late Call for the sealed bucket re-marks it; the next pass emits a
	// patch file with only the new row.
	call(baseMs+2, 200)
	sealed, err = store.SealDue(ctx, bucketEndMs+graceMs)
	require.NoError(t, err)
	assert.Equal(t, 1, sealed)
	files, err = store.LocalParquet(key)
	require.NoError(t, err)
	require.Len(t, files, 2)
	assert.Equal(t, 1, files[1].RowCount, "a patch seal covers only the rows past the watermark")
	assert.ElementsMatch(t, []int{0, 1}, []int{files[0].Seq, files[1].Seq})
}
