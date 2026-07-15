package integration

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	io2 "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/server"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The synthetic pod-restart driven through the live TCP server.
const (
	hotstorePort = 17151
	hotstoreNs   = "test_namespace"
	hotstoreSvc  = "test_service"
	hotstorePod  = "pod-hotstore"

	baseMs       = int64(1_700_000_000_000)
	timerStartMs = baseMs - 1_000

	methodHandle = 7 // dictionary ids the calls reference
	methodQuery  = 8

	threadHandle = uint64(101) // trace chunk threads
	threadQuery  = uint64(202)
)

// Dictionary arrival order fixes the word ids (06 §3: V2 format).
var dictWords = []string{"com.example.Service.handle", "com.example.Db.query", "call.red", "request.id"}

const (
	dictIdCallRed   = 2
	dictIdRequestId = 3
)

// TestHotStoreIngest drives the emulator through handshake → INIT_STREAM_V2 →
// RCV_DATA → flush for all seven streams and checks the Stage 1 hot store:
//
//  1. trace segments are named by rolling_seq + 1, and each Call's
//     trace_file_index resolves to the segment holding its root ENTER
//     (01-write-contract.md §4.4, guard M7);
//  2. the SQLite call index reconstructs absolute ts_ms by accumulating the
//     per-record deltas (01 §5.1, guard B1);
//  3. chunk_index and the segment catalog are populated; the dictionary,
//     params, and suspend WALs are written;
//  4. after a simulated restart, recovery rebuilds chunk_index, the catalog,
//     and the call index from the PV alone (03-lifecycle.md §3).
func TestHotStoreIngest(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	svc := startCollector(t, ctx, dataDir)
	store := svc.Store()

	traceFile1, offsets1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: threadHandle, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, methodHandle), wire.Tag(1, dictIdRequestId, "req-42"), wire.Exit(2),
		}},
		{ThreadId: threadQuery, StartMs: baseMs + 3, Events: []wire.TraceEvent{
			wire.Enter(0, methodQuery), wire.Exit(1),
		}},
		{ThreadId: threadHandle, StartMs: baseMs + 50, Events: []wire.TraceEvent{
			wire.Enter(0, methodHandle), wire.Exit(5),
		}},
	})
	traceFile2, offsets2 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: threadQuery, StartMs: baseMs + 59_000, Events: []wire.TraceEvent{
			wire.Enter(0, methodQuery), wire.Exit(3),
		}},
		{ThreadId: threadHandle, StartMs: baseMs + 180_000, Events: []wire.TraceEvent{
			wire.Enter(0, methodHandle), wire.Exit(7),
		}},
	})

	// Three records whose wire times are deltas: 5 ms, then one and two
	// minutes apart. The third lands in the next 5-minute bucket and carries
	// the call.red error marker.
	callRecords := []wire.CallRecord{
		{DeltaMs: 5, Method: methodHandle, DurationMs: 42, ChildCalls: 1, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(offsets1[0]), RecordIndex: 0,
			Params: map[int][]string{dictIdRequestId: {"req-42"}}},
		{DeltaMs: 60_000, Method: methodQuery, DurationMs: 500, ChildCalls: 2, ThreadName: "exec-2",
			TraceFileIndex: 2, BufferOffset: int(offsets2[0]), RecordIndex: 0},
		{DeltaMs: 120_000, Method: methodHandle, DurationMs: 2_000, ChildCalls: 3, ThreadName: "exec-1",
			TraceFileIndex: 2, BufferOffset: int(offsets2[1]), RecordIndex: 0,
			Params: map[int][]string{dictIdCallRed: {"1"}}},
	}
	wantTs := []int64{baseMs + 5, baseMs + 60_005, baseMs + 180_005}

	ac := connectAgent(t, ctx)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(dictWords))

	// Barrier: the calls records below derive error_flag from the dictionary,
	// which decodes on its own pipeline; wait for it before sending calls.
	key := waitForPodRestart(t, store)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		return pr.Dictionary()[dictIdCallRed] == "call.red"
	}, 5*time.Second, 10*time.Millisecond, "dictionary must decode before dependent streams")

	sendStream(t, ac, model.StreamParams, 0, wire.ParamsStream([]wire.ParamDef{
		{Name: "request.id", IsIndex: true, Order: 1},
	}))
	sendStream(t, ac, model.StreamSuspend, 0, wire.SuspendStream(baseMs-500, []wire.SuspendEvent{
		{DeltaMs: 100, AmountMs: 10}, {DeltaMs: 5_000, AmountMs: 25},
	}))
	sendStream(t, ac, model.StreamTrace, 0, traceFile1) // agent file index 1
	sendStream(t, ac, model.StreamTrace, 1, traceFile2) // rotation: agent file index 2
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callRecords))

	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()

	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond,
		"disconnect must finalize the pod-restart")

	podDir := filepath.Join(dataDir, "pods", hotstoreNs, hotstoreSvc, hotstorePod,
		fmt.Sprintf("%d", key.RestartTimeMs))

	t.Run("trace segments named by rolling_seq+1 and pointers resolve", func(t *testing.T) {
		assertCallPointersResolve(t, podDir, callRecords)
	})
	t.Run("call index reconstructs absolute ts_ms across buckets", func(t *testing.T) {
		assertCallIndex(t, store, key, wantTs)
	})
	t.Run("chunk index, segment catalog, and WALs are populated", func(t *testing.T) {
		assertChunkIndex(t, pr, offsets1, offsets2, len(traceFile1), len(traceFile2))
		assertSegmentCatalog(t, store, key, len(traceFile1), len(traceFile2))
		assertWals(t, podDir)
	})

	// Simulated restart: stop the collector, wipe every SQLite file, and
	// recover from the PV alone — gzip segments and WALs (03 §3.2 step 4,
	// §3.4-§3.5).
	cancel()
	waitForCollectorStop(t, svc)
	wipeSqlite(t, dataDir)

	store2, err := hotstore.Open(hotstore.Config{DataDir: dataDir})
	require.NoError(t, err)
	defer func() { _ = store2.Close() }()
	require.NoError(t, store2.Recover(context.Background()))

	t.Run("recovery rebuilds chunk index, catalog, and call index from PV", func(t *testing.T) {
		pr2, ok := store2.PodRestart(key)
		require.True(t, ok, "recovery must find the pod-restart on the PV")
		assert.True(t, pr2.Closed(), "recovered pod-restarts are closed")
		assert.Equal(t, timerStartMs, pr2.TimerStartMs(), "trace epoch re-read from the first segment")
		assert.Equal(t, pr.Dictionary(), pr2.Dictionary(), "dictionary replayed from its WAL")
		assert.Equal(t, pr.ChunkIndex(threadHandle), pr2.ChunkIndex(threadHandle))
		assert.Equal(t, pr.ChunkIndex(threadQuery), pr2.ChunkIndex(threadQuery))
		assertSegmentCatalog(t, store2, key, len(traceFile1), len(traceFile2))
		assertCallIndex(t, store2, key, wantTs)
	})
}

func startCollector(t *testing.T, ctx context.Context, dataDir string) *collector.Service {
	svc, err := collector.New(ctx, collector.Options{
		Store: hotstore.Config{DataDir: dataDir},
		Server: server.ConnectionOpts{
			ProtocolPort: hotstorePort,
			Timeout: io2.TcpTimeout{
				ConnectTimeout: 10 * time.Second,
				SessionTimeout: 60 * time.Second,
				ReadTimeout:    40 * time.Second,
				WriteTimeout:   2 * time.Second,
			},
		},
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- svc.Run(ctx) }()
	t.Cleanup(func() {
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("collector service did not stop")
		}
	})
	time.Sleep(100 * time.Millisecond) // wait for the TCP listener to bind
	return svc
}

func waitForCollectorStop(t *testing.T, svc *collector.Service) {
	// The service releases the PV flock on stop; poll by re-taking it.
	require.Eventually(t, func() bool {
		s, err := hotstore.Open(hotstore.Config{DataDir: svc.Store().Config().DataDir})
		if err != nil {
			return false
		}
		_ = s.Close()
		return true
	}, 10*time.Second, 50*time.Millisecond, "collector must release the PV lock")
}

func connectAgent(t *testing.T, ctx context.Context) *emulator.AgentConnection {
	ac := emulator.PrepareAgent(ctx, nil, CreateMockAgentListener(), hotstorePod)
	err := ac.Prepare(emulator.ConnectionOpts{
		ProtocolAddress: fmt.Sprintf("localhost:%d", hotstorePort),
		Timeout: io2.TcpTimeout{
			ConnectTimeout: 10 * time.Second,
			SessionTimeout: 20 * time.Second,
			ReadTimeout:    2 * time.Second,
			WriteTimeout:   2 * time.Second,
		},
	}).Connect()
	require.NoError(t, err)
	require.NoError(t, ac.InitializeConnection(model.PROTOCOL_VERSION_V3, hotstoreNs, hotstoreSvc, hotstorePod))
	require.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())
	return ac
}

// sendStream opens one agent stream file and feeds its bytes in RCV_DATA
// payloads, splitting the first payload to exercise payload-boundary
// reassembly (a logical chunk spans many payloads; backend/CLAUDE.md).
func sendStream(t *testing.T, ac *emulator.AgentConnection, stream string, requestedSeq int, data []byte) {
	handle, err := ac.CommandInitStream(stream, requestedSeq, false)
	require.NoError(t, err)
	require.NotEqual(t, [16]byte{}, handle.ToBin(), "stream %s must get a non-nil handle", stream)

	split := 10
	if split > len(data) {
		split = len(data)
	}
	require.NoError(t, ac.CommandRcvData(stream, handle, data[:split]))
	for pos := split; pos < len(data); pos += emulator.MaxBufSize {
		end := pos + emulator.MaxBufSize
		if end > len(data) {
			end = len(data)
		}
		require.NoError(t, ac.CommandRcvData(stream, handle, data[pos:end]))
	}
	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
}

func waitForPodRestart(t *testing.T, store *hotstore.Store) hotstore.PodRestartKey {
	var key hotstore.PodRestartKey
	require.Eventually(t, func() bool {
		keys := store.PodRestartKeys()
		if len(keys) != 1 {
			return false
		}
		key = keys[0]
		return true
	}, 5*time.Second, 10*time.Millisecond, "handshake must register the pod-restart")
	assert.Equal(t, hotstoreNs, key.Namespace)
	assert.Equal(t, hotstoreSvc, key.Service)
	assert.Equal(t, hotstorePod, key.PodName)
	assert.NotZero(t, key.RestartTimeMs, "restart time is stamped at TCP accept")
	return key
}

// assertCallPointersResolve is the M7 guard: each Call's trace_file_index must
// name the segment file that holds its root ENTER at buffer_offset.
func assertCallPointersResolve(t *testing.T, podDir string, records []wire.CallRecord) {
	segments := map[int][]byte{}
	for _, rec := range records {
		data, ok := segments[rec.TraceFileIndex]
		if !ok {
			path := filepath.Join(podDir, "trace", fmt.Sprintf("%06d.gz", rec.TraceFileIndex))
			data = gunzipFile(t, path)
			segments[rec.TraceFileIndex] = data
		}
		method, threadId := rootEnter(t, data, rec.BufferOffset)
		assert.Equal(t, rec.Method, method,
			"pointer (%d, %d) must land on the call's root ENTER", rec.TraceFileIndex, rec.BufferOffset)
		wantThread := threadHandle
		if rec.Method == methodQuery {
			wantThread = threadQuery
		}
		assert.Equal(t, wantThread, threadId)
	}
	_, err := os.Stat(filepath.Join(podDir, "trace", "000000.gz"))
	assert.True(t, os.IsNotExist(err), "no segment may be named by the echoed id (off-by-one)")
}

// rootEnter parses the chunk at offset: the 16-byte [threadId, startTime]
// header, then the first event, which must be a method ENTER.
func rootEnter(t *testing.T, segment []byte, offset int) (method int, threadId uint64) {
	require.Less(t, offset+17, len(segment), "chunk at %d must fit the segment", offset)
	threadId = binary.BigEndian.Uint64(segment[offset:])
	header := segment[offset+16]
	require.Equal(t, byte(0), header&0x3, "first event at the pointer must be an ENTER")
	require.Zero(t, header&0x80, "synthetic root ENTER carries no delta continuation")
	tagId, n := binary.Uvarint(segment[offset+17:])
	require.Positive(t, n)
	return int(tagId), threadId
}

func gunzipFile(t *testing.T, path string) []byte {
	f, err := os.Open(path)
	require.NoError(t, err, "segment file must exist")
	defer func() { _ = f.Close() }()
	gz, err := gzip.NewReader(f)
	require.NoError(t, err)
	data, err := io.ReadAll(gz)
	require.NoError(t, err)
	return data
}

// assertCallIndex is the B1 guard: absolute ts_ms comes from accumulating the
// record deltas, and the rows land in the buckets of their call START.
func assertCallIndex(t *testing.T, store *hotstore.Store, key hotstore.PodRestartKey, wantTs []int64) {
	buckets, err := store.Buckets()
	require.NoError(t, err)
	require.Len(t, buckets, 2, "the third record starts in the next 5-minute bucket")

	var rows []hotstore.CallIndexRow
	for _, bucket := range buckets {
		bucketRows, err := store.Calls(bucket)
		require.NoError(t, err)
		for _, row := range bucketRows {
			assert.Equal(t, bucket, store.Config().Bucket(row.TsMs), "a row must sit in its own bucket")
			rows = append(rows, row)
		}
	}
	require.Len(t, rows, 3)

	gotTs := []int64{rows[0].TsMs, rows[1].TsMs, rows[2].TsMs}
	assert.Equal(t, wantTs, gotTs, "ts_ms must accumulate deltas, not offset each from base_ms")

	for _, row := range rows {
		assert.Equal(t, key.String(), row.PodRestart)
	}
	assert.Equal(t, "exec-1", rows[0].ThreadName)
	assert.Equal(t, hotstore.RetentionShortClean, rows[0].RetentionClass)
	assert.False(t, rows[0].ErrorFlag)
	assert.JSONEq(t, `{"request.id":["req-42"]}`, rows[0].ParamsJson, "param ids resolve against the dictionary")

	assert.Equal(t, "exec-2", rows[1].ThreadName)
	assert.Equal(t, hotstore.RetentionNormalClean, rows[1].RetentionClass)
	assert.Equal(t, methodQuery, rows[1].MethodId)

	assert.True(t, rows[2].ErrorFlag, "call.red in Params sets error_flag (01 §5.6)")
	assert.Equal(t, hotstore.RetentionAnyError, rows[2].RetentionClass,
		"an errored call outranks its duration class")
	assert.Equal(t, 2_000, rows[2].DurationMs)
}

func assertChunkIndex(t *testing.T, pr *hotstore.PodRestart, offsets1, offsets2 []int64, len1, len2 int) {
	handleChunks := pr.ChunkIndex(threadHandle)
	require.Len(t, handleChunks, 3, "thread %d wrote two chunks in file 1 and one in file 2", threadHandle)
	assert.Equal(t, hotstore.ChunkRef{RollingSeq: 1, Offset: offsets1[0],
		Length: int(offsets1[1] - offsets1[0]), StartMs: baseMs}, handleChunks[0])
	assert.Equal(t, hotstore.ChunkRef{RollingSeq: 1, Offset: offsets1[2],
		Length: len1 - int(offsets1[2]), StartMs: baseMs + 50}, handleChunks[1])
	assert.Equal(t, hotstore.ChunkRef{RollingSeq: 2, Offset: offsets2[1],
		Length: len2 - int(offsets2[1]), StartMs: baseMs + 180_000}, handleChunks[2])

	queryChunks := pr.ChunkIndex(threadQuery)
	require.Len(t, queryChunks, 2)
	assert.Equal(t, hotstore.ChunkRef{RollingSeq: 1, Offset: offsets1[1],
		Length: int(offsets1[2] - offsets1[1]), StartMs: baseMs + 3}, queryChunks[0])
	assert.Equal(t, hotstore.ChunkRef{RollingSeq: 2, Offset: offsets2[0],
		Length: int(offsets2[1] - offsets2[0]), StartMs: baseMs + 59_000}, queryChunks[1])
}

func assertSegmentCatalog(t *testing.T, store *hotstore.Store, key hotstore.PodRestartKey, len1, len2 int) {
	segments, err := store.Segments(key)
	require.NoError(t, err)
	require.Len(t, segments, 2)

	first, second := segments[0], segments[1]
	assert.Equal(t, "trace", first.Stream)
	assert.Equal(t, 1, first.RollingSeq)
	assert.Equal(t, int64(len1), first.LogicalSize)
	assert.Equal(t, "closed", first.Status)
	require.NotNil(t, first.TimeMinMs)
	require.NotNil(t, first.TimeMaxMs)
	assert.Equal(t, baseMs, *first.TimeMinMs)
	assert.Equal(t, baseMs+50, *first.TimeMaxMs)

	assert.Equal(t, 2, second.RollingSeq)
	assert.Equal(t, int64(len2), second.LogicalSize)
	require.NotNil(t, second.TimeMinMs)
	require.NotNil(t, second.TimeMaxMs)
	assert.Equal(t, baseMs+59_000, *second.TimeMinMs)
	assert.Equal(t, baseMs+180_000, *second.TimeMaxMs)
}

func assertWals(t *testing.T, podDir string) {
	// dictionary.wal replays to the words in arrival order (01 §3.2).
	var words []string
	clean, err := hotstore.ReplayWal(filepath.Join(podDir, "dictionary.wal"), func(_ int64, body []byte) error {
		id, n := binary.Uvarint(body)
		wordLen, m := binary.Uvarint(body[n:])
		require.Equal(t, len(body), n+m+int(wordLen))
		require.Equal(t, uint64(len(words)), id, "word ids follow arrival order")
		words = append(words, string(body[n+m:]))
		return nil
	})
	require.NoError(t, err)
	assert.True(t, clean, "a closed pod-restart leaves a CRC-sealed WAL")
	assert.Equal(t, dictWords, words)

	// params.wal and suspend.wal carry one JSON record per stream record.
	var params []map[string]any
	_, err = hotstore.ReplayWal(filepath.Join(podDir, "params.wal"), func(_ int64, body []byte) error {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(body, &rec))
		params = append(params, rec)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, params, 1)
	assert.Equal(t, "request.id", params[0]["name"])
	assert.Equal(t, true, params[0]["is_index"])

	var pauses []map[string]any
	_, err = hotstore.ReplayWal(filepath.Join(podDir, "suspend.wal"), func(_ int64, body []byte) error {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(body, &rec))
		pauses = append(pauses, rec)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, pauses, 2)
	assert.Equal(t, float64(baseMs-400), pauses[0]["time_ms"], "suspend times accumulate from the stream base")
	assert.Equal(t, float64(10), pauses[0]["duration_ms"])
	assert.Equal(t, float64(baseMs+4_600), pauses[1]["time_ms"])
}

// wipeSqlite deletes metadata.sqlite and every call-index partition (plus
// SQLite sidecars), so the recovery assertions can only pass if the state is
// rebuilt from the gzip segments and WALs.
func wipeSqlite(t *testing.T, dataDir string) {
	entries, err := os.ReadDir(dataDir)
	require.NoError(t, err)
	for _, e := range entries {
		name := e.Name()
		if name == "metadata.sqlite" || name == "metadata.sqlite-wal" || name == "metadata.sqlite-shm" ||
			(len(name) > 6 && name[:6] == "calls-") {
			require.NoError(t, os.Remove(filepath.Join(dataDir, name)))
		}
	}
}
