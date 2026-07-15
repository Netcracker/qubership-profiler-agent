package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	io2 "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	coldPodA = "pod-cold-a"
	coldPodB = "pod-cold-b"
)

// coldFakeStore is the in-test S3: it takes the Uploader's PUTs
// (hotstore.ObjectStore) and serves the query service's reads
// (cold.ObjectStore), recording every LIST prefix, every Open, and every
// read byte range so the tests can prove class pruning, manifest-only
// /pods, and the trace_blob column projection.
type coldFakeStore struct {
	mu             sync.Mutex
	objects        map[string][]byte
	lists          []string
	opens          []string
	gets           []string
	reads          map[string][][2]int64
	notFoundOnOpen map[string]bool
}

func newColdFakeStore() *coldFakeStore {
	return &coldFakeStore{
		objects:        map[string][]byte{},
		reads:          map[string][][2]int64{},
		notFoundOnOpen: map[string]bool{},
	}
}

func (f *coldFakeStore) PutFile(_ context.Context, key, localPath string) error {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = data
	return nil
}

func (f *coldFakeStore) PutBytes(_ context.Context, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = append([]byte(nil), body...)
	return nil
}

func (f *coldFakeStore) List(_ context.Context, prefix string) ([]cold.ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists = append(f.lists, prefix)
	var out []cold.ObjectInfo
	for key, data := range f.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, cold.ObjectInfo{Key: key, Size: int64(len(data))})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *coldFakeStore) Open(_ context.Context, key string) (cold.Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens = append(f.opens, key)
	if f.notFoundOnOpen[key] {
		return nil, cold.ErrNotFound
	}
	data, ok := f.objects[key]
	if !ok {
		return nil, cold.ErrNotFound
	}
	return &recordingObject{store: f, key: key, data: data}, nil
}

func (f *coldFakeStore) Get(_ context.Context, key string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets = append(f.gets, key)
	data, ok := f.objects[key]
	if !ok || f.notFoundOnOpen[key] {
		return nil, cold.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *coldFakeStore) object(t *testing.T, key string) []byte {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[key]
	require.True(t, ok, "object %s must exist", key)
	return data
}

func (f *coldFakeStore) duplicate(t *testing.T, key, newKey string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.objects[key]
	require.True(t, ok, "object %s must exist to duplicate", key)
	f.objects[newKey] = append([]byte(nil), data...)
}

// reset clears the recorded access log (not the objects) between subtests.
func (f *coldFakeStore) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lists = nil
	f.opens = nil
	f.reads = map[string][][2]int64{}
}

func (f *coldFakeStore) listedPrefixes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.lists...)
}

func (f *coldFakeStore) openedKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.opens...)
}

func (f *coldFakeStore) gotKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.gets...)
}

// allKeys lists every stored object key.
func (f *coldFakeStore) allKeys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (f *coldFakeStore) readRanges(key string) [][2]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]int64(nil), f.reads[key]...)
}

// recordingObject serves ReadAt from memory and logs each byte range.
type recordingObject struct {
	store *coldFakeStore
	key   string
	data  []byte
}

func (o *recordingObject) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(o.data)) {
		return 0, io.EOF
	}
	n := copy(p, o.data[off:])
	o.store.mu.Lock()
	o.store.reads[o.key] = append(o.store.reads[o.key], [2]int64{off, off + int64(n)})
	o.store.mu.Unlock()
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (o *recordingObject) Close() error { return nil }
func (o *recordingObject) Size() int64  { return int64(len(o.data)) }

// connectAgentAs mirrors connectAgent for an explicit pod name.
func connectAgentAs(t *testing.T, ctx context.Context, pod string) *emulator.AgentConnection {
	ac := emulator.PrepareAgent(ctx, nil, CreateMockAgentListener(), pod)
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
	require.NoError(t, ac.InitializeConnection(model.PROTOCOL_VERSION_V3, hotstoreNs, hotstoreSvc, pod))
	require.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion())
	return ac
}

// callsPage is the decoded /calls response (02 §2.3).
type callsPage struct {
	Calls []struct {
		PK struct {
			PodNamespace   string `json:"pod_namespace"`
			PodService     string `json:"pod_service"`
			PodName        string `json:"pod_name"`
			RestartTimeMs  int64  `json:"restart_time_ms"`
			TraceFileIndex int32  `json:"trace_file_index"`
			BufferOffset   int32  `json:"buffer_offset"`
			RecordIndex    int32  `json:"record_index"`
		} `json:"pk"`
		TsMs            int64               `json:"ts_ms"`
		DurationMs      int32               `json:"duration_ms"`
		Method          string              `json:"method"`
		ThreadName      string              `json:"thread_name"`
		ErrorFlag       bool                `json:"error_flag"`
		RetentionClass  string              `json:"retention_class"`
		Params          map[string][]string `json:"params"`
		TraceBlobSize   *int64              `json:"trace_blob_size"`
		TruncatedReason *string             `json:"truncated_reason"`
	} `json:"calls"`
	NextCursor     *string  `json:"next_cursor"`
	Partial        bool     `json:"partial"`
	PartialReasons []string `json:"partial_reasons"`
}

type problemBody struct {
	Status           int              `json:"status"`
	Detail           string           `json:"detail"`
	SuggestedFilters []string         `json:"suggested_filters"`
	EstimatedFiles   *int             `json:"estimated_files"`
	EstimatedBytes   *int64           `json:"estimated_bytes"`
	ByClass          map[string]int64 `json:"by_class"`
}

// TestColdReadPath drives the Stage 1 cold read slice acceptance: sealed
// parquet from two pods across two UTC days plus a late-arrival patch file
// are uploaded to the in-test S3, and the query service serves /api/v1/calls
// and /api/v1/pods from that cold tier alone (02-read-contract.md §2.3,
// §2.3.1, §2.3.2, §2.7, §5.1, §6).
func TestColdReadPath(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	svc := startCollector(t, ctx, dataDir)
	store := svc.Store()
	bucket1 := store.Config().Bucket(baseMs + 5)
	day2Ms := baseMs + 2*60*60*1000 // 2023-11-15T00:13:20Z, past UTC midnight
	bucket2 := store.Config().Bucket(day2Ms)

	// --- Pod A: four calls in one day-1 bucket, then a late call that
	// re-seals the bucket into a patch file (01 §6.6). The late call sits 30 s
	// after the others so the key stamps land in a different second and
	// discovery can tell the patch from the original by time range alone.
	fileA1, offA1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 11, StartMs: baseMs + 5, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Tag(1, sealDictRequestId, "req-1"), wire.Exit(2),
		}},
		{ThreadId: 22, StartMs: baseMs + 10, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(2),
		}},
		{ThreadId: 33, StartMs: baseMs + 15, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodProcess), wire.Exit(3),
		}},
		{ThreadId: 44, StartMs: baseMs + 20, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1),
		}},
	})
	fileA2, offA2 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 55, StartMs: baseMs + 30_000, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	callsA := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1", // A1: short_clean
			TraceFileIndex: 1, BufferOffset: int(offA1[0]), RecordIndex: 0,
			Params: map[int][]string{sealDictRequestId: {"req-1"}}},
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 500, ThreadName: "exec-2", // A2: normal_clean
			TraceFileIndex: 1, BufferOffset: int(offA1[1]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodProcess, DurationMs: 2000, ThreadName: "exec-3", // A3: long_clean
			TraceFileIndex: 1, BufferOffset: int(offA1[2]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 50, ThreadName: "exec-4", // A4: any_error
			TraceFileIndex: 1, BufferOffset: int(offA1[3]), RecordIndex: 0,
			Params: map[int][]string{sealDictCallRed: {"1"}}},
	}
	lateA := []wire.CallRecord{
		{DeltaMs: 30_000, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-5", // A5: short_clean patch
			TraceFileIndex: 2, BufferOffset: int(offA2[0]), RecordIndex: 0},
	}

	acA := connectAgentAs(t, ctx, coldPodA)
	keyA := waitForPodNamed(t, store, coldPodA, 1)
	prA, ok := store.PodRestart(keyA)
	require.True(t, ok)
	sendStream(t, acA, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, acA, model.StreamTrace, 0, fileA1)
	sendStream(t, acA, model.StreamTrace, 1, fileA2)
	sendStream(t, acA, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callsA))
	waitForIndexedCalls(t, store, bucket1, keyA, 4)
	// The dictionary decodes on its own pipeline; the seal below re-derives
	// error_flag against it and resolves the blob methods, so it must have
	// landed before the pass runs.
	require.Eventually(t, func() bool {
		return prA.Dictionary()[sealDictCallRed] == "call.red"
	}, 5*time.Second, 10*time.Millisecond, "dictionary must decode before the seal")

	resA1, err := store.Seal(ctx, keyA, bucket1)
	require.NoError(t, err)
	require.Len(t, resA1.Files, 4, "one file per touched retention class")

	// The late call arrives after the seal and re-marks the bucket dirty.
	sendStream(t, acA, model.StreamCalls, 1, wire.CallsStreamRecords(baseMs, lateA))
	waitForIndexedCalls(t, store, bucket1, keyA, 5)
	resA2, err := store.Seal(ctx, keyA, bucket1)
	require.NoError(t, err)
	require.Len(t, resA2.Files, 1, "the late call seals into a patch file")
	patchA := resA2.Files[0]
	assert.Equal(t, hotstore.RetentionShortClean, patchA.RetentionClass)
	assert.Equal(t, 1, patchA.Seq, "the patch takes the next <seq>")

	require.NoError(t, acA.CommandClose())
	_ = acA.Close()
	require.Eventually(t, prA.Finalized, 5*time.Second, 10*time.Millisecond)

	// --- Pod B: one day-1 call at the exact ts_ms of A1 (PK tiebreak) and
	// one day-2 call two hours later, past UTC midnight.
	fileB1, offB1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 66, StartMs: baseMs + 5, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1),
		}},
		{ThreadId: 77, StartMs: day2Ms, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodProcess), wire.Exit(2),
		}},
	})
	callsB := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodQuery, DurationMs: 20, ThreadName: "exec-1", // B1: short_clean, ts == A1.ts
			TraceFileIndex: 1, BufferOffset: int(offB1[0]), RecordIndex: 0},
		{DeltaMs: 2*60*60*1000 - 5, Method: sealMethodProcess, DurationMs: 1500, ThreadName: "exec-2", // B2: long_clean, day 2
			TraceFileIndex: 1, BufferOffset: int(offB1[1]), RecordIndex: 0},
	}

	acB := connectAgentAs(t, ctx, coldPodB)
	keyB := waitForPodNamed(t, store, coldPodB, 2)
	prB, ok := store.PodRestart(keyB)
	require.True(t, ok)
	sendStream(t, acB, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, acB, model.StreamTrace, 0, fileB1)
	sendStream(t, acB, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callsB))
	require.NoError(t, acB.CommandClose())
	_ = acB.Close()
	require.Eventually(t, prB.Finalized, 5*time.Second, 10*time.Millisecond)

	resB1, err := store.Seal(ctx, keyB, bucket1)
	require.NoError(t, err)
	require.Len(t, resB1.Files, 1)
	resB2, err := store.Seal(ctx, keyB, bucket2)
	require.NoError(t, err)
	require.Len(t, resB2.Files, 1)
	fileB2 := resB2.Files[0]

	// --- Upload everything to the in-test S3 and plant a byte-identical
	// duplicate of B2 under another <seq>, as a replica transition would
	// (02 §6.2); PK dedup must collapse it.
	fake := newColdFakeStore()
	_, err = hotstore.NewUploader(store, fake).Pass(ctx)
	require.NoError(t, err)
	dupB2Key := strings.TrimSuffix(fileB2.S3Key, "-0.parquet") + "-9.parquet"
	fake.duplicate(t, fileB2.S3Key, dupB2Key)

	newService := func(cfg query.Config) *httptest.Server {
		srv := httptest.NewServer(query.New(query.Options{Config: cfg, ColdStore: fake}).Handler())
		t.Cleanup(srv.Close)
		return srv
	}
	api := newService(query.Config{})

	windowFrom, windowTo := baseMs, day2Ms+60_000

	t.Run("calls: rows, order, filters, projection", func(t *testing.T) {
		fake.reset()
		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)},
		})
		require.Len(t, page.Calls, 7)
		assert.False(t, page.Partial)
		assert.Nil(t, page.NextCursor)

		var got []string
		for _, call := range page.Calls {
			got = append(got, fmt.Sprintf("%s@%d", call.PK.PodName, call.TsMs-baseMs))
		}
		assert.Equal(t, []string{
			coldPodB + "@7200000", // B2, day 2
			coldPodA + "@30000",   // A5, the patch row
			coldPodA + "@20",      // A4
			coldPodA + "@15",      // A3
			coldPodA + "@10",      // A2
			coldPodA + "@5",       // A1: ts tie with B1 breaks by pk ASC
			coldPodB + "@5",       // B1
		}, got, "rows come back in (ts_ms DESC, pk ASC) order (02 §2.3.1)")

		a1 := page.Calls[5]
		assert.Equal(t, "com.example.Service.handle", a1.Method)
		assert.Equal(t, "exec-1", a1.ThreadName)
		assert.Equal(t, map[string][]string{"request.id": {"req-1"}}, a1.Params)
		assert.EqualValues(t, 10, a1.DurationMs)
		assert.Equal(t, hotstore.RetentionShortClean, a1.RetentionClass)
		assert.Equal(t, keyA.RestartTimeMs, a1.PK.RestartTimeMs)
		assert.Nil(t, a1.TraceBlobSize, "the cold list path cannot know the blob size without reading it")
		assert.Nil(t, a1.TruncatedReason)

		a4 := page.Calls[2]
		assert.True(t, a4.ErrorFlag)
		assert.Equal(t, hotstore.RetentionAnyError, a4.RetentionClass)

		assertTraceBlobNotRead(t, fake)
	})

	t.Run("projection control: an unprojected read does enter the blob chunk", func(t *testing.T) {
		// Sanity check for assertTraceBlobNotRead: read the same object with
		// the full CallV2 schema through the same recording store and confirm
		// the reader then DOES start reads inside the trace_blob chunk — so
		// the passing assertion above is meaningful, not vacuous.
		fake.reset()
		obj, err := fake.Open(ctx, fileB2.S3Key)
		require.NoError(t, err)
		defer func() { _ = obj.Close() }()
		rows := readParquetRows[storageparquet.CallV2](t, obj)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].TraceBlob, "the unprojected read returns the blob")

		hit := false
		for _, chunk := range traceBlobChunkRanges(t, fake.object(t, fileB2.S3Key)) {
			for _, r := range fake.readRanges(fileB2.S3Key) {
				if r[0] >= chunk[0] && r[0] < chunk[1] {
					hit = true
				}
			}
		}
		assert.True(t, hit, "an unprojected read must start inside the trace_blob chunk")
	})

	t.Run("calls: row filters", func(t *testing.T) {
		window := url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}}

		errOnly := cloneValues(window)
		errOnly.Set("error_only", "true")
		fake.reset()
		page := getCalls(t, api, errOnly)
		require.Len(t, page.Calls, 1)
		assert.True(t, page.Calls[0].ErrorFlag)
		for _, prefix := range fake.listedPrefixes() {
			assert.NotContains(t, prefix, "_clean/", "error_only lists only the error classes (02 §5.5)")
		}

		byMethod := cloneValues(window)
		byMethod.Set("method", "Db.query")
		page = getCalls(t, api, byMethod)
		require.Len(t, page.Calls, 3, "A4, A2, B1 run com.example.Db.query")
		for _, call := range page.Calls {
			assert.Equal(t, "com.example.Db.query", call.Method)
		}

		byPod := cloneValues(window)
		byPod.Set("pod", hotstoreNs+"/"+hotstoreSvc+"/"+coldPodB)
		page = getCalls(t, api, byPod)
		require.Len(t, page.Calls, 2)
		for _, call := range page.Calls {
			assert.Equal(t, coldPodB, call.PK.PodName)
		}
	})

	t.Run("pagination: stable pages, expired and mismatched cursors", func(t *testing.T) {
		window := url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}, "limit": {"3"}}
		page1 := getCalls(t, api, window)
		require.Len(t, page1.Calls, 3)
		require.NotNil(t, page1.NextCursor)
		assert.Equal(t, coldPodB, page1.Calls[0].PK.PodName) // B2

		page2 := getCalls(t, api, url.Values{"cursor": {*page1.NextCursor}, "limit": {"3"}})
		require.Len(t, page2.Calls, 3)
		require.NotNil(t, page2.NextCursor)
		assert.Equal(t, []int64{baseMs + 15, baseMs + 10, baseMs + 5},
			[]int64{page2.Calls[0].TsMs, page2.Calls[1].TsMs, page2.Calls[2].TsMs},
			"page 2 continues exactly past the cursor position")

		again := getCalls(t, api, url.Values{"cursor": {*page1.NextCursor}, "limit": {"3"}})
		require.Len(t, again.Calls, 3)
		assert.Equal(t, page2.Calls, again.Calls, "the same cursor replays the same page")

		page3 := getCalls(t, api, url.Values{"cursor": {*page2.NextCursor}, "limit": {"3"}})
		require.Len(t, page3.Calls, 1)
		assert.Equal(t, coldPodB, page3.Calls[0].PK.PodName) // B1
		assert.Nil(t, page3.NextCursor, "the window is exhausted")

		// Re-sent filters must match the frozen query (02 §2.3.1).
		mismatched := url.Values{"cursor": {*page1.NextCursor}, "method": {"Db.query"}}
		problem := getProblem(t, api, "/api/v1/calls", mismatched, http.StatusBadRequest)
		assert.Contains(t, problem.Detail, "frozen")

		// An expired cursor is a 400 and a page-1 restart (02 §2.3.1). The
		// short-TTL service accepts the same token format but ages it out.
		ttlApi := newService(query.Config{CursorTTL: time.Millisecond})
		time.Sleep(5 * time.Millisecond)
		problem = getProblem(t, ttlApi, "/api/v1/calls", url.Values{"cursor": {*page1.NextCursor}}, http.StatusBadRequest)
		assert.Contains(t, problem.Detail, "expired")
	})

	t.Run("dedup: a duplicate PK object collapses to one row", func(t *testing.T) {
		fake.reset()
		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(day2Ms - 60_000)}, "to": {fmt.Sprint(day2Ms + 60_000)},
		})
		require.Len(t, page.Calls, 1, "B2 exists in two objects but is one call (02 §6)")
		assert.Equal(t, day2Ms, page.Calls[0].TsMs)
		opened := fake.openedKeys()
		assert.Contains(t, opened, fileB2.S3Key)
		assert.Contains(t, opened, dupB2Key, "both copies are scanned; dedup collapses them")
	})

	t.Run("wide-query guard: span layer", func(t *testing.T) {
		guardApi := newService(query.Config{WideRangeLimit: 30 * time.Minute})
		window := url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}}

		fake.reset()
		problem := getProblem(t, guardApi, "/api/v1/calls", window, http.StatusBadRequest)
		assert.Equal(t, []string{"pod", "retention_class", "duration_min_ms", "error_only"}, problem.SuggestedFilters)
		assert.Nil(t, problem.EstimatedFiles, "the span layer fires before the LIST (02 §2.3.2)")
		assert.Empty(t, fake.listedPrefixes(), "no discovery LIST ran")
		assert.Empty(t, fake.openedKeys(), "no parquet was opened")

		// duration_min_ms is a narrowing filter: the query passes and lists
		// only the classes that can hold a call that long (02 §2.3.2, §5.5).
		fake.reset()
		narrowed := cloneValues(window)
		narrowed.Set("duration_min_ms", "1000")
		page := getCalls(t, guardApi, narrowed)
		require.Len(t, page.Calls, 2, "B2 and A3 are the only calls ≥ 1s")
		prefixes := fake.listedPrefixes()
		require.NotEmpty(t, prefixes)
		for _, prefix := range prefixes {
			assert.NotContains(t, prefix, "/short_clean/")
			assert.NotContains(t, prefix, "/normal_clean/")
		}
	})

	t.Run("wide-query guard: cost layer", func(t *testing.T) {
		costApi := newService(query.Config{MaxScanFiles: 1})
		fake.reset()
		problem := getProblem(t, costApi, "/api/v1/calls",
			url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}}, http.StatusBadRequest)
		require.NotNil(t, problem.EstimatedFiles)
		assert.Equal(t, 8, *problem.EstimatedFiles, "5 pod-A files + B1 + B2 + the planted duplicate")
		require.NotNil(t, problem.EstimatedBytes)
		assert.Positive(t, *problem.EstimatedBytes)
		assert.Contains(t, problem.ByClass, hotstore.RetentionShortClean)
		assert.Empty(t, fake.openedKeys(), "the verdict lands before any parquet is opened (02 §2.3.2)")
	})

	t.Run("pods: identity from manifests, no parquet", func(t *testing.T) {
		fake.reset()
		pods := getPods(t, api, url.Values{"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)}})
		require.Len(t, pods.Pods, 2)
		assert.Equal(t, coldPodA, pods.Pods[0].Pod)
		assert.Equal(t, keyA.RestartTimeMs, pods.Pods[0].RestartTimeMs)
		assert.Equal(t, coldPodB, pods.Pods[1].Pod)
		assert.Equal(t, keyB.RestartTimeMs, pods.Pods[1].RestartTimeMs)
		for _, key := range fake.openedKeys() {
			assert.NotContains(t, key, ".parquet", "cold /pods reads manifests only (02 §2.7)")
		}
		for _, prefix := range fake.listedPrefixes() {
			assert.True(t, strings.HasPrefix(prefix, "pods/v1/"), "unexpected LIST %s", prefix)
		}

		// A day-2-only window sees only pod B: pod A never sealed into that day.
		day2Only := getPods(t, api, url.Values{"from": {fmt.Sprint(day2Ms)}, "to": {fmt.Sprint(day2Ms + 60_000)}})
		require.Len(t, day2Only.Pods, 1)
		assert.Equal(t, coldPodB, day2Only.Pods[0].Pod)
	})

	t.Run("discovery: patch found by overlap, original pruned by key range", func(t *testing.T) {
		fake.reset()
		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(baseMs + 30_000)}, "to": {fmt.Sprint(baseMs + 30_001)},
		})
		require.Len(t, page.Calls, 1)
		assert.Equal(t, baseMs+30_000, page.Calls[0].TsMs, "the late-arrival row comes from the patch file")
		assert.Equal(t, []string{patchA.S3Key}, fake.openedKeys(),
			"the timeMin/timeMax overlap test opens the patch alone (02 §5.1)")
	})

	t.Run("discovery: listed-then-deleted object is empty, not an error", func(t *testing.T) {
		var normalKey string
		for _, f := range resA1.Files {
			if f.RetentionClass == hotstore.RetentionNormalClean {
				normalKey = f.S3Key
			}
		}
		require.NotEmpty(t, normalKey)
		fake.mu.Lock()
		fake.notFoundOnOpen[normalKey] = true
		fake.mu.Unlock()
		defer func() {
			fake.mu.Lock()
			delete(fake.notFoundOnOpen, normalKey)
			fake.mu.Unlock()
		}()

		page := getCalls(t, api, url.Values{
			"from": {fmt.Sprint(windowFrom)}, "to": {fmt.Sprint(windowTo)},
		})
		require.Len(t, page.Calls, 6, "the vanished object contributes nothing")
		assert.False(t, page.Partial, "a 404 on a listed key is empty, not a failure (02 §5.1)")
		for _, call := range page.Calls {
			assert.NotEqual(t, hotstore.RetentionNormalClean, call.RetentionClass)
		}
	})
}

// getCalls fetches one /calls page and requires a 200.
func getCalls(t *testing.T, srv *httptest.Server, params url.Values) callsPage {
	t.Helper()
	resp, err := http.Get(srv.URL + "/api/v1/calls?" + params.Encode())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status, body: %s", body)
	var page callsPage
	require.NoError(t, json.Unmarshal(body, &page))
	return page
}

type podsBody struct {
	Pods []struct {
		Namespace     string `json:"namespace"`
		Service       string `json:"service"`
		Pod           string `json:"pod"`
		RestartTimeMs int64  `json:"restart_time_ms"`
		TimeMinMs     int64  `json:"time_min_ms"`
		TimeMaxMs     int64  `json:"time_max_ms"`
	} `json:"pods"`
	Partial bool `json:"partial"`
}

func getPods(t *testing.T, srv *httptest.Server, params url.Values) podsBody {
	t.Helper()
	resp, err := http.Get(srv.URL + "/api/v1/pods?" + params.Encode())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status, body: %s", body)
	var pods podsBody
	require.NoError(t, json.Unmarshal(body, &pods))
	return pods
}

// getProblem fetches a path expecting an RFC 7807 rejection.
func getProblem(t *testing.T, srv *httptest.Server, path string, params url.Values, wantStatus int) problemBody {
	t.Helper()
	resp, err := http.Get(srv.URL + path + "?" + params.Encode())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, wantStatus, resp.StatusCode, "unexpected status, body: %s", body)
	assert.Equal(t, "application/problem+json", resp.Header.Get("Content-Type"))
	var problem problemBody
	require.NoError(t, json.Unmarshal(body, &problem))
	return problem
}

// waitForPodNamed waits until the store holds want pod-restarts and returns
// the one for pod.
func waitForPodNamed(t *testing.T, store *hotstore.Store, pod string, want int) hotstore.PodRestartKey {
	t.Helper()
	var key hotstore.PodRestartKey
	require.Eventually(t, func() bool {
		keys := store.PodRestartKeys()
		if len(keys) != want {
			return false
		}
		for _, k := range keys {
			if k.PodName == pod {
				key = k
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "pod-restart %s must register", pod)
	return key
}

// waitForIndexedCalls waits until the bucket's call index holds want rows of
// the pod-restart — the calls pipeline is asynchronous to the TCP acks.
func waitForIndexedCalls(t *testing.T, store *hotstore.Store, bucket int64, key hotstore.PodRestartKey, want int) {
	t.Helper()
	require.Eventually(t, func() bool {
		rows, err := store.Calls(bucket)
		if err != nil {
			return false
		}
		n := 0
		for _, row := range rows {
			if row.PodRestart == key.String() {
				n++
			}
		}
		return n == want
	}, 5*time.Second, 10*time.Millisecond, "bucket must index %d calls of %s", want, key)
}

// assertTraceBlobNotRead proves the column projection: no recorded read of
// any opened parquet object may START inside a trace_blob column chunk. The
// reader positions a column's transport exactly at its chunk start, so an
// actually-read blob column always produces such a read; a neighbouring
// column's buffered over-read may sweep ACROSS the chunk but never starts
// past its own chunk end. The positive-control subtest keeps this assertion
// honest.
func assertTraceBlobNotRead(t *testing.T, fake *coldFakeStore) {
	t.Helper()
	checked := 0
	for _, key := range fake.openedKeys() {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		checked++
		for _, chunk := range traceBlobChunkRanges(t, fake.object(t, key)) {
			for _, r := range fake.readRanges(key) {
				assert.False(t, r[0] >= chunk[0] && r[0] < chunk[1],
					"read [%d,%d) of %s starts inside trace_blob chunk [%d,%d)", r[0], r[1], key, chunk[0], chunk[1])
			}
		}
	}
	require.Positive(t, checked, "the projection check must cover at least one parquet object")
}

// readParquetRows reads every row of a cold object through the T read schema
// with parquet-go's name-based column matching (mirrors the production reader
// in libs/query/cold).
func readParquetRows[T any](t *testing.T, obj cold.Object) []T {
	t.Helper()
	f, err := parquet.OpenFile(obj, obj.Size())
	require.NoError(t, err)
	r := parquet.NewGenericReader[T](f)
	defer func() { _ = r.Close() }()
	rows := make([]T, r.NumRows())
	n, err := r.Read(rows)
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.Equal(t, len(rows), n, "the reader must return every footer-promised row")
	return rows
}

// traceBlobChunkRanges returns the [start, end) byte ranges of every
// trace_blob column chunk in the file.
func traceBlobChunkRanges(t *testing.T, data []byte) [][2]int64 {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)

	var ranges [][2]int64
	for _, rg := range f.Metadata().RowGroups {
		for _, col := range rg.Columns {
			schemaPath := col.MetaData.PathInSchema
			if len(schemaPath) == 0 || !strings.EqualFold(schemaPath[0], "trace_blob") {
				continue
			}
			start := col.MetaData.DataPageOffset
			if col.MetaData.DictionaryPageOffset > 0 && col.MetaData.DictionaryPageOffset < start {
				start = col.MetaData.DictionaryPageOffset
			}
			ranges = append(ranges, [2]int64{start, start + col.MetaData.TotalCompressedSize})
		}
	}
	require.NotEmpty(t, ranges, "the file must carry a trace_blob column")
	return ranges
}

func cloneValues(v url.Values) url.Values {
	out := url.Values{}
	for k, vs := range v {
		out[k] = append([]string(nil), vs...)
	}
	return out
}
