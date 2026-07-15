package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The seal fixture: five closed root calls of one pod-restart, all in one
// 5-minute bucket. Dictionary word ids follow arrival order.
const (
	sealMethodHandle  = 0 // "com.example.Service.handle"
	sealMethodProcess = 1 // "com.example.Service.process"
	sealMethodQuery   = 2 // "com.example.Db.query"
	sealDictCallRed   = 3
	sealDictRequestId = 4

	sealThread1 = uint64(11)
	sealThread2 = uint64(22)
	sealThread3 = uint64(33)
)

var sealDictWords = []string{
	"com.example.Service.handle", "com.example.Service.process", "com.example.Db.query",
	"call.red", "request.id",
}

// TestSealPass drives the acceptance scenario of the Stage 1 seal slice with
// production-like stream ordering: the calls stream — including the errored
// call — is sent and indexed BEFORE the dictionary decodes "call.red", so the
// write-time error_flag is provably the racy false. The seal pass must
// re-derive it from calls.wal raw param ids against the full dictionary
// (01-write-contract.md §5.6) and prove the rest of §5-§7:
//
//  1. the errored row lands in the any_error class (file and column agree);
//  2. every trace_blob is timerStartTime + the exact full chunks of its call,
//     and decodes to the right tree once the §4.5 reader trims tail/head noise;
//  3. files are ZSTD, rows sorted (ts_ms DESC, pk ASC), names carry
//     timeMin/timeMax (§7);
//  4. suspend_ms is the call-interval ∩ suspend.wal pause overlap (§5.1);
//  5. a call whose source segment was evicted seals as trace_blob = NULL with
//     truncated_reason = disk_budget (§4.6), and segment refcounts pin only
//     the segments that sealed rows actually source (§6.2).
func TestSealPass(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	dataDir := t.TempDir()

	svc := startCollector(t, ctx, dataDir)
	store := svc.Store()

	// Trace file 1: call P and multi-chunk call C1 interleave on thread 1 with
	// call C2 on thread 2. Chunk 0 ends with C1 still open (depth 1); chunk 2
	// closes it and starts the never-closed next call (head noise).
	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1), // call P, record_index 0-1
			wire.Enter(1, sealMethodProcess), // C1 root ENTER, record_index 2
		}},
		{ThreadId: sealThread2, StartMs: baseMs + 3, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Tag(0, sealDictRequestId, "req-1"), wire.Exit(2), // call C2
		}},
		{ThreadId: sealThread1, StartMs: baseMs + 8, Events: []wire.TraceEvent{
			wire.Tag(0, sealDictRequestId, "deep"),                     // tag on C1's root
			wire.Enter(1, sealMethodQuery), wire.Exit(1), wire.Exit(2), // child, then C1's depth-0 exit
			wire.Enter(3, sealMethodHandle), // head noise: next call, never closed
		}},
	})
	// Trace file 2: the errored call C4 on thread 2.
	file2, off2 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread2, StartMs: baseMs + 20, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(4),
		}},
	})
	// Trace file 3: call C5, whose segment is evicted before the seal.
	file3, off3 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread3, StartMs: baseMs + 30, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(2),
		}},
	})

	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 1, ThreadName: "exec-1", // P, ts base+5
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodProcess, DurationMs: 100, ChildCalls: 1, ThreadName: "exec-1", // C1, ts base+10
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 2,
			Params: map[int][]string{sealDictRequestId: {"req-1"}}},
		{DeltaMs: -5, Method: sealMethodQuery, DurationMs: 50, ThreadName: "exec-2", // C2, ts base+5 (negative delta)
			TraceFileIndex: 1, BufferOffset: int(off1[1]), RecordIndex: 0},
		{DeltaMs: 15, Method: sealMethodQuery, DurationMs: 700, ThreadName: "exec-2", // C4, ts base+20, errored
			TraceFileIndex: 2, BufferOffset: int(off2[0]), RecordIndex: 0,
			Params: map[int][]string{sealDictCallRed: {"1"}}},
		{DeltaMs: 10, Method: sealMethodHandle, DurationMs: 2_000, ThreadName: "exec-3", // C5, ts base+30
			TraceFileIndex: 3, BufferOffset: int(off3[0]), RecordIndex: 0},
	}
	bucket := store.Config().Bucket(baseMs + 5)

	ac := connectAgent(t, ctx)
	key := waitForPodRestart(t, store)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)

	// Production-like ordering, deliberately WITHOUT the slice-1 dictionary
	// barrier: the calls stream lands while the dictionary has not decoded a
	// single word, so "call.red" cannot resolve at index time.
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamTrace, 1, file2)
	sendStream(t, ac, model.StreamTrace, 2, file3)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))

	var indexed []hotstore.CallIndexRow
	require.Eventually(t, func() bool {
		rows, err := store.Calls(bucket)
		require.NoError(t, err)
		indexed = rows
		return len(rows) == len(calls)
	}, 5*time.Second, 10*time.Millisecond, "all calls must be indexed before the dictionary arrives")
	for _, row := range indexed {
		if row.TraceFileIndex == 2 { // C4
			assert.False(t, row.ErrorFlag,
				"the write-time error_flag must have lost the race: call.red is not in the dictionary yet")
			assert.Equal(t, hotstore.RetentionNormalClean, row.RetentionClass,
				"the provisional retention class misses the error")
		}
	}

	// Now the dictionary, the pauses, and the params metadata arrive.
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamSuspend, 0, wire.SuspendStream(baseMs, []wire.SuspendEvent{
		// DeltaMs is the delta to the pause END; a pause spans [end−amount, end]
		// (the agent timestamps a delay after detecting it, №4).
		{DeltaMs: 50, AmountMs: 30}, // end base+50 → pause [base+20, base+50)
		{DeltaMs: 55, AmountMs: 20}, // end base+105 → pause [base+85, base+105)
	}))
	sendStream(t, ac, model.StreamParams, 0, wire.ParamsStream([]wire.ParamDef{
		{Name: "request.id", IsIndex: true, Order: 1},
	}))

	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond,
		"disconnect must finalize the pod-restart")

	podDir := filepath.Join(dataDir, "pods", hotstoreNs, hotstoreSvc, hotstorePod,
		fmt.Sprintf("%d", key.RestartTimeMs))

	// Evict C5's segment, as the §4.6 disk budget would.
	require.NoError(t, os.Remove(filepath.Join(podDir, "trace", "000003.gz")))

	res, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	require.Len(t, res.Files, 4, "short_clean, normal_clean, long_clean, any_error")
	assert.Equal(t, len(calls), res.Rows)
	assert.Equal(t, map[string]int{hotstore.TruncDiskBudget: 1}, res.Truncated)

	filesByClass := map[string]hotstore.SealedFile{}
	for _, f := range res.Files {
		filesByClass[f.RetentionClass] = f
	}

	blobPrefix := make([]byte, 8)
	binary.BigEndian.PutUint64(blobPrefix, uint64(timerStartMs))
	chunk := func(file []byte, offsets []int64, i int) []byte {
		end := int64(len(file))
		if i+1 < len(offsets) {
			end = offsets[i+1]
		}
		return file[offsets[i]:end]
	}
	concat := func(parts ...[]byte) []byte {
		var b []byte
		for _, p := range parts {
			b = append(b, p...)
		}
		return b
	}

	t.Run("any_error re-derived against the full dictionary", func(t *testing.T) {
		f, ok := filesByClass[hotstore.RetentionAnyError]
		require.True(t, ok, "the errored call must seal into the any_error class despite the racy index value")
		rows := readCallV2(t, f.Path)
		require.Len(t, rows, 1)
		c4 := rows[0]
		assert.True(t, c4.ErrorFlag)
		assert.Equal(t, hotstore.RetentionAnyError, c4.RetentionClass,
			"the retention_class column must match the class in the file name")
		assert.Equal(t, baseMs+20, c4.TsMs)
		assert.Equal(t, "com.example.Db.query", c4.Method)
		assert.Equal(t, []string{"1"}, c4.Params.Get("call.red"), "raw param ids resolve at seal")
		require.NotNil(t, c4.TraceBlob)
		assert.Equal(t, concat(blobPrefix, chunk(file2, off2, 0)), c4.TraceBlob)
		root := decodeBlobTree(t, c4.TraceBlob, timerStartMs, 0)
		assert.Equal(t, sealMethodQuery, root.method)
		assert.Empty(t, root.children)
	})

	t.Run("blobs carry full chunks and decode to the right trees", func(t *testing.T) {
		short := readCallV2(t, filesByClass[hotstore.RetentionShortClean].Path)
		require.Len(t, short, 2)
		// P and C2 share ts_ms = base+5, so the pk ASC tie-break orders P
		// (buffer_offset 8) before C2 (§5.2 row order).
		p, c2 := short[0], short[1]
		assert.Equal(t, baseMs+5, p.TsMs)
		assert.Equal(t, baseMs+5, c2.TsMs)
		assert.Less(t, p.BufferOffset, c2.BufferOffset, "equal ts_ms rows sort by pk ASC")

		// C2: one chunk of thread 2, an inline tag on the root.
		require.NotNil(t, c2.TraceBlob)
		assert.Equal(t, concat(blobPrefix, chunk(file1, off1, 1)), c2.TraceBlob)
		c2Root := decodeBlobTree(t, c2.TraceBlob, timerStartMs, 0)
		assert.Equal(t, sealMethodQuery, c2Root.method)
		assert.Equal(t, []string{"req-1"}, c2Root.tags[sealDictRequestId])

		// P shares its first (and only) chunk with C1's root ENTER: the head
		// noise past P's depth-0 exit stays in the blob and the reader trims it.
		require.NotNil(t, p.TraceBlob)
		assert.Equal(t, concat(blobPrefix, chunk(file1, off1, 0)), p.TraceBlob)
		pRoot := decodeBlobTree(t, p.TraceBlob, timerStartMs, 0)
		assert.Equal(t, sealMethodHandle, pRoot.method)
		assert.Empty(t, pRoot.children, "head noise (C1's ENTER) must not join P's tree")

		// C1 spans chunks 0 and 2 of thread 1: tail noise (all of P) before its
		// root ENTER at record_index 2, head noise (the never-closed call) after
		// its depth-0 exit.
		normal := readCallV2(t, filesByClass[hotstore.RetentionNormalClean].Path)
		require.Len(t, normal, 1)
		c1 := normal[0]
		assert.Equal(t, "com.example.Service.process", c1.Method)
		assert.Equal(t, []string{"req-1"}, c1.Params.Get("request.id"))
		require.NotNil(t, c1.TraceBlob)
		assert.Equal(t, concat(blobPrefix, chunk(file1, off1, 0), chunk(file1, off1, 2)), c1.TraceBlob,
			"the blob is the concatenation of the call's FULL chunks after the timer epoch")
		c1Root := decodeBlobTree(t, c1.TraceBlob, timerStartMs, 2)
		assert.Equal(t, sealMethodProcess, c1Root.method)
		assert.Equal(t, []string{"deep"}, c1Root.tags[sealDictRequestId])
		require.Len(t, c1Root.children, 1)
		assert.Equal(t, sealMethodQuery, c1Root.children[0].method)
	})

	t.Run("suspend_ms intersects the call interval with the pauses", func(t *testing.T) {
		// Pauses [20,50) and [85,105) (their ends are what the agent records, №4).
		c1 := readCallV2(t, filesByClass[hotstore.RetentionNormalClean].Path)[0]
		assert.Equal(t, int32(50), c1.SuspendMs, "C1 [10, 110]: [20,50) gives 30, [85,105) gives 20")
		short := readCallV2(t, filesByClass[hotstore.RetentionShortClean].Path)
		assert.Equal(t, int32(0), short[0].SuspendMs, "P [5, 6] overlaps no pause")
		assert.Equal(t, int32(30), short[1].SuspendMs, "C2 [5, 55]: [20,50) gives 30")
		c4 := readCallV2(t, filesByClass[hotstore.RetentionAnyError].Path)[0]
		assert.Equal(t, int32(50), c4.SuspendMs, "C4 [20, 720] covers both pauses: 30 + 20")
		c5 := readCallV2(t, filesByClass[hotstore.RetentionLongClean].Path)[0]
		assert.Equal(t, int32(40), c5.SuspendMs, "C5 [30, 2030]: [30,50) gives 20, [85,105) gives 20")
	})

	t.Run("evicted segment seals as NULL blob with disk_budget", func(t *testing.T) {
		long := readCallV2(t, filesByClass[hotstore.RetentionLongClean].Path)
		require.Len(t, long, 1)
		c5 := long[0]
		assert.Equal(t, baseMs+30, c5.TsMs)
		assert.Nil(t, c5.TraceBlob)
		require.NotNil(t, c5.TruncatedReason)
		assert.Equal(t, hotstore.TruncDiskBudget, *c5.TruncatedReason)
		assert.Equal(t, int64(1), store.SealCountersSnapshot().Truncated[hotstore.TruncDiskBudget])
	})

	t.Run("files are ZSTD and named per the S3 key contract", func(t *testing.T) {
		bucketStart := time.UnixMilli(store.Config().BucketStartMs(bucket)).UTC()
		stamp := func(ms int64) string { return time.UnixMilli(ms).UTC().Format("20060102T150405Z") }
		wantTimes := map[string][2]int64{
			hotstore.RetentionShortClean:  {baseMs + 5, baseMs + 5},
			hotstore.RetentionNormalClean: {baseMs + 10, baseMs + 10},
			hotstore.RetentionLongClean:   {baseMs + 30, baseMs + 30},
			hotstore.RetentionAnyError:    {baseMs + 20, baseMs + 20},
		}
		for class, f := range filesByClass {
			want := wantTimes[class]
			assert.Equal(t, want[0], f.TimeMinMs)
			assert.Equal(t, want[1], f.TimeMaxMs)
			wantName := fmt.Sprintf("collector-0-%s-%s-%s-%s-0.parquet",
				hotstore.PodRestartHash(key), bucketStart.Format("20060102T150405Z"),
				stamp(want[0]), stamp(want[1]))
			assert.Equal(t, wantName, filepath.Base(f.Path))
			wantKey := fmt.Sprintf("parquet/v1/%s/%s/%s", class, bucketStart.Format("2006/01/02/15"), wantName)
			assert.Equal(t, wantKey, f.S3Key, "the sealed name IS the S3 key (01 §7)")
			assert.Equal(t, filepath.Join(dataDir, filepath.FromSlash(wantKey)), f.Path,
				"the local copy mirrors the S3 key under the data dir")
			assertZstd(t, f.Path)
		}
	})

	t.Run("segment refcounts pin what sealed rows source", func(t *testing.T) {
		segments, err := store.Segments(key)
		require.NoError(t, err)
		refcounts := map[int]int{}
		for _, seg := range segments {
			refcounts[seg.RollingSeq] = seg.Refcount
		}
		assert.Equal(t, map[int]int{1: 3, 2: 1, 3: 0}, refcounts,
			"P, C1, C2 source segment 1; C4 sources segment 2; the evicted C5 pins nothing")
	})

	t.Run("catalog rows await the uploader", func(t *testing.T) {
		files, err := store.LocalParquet(key)
		require.NoError(t, err)
		require.Len(t, files, 4)
		for _, f := range files {
			assert.Nil(t, f.UploadedAtMs, "the seal pass itself never uploads")
			assert.True(t, strings.HasPrefix(f.S3Key, "parquet/v1/"))
		}
	})

	t.Run("a second seal of the bucket is a no-op", func(t *testing.T) {
		again, err := store.Seal(ctx, key, bucket)
		require.NoError(t, err)
		assert.Empty(t, again.Files, "the watermark covers every indexed call")
		assert.Zero(t, again.Rows)
	})
}

// readCallV2 reads a sealed parquet file back with the CallV2 schema.
func readCallV2(t *testing.T, path string) []storageparquet.CallV2 {
	rows, err := parquet.ReadFile[storageparquet.CallV2](path)
	require.NoError(t, err)
	return rows
}

// assertZstd checks every column chunk of the file is ZSTD-compressed (§5.2)
// and that the seal stamped the schema version into the footer metadata.
func assertZstd(t *testing.T, path string) {
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	require.NoError(t, err)
	pf, err := parquet.OpenFile(f, info.Size())
	require.NoError(t, err)

	version, ok := pf.Lookup(storageparquet.SchemaVersionKey)
	assert.True(t, ok, "%s must carry the schema-version stamp", filepath.Base(path))
	assert.Equal(t, storageparquet.SchemaVersion, version)

	require.NotEmpty(t, pf.Metadata().RowGroups)
	for _, rg := range pf.Metadata().RowGroups {
		for _, col := range rg.Columns {
			assert.Equal(t, format.Zstd, col.MetaData.Codec,
				"column %v of %s", col.MetaData.PathInSchema, filepath.Base(path))
		}
	}
}

// blobNode is one call in a decoded trace_blob tree.
type blobNode struct {
	method   int
	tags     map[int][]string
	children []*blobNode
}

// decodeBlobTree applies the §4.5 reader semantics to a sealed blob: verify
// the 8-byte timerStartTime prefix, then walk whole chunks, skipping tail
// noise up to the depth-0 ENTER at recordIndex in the first chunk and stopping
// at the matching depth-0 EXIT (head noise ignored).
func decodeBlobTree(t *testing.T, blob []byte, wantTimerStartMs int64, recordIndex int) *blobNode {
	require.GreaterOrEqual(t, len(blob), 8, "blob must open with the timer epoch")
	require.Equal(t, uint64(wantTimerStartMs), binary.BigEndian.Uint64(blob),
		"blob must open with the unmodified timerStartTime (01 §4.5)")

	var root *blobNode
	var stack []*blobNode
	started, finished := false, false
	chunkNo := 0
	for pos := 8; pos < len(blob) && !finished; chunkNo++ {
		_, consumed, err := hotstore.ParseChunk(blob[pos:], func(index int, ev hotstore.TraceEvent) bool {
			if !started {
				if chunkNo > 0 || index < recordIndex {
					require.Zero(t, chunkNo, "the root ENTER must sit in the first chunk")
					return true // tail noise: the previous call of this thread
				}
				require.Equal(t, hotstore.TraceEnter, ev.Kind, "record_index must land on the root ENTER")
				root = &blobNode{method: ev.TagId, tags: map[int][]string{}}
				stack = []*blobNode{root}
				started = true
				return true
			}
			switch ev.Kind {
			case hotstore.TraceEnter:
				child := &blobNode{method: ev.TagId, tags: map[int][]string{}}
				top := stack[len(stack)-1]
				top.children = append(top.children, child)
				stack = append(stack, child)
			case hotstore.TraceExit:
				stack = stack[:len(stack)-1]
				if len(stack) == 0 {
					finished = true
					return false // depth-0 exit; the rest is head noise
				}
			case hotstore.TraceTag:
				top := stack[len(stack)-1]
				top.tags[ev.TagId] = append(top.tags[ev.TagId], ev.Value)
			}
			return true
		})
		if !finished {
			require.NoError(t, err, "every blob chunk must be structurally complete")
		}
		pos += consumed
	}
	require.True(t, started, "the blob must contain the root ENTER")
	require.True(t, finished, "the blob must reach the call's depth-0 EXIT")
	return root
}
