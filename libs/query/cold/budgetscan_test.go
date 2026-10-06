package cold

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/budget"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memStore is the in-test S3 for the budgeted readers: a key→bytes map that
// records every read range in a shared, ordered event log (with the budget
// hooks writing to the same log), so tests can prove WHAT was read and that
// the charge landed BEFORE the bytes moved.
type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	// events interleaves "read <key> <off>+<len>" with the budget hook's
	// "used <n>" entries, in real order.
	events []string
	// failOpenAt returns ErrNotFound on the n-th Open of a key (1-based).
	failOpenAt map[string]int
	openCount  map[string]int
	// truncateTo serves only the first n bytes of a key (short read).
	truncateTo map[string]int
}

func newMemStore() *memStore {
	return &memStore{objects: map[string][]byte{}, failOpenAt: map[string]int{},
		openCount: map[string]int{}, truncateTo: map[string]int{}}
}

func (m *memStore) put(key string, data []byte) { m.objects[key] = data }

func (m *memStore) log(ev string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, ev)
}

func (m *memStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []ObjectInfo
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, ObjectInfo{Key: key, Size: int64(len(data))})
		}
	}
	return out, nil
}

func (m *memStore) Open(_ context.Context, key string) (Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openCount[key]++
	if at := m.failOpenAt[key]; at > 0 && m.openCount[key] >= at {
		return nil, ErrNotFound
	}
	data, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	if n := m.truncateTo[key]; n > 0 && n < len(data) {
		// A short object with the original size: reads past n fail like a
		// connection dropped mid-column.
		return &memObject{store: m, key: key, data: data[:n], size: int64(len(data))}, nil
	}
	return &memObject{store: m, key: key, data: data, size: int64(len(data))}, nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

type memObject struct {
	store *memStore
	key   string
	data  []byte
	size  int64
}

func (o *memObject) ReadAt(p []byte, off int64) (int, error) {
	o.store.log(fmt.Sprintf("read %s %d+%d", o.key, off, len(p)))
	if off >= int64(len(o.data)) {
		return 0, errors.New("short object: read past truncation")
	}
	n := copy(p, o.data[off:])
	if n < len(p) {
		return n, errors.New("short object: read past truncation")
	}
	return n, nil
}

func (o *memObject) Close() error { return nil }
func (o *memObject) Size() int64  { return o.size }

var testTuple = model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod-1", RestartTimeMs: 1000}

// makeRow builds one sorted-order CallV2 row; idx descends ts_ms so ascending
// idx produces the file's (ts_ms DESC, pk ASC) order.
func makeRow(idx int, blob []byte, params storageparquet.Parameters) storageparquet.CallV2 {
	return storageparquet.CallV2{
		TsMs:  1_000_000 - int64(idx),
		PodId: "ns/svc/pod-1", Namespace: testTuple.Namespace, ServiceName: testTuple.Service,
		PodName: testTuple.Pod, RestartTimeMs: testTuple.RestartTimeMs,
		TraceFileIndex: 1, BufferOffset: int32(idx), RecordIndex: 0,
		ThreadName: "worker", Method: fmt.Sprintf("com.example.M%03d.handle", idx%7),
		DurationMs: int32(10 + idx), RetentionClass: model.RetentionShortClean,
		Params:    params,
		TraceBlob: blob,
	}
}

// writeFile seals rows into an in-memory parquet object. flushEvery > 0
// closes a row group every that many rows; pageBuffer > 0 shrinks the data
// pages so one column chunk holds several.
func writeFile(t *testing.T, rows []storageparquet.CallV2, flushEvery, pageBuffer int) []byte {
	t.Helper()
	opts := storageparquet.CallV2WriterOptions()
	if pageBuffer > 0 {
		opts = append(opts, parquet.PageBufferSize(pageBuffer))
	}
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[storageparquet.CallV2](&buf, opts...)
	for i := range rows {
		_, err := w.Write(rows[i : i+1])
		require.NoError(t, err)
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			require.NoError(t, w.Flush())
		}
	}
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func fileRef(key string, data []byte) FileRef {
	return FileRef{Key: key, Size: int64(len(data)), Class: model.RetentionShortClean,
		Replica: "collector-0", Hash: model.PodRestartHash(testTuple)}
}

func wideQuery() model.CallsQuery {
	return model.CallsQuery{FromMs: 0, ToMs: 2_000_000}
}

// TestPerRowEstimate pins the charge formula on fabricated footer metadata:
// only charged top-level columns count, a missing additive column simply
// contributes nothing, and the struct floor holds against dictionary-shrunk
// uncompressed sizes.
func TestPerRowEstimate(t *testing.T) {
	rg := &format.RowGroup{
		NumRows: 100,
		Columns: []format.ColumnChunk{
			{MetaData: format.ColumnMetaData{PathInSchema: []string{"ts_ms"}, TotalUncompressedSize: 800}},
			{MetaData: format.ColumnMetaData{PathInSchema: []string{"params", "key_value", "key"}, TotalUncompressedSize: 100_000}},
			{MetaData: format.ColumnMetaData{PathInSchema: []string{"trace_blob"}, TotalUncompressedSize: 5_000_000}},
		},
	}
	cols := map[string]bool{"ts_ms": true, "params": true} // trace_blob not projected
	require.Equal(t, int64((800+100_000)/100), perRowEstimate(rg, cols, 0),
		"charged columns average over rows; the unprojected blob is free")

	// The struct floor wins when dictionary encoding shrinks the columns.
	tiny := &format.RowGroup{NumRows: 1000, Columns: []format.ColumnChunk{
		{MetaData: format.ColumnMetaData{PathInSchema: []string{"ts_ms"}, TotalUncompressedSize: 10}},
	}}
	require.Equal(t, int64(500+containerOverhead), perRowEstimate(tiny, cols, 500))

	// A column absent from the file (older schema) is simply not summed.
	require.Equal(t, int64(500+containerOverhead),
		perRowEstimate(tiny, map[string]bool{"no_such_column": true}, 500))
}

// TestChargedColumnsCoverNestedParams pins the top-level matching: the params
// MAP contributes all its leaves under the one "params" name.
func TestChargedColumnsCoverNestedParams(t *testing.T) {
	cols := chargedColumns(parquet.SchemaOf(&storageparquet.CallV2Projected{}))
	assert.True(t, cols["params"], "nested MAP folds into its top-level name")
	assert.True(t, cols["ts_ms"])
	assert.False(t, cols["trace_blob"], "the projection must not charge the blob")
}

// scanNaive is the reference implementation: unbudgeted whole-file scans fed
// to one big MergeRuns.
func scanNaive(t *testing.T, store ObjectStore, refs []FileRef, q model.CallsQuery, after *model.Position, limit int) ([]model.CallRow, bool) {
	t.Helper()
	var runs [][]model.CallRow
	for _, ref := range refs {
		rows, err := ScanFile(context.Background(), store, ref, q, after)
		require.NoError(t, err)
		if len(rows) > 0 {
			runs = append(runs, rows)
		}
	}
	return model.MergeRuns(runs, limit)
}

// TestCallsMatchesSingleMerge is the merge-equivalence property test: the
// incremental capped merge of the budgeted scan returns exactly the rows and
// More verdict of one MergeRuns over whole-file runs — across multiple
// files, multiple row groups, duplicates, and a keyset cursor.
func TestCallsMatchesSingleMerge(t *testing.T) {
	store := newMemStore()
	var refs []FileRef
	for f := 0; f < 3; f++ {
		var rows []storageparquet.CallV2
		for i := 0; i < 90; i++ {
			idx := f*60 + i // overlapping ranges across files → duplicates
			rows = append(rows, makeRow(idx, []byte("b"), storageparquet.Parameters{
				"request.id": {fmt.Sprintf("req-%d", idx)},
			}))
		}
		key := fmt.Sprintf("parquet/v1/short_clean/f%d.parquet", f)
		data := writeFile(t, rows, 40, 0) // 3 row groups per file
		store.put(key, data)
		refs = append(refs, fileRef(key, data))
	}

	b := budget.New(64<<20, time.Second, budget.Hooks{})
	src := &Source{Store: store, Budget: b}
	for _, tc := range []struct {
		name  string
		limit int
		after *model.Position
	}{
		{"first page", 25, nil},
		{"deep page", 25, &model.Position{TsMs: 1_000_000 - 37, PK: model.PK{PodNamespace: "ns"}}},
		{"window exhausted", 1000, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page := b.NewLease()
			defer page.Release()
			got, err := src.Calls(context.Background(), Discovery{Files: refs}, wideQuery(), tc.after, tc.limit, page)
			require.NoError(t, err)
			wantRows, wantMore := scanNaive(t, store, refs, wideQuery(), tc.after, tc.limit)
			assert.Equal(t, wantRows, got.Rows)
			assert.Equal(t, wantMore, got.More)
		})
	}
	assert.Equal(t, int64(0), b.Used(), "every lease returned after the requests")
}

// TestCallsBudgetLifecycle pins the ledger: a completed scan leaves only the
// page lease charged, and releasing it zeroes the budget; a denial surfaces
// as a budget error, not a partial reason.
func TestCallsBudgetLifecycle(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	for i := 0; i < 200; i++ {
		rows = append(rows, makeRow(i, nil, storageparquet.Parameters{
			"request.id": {fmt.Sprintf("req-%d", i)}, "sql": {strings.Repeat("SELECT 1 -- pad", 20)},
		}))
	}
	key := "parquet/v1/short_clean/a.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)
	refs := []FileRef{fileRef(key, data)}

	t.Run("completed scan settles into the page lease", func(t *testing.T) {
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		src := &Source{Store: store, Budget: b}
		page := b.NewLease()
		res, err := src.Calls(context.Background(), Discovery{Files: refs}, wideQuery(), nil, 50, page)
		require.NoError(t, err)
		require.Len(t, res.Rows, 50)
		assert.Equal(t, page.Held(), b.Used(), "only the page lease stays charged")
		assert.Equal(t, footprintSum(res.Rows), page.Held(), "the page lease holds exactly the retained rows")
		page.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("a batch that can never fit denies structurally", func(t *testing.T) {
		b := budget.New(10_000, 20*time.Millisecond, budget.Hooks{})
		src := &Source{Store: store, Budget: b}
		page := b.NewLease()
		defer page.Release()
		_, err := src.Calls(context.Background(), Discovery{Files: refs}, wideQuery(), nil, 50, page)
		require.Error(t, err)
		assert.True(t, IsBudgetDenial(err), "got: %v", err)
		assert.ErrorIs(t, err, budget.ErrNeverFits)
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("contention denies transiently and leaks nothing", func(t *testing.T) {
		b := budget.New(64<<20, 30*time.Millisecond, budget.Hooks{})
		blocker, err := b.Acquire(context.Background(), (64<<20)-20_000)
		require.NoError(t, err)
		defer blocker.Release()
		src := &Source{Store: store, Budget: b}
		page := b.NewLease()
		defer page.Release()
		_, err = src.Calls(context.Background(), Discovery{Files: refs}, wideQuery(), nil, 50, page)
		require.Error(t, err)
		assert.ErrorIs(t, err, budget.ErrExhausted)
		page.Release()
		blocker.Release()
		assert.Equal(t, int64(0), b.Used())
	})
}

// TestGrowForCopiesDeniedUnderContention pins the accepted §7.5 outcome: the
// batch admission fits, but the survivor-copy reservation (Grow) hits a
// budget held elsewhere and the scan aborts with a denial.
func TestGrowForCopiesDeniedUnderContention(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	// Dictionary-friendly fat params: identical values compress into one
	// dictionary entry, so the footer estimate floors near the struct size
	// while every decoded row (and every copy) carries the full value.
	fat := strings.Repeat("v", 2000)
	for i := 0; i < 300; i++ {
		rows = append(rows, makeRow(i, nil, storageparquet.Parameters{"sql": {fat}}))
	}
	key := "parquet/v1/short_clean/fat.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)
	refs := []FileRef{fileRef(key, data)}

	// Measure the peak with a generous budget first.
	var mu sync.Mutex
	var peak int64
	b := budget.New(1<<30, time.Second, budget.Hooks{OnUsed: func(used int64) {
		mu.Lock()
		defer mu.Unlock()
		if used > peak {
			peak = used
		}
	}})
	src := &Source{Store: store, Budget: b}
	page := b.NewLease()
	_, err := src.Calls(context.Background(), Discovery{Files: refs}, wideQuery(), nil, 300, page)
	require.NoError(t, err)
	page.Release()
	require.Equal(t, int64(0), b.Used())

	// Now hold everything but a sliver below the measured peak: some charge
	// along the way — admission or the copy Grow — must deny.
	b2 := budget.New(peak, 30*time.Millisecond, budget.Hooks{})
	blocker, err := b2.Acquire(context.Background(), peak/10)
	require.NoError(t, err)
	defer blocker.Release()
	src2 := &Source{Store: store, Budget: b2, OverrunHook: func() {}}
	page2 := b2.NewLease()
	defer page2.Release()
	_, err = src2.Calls(context.Background(), Discovery{Files: refs}, wideQuery(), nil, 300, page2)
	require.Error(t, err)
	assert.True(t, IsBudgetDenial(err), "got: %v", err)
	page2.Release()
	blocker.Release()
	assert.Equal(t, int64(0), b2.Used())
}

// TestReconcileOverrunCounter pins the reconcile mechanics deterministically:
// with an accounting model whose per-row footprint dwarfs the footer
// estimate (the dictionary-expansion shape), the batch reconcile grows the
// ledger to the actual size, fires the overrun hook, and still settles to
// zero. The end-to-end dictionary case rides in
// TestGrowForCopiesDeniedUnderContention.
func TestReconcileOverrunCounter(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	for i := 0; i < 60; i++ {
		rows = append(rows, makeRow(i, nil, nil))
	}
	key := "parquet/v1/short_clean/dict.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)

	overruns := 0
	var peak int64
	b := budget.New(1<<30, time.Second, budget.Hooks{OnUsed: func(used int64) {
		if used > peak {
			peak = used
		}
	}})
	const inflatedRow = 1 << 18
	err := scanBatches(context.Background(), store, fileRef(key, data), b,
		func(*storageparquet.CallV2Projected) int64 { return inflatedRow },
		func() { overruns++ },
		func(context.Context, int, int64, []storageparquet.CallV2Projected, *budget.Lease) (bool, error) {
			return false, nil
		})
	require.NoError(t, err)
	assert.Positive(t, overruns, "an actual footprint above the charge must be visible as an overrun")
	assert.GreaterOrEqual(t, peak, int64(60*inflatedRow), "the reconcile must true the ledger up to the actual size")
	assert.Equal(t, int64(0), b.Used())
}

// TestCopyDetachesFromDecoder pins the deep copy: no string in a survivor
// aliases the source row's backing bytes.
func TestCopyDetachesFromDecoder(t *testing.T) {
	backing := []byte("namespace-service-pod-method-value")
	src := model.CallRow{
		PK: model.PK{
			PodNamespace: unsafeString(backing[0:9]),
			PodService:   unsafeString(backing[10:17]),
			PodName:      unsafeString(backing[18:21]),
		},
		Method: unsafeString(backing[22:28]),
		Params: map[string][]string{unsafeString(backing[22:28]): {unsafeString(backing[29:34])}},
	}
	cp := copyCallRow(&src)
	assert.Equal(t, src.PK, cp.PK)
	assert.NotSame(t, unsafe.StringData(src.PK.PodNamespace), unsafe.StringData(cp.PK.PodNamespace))
	assert.NotSame(t, unsafe.StringData(src.Method), unsafe.StringData(cp.Method))
	for k, vs := range cp.Params {
		assert.NotSame(t, unsafe.StringData(src.Method), unsafe.StringData(k))
		for _, v := range vs {
			assert.NotSame(t, unsafe.StringData(unsafeString(backing[29:34])), unsafe.StringData(v))
		}
	}
}

func unsafeString(b []byte) string { return unsafe.String(&b[0], len(b)) }

// TestScanErrorPathsLeakNothing injects failures at every stage and checks
// the ledger returns to zero (the ownership-discipline test of §7.5).
func TestScanErrorPathsLeakNothing(t *testing.T) {
	rowsOK := make([]storageparquet.CallV2, 0, 50)
	for i := 0; i < 50; i++ {
		rowsOK = append(rowsOK, makeRow(i, []byte("blob"), nil))
	}
	data := writeFile(t, rowsOK, 0, 0)

	t.Run("short read fails the file, budget settles", func(t *testing.T) {
		store := newMemStore()
		key := "parquet/v1/short_clean/short.parquet"
		store.put(key, data)
		store.truncateTo[key] = len(data) / 2
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		src := &Source{Store: store, Budget: b}
		page := b.NewLease()
		res, err := src.Calls(context.Background(), Discovery{Files: []FileRef{fileRef(key, data)}}, wideQuery(), nil, 10, page)
		require.NoError(t, err, "a broken file degrades to a partial reason")
		assert.NotEmpty(t, res.PartialReasons)
		page.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("garbage object fails the file, budget settles", func(t *testing.T) {
		store := newMemStore()
		key := "parquet/v1/short_clean/garbage.parquet"
		store.put(key, []byte("this is not parquet at all, but long enough to look like a file"))
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		src := &Source{Store: store, Budget: b}
		page := b.NewLease()
		res, err := src.Calls(context.Background(), Discovery{Files: []FileRef{{Key: key, Size: 64}}}, wideQuery(), nil, 10, page)
		require.NoError(t, err)
		assert.NotEmpty(t, res.PartialReasons)
		page.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("deleted after LIST is an empty result", func(t *testing.T) {
		store := newMemStore()
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		src := &Source{Store: store, Budget: b}
		page := b.NewLease()
		res, err := src.Calls(context.Background(), Discovery{Files: []FileRef{{Key: "gone.parquet", Size: 1}}}, wideQuery(), nil, 10, page)
		require.NoError(t, err)
		assert.Empty(t, res.Rows)
		assert.Empty(t, res.PartialReasons)
		page.Release()
		assert.Equal(t, int64(0), b.Used())
	})

	t.Run("point fetch on a truncated candidate settles", func(t *testing.T) {
		store := newMemStore()
		key := "parquet/v1/short_clean/short2.parquet"
		store.put(key, data)
		store.truncateTo[key] = len(data) / 2
		b := budget.New(64<<20, time.Second, budget.Hooks{})
		src := &Source{Store: store, Budget: b}
		point := b.NewLease()
		_, ok, err := src.FetchCall(context.Background(), []FileRef{fileRef(key, data)}, pkOf(3), point, TraceColumns)
		require.Error(t, err)
		assert.False(t, ok)
		point.Release()
		assert.Equal(t, int64(0), b.Used())
	})
}

func pkOf(idx int) model.PK {
	return model.PK{
		PodNamespace: testTuple.Namespace, PodService: testTuple.Service, PodName: testTuple.Pod,
		RestartTimeMs: testTuple.RestartTimeMs, TraceFileIndex: 1, BufferOffset: int32(idx), RecordIndex: 0,
	}
}

// TestFetchCallIdentity is the silent-corruption test: the blob phase 2
// returns must belong to the exact PK phase 1 matched — across row-group
// boundaries, batch boundaries, and near-identical PK prefixes with
// distinct payloads. A positioning bug returns a neighbor's blob without any
// error, which is why the payloads encode their index.
func TestFetchCallIdentity(t *testing.T) {
	store := newMemStore()
	const total = 150
	var rows []storageparquet.CallV2
	for i := 0; i < total; i++ {
		blob := []byte(fmt.Sprintf("blob-payload-%04d", i))
		rows = append(rows, makeRow(i, blob, storageparquet.Parameters{"request.id": {fmt.Sprintf("r%d", i)}}))
	}
	key := "parquet/v1/short_clean/identity.parquet"
	data := writeFile(t, rows, 40, 512) // several row groups, small pages
	store.put(key, data)
	refs := []FileRef{fileRef(key, data)}

	b := budget.New(64<<20, time.Second, budget.Hooks{})
	src := &Source{Store: store, Budget: b}
	for _, idx := range []int{0, 1, 39, 40, 41, 79, 80, 99, total - 1} {
		t.Run(fmt.Sprintf("row %d", idx), func(t *testing.T) {
			point := b.NewLease()
			defer point.Release()
			row, ok, err := src.FetchCall(context.Background(), refs, pkOf(idx), point, TreeColumns)
			require.NoError(t, err)
			require.True(t, ok, "PK %d must be found", idx)
			assert.Equal(t, fmt.Sprintf("blob-payload-%04d", idx), string(row.TraceBlob),
				"phase 2 must return the blob of the PK phase 1 matched")
			assert.Nil(t, row.TruncatedReason)
		})
	}
	_, ok, err := src.FetchCall(context.Background(), refs, pkOf(total+5), b.NewLease(), TreeColumns)
	require.NoError(t, err)
	assert.False(t, ok, "an absent PK is an honest miss")
	assert.Equal(t, int64(0), b.Used())
}

// TestPKScanReadsNoBlob pins the two-phase promise: until a PK matches, the
// trace_blob column chunks are never read. The probe PK misses, so phase 2
// never runs and no read range may touch the blob chunks.
func TestPKScanReadsNoBlob(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	for i := 0; i < 100; i++ {
		rows = append(rows, makeRow(i, bytes.Repeat([]byte("B"), 2048), nil))
	}
	key := "parquet/v1/short_clean/noblob.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)

	// The blob chunk's byte span comes from the footer.
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	leaf, ok := f.Schema().Lookup("trace_blob")
	require.True(t, ok)
	md := f.Metadata().RowGroups[0].Columns[leaf.ColumnIndex].MetaData
	blobStart := md.DataPageOffset
	if md.DictionaryPageOffset > 0 && md.DictionaryPageOffset < blobStart {
		blobStart = md.DictionaryPageOffset
	}
	blobEnd := blobStart + md.TotalCompressedSize

	b := budget.New(64<<20, time.Second, budget.Hooks{})
	src := &Source{Store: store, Budget: b}
	_, found, err := src.FetchCall(context.Background(), []FileRef{fileRef(key, data)}, pkOf(999), b.NewLease(), TraceColumns)
	require.NoError(t, err)
	require.False(t, found)

	footerStart := int64(len(data) - 64*1024) // ranged footer reads are fine
	for _, ev := range store.events {
		var evKey string
		var off, n int64
		if _, err := fmt.Sscanf(ev, "read %s %d+%d", &evKey, &off, &n); err != nil {
			continue
		}
		if off+n <= blobStart || off >= blobEnd || off >= footerStart {
			continue
		}
		t.Fatalf("PK scan read %s inside the blob chunk [%d, %d)", ev, blobStart, blobEnd)
	}
}

// TestSurgicalPageChargePrecedesRead pins charge-before-I/O for phase 2: the
// budget grows past the PK-scan settled level BEFORE any byte of the blob
// chunk is read.
func TestSurgicalPageChargePrecedesRead(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	for i := 0; i < 60; i++ {
		rows = append(rows, makeRow(i, bytes.Repeat([]byte("Z"), 4096), nil))
	}
	key := "parquet/v1/short_clean/order.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	leaf, _ := f.Schema().Lookup("trace_blob")
	md := f.Metadata().RowGroups[0].Columns[leaf.ColumnIndex].MetaData
	blobStart, blobEnd := md.DataPageOffset, md.DataPageOffset+md.TotalCompressedSize

	b := budget.New(64<<20, time.Second, budget.Hooks{OnUsed: func(used int64) {
		store.log(fmt.Sprintf("used %d", used))
	}})
	src := &Source{Store: store, Budget: b}
	point := b.NewLease()
	defer point.Release()
	_, found, err := src.FetchCall(context.Background(), []FileRef{fileRef(key, data)}, pkOf(30), point, TraceColumns)
	require.NoError(t, err)
	require.True(t, found)

	// Walk the shared log: at the first blob-chunk read, the last budget
	// event must show a charge (the page lease) already in place.
	var lastUsed int64
	for _, ev := range store.events {
		var evKey string
		var off, n, used int64
		if _, err := fmt.Sscanf(ev, "used %d", &used); err == nil {
			lastUsed = used
			continue
		}
		if _, err := fmt.Sscanf(ev, "read %s %d+%d", &evKey, &off, &n); err != nil {
			continue
		}
		if off+n > blobStart && off < blobEnd { // inside the blob chunk
			assert.Positive(t, lastUsed, "the blob page must be charged before its bytes are read")
			return
		}
	}
	t.Fatal("no blob-chunk read observed")
}

// TestSurgicalOversizedPage pins the structural denial: a single blob page
// larger than the whole budget answers ErrNeverFits (the PK scan itself
// passes), while a file whose TOTAL size dwarfs the budget still reads fine —
// only pages must fit, never files.
func TestSurgicalOversizedPage(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	for i := 0; i < 40; i++ {
		// ~1 MiB of incompressible-ish blob per row: the file is far larger
		// than the budget below, single row group.
		blob := bytes.Repeat([]byte(fmt.Sprintf("x%03d", i)), 256*1024/4)
		rows = append(rows, makeRow(i, blob, nil))
	}
	key := "parquet/v1/short_clean/hugeblob.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)
	refs := []FileRef{fileRef(key, data)}

	// 4 MiB budget: the PK batches and one blob page fit, the whole file
	// (≈10 MiB) does not — and must not need to.
	b := budget.New(4<<20, 50*time.Millisecond, budget.Hooks{})
	src := &Source{Store: store, Budget: b}
	point := b.NewLease()
	row, ok, err := src.FetchCall(context.Background(), refs, pkOf(7), point, TraceColumns)
	require.NoError(t, err, "a file larger than the budget reads page by page")
	require.True(t, ok)
	assert.NotEmpty(t, row.TraceBlob)
	point.Release()
	require.Equal(t, int64(0), b.Used())

	// A budget smaller than one blob page is a structural misfit.
	small := budget.New(100_000, 50*time.Millisecond, budget.Hooks{})
	srcSmall := &Source{Store: store, Budget: small}
	pointSmall := small.NewLease()
	defer pointSmall.Release()
	_, _, err = srcSmall.FetchCall(context.Background(), refs, pkOf(7), pointSmall, TraceColumns)
	require.Error(t, err)
	assert.ErrorIs(t, err, budget.ErrNeverFits)
	assert.Equal(t, int64(0), small.Used())
}

// TestSurgicalGoneBetweenPhases pins the §5.1 semantics across the phase
// gap: an object deleted after the PK scan but before the surgical read is a
// not-found, not an internal error.
func TestSurgicalGoneBetweenPhases(t *testing.T) {
	store := newMemStore()
	var rows []storageparquet.CallV2
	for i := 0; i < 20; i++ {
		rows = append(rows, makeRow(i, []byte("blob"), nil))
	}
	key := "parquet/v1/short_clean/vanish.parquet"
	data := writeFile(t, rows, 0, 0)
	store.put(key, data)
	store.failOpenAt[key] = 2 // phase 1 opens once; phase 2's open vanishes

	b := budget.New(64<<20, time.Second, budget.Hooks{})
	src := &Source{Store: store, Budget: b}
	point := b.NewLease()
	defer point.Release()
	_, ok, err := src.FetchCall(context.Background(), []FileRef{fileRef(key, data)}, pkOf(3), point, TraceColumns)
	require.NoError(t, err, "a vanish between the phases is the LIST-race semantics, not an error")
	assert.False(t, ok)
	assert.Equal(t, int64(0), b.Used())
}
