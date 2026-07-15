package maintain

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStore is the in-test S3 with a controllable LastModified per key, so
// the delete-grace clock can be steered without waiting.
type fakeStore struct {
	mu      sync.Mutex
	objects map[string]fakeObject
}

type fakeObject struct {
	data         []byte
	lastModified time.Time
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string]fakeObject{}}
}

func (f *fakeStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ObjectInfo
	for key, obj := range f.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, ObjectInfo{Key: key, Size: int64(len(obj.data)), LastModified: obj.lastModified})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (f *fakeStore) Open(_ context.Context, key string) (Object, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return &memObject{data: obj.data}, nil
}

func (f *fakeStore) Put(_ context.Context, key string, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{data: append([]byte(nil), body...), lastModified: time.Now()}
	return nil
}

// putAt seeds one object with an explicit LastModified.
func (f *fakeStore) putAt(key string, body []byte, lastModified time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeObject{data: append([]byte(nil), body...), lastModified: lastModified}
}

func (f *fakeStore) Delete(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.objects, key)
	return nil
}

func (f *fakeStore) keys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (f *fakeStore) setLastModified(key string, at time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		panic("no object " + key)
	}
	obj.lastModified = at
	f.objects[key] = obj
}

func (f *fakeStore) singleKeyWithPrefix(t *testing.T, prefix string) string {
	t.Helper()
	var found []string
	for _, key := range f.keys() {
		if strings.HasPrefix(key, prefix) {
			found = append(found, key)
		}
	}
	require.Len(t, found, 1, "want exactly one key under %s", prefix)
	return found[0]
}

type memObject struct{ data []byte }

func (o *memObject) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(o.data)) {
		return 0, io.EOF
	}
	n := copy(p, o.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
func (o *memObject) Close() error { return nil }
func (o *memObject) Size() int64  { return int64(len(o.data)) }

// --- seeding helpers -------------------------------------------------------

var testBucketStart = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

func testRow(pod string, restartMs, tsMs int64, seqNo int32, class string) storageparquet.CallV2 {
	blob := []byte(fmt.Sprintf("blob-%s-%d-%d", pod, tsMs, seqNo))
	big := fmt.Sprintf(`{"sql:0:%d":"select %d"}`, seqNo, seqNo)
	return storageparquet.CallV2{
		TsMs:           tsMs,
		PodId:          "ns/svc/" + pod,
		RestartTimeMs:  restartMs,
		TraceFileIndex: 1,
		BufferOffset:   seqNo,
		RecordIndex:    0,
		ThreadName:     "exec-1",
		Namespace:      "ns",
		ServiceName:    "svc",
		PodName:        pod,
		Method:         "com.example.Service.handle",
		DurationMs:     500,
		CpuTimeMs:      42,
		ErrorFlag:      class == model.RetentionAnyError,
		RetentionClass: class,
		Params:         storageparquet.Parameters{"request.id": {fmt.Sprintf("req-%d", seqNo)}},
		TraceBlob:      blob,
		BigParamsJson:  &big,
	}
}

// sealStyleKey renders the key a collector seal would produce (01 §7).
func sealStyleKey(class string, bucketStart time.Time, replica, hash string, timeMinMs, timeMaxMs int64, seq int) string {
	name := fmt.Sprintf("%s-%s-%s-%s-%s-%d.parquet",
		replica, hash,
		bucketStart.UTC().Format(keyStamp),
		time.UnixMilli(timeMinMs).UTC().Format(keyStamp),
		time.UnixMilli(timeMaxMs).UTC().Format(keyStamp),
		seq)
	return path.Join(parquetPrefix, class, bucketStart.UTC().Format("2006/01/02/15"), name)
}

// writeRowsBody renders rows through the shared CallV2 writer invariants,
// like a seal pass would, and reports the true ts bounds for the key stamps.
func writeRowsBody(t *testing.T, rows []storageparquet.CallV2) (body []byte, timeMinMs, timeMaxMs int64) {
	t.Helper()
	timeMinMs, timeMaxMs = rows[0].TsMs, rows[0].TsMs
	for i := range rows {
		if rows[i].TsMs < timeMinMs {
			timeMinMs = rows[i].TsMs
		}
		if rows[i].TsMs > timeMaxMs {
			timeMaxMs = rows[i].TsMs
		}
	}
	var buf bytes.Buffer
	w := parquetgo.NewGenericWriter[storageparquet.CallV2](&buf, storageparquet.CallV2WriterOptions()...)
	_, err := w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes(), timeMinMs, timeMaxMs
}

// seedParquet writes rows as one seal-style object and returns its key. Rows
// are sorted into the 01 §5.2 file order first, like a real sealed file.
func seedParquet(t *testing.T, store *fakeStore, class string, bucketStart time.Time,
	replica, hash string, seq int, lastModified time.Time, rows []storageparquet.CallV2) string {
	t.Helper()
	sort.SliceStable(rows, func(a, b int) bool { return rowCompare(&rows[a], &rows[b]) < 0 })
	body, timeMinMs, timeMaxMs := writeRowsBody(t, rows)
	key := sealStyleKey(class, bucketStart, replica, hash, timeMinMs, timeMaxMs, seq)
	store.putAt(key, body, lastModified)
	return key
}

func testJob(store *fakeStore) *Job {
	return NewJob(store, Config{
		TimeBucket:  5 * time.Minute,
		MinAge:      30 * time.Minute,
		MinFiles:    3,
		DeleteGrace: 5 * time.Minute,
	})
}

func readObjectRows(t *testing.T, store *fakeStore, key string) []storageparquet.CallV2 {
	t.Helper()
	store.mu.Lock()
	data := store.objects[key].data
	store.mu.Unlock()
	pf, err := parquetgo.OpenFile(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	r := parquetgo.NewGenericReader[storageparquet.CallV2](pf)
	defer func() { _ = r.Close() }()
	rows := make([]storageparquet.CallV2, r.NumRows())
	n, err := r.Read(rows)
	if err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	require.Equal(t, len(rows), n)
	return rows
}

func rowPKs(rows []storageparquet.CallV2) []model.PK {
	out := make([]model.PK, len(rows))
	for i := range rows {
		out[i] = rowPK(&rows[i])
	}
	return out
}

// --- tests ------------------------------------------------------------------

// TestCompactionLifecycle drives one (bucket, class) group through the full
// write → grace → delete protocol of 01 §6.6 and pins the output invariants:
// every input PK survives exactly once, rows keep the (ts_ms DESC, pk ASC)
// order, all columns round-trip, and the second pass over the converged
// bucket is a no-op.
func TestCompactionLifecycle(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	job := testJob(store)
	seeded := time.Now().Add(-2 * time.Hour)
	base := testBucketStart.UnixMilli()

	// Three pod-restarts plus a patch file of pod-a; pod-b's file shares a
	// ts_ms with pod-a's second row to exercise the pk ASC tiebreak, and the
	// patch repeats one pod-a row verbatim to exercise PK-dedup (an
	// idempotent overlap; 01 §6.2 makes the copies identical).
	dup := testRow("pod-a", 1000, base+7_000, 2, model.RetentionNormalClean)
	inputs := []string{
		seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0", "aaaa1111", 0, seeded,
			[]storageparquet.CallV2{
				testRow("pod-a", 1000, base+4_000, 1, model.RetentionNormalClean),
				dup,
			}),
		seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-1", "bbbb2222", 0, seeded,
			[]storageparquet.CallV2{testRow("pod-b", 2000, base+7_000, 3, model.RetentionNormalClean)}),
		seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0", "cccc3333", 0, seeded,
			[]storageparquet.CallV2{testRow("pod-c", 3000, base+9_500, 4, model.RetentionNormalClean)}),
		seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0", "aaaa1111", 1, seeded,
			[]storageparquet.CallV2{dup, testRow("pod-a", 1000, base+11_000, 5, model.RetentionNormalClean)}),
	}
	require.Len(t, store.keys(), 4)

	// Pass 1: writes the compacted object, deletes nothing.
	now := time.Now()
	stats, err := job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.CompactedGroups)
	assert.Equal(t, 4, stats.CompactedInputFiles)
	assert.Equal(t, 5, stats.CompactedRows, "5 distinct PKs across 6 input rows")
	assert.Equal(t, 1, stats.DedupedRows)
	assert.Equal(t, 0, stats.DeletedInputFiles)
	require.Len(t, store.keys(), 5, "inputs must survive until the grace elapses")

	outKey := ""
	for _, key := range store.keys() {
		if strings.Contains(key, "/maintain-") {
			outKey = key
		}
	}
	require.NotEmpty(t, outKey, "compacted object present")

	// The compacted key parses by the discovery rules (02 §5.1) with the
	// true row bounds: floor(min ts) and the truncated max widened by 999 ms.
	ref, ok := cold.ParseKey(outKey, 1)
	require.True(t, ok, "cold discovery must parse the maintain key")
	assert.Equal(t, model.RetentionNormalClean, ref.Class)
	assert.Equal(t, base+4_000, ref.TimeMinMs)
	assert.Equal(t, base+11_000+999, ref.TimeMaxMs)

	outRows := readObjectRows(t, store, outKey)
	require.Len(t, outRows, 5)
	for i := 1; i < len(outRows); i++ {
		assert.Negative(t, rowCompare(&outRows[i-1], &outRows[i]),
			"rows must be strictly (ts_ms DESC, pk ASC) ordered")
	}
	// All columns survive the rewrite, blob and big params included.
	for _, row := range outRows {
		assert.NotEmpty(t, row.TraceBlob)
		require.NotNil(t, row.BigParamsJson)
		assert.Contains(t, *row.BigParamsJson, "sql:0:")
		assert.Equal(t, model.RetentionNormalClean, row.RetentionClass)
		assert.Len(t, row.Params["request.id"], 1)
	}

	// Pass 2, still within the grace: nothing changes.
	stats, err = job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, Stats{PendingDeleteGroups: 1}, stats)
	require.Len(t, store.keys(), 5)

	// Pass 3, grace elapsed: the inputs go, the output stays.
	stats, err = job.Pass(ctx, now.Add(job.cfg.DeleteGrace+time.Second))
	require.NoError(t, err)
	assert.Equal(t, Stats{DeletedInputFiles: 4}, stats)
	assert.Equal(t, []string{outKey}, store.keys())
	for _, in := range inputs {
		assert.NotContains(t, store.keys(), in)
	}

	// Pass 4: the converged bucket is a no-op (idempotency).
	stats, err = job.Pass(ctx, now.Add(job.cfg.DeleteGrace+2*time.Second))
	require.NoError(t, err)
	assert.Equal(t, Stats{}, stats)
	assert.Equal(t, []string{outKey}, store.keys())
}

// TestCompactionSkipsUnsettledAndSmallGroups pins the two 01 §6.6 guards: a
// bucket younger than its end + MinAge is left alone however many files it
// holds, and a settled bucket below MinFiles is not worth a rewrite.
func TestCompactionSkipsUnsettledAndSmallGroups(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	job := testJob(store)
	seeded := time.Now()

	youngStart := time.Now().UTC().Truncate(5 * time.Minute)
	for i := 0; i < 4; i++ {
		seedParquet(t, store, model.RetentionNormalClean, youngStart, "collector-0",
			fmt.Sprintf("hash%04d", i), i, seeded,
			[]storageparquet.CallV2{testRow("pod-y", 1000, youngStart.UnixMilli()+int64(i)*1000, int32(i), model.RetentionNormalClean)})
	}
	oldBase := testBucketStart.UnixMilli()
	for i := 0; i < 2; i++ {
		seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0",
			fmt.Sprintf("old%05d", i), i, seeded,
			[]storageparquet.CallV2{testRow("pod-o", 1000, oldBase+int64(i)*1000, int32(i), model.RetentionNormalClean)})
	}

	before := store.keys()
	stats, err := job.Pass(ctx, time.Now())
	require.NoError(t, err)
	assert.Equal(t, Stats{SkippedUnsettled: 1, SkippedSmallGroups: 1}, stats)
	assert.Equal(t, before, store.keys())
}

// TestCompactionRecompactsResidueBelowMinFiles pins convergence: a stale
// maintain output next to a straggler patch recompacts even below MinFiles,
// so the bucket ends at exactly one object instead of parking at two.
func TestCompactionRecompactsResidueBelowMinFiles(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	job := testJob(store)
	seeded := time.Now().Add(-2 * time.Hour)
	base := testBucketStart.UnixMilli()

	// A maintain object whose hash matches nothing in the group (its inputs
	// are long gone) plus one late patch.
	seedParquet(t, store, model.RetentionNormalClean, testBucketStart, producerToken, "deadbeef", 0, seeded,
		[]storageparquet.CallV2{testRow("pod-a", 1000, base+1_000, 1, model.RetentionNormalClean)})
	seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0", "aaaa1111", 7, seeded,
		[]storageparquet.CallV2{testRow("pod-a", 1000, base+2_000, 2, model.RetentionNormalClean)})

	now := time.Now()
	stats, err := job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.CompactedGroups)
	assert.Equal(t, 2, stats.CompactedInputFiles)
	assert.Equal(t, 2, stats.CompactedRows)
	require.Len(t, store.keys(), 3)

	// Grace elapsed: the residue and the patch go; one object remains with
	// both rows.
	stats, err = job.Pass(ctx, now.Add(job.cfg.DeleteGrace+time.Second))
	require.NoError(t, err)
	assert.Equal(t, Stats{DeletedInputFiles: 2}, stats)
	require.Len(t, store.keys(), 1)
	rows := readObjectRows(t, store, store.keys()[0])
	assert.Len(t, rows, 2)
}

// TestCompactionSplitsOversizedGroup pins the №11 fix: a (bucket, class)
// group whose total input bytes exceed MaxGroupBytes is no longer parked
// forever — it splits into sub-budget subgroups along the time axis, each
// compacts into its own output, and the footer-recorded input lists let the
// delete step remove exactly each output's inputs.
func TestCompactionSplitsOversizedGroup(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	seeded := time.Now().Add(-2 * time.Hour)
	base := testBucketStart.UnixMilli()

	inputs := make([]string, 4)
	var maxSize int64
	for i := 0; i < 4; i++ {
		inputs[i] = seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0",
			fmt.Sprintf("hash%04d", i), i, seeded,
			[]storageparquet.CallV2{testRow("pod-a", 1000, base+int64(i)*1000, int32(i), model.RetentionNormalClean)})
		if size := int64(len(store.objects[inputs[i]].data)); size > maxSize {
			maxSize = size
		}
	}
	// The budget takes two near-equal inputs but not three: the group of four
	// must split into two subgroups of two.
	job := NewJob(store, Config{
		TimeBucket:    5 * time.Minute,
		MinAge:        30 * time.Minute,
		MinFiles:      4,
		DeleteGrace:   5 * time.Minute,
		MaxGroupBytes: 2 * maxSize,
	})

	now := time.Now()
	stats, err := job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 2, stats.CompactedGroups, "the oversized group compacts piecewise")
	assert.Equal(t, 4, stats.CompactedInputFiles)
	assert.Zero(t, stats.SkippedOversized, "no single input exceeds the budget")
	require.Len(t, store.keys(), 6, "two outputs written, inputs still inside the grace")

	// Grace elapsed: each output's footer names exactly its own inputs, so
	// all four inputs go and both outputs stay.
	stats, err = job.Pass(ctx, now.Add(job.cfg.DeleteGrace+time.Second))
	require.NoError(t, err)
	assert.Equal(t, Stats{DeletedInputFiles: 4}, stats)
	keys := store.keys()
	require.Len(t, keys, 2)
	var pks []model.PK
	for _, key := range keys {
		assert.Contains(t, key, "/maintain-")
		pks = append(pks, rowPKs(readObjectRows(t, store, key))...)
	}
	assert.Len(t, pks, 4, "every seeded PK survives across the split outputs")
}

// TestCompactionSkipsSingleOversizedObject pins the one remaining
// SkippedOversized case (№11): an object that alone exceeds MaxGroupBytes
// can never join a subgroup; the counter is the raise-the-budget alert.
func TestCompactionSkipsSingleOversizedObject(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	job := NewJob(store, Config{
		TimeBucket:    5 * time.Minute,
		MinAge:        30 * time.Minute,
		MinFiles:      2,
		DeleteGrace:   5 * time.Minute,
		MaxGroupBytes: 1, // any real parquet body exceeds this
	})
	seeded := time.Now().Add(-2 * time.Hour)
	base := testBucketStart.UnixMilli()
	for i := 0; i < 2; i++ {
		seedParquet(t, store, model.RetentionNormalClean, testBucketStart, "collector-0",
			fmt.Sprintf("hash%04d", i), i, seeded,
			[]storageparquet.CallV2{testRow("pod-a", 1000, base+int64(i)*1000, int32(i), model.RetentionNormalClean)})
	}

	before := store.keys()
	stats, err := job.Pass(ctx, time.Now())
	require.NoError(t, err)
	assert.Equal(t, Stats{SkippedOversized: 2}, stats)
	assert.Equal(t, before, store.keys())
}

// TestTTLParquet pins the 01 §6.4 rule: expiry is judged from the key's
// timeMax stamp alone, an object inside its TTL is never deleted, and the
// widened stamp keeps a boundary object alive through its full last second.
func TestTTLParquet(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	job := testJob(store)
	now := time.Now()
	seeded := now.Add(-time.Hour)

	// The short_clean TTL comes from the tier table (№10). One object twice
	// the TTL old, one exactly at the cutoff (its widened timeMax equals the
	// cutoff instant), one young.
	ttl := model.DefaultClassTTL()[model.RetentionShortClean]
	mkRows := func(tsMs int64) []storageparquet.CallV2 {
		return []storageparquet.CallV2{testRow("pod-t", 1000, tsMs, 1, model.RetentionShortClean)}
	}
	bucketOf := func(tsMs int64) time.Time {
		return time.UnixMilli(tsMs).UTC().Truncate(5 * time.Minute)
	}
	expiredTs := now.Add(-2 * ttl).UnixMilli()
	expiredKey := seedParquet(t, store, model.RetentionShortClean, bucketOf(expiredTs),
		"collector-0", "aaaa1111", 0, seeded, mkRows(expiredTs))
	// The key stamp truncates ts to its second and the parser widens it back
	// by 999 ms: pick a whole-second ts so the boundary is exact.
	boundaryTs := now.Add(-ttl).Truncate(time.Second).UnixMilli()
	boundaryKey := seedParquet(t, store, model.RetentionShortClean, bucketOf(boundaryTs),
		"collector-0", "bbbb2222", 0, seeded, mkRows(boundaryTs))
	youngTs := now.Add(-time.Hour).UnixMilli()
	youngKey := seedParquet(t, store, model.RetentionShortClean, bucketOf(youngTs),
		"collector-0", "cccc3333", 0, seeded, mkRows(youngTs))
	// A foreign object under the class prefix is never touched.
	foreignKey := parquetPrefix + "/" + model.RetentionShortClean + "/some-foreign-object.txt"
	store.putAt(foreignKey, []byte("not parquet"), seeded)

	stats, err := job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.TTLParquetDeleted)
	keys := store.keys()
	assert.NotContains(t, keys, expiredKey)
	assert.Contains(t, keys, boundaryKey, "an object at the TTL boundary is inside its TTL")
	assert.Contains(t, keys, youngKey)
	assert.Contains(t, keys, foreignKey)
}

// TestTTLPodsManifests pins the 01 §3.6 manifest expiry: the day in the key
// is the only clock, and aging counts from the day's end. The pods/v1 family
// is the only snapshot family left — the dictionary and suspend snapshots
// are gone since parquet rows became self-contained (№3, №23).
func TestTTLPodsManifests(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	job := NewJob(store, Config{PodsManifestTTL: 35 * 24 * time.Hour})
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	seeded := now.Add(-time.Hour)

	oldDay := now.AddDate(0, 0, -40).Format("2006/01/02")
	// 35 days back: the day ended 34.5 days ago, still inside the 35 d TTL.
	edgeDay := now.AddDate(0, 0, -35).Format("2006/01/02")
	youngDay := now.AddDate(0, 0, -1).Format("2006/01/02")
	oldKey := "pods/v1/" + oldDay + "/aaaa1111.json"
	keptKeys := []string{"pods/v1/" + edgeDay + "/bbbb2222.json", "pods/v1/" + youngDay + "/cccc3333.json"}
	for _, key := range append([]string{oldKey}, keptKeys...) {
		store.putAt(key, []byte("{}"), seeded)
	}
	foreign := "pods/v1/README.txt"
	store.putAt(foreign, []byte("x"), seeded)

	stats, err := job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.TTLManifestsDeleted)
	assert.NotContains(t, store.keys(), oldKey)
	for _, key := range keptKeys {
		assert.Contains(t, store.keys(), key)
	}
	assert.Contains(t, store.keys(), foreign)
}

// TestDefaultTTLsDeriveFromTierTable is the №10 guard on the TTL axis: the
// maintain defaults must BE the tier table, and the pods-manifest TTL must
// outlive the longest class TTL so a readable row never outlives the
// manifest naming its pod-restart.
func TestDefaultTTLsDeriveFromTierTable(t *testing.T) {
	cfg := Config{}.Normalize()
	assert.Equal(t, model.DefaultClassTTL(), cfg.ClassTTL)
	assert.Greater(t, cfg.PodsManifestTTL, model.MaxClassTTL())
}

// TestParseParquetKeyRejectsForeignNames pins the parser against the key
// shapes it must skip rather than misread.
func TestParseParquetKeyRejectsForeignNames(t *testing.T) {
	good := ObjectInfo{Key: sealStyleKey(model.RetentionNormalClean, testBucketStart,
		"collector-0", "aaaa1111", testBucketStart.UnixMilli(), testBucketStart.UnixMilli()+1000, 0), Size: 1}
	po, ok := parseParquetKey(good)
	require.True(t, ok)
	assert.Equal(t, "collector-0", po.replica, "a dashed replica parses from the right")
	assert.Equal(t, "aaaa1111", po.hash)
	assert.Equal(t, testBucketStart.UnixMilli(), po.bucketStartMs)

	for name, key := range map[string]string{
		"not parquet root": "other/v1/normal_clean/2026/07/01/12/collector-0-a-b-c-d-0.parquet",
		"unknown class":    "parquet/v1/weird_class/2026/07/01/12/collector-0-a-b-c-d-0.parquet",
		"no .parquet":      "parquet/v1/normal_clean/2026/07/01/12/collector-0-aaaa1111-x-y-z-0.txt",
		"too few parts":    "parquet/v1/normal_clean/2026/07/01/12/a-b-c.parquet",
		"bad stamps":       "parquet/v1/normal_clean/2026/07/01/12/collector-0-aaaa1111-not-a-stamp-0.parquet",
		"missing hour seg": "parquet/v1/normal_clean/2026/07/01/collector-0-aaaa1111-x-y-z-0.parquet",
		"non-numeric seq":  "parquet/v1/normal_clean/2026/07/01/12/collector-0-aaaa1111-20260701T120000Z-20260701T120000Z-20260701T120001Z-x.parquet",
	} {
		_, ok := parseParquetKey(ObjectInfo{Key: key, Size: 1})
		assert.False(t, ok, name)
	}
}

// TestInputsHashIsOrderIndependent pins the determinism two concurrent
// maintainers rely on for idempotent PUTs.
func TestInputsHashIsOrderIndependent(t *testing.T) {
	a := parquetObject{key: "parquet/v1/x/a"}
	b := parquetObject{key: "parquet/v1/x/b"}
	c := parquetObject{key: "parquet/v1/x/c"}
	assert.Equal(t, inputsHash([]parquetObject{a, b, c}), inputsHash([]parquetObject{c, a, b}))
	assert.NotEqual(t, inputsHash([]parquetObject{a, b}), inputsHash([]parquetObject{a, c}))
	assert.Len(t, inputsHash([]parquetObject{a}), 8, "same width as the pod-restart hash")
}
