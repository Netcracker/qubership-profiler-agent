//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/maintain"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
	parquetgo "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	maintainNs  = "maint-ns"
	maintainSvc = "maint-svc"
)

// maintainKeyStamp mirrors the 01 §7 second-precision key stamp.
const maintainKeyStamp = "20060102T150405Z"

// maintainRow builds one CallV2 row with every column family populated, so
// the test can prove the compaction rewrite preserves them all.
func maintainRow(pod string, restartMs, tsMs int64, off int32, class string) storageparquet.CallV2 {
	blob := []byte(fmt.Sprintf("blob-%s-%d-%d", pod, tsMs, off))
	big := fmt.Sprintf(`{"sql:0:%d":"select %d"}`, off, off)
	return storageparquet.CallV2{
		TsMs:           tsMs,
		PodId:          maintainNs + "/" + maintainSvc + "/" + pod,
		RestartTimeMs:  restartMs,
		TraceFileIndex: 1,
		BufferOffset:   off,
		RecordIndex:    0,
		ThreadName:     "exec-1",
		Namespace:      maintainNs,
		ServiceName:    maintainSvc,
		PodName:        pod,
		Method:         "com.example.Service.handle",
		DurationMs:     500,
		CpuTimeMs:      42,
		RetentionClass: class,
		Params:         storageparquet.Parameters{"request.id": {fmt.Sprintf("req-%d", off)}},
		TraceBlob:      blob,
		BigParamsJson:  &big,
	}
}

// seedMaintainObject writes rows as one seal-style S3 object (01 §7) the way
// a collector upload would land it: ZSTD, (ts_ms DESC, pk ASC) order, the
// schema-version stamp, true min/max ts in the key.
func seedMaintainObject(t *testing.T, ctx context.Context, store *maintain.S3ObjectStore,
	class string, bucketStart time.Time, replica, hash string, seq int, rows []storageparquet.CallV2) string {
	t.Helper()
	sort.SliceStable(rows, func(a, b int) bool {
		if rows[a].TsMs != rows[b].TsMs {
			return rows[a].TsMs > rows[b].TsMs
		}
		return maintainRowPK(&rows[a]).Compare(maintainRowPK(&rows[b])) < 0
	})
	var buf bytes.Buffer
	w := parquetgo.NewGenericWriter[storageparquet.CallV2](&buf, storageparquet.CallV2WriterOptions()...)
	_, err := w.Write(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	timeMin, timeMax := rows[0].TsMs, rows[0].TsMs
	for _, row := range rows {
		if row.TsMs < timeMin {
			timeMin = row.TsMs
		}
		if row.TsMs > timeMax {
			timeMax = row.TsMs
		}
	}
	name := fmt.Sprintf("%s-%s-%s-%s-%s-%d.parquet",
		replica, hash,
		bucketStart.UTC().Format(maintainKeyStamp),
		time.UnixMilli(timeMin).UTC().Format(maintainKeyStamp),
		time.UnixMilli(timeMax).UTC().Format(maintainKeyStamp),
		seq)
	key := path.Join("parquet/v1", class, bucketStart.UTC().Format("2006/01/02/15"), name)
	require.NoError(t, store.Put(ctx, key, buf.Bytes()))
	return key
}

func maintainRowPK(r *storageparquet.CallV2) model.PK {
	return model.PK{
		PodNamespace: r.Namespace, PodService: r.ServiceName, PodName: r.PodName,
		RestartTimeMs: r.RestartTimeMs, TraceFileIndex: r.TraceFileIndex,
		BufferOffset: r.BufferOffset, RecordIndex: r.RecordIndex,
	}
}

// fetchCallSet reads one /calls page without testify (safe off the test
// goroutine) and returns the row identities as "pod/restart/off@ts" strings.
func fetchCallSet(api *httptest.Server, params url.Values) (map[string]bool, error) {
	resp, err := http.Get(api.URL + "/api/v1/calls?" + params.Encode())
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /calls: status %d", resp.StatusCode)
	}
	var page callsPage
	if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, err
	}
	if page.Partial {
		return nil, fmt.Errorf("GET /calls: partial response: %v", page.PartialReasons)
	}
	set := map[string]bool{}
	for _, call := range page.Calls {
		set[fmt.Sprintf("%s/%d/%d@%d", call.PK.PodName, call.PK.RestartTimeMs, call.PK.BufferOffset, call.TsMs)] = true
	}
	return set, nil
}

func listKeys(t *testing.T, ctx context.Context, mc interface {
	List(context.Context, string) ([]maintain.ObjectInfo, error)
}, prefix string) []string {
	t.Helper()
	objects, err := mc.List(ctx, prefix)
	require.NoError(t, err)
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		keys = append(keys, obj.Key)
	}
	sort.Strings(keys)
	return keys
}

// TestMaintainMinio drives the Stage 1 maintain acceptance against a real
// MinIO (01-write-contract.md §6.4, §6.6):
//
//  1. a (bucket, class) seeded with small per-pod-restart files and a patch
//     file compacts into one object holding the identical PK set in
//     (ts_ms DESC, pk ASC) order, with every column preserved and the key
//     carrying the true floor/ceil time bounds;
//  2. /api/v1/calls answers the same rows through every phase of
//     write → grace → delete, with a reader running concurrently;
//  3. repeated passes over the converged bucket are no-ops;
//  4. per-class TTL removes only objects past their TTL, and pods manifests
//     past the pods-manifest TTL.
func TestMaintainMinio(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	mc := helpers.CreateMinioContainer(ctx)
	defer func() { _ = mc.Terminate(ctx) }()

	store := maintain.NewS3ObjectStore(mc.Client, "")
	now := time.Now()
	bucketStart := now.Add(-2 * time.Hour).UTC().Truncate(5 * time.Minute)
	base := bucketStart.UnixMilli()
	class := model.RetentionNormalClean
	classPrefix := "parquet/v1/" + class + "/"

	// Three pod-restarts' seal files plus a late patch of pod-a (01 §6.6).
	// pod-b's row shares its ts_ms with a pod-a row to exercise the pk ASC
	// tiebreak; the patch repeats one pod-a row verbatim — the §6.2
	// idempotent overlap PK-dedup must collapse.
	hashA := model.PodRestartHash(model.PodTuple{Namespace: maintainNs, Service: maintainSvc, Pod: "pod-a", RestartTimeMs: 1000})
	hashB := model.PodRestartHash(model.PodTuple{Namespace: maintainNs, Service: maintainSvc, Pod: "pod-b", RestartTimeMs: 2000})
	hashC := model.PodRestartHash(model.PodTuple{Namespace: maintainNs, Service: maintainSvc, Pod: "pod-c", RestartTimeMs: 3000})
	dup := maintainRow("pod-a", 1000, base+7_000, 200, class)
	expectedBlobs := map[string][]byte{}
	rowsSeeded := []storageparquet.CallV2{
		maintainRow("pod-a", 1000, base+5_000, 100, class),
		dup,
		maintainRow("pod-b", 2000, base+7_000, 50, class),
		maintainRow("pod-c", 3000, base+30_000, 10, class),
		maintainRow("pod-a", 1000, base+90_000, 300, class),
	}
	for _, row := range rowsSeeded {
		expectedBlobs[fmt.Sprintf("%s/%d", row.PodName, row.BufferOffset)] = row.TraceBlob
	}
	seedMaintainObject(t, ctx, store, class, bucketStart, "collector-0", hashA, 0,
		[]storageparquet.CallV2{rowsSeeded[0], rowsSeeded[1]})
	seedMaintainObject(t, ctx, store, class, bucketStart, "collector-1", hashB, 0,
		[]storageparquet.CallV2{rowsSeeded[2]})
	seedMaintainObject(t, ctx, store, class, bucketStart, "collector-0", hashC, 0,
		[]storageparquet.CallV2{rowsSeeded[3]})
	seedMaintainObject(t, ctx, store, class, bucketStart, "collector-0", hashA, 1,
		[]storageparquet.CallV2{dup, rowsSeeded[4]})
	require.Len(t, listKeys(t, ctx, store, classPrefix), 4)

	api := httptest.NewServer(query.New(query.Options{
		ColdStore: query.NewS3ObjectReader(mc.Client, ""),
	}).Handler())
	defer api.Close()
	window := url.Values{
		"from": {fmt.Sprint(base - 60_000)},
		"to":   {fmt.Sprint(base + 300_000)},
	}

	wantSet, err := fetchCallSet(api, window)
	require.NoError(t, err)
	require.Len(t, wantSet, 5, "5 distinct PKs before compaction (the planted duplicate collapses)")

	// The §6.6 reader-safety acceptance: hammer /calls concurrently with
	// every compaction phase; each response must carry the full row set.
	readerErr := make(chan error, 1)
	readerDone := make(chan struct{})
	readerStop := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-readerStop:
				return
			default:
			}
			got, err := fetchCallSet(api, window)
			if err == nil && len(got) != len(wantSet) {
				err = fmt.Errorf("row set changed mid-compaction: got %d rows, want %d", len(got), len(wantSet))
			}
			if err == nil {
				for key := range wantSet {
					if !got[key] {
						err = fmt.Errorf("row %s lost mid-compaction", key)
						break
					}
				}
			}
			if err != nil {
				select {
				case readerErr <- err:
				default:
				}
				return
			}
		}
	}()

	job := maintain.NewJob(store, maintain.Config{
		TimeBucket:  5 * time.Minute,
		MinAge:      30 * time.Minute,
		MinFiles:    3,
		DeleteGrace: 2 * time.Minute,
	})

	// Pass 1: write the compacted object; the inputs must survive the grace.
	stats, err := job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.CompactedGroups)
	assert.Equal(t, 4, stats.CompactedInputFiles)
	assert.Equal(t, 5, stats.CompactedRows)
	assert.Equal(t, 1, stats.DedupedRows)
	keys := listKeys(t, ctx, store, classPrefix)
	require.Len(t, keys, 5, "compacted object next to its inputs during the grace")

	var outKey string
	for _, key := range keys {
		if strings.Contains(key, "/maintain-") {
			outKey = key
		}
	}
	require.NotEmpty(t, outKey)

	// Pass 2, inside the grace: nothing changes.
	stats, err = job.Pass(ctx, now)
	require.NoError(t, err)
	assert.Equal(t, 1, stats.PendingDeleteGroups)
	assert.Zero(t, stats.DeletedInputFiles)
	assert.Equal(t, keys, listKeys(t, ctx, store, classPrefix))

	got, err := fetchCallSet(api, window)
	require.NoError(t, err)
	assert.Equal(t, wantSet, got, "/calls parity while both copies are visible")

	// Pass 3, grace elapsed (simulated future against MinIO's real
	// LastModified): the inputs go, the compacted object alone answers.
	stats, err = job.Pass(ctx, time.Now().Add(3*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 4, stats.DeletedInputFiles)
	assert.Equal(t, []string{outKey}, listKeys(t, ctx, store, classPrefix))

	got, err = fetchCallSet(api, window)
	require.NoError(t, err)
	assert.Equal(t, wantSet, got, "/calls parity after the inputs are deleted")

	// Point fetch (the /tree, /trace path) once the row lives only in the
	// compacted object. Its key is `maintain-<hashOfInputs>-…`, so its hash
	// is not pod-a's pod-restart hash; FetchCall must treat the reserved
	// `maintain` replica as a candidate for every PK (01 §6.6, §7) or the
	// point endpoints 404 a call the compaction absorbed.
	coldStore := query.NewS3ObjectReader(mc.Client, "")
	coldSource := &cold.Source{Store: coldStore}
	target := maintainRow("pod-a", 1000, base+90_000, 300, class) // only in the compacted object now
	targetPK := maintainRowPK(&target)
	pointQuery := model.CallsQuery{
		FromMs:           target.TsMs,
		ToMs:             target.TsMs + 1,
		RetentionClasses: []string{class},
	}
	disc, err := coldSource.Discover(ctx, pointQuery)
	require.NoError(t, err)
	require.Zero(t, disc.FailedPrefixes)
	require.Len(t, disc.Files, 1, "only the compacted object overlaps the point window")
	require.Equal(t, cold.MaintainReplica, disc.Files[0].Replica, "the surviving object is a maintain compaction")
	row, ok, err := cold.FetchCall(ctx, coldStore, disc.Files, targetPK)
	require.NoError(t, err)
	require.True(t, ok, "point fetch must find a PK that now lives only in the compacted object")
	assert.Equal(t, target.TraceBlob, row.TraceBlob, "the compacted row carries its trace_blob")

	// Pass 4: the converged bucket is a no-op.
	stats, err = job.Pass(ctx, time.Now().Add(3*time.Minute))
	require.NoError(t, err)
	assert.Equal(t, maintain.Stats{}, stats)
	assert.Equal(t, []string{outKey}, listKeys(t, ctx, store, classPrefix))

	close(readerStop)
	<-readerDone
	select {
	case err := <-readerErr:
		t.Fatalf("concurrent reader: %v", err)
	default:
	}

	// The compacted object holds the read invariants of 01 §5.2 / §7.
	ref, ok := cold.ParseKey(outKey, 1)
	require.True(t, ok, "discovery must parse the compacted key")
	assert.Equal(t, class, ref.Class)
	assert.Equal(t, base+5_000, ref.TimeMinMs, "timeMin floors to the earliest row's second")
	assert.Equal(t, base+90_000+999, ref.TimeMaxMs, "timeMax covers the newest row's whole second")

	outPath := filepath.Join(t.TempDir(), "compacted.parquet")
	require.NoError(t, os.WriteFile(outPath, getObjectBytes(t, ctx, mc.Client, outKey), 0o600))
	assertZstd(t, outPath) // ZSTD everywhere + the schema-version stamp
	outRows := readCallV2(t, outPath)
	require.Len(t, outRows, 5)
	for i := 1; i < len(outRows); i++ {
		prev, cur := &outRows[i-1], &outRows[i]
		if prev.TsMs != cur.TsMs {
			assert.Greater(t, prev.TsMs, cur.TsMs, "ts_ms DESC")
		} else {
			assert.Negative(t, maintainRowPK(prev).Compare(maintainRowPK(cur)), "pk ASC within a ts_ms")
		}
	}
	for _, row := range outRows {
		assert.Equal(t, expectedBlobs[fmt.Sprintf("%s/%d", row.PodName, row.BufferOffset)], row.TraceBlob,
			"trace_blob survives the rewrite byte-identical")
		require.NotNil(t, row.BigParamsJson)
		assert.Contains(t, *row.BigParamsJson, "sql:0:")
		assert.Equal(t, class, row.RetentionClass)
		assert.Len(t, row.Params["request.id"], 1)
	}

	t.Run("per-class TTL and pods-manifest TTL", func(t *testing.T) {
		ttlNow := time.Now()
		// short_clean has a 2 d TTL (the tier table, №10): an object 3 days
		// old expires, one 1 hour old stays. The compacted normal_clean
		// object (7 d TTL) must stay.
		expiredTs := ttlNow.Add(-3 * 24 * time.Hour).UnixMilli()
		expiredBucket := time.UnixMilli(expiredTs).UTC().Truncate(5 * time.Minute)
		expiredKey := seedMaintainObject(t, ctx, store, model.RetentionShortClean, expiredBucket,
			"collector-0", "aaaa1111", 0,
			[]storageparquet.CallV2{maintainRow("pod-t", 1000, expiredTs, 1, model.RetentionShortClean)})
		youngTs := ttlNow.Add(-time.Hour).UnixMilli()
		youngBucket := time.UnixMilli(youngTs).UTC().Truncate(5 * time.Minute)
		youngKey := seedMaintainObject(t, ctx, store, model.RetentionShortClean, youngBucket,
			"collector-0", "bbbb2222", 0,
			[]storageparquet.CallV2{maintainRow("pod-t", 1000, youngTs, 2, model.RetentionShortClean)})

		// The pods/v1 manifests are the only snapshot family left (№3/№23).
		oldDay := ttlNow.AddDate(0, 0, -200).UTC().Format("2006/01/02")
		youngDay := ttlNow.AddDate(0, 0, -1).UTC().Format("2006/01/02")
		expiredManifest := "pods/v1/" + oldDay + "/aaaa1111.json"
		youngManifest := "pods/v1/" + youngDay + "/bbbb2222.json"
		require.NoError(t, store.Put(ctx, expiredManifest, []byte("{}")))
		require.NoError(t, store.Put(ctx, youngManifest, []byte("{}")))

		stats, err := job.Pass(ctx, ttlNow)
		require.NoError(t, err)
		assert.Equal(t, 1, stats.TTLParquetDeleted)
		assert.Equal(t, 1, stats.TTLManifestsDeleted)

		shortKeys := listKeys(t, ctx, store, "parquet/v1/"+model.RetentionShortClean+"/")
		assert.NotContains(t, shortKeys, expiredKey)
		assert.Contains(t, shortKeys, youngKey, "an object inside its TTL is never deleted")
		assert.Contains(t, listKeys(t, ctx, store, classPrefix), outKey)
		assert.Empty(t, listKeys(t, ctx, store, expiredManifest), "expired manifest %s", expiredManifest)
		assert.Len(t, listKeys(t, ctx, store, youngManifest), 1, "young manifest %s", youngManifest)
	})
}
