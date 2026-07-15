package query

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// memColdStore is a minimal in-memory cold.ObjectStore for the /tree handler
// tests: it serves the planted parquet objects and records every LIST prefix
// so a test can prove which retention classes discovery scanned. Snapshot Gets
// (dictionary, suspend) that were never planted return ErrNotFound, so the
// tree degrades to placeholder method names and zero suspension — exactly the
// missing-snapshot path (02 §2.5, cold.Dictionary / cold.Suspend).
type memColdStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	lists   []string
}

func newMemColdStore() *memColdStore {
	return &memColdStore{objects: map[string][]byte{}}
}

func (m *memColdStore) put(key string, body []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), body...)
}

// listedClasses returns the distinct retention classes discovery LISTed, from
// the parquet/v1/<class>/... prefix of each recorded LIST.
func (m *memColdStore) listedClasses() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]struct{}{}
	for _, prefix := range m.lists {
		segs := strings.Split(prefix, "/")
		if len(segs) >= 3 && segs[0] == "parquet" && segs[1] == "v1" {
			seen[segs[2]] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func (m *memColdStore) List(_ context.Context, prefix string) ([]cold.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lists = append(m.lists, prefix)
	var out []cold.ObjectInfo
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			out = append(out, cold.ObjectInfo{Key: key, Size: int64(len(data))})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func (m *memColdStore) Open(_ context.Context, key string) (cold.Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, cold.ErrNotFound
	}
	return memObject{data: data}, nil
}

func (m *memColdStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, cold.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

type memObject struct{ data []byte }

func (o memObject) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(o.data)) {
		return 0, io.EOF
	}
	n := copy(p, o.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (o memObject) Close() error { return nil }
func (o memObject) Size() int64  { return int64(len(o.data)) }

// writeCallParquet renders one CallV2 row into a ZSTD parquet file with the
// schema-version footer, matching the seal writer (libs/collector/hotstore
// seal.go). The bytes are what the cold read path opens.
func writeCallParquet(t *testing.T, row storageparquet.CallV2) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[storageparquet.CallV2](&buf,
		parquet.Compression(&parquet.Zstd),
		parquet.KeyValueMetadata(storageparquet.SchemaVersionKey, storageparquet.SchemaVersion),
	)
	_, err := w.Write([]storageparquet.CallV2{row})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// sealedKey builds the 01 §7 object key for one class, mirroring the seal
// writer's baseName format so ParseKey recovers the hash and time bounds.
func sealedKey(class string, tuple model.PodTuple, tsMs int64) string {
	const stamp = "20060102T150405Z"
	ts := time.UnixMilli(tsMs).UTC()
	bucketStart := ts.Truncate(5 * time.Minute)
	name := strings.Join([]string{
		"collector-0",
		model.PodRestartHash(tuple),
		bucketStart.Format(stamp),
		ts.Format(stamp),
		ts.Format(stamp),
		"0",
	}, "-") + ".parquet"
	return path.Join("parquet/v1", class, bucketStart.Format("2006/01/02/15"), name)
}

// TestTreeResolvesUnderReclassifiedClass is the №16 regression: seal can
// reclassify a call after the UI baked the old retention_class into the /tree
// URL (a late call.red registration bumps the class). The hint then points at
// a prefix that holds nothing, so a bookmarked link must not 404 forever — the
// point fetch retries discovery across every class before giving up (09 §5:
// the class hint is optional and only sharpens pruning).
func TestTreeResolvesUnderReclassifiedClass(t *testing.T) {
	const restartMs = int64(1_700_000_000_000)
	tuple := model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod", RestartTimeMs: restartMs}
	pk := model.PK{
		PodNamespace: "ns", PodService: "svc", PodName: "pod", RestartTimeMs: restartMs,
		TraceFileIndex: 1, BufferOffset: 100, RecordIndex: 0,
	}
	tsMs := restartMs + 42

	// A minimal, valid per-call trace blob: one method entered at +5 and
	// exited at +15 (10 ms). calltree.Build accepts it; without a dictionary
	// snapshot the method renders as a placeholder, which is fine — the test
	// asserts the tree structure, not its names.
	blob, _ := wire.TraceStream(restartMs, []wire.TraceChunk{
		{ThreadId: 7, StartMs: restartMs, Events: []wire.TraceEvent{
			wire.Enter(5, 1), wire.Exit(10),
		}},
	})

	// The row was SEALED under long_clean (the call ran long), but the UI
	// still holds the short_clean hint it captured before the reclassification.
	const sealedClass = model.RetentionLongClean
	const staleHint = model.RetentionShortClean
	row := storageparquet.CallV2{
		TsMs:           tsMs,
		PodId:          tuple.Namespace + "/" + tuple.Service + "/" + tuple.Pod,
		RestartTimeMs:  restartMs,
		TraceFileIndex: pk.TraceFileIndex,
		BufferOffset:   pk.BufferOffset,
		RecordIndex:    pk.RecordIndex,
		Namespace:      "ns", ServiceName: "svc", PodName: "pod",
		Method:         "com.example.Service.handle",
		DurationMs:     10,
		RetentionClass: sealedClass,
		TraceBlob:      blob,
	}

	store := newMemColdStore()
	store.put(sealedKey(sealedClass, tuple, tsMs), writeCallParquet(t, row))

	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()

	treeURL := api.URL + "/api/v1/calls/" + url.PathEscape(pk.PathString()) + "/tree"

	t.Run("stale class hint still resolves via the all-class retry", func(t *testing.T) {
		resp, err := http.Get(treeURL + "?" + url.Values{
			"ts_ms":           {strconv.FormatInt(tsMs, 10)},
			"retention_class": {staleHint},
		}.Encode())
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
		assert.Equal(t, "application/x-msgpack", resp.Header.Get("Content-Type"))

		tree, version, err := calltree.Decode(body)
		require.NoError(t, err)
		assert.EqualValues(t, calltree.Version, version)
		require.NotNil(t, tree.Root)
		assert.EqualValues(t, 10, tree.Root.DurationMs, "the sealed call's duration survives the retry")

		// Discovery scanned the hinted class, then widened to every class (the
		// retry) — including the class the row actually sealed under.
		listed := store.listedClasses()
		assert.Contains(t, listed, staleHint, "the hinted class is listed")
		assert.Contains(t, listed, sealedClass, "the retry widens to the sealed class")
	})

	t.Run("no hint at all still resolves via a single all-class scan", func(t *testing.T) {
		resp, err := http.Get(treeURL + "?" + url.Values{"ts_ms": {strconv.FormatInt(tsMs, 10)}}.Encode())
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	})
}
