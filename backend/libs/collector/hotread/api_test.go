package hotread_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNs  = "ns"
	testSvc = "svc"
	baseMs  = int64(1_700_000_000_000)
)

// openTestStore builds a hot store fed through the write-path API directly
// (no TCP): enough to exercise the read side.
func openTestStore(t *testing.T) *hotstore.Store {
	t.Helper()
	store, err := hotstore.Open(hotstore.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func addPod(t *testing.T, store *hotstore.Store, pod string, restartMs int64, words ...string) *hotstore.PodRestart {
	t.Helper()
	pr, err := store.OpenPodRestart(hotstore.PodRestartKey{
		Namespace: testNs, Service: testSvc, PodName: pod, RestartTimeMs: restartMs,
	})
	require.NoError(t, err)
	for _, w := range words {
		_, err := pr.AppendDictionaryWord(w)
		require.NoError(t, err)
	}
	return pr
}

func addCall(t *testing.T, pr *hotstore.PodRestart, tsMs int64, call data.Call) {
	t.Helper()
	require.NoError(t, pr.AppendCall(tsMs, call))
}

type page struct {
	Calls []struct {
		PK struct {
			PodName        string `json:"pod_name"`
			RestartTimeMs  int64  `json:"restart_time_ms"`
			TraceFileIndex int32  `json:"trace_file_index"`
		} `json:"pk"`
		TsMs           int64               `json:"ts_ms"`
		Method         string              `json:"method"`
		DurationMs     int32               `json:"duration_ms"`
		ErrorFlag      bool                `json:"error_flag"`
		RetentionClass string              `json:"retention_class"`
		Params         map[string][]string `json:"params"`
		TraceBlobSize  *int64              `json:"trace_blob_size"`
	} `json:"calls"`
	NextCursor *string `json:"next_cursor"`
	Partial    bool    `json:"partial"`
}

func getJSON(t *testing.T, srv *httptest.Server, path string, params url.Values, into any) int {
	t.Helper()
	u := srv.URL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	if into != nil && resp.StatusCode == http.StatusOK {
		require.NoError(t, json.Unmarshal(body, into), "body: %s", body)
	}
	return resp.StatusCode
}

// TestInternalCallsOrder pins the §2.3.1 trap: the (ts_ms DESC, pk ASC) order
// must apply the component-wise binary collation, which the scalar
// pod_restart string does NOT deliver — a pod name that prefixes another
// compares through the '/' separator ('/' > '-'), and restart_time_ms as text
// puts 1000 before 999. Both cases would return the wrong order from a naive
// ORDER BY pod_restart.
func TestInternalCallsOrder(t *testing.T) {
	store := openTestStore(t)
	// Three pod-restarts whose string keys collate differently from their PKs:
	// "ns/svc/a-b/1000" < "ns/svc/a/1000" < "ns/svc/a/999" as strings, but the
	// PK order is a@999 < a@1000 < a-b@1000.
	for _, p := range []struct {
		pod     string
		restart int64
	}{{"a", 999}, {"a", 1000}, {"a-b", 1000}} {
		pr := addPod(t, store, p.pod, p.restart, "com.example.M")
		addCall(t, pr, baseMs, data.Call{
			Method: 0, Duration: 10, ThreadName: "t", TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
		})
	}
	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)

	window := url.Values{"from": {fmt.Sprint(baseMs - 1000)}, "to": {fmt.Sprint(baseMs + 1000)}}
	var got page
	require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls", window, &got))
	require.Len(t, got.Calls, 3)

	type ident struct {
		pod     string
		restart int64
	}
	var order []ident
	for _, call := range got.Calls {
		order = append(order, ident{call.PK.PodName, call.PK.RestartTimeMs})
	}
	assert.Equal(t, []ident{{"a", 999}, {"a", 1000}, {"a-b", 1000}}, order,
		"a ts tie must break by the component-wise PK collation, not the pod_restart string")
	assert.Nil(t, got.Calls[0].TraceBlobSize, "the hot index does not know the blob size")

	t.Run("keyset seek continues mid-tie", func(t *testing.T) {
		params := url.Values{
			"from": window["from"], "to": window["to"],
			"after_ts_ms": {fmt.Sprint(baseMs)},
			"after_pk":    {fmt.Sprintf("%s:%s:a:999:1:0:0", testNs, testSvc)},
		}
		var got page
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls", params, &got))
		require.Len(t, got.Calls, 2, "the seek passes exactly the cursor row")
		assert.Equal(t, []ident{{"a", 1000}, {"a-b", 1000}},
			[]ident{{got.Calls[0].PK.PodName, got.Calls[0].PK.RestartTimeMs},
				{got.Calls[1].PK.PodName, got.Calls[1].PK.RestartTimeMs}})
	})

	t.Run("limit truncates in order", func(t *testing.T) {
		params := url.Values{"from": window["from"], "to": window["to"], "limit": {"1"}}
		var got page
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls", params, &got))
		require.Len(t, got.Calls, 1)
		assert.Equal(t, "a", got.Calls[0].PK.PodName)
		assert.EqualValues(t, 999, got.Calls[0].PK.RestartTimeMs)
	})

	t.Run("lone after_pk is rejected", func(t *testing.T) {
		params := url.Values{"from": window["from"], "to": window["to"],
			"after_pk": {fmt.Sprintf("%s:%s:a:999:1:0:0", testNs, testSvc)}}
		assert.Equal(t, http.StatusBadRequest, getJSON(t, srv, "/internal/v1/calls", params, nil))
	})
}

// TestInternalCallsBuckets covers the partition walk: rows spread over
// several 5-minute buckets come back newest-first, filters apply, and the
// hot-window report tracks the oldest indexed ts.
func TestInternalCallsBuckets(t *testing.T) {
	store := openTestStore(t)
	pr := addPod(t, store, "pod-1", 1, "com.example.Service.handle", "com.example.Db.query")
	addCall(t, pr, baseMs+5, data.Call{Method: 0, Duration: 10, ThreadName: "exec-1",
		TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0})
	addCall(t, pr, baseMs+6*60_000, data.Call{Method: 1, Duration: 500, ThreadName: "exec-2",
		TraceFileIndex: 1, BufferOffset: 100, RecordIndex: 0})
	addCall(t, pr, baseMs+12*60_000, data.Call{Method: 0, Duration: 2000, ThreadName: "exec-3",
		TraceFileIndex: 1, BufferOffset: 200, RecordIndex: 0})
	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)
	window := url.Values{"from": {fmt.Sprint(baseMs)}, "to": {fmt.Sprint(baseMs + 30*60_000)}}

	var got page
	require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls", window, &got))
	require.Len(t, got.Calls, 3)
	assert.Equal(t, []int64{baseMs + 12*60_000, baseMs + 6*60_000, baseMs + 5},
		[]int64{got.Calls[0].TsMs, got.Calls[1].TsMs, got.Calls[2].TsMs},
		"partitions concatenate newest-first")
	assert.Equal(t, "com.example.Service.handle", got.Calls[2].Method,
		"method ids resolve against the pod-restart dictionary")

	t.Run("filters", func(t *testing.T) {
		byMethod := url.Values{"from": window["from"], "to": window["to"], "method": {"Db.query"}}
		var got page
		getJSON(t, srv, "/internal/v1/calls", byMethod, &got)
		require.Len(t, got.Calls, 1)
		assert.Equal(t, "com.example.Db.query", got.Calls[0].Method)

		byDuration := url.Values{"from": window["from"], "to": window["to"], "duration_min_ms": {"1000"}}
		getJSON(t, srv, "/internal/v1/calls", byDuration, &got)
		require.Len(t, got.Calls, 1)
		assert.EqualValues(t, 2000, got.Calls[0].DurationMs)

		byClass := url.Values{"from": window["from"], "to": window["to"], "retention_class": {"normal_clean"}}
		getJSON(t, srv, "/internal/v1/calls", byClass, &got)
		require.Len(t, got.Calls, 1)
		assert.Equal(t, "normal_clean", got.Calls[0].RetentionClass)
	})

	t.Run("hot-window reports the oldest indexed ts", func(t *testing.T) {
		var w struct {
			OldestMs int64 `json:"hot_window_oldest_ms"`
			NowMs    int64 `json:"hot_window_now_ms"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/health/hot-window", nil, &w))
		assert.Equal(t, baseMs+5, w.OldestMs)
		assert.Greater(t, w.NowMs, baseMs)
	})

	t.Run("single-call fetch", func(t *testing.T) {
		pkPath := fmt.Sprintf("%s:%s:pod-1:1:1:100:0", testNs, testSvc)
		var call struct {
			TsMs   int64  `json:"ts_ms"`
			Method string `json:"method"`
		}
		require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/calls/"+pkPath, nil, &call))
		assert.Equal(t, baseMs+6*60_000, call.TsMs)
		assert.Equal(t, "com.example.Db.query", call.Method)

		missing := fmt.Sprintf("%s:%s:pod-1:1:9:9:9", testNs, testSvc)
		assert.Equal(t, http.StatusNotFound, getJSON(t, srv, "/internal/v1/calls/"+missing, nil, nil))
	})

	t.Run("trace of a pointer with no chunks is 404", func(t *testing.T) {
		pkPath := fmt.Sprintf("%s:%s:pod-1:1:1:100:0", testNs, testSvc)
		assert.Equal(t, http.StatusNotFound, getJSON(t, srv, "/internal/v1/calls/"+pkPath+"/trace", nil, nil),
			"an absent blob is a state, not an error (02 §2.4)")
	})
}

// TestInternalEmptyWindow pins the empty-replica report: oldest == now, so
// the query-side cutoff keeps the cold tier covering the whole range.
func TestInternalEmptyWindow(t *testing.T) {
	store := openTestStore(t)
	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)
	var w struct {
		OldestMs int64 `json:"hot_window_oldest_ms"`
		NowMs    int64 `json:"hot_window_now_ms"`
	}
	require.Equal(t, http.StatusOK, getJSON(t, srv, "/internal/v1/health/hot-window", nil, &w))
	assert.Equal(t, w.NowMs, w.OldestMs, "an empty index reports an empty window")
}

// TestInternalDictionary covers §2.6 over a live pod-restart: the full word
// list in both arrays, version = word count, and ETag revalidation.
func TestInternalDictionary(t *testing.T) {
	store := openTestStore(t)
	addPod(t, store, "pod-1", 7, "m.a", "m.b", "call.red")
	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)

	path := srv.URL + fmt.Sprintf("/internal/v1/pods/%s:%s:pod-1:7/dictionary", testNs, testSvc)
	resp, err := http.Get(path)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var snap struct {
		Version int      `json:"version"`
		Methods []string `json:"methods"`
		Params  []string `json:"params"`
	}
	require.NoError(t, json.Unmarshal(body, &snap))
	assert.Equal(t, 3, snap.Version)
	assert.Equal(t, []string{"m.a", "m.b", "call.red"}, snap.Methods)
	assert.Equal(t, snap.Methods, snap.Params, "one id space: both arrays carry the full list (01 §3.6)")
	etag := resp.Header.Get("ETag")
	require.NotEmpty(t, etag)

	req, err := http.NewRequest(http.MethodGet, path, nil)
	require.NoError(t, err)
	req.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp2.Body.Close()
	assert.Equal(t, http.StatusNotModified, resp2.StatusCode)

	missing := srv.URL + fmt.Sprintf("/internal/v1/pods/%s:%s:pod-x:7/dictionary", testNs, testSvc)
	respMissing, err := http.Get(missing)
	require.NoError(t, err)
	_ = respMissing.Body.Close()
	assert.Equal(t, http.StatusNotFound, respMissing.StatusCode)
}

// TestInternalValues drives the big-parameter values endpoint the query
// service's hot /tree path batches its references through (01 §4.4).
func TestInternalValues(t *testing.T) {
	store := openTestStore(t)
	pr := addPod(t, store, "pod-1", 7)
	sqlData, sqlOffs := wire.ValueStream([]string{"SELECT 1", "SELECT 2"})
	seg, err := pr.OpenSegment(hotstore.StreamSql, 1)
	require.NoError(t, err)
	_, err = seg.Write(sqlData)
	require.NoError(t, err)

	srv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(srv.Close)
	base := srv.URL + fmt.Sprintf("/internal/v1/pods/%s:%s:pod-1:7/values", testNs, testSvc)

	var body struct {
		Values map[string]string `json:"values"`
	}
	params := url.Values{"ref": {
		fmt.Sprintf("sql:1:%d", sqlOffs[1]),
		"sql:9:0", // a segment this replica never had: absent, not an error
	}}
	resp, err := http.Get(base + "?" + params.Encode())
	require.NoError(t, err)
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", raw)
	require.NoError(t, json.Unmarshal(raw, &body))
	assert.Equal(t, map[string]string{fmt.Sprintf("sql:1:%d", sqlOffs[1]): "SELECT 2"}, body.Values)

	for path, want := range map[string]int{
		base:                    http.StatusBadRequest, // no refs
		base + "?ref=trace:1:0": http.StatusBadRequest, // not a value stream
		srv.URL + fmt.Sprintf("/internal/v1/pods/%s:%s:pod-x:7/values?ref=sql:1:0", testNs, testSvc): http.StatusNotFound,
	} {
		resp, err := http.Get(path)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, want, resp.StatusCode, path)
	}
}
