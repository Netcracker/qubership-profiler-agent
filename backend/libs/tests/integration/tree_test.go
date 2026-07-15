package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	treePodHot  = "pod-tree-hot"
	treePodCold = "pod-tree-cold"

	// The tree fixture extends the seal dictionary with the big-param key
	// words; ids follow arrival order (sealDictWords covers 0..4).
	treeDictSql = 5
	treeDictXml = 6
)

var treeDictWords = append(append([]string{}, sealDictWords...), "sql", "xml")

// TestTreeAndTraceAPI drives the Stage 1 slice-6 acceptance
// (02-read-contract.md §2.4, §2.5): the external /calls/{pk}/tree and
// /calls/{pk}/trace over both tiers. A cold pod's calls — one of them behind
// tail noise (record_index > 0) and carrying sql / xml big-parameter
// references — are sealed and uploaded, and their collector disappears; a hot
// pod's calls stay un-sealed on a live replica. The tree endpoint must
// resolve method and param names, inline big-parameter values (from the
// replica's value segments on hot, from the sealed big_params_json on cold),
// stay self-contained, and honour the §2.5.2-§2.5.5 envelope (msgpack int
// keys, version, ETag, gzip).
func TestTreeAndTraceAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	fake := newColdFakeStore()

	coldSqlValue := "SELECT c FROM t WHERE id = ?"
	coldXmlValue := `<payload size="big"/>`
	hotSqlValue := "SELECT h FROM hot"

	// --- Phase 1: the cold pod on a collector that then disappears.
	ctxB, cancelB := context.WithCancel(ctx)
	svcB := startCollector(t, ctxB, t.TempDir())
	storeB := svcB.Store()
	bucketOld := storeB.Config().Bucket(baseMs + 5)

	sqlColdData, sqlColdOffs := wire.ValueStream([]string{coldSqlValue})
	xmlColdData, xmlColdOffs := wire.ValueStream([]string{coldXmlValue})
	// One chunk, two root calls: C1 = [idx 0..1] is the tail noise in front of
	// C2 = [idx 2..10], whose record_index is 2 (01 §4.5).
	fileC, offC := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 71, StartMs: baseMs + 40, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodQuery), wire.Exit(1), // C1
			wire.Enter(5, sealMethodHandle), // C2 root: enter at timer+5
			wire.Tag(1, sealDictRequestId, "req-cold"),
			wire.Enter(2, sealMethodQuery), // child 1: enter at +8
			wire.BigTag(1, treeDictSql, true, 1, int(sqlColdOffs[0])),
			wire.Exit(4),                     // child 1 exits at +13
			wire.Enter(1, sealMethodProcess), // child 2: enter at +14
			wire.BigTag(0, treeDictXml, false, 1, int(xmlColdOffs[0])),
			wire.Exit(3), // child 2 exits at +17
			wire.Exit(7), // C2 exits at +24
		}},
	})
	callsC := []wire.CallRecord{
		{DeltaMs: 40, Method: sealMethodQuery, DurationMs: 1, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(offC[0]), RecordIndex: 0},
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 19, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(offC[0]), RecordIndex: 2},
	}

	acC := connectAgentAs(t, ctx, treePodCold)
	keyC := waitForPodNamed(t, storeB, treePodCold, 1)
	prC, ok := storeB.PodRestart(keyC)
	require.True(t, ok)
	sendStream(t, acC, model.StreamDictionary, 0, wire.DictionaryStream(treeDictWords))
	// The R7 timeline against C2's windows (root [5, 24), q [8, 13),
	// p [14, 17)): [6, 8) is root self time, [9, 11) falls inside q,
	// [15, 16) inside p. DeltaMs is the delta to the pause END, so a pause spans
	// [end−amount, end] (№4): ends 8, 11, 16. The snapshot rides to S3 with the
	// dictionary.
	sendStream(t, acC, model.StreamSuspend, 0, wire.SuspendStream(timerStartMs, []wire.SuspendEvent{
		{DeltaMs: 8, AmountMs: 2}, {DeltaMs: 3, AmountMs: 2}, {DeltaMs: 5, AmountMs: 1},
	}))
	sendStream(t, acC, model.StreamSql, 0, sqlColdData)
	sendStream(t, acC, model.StreamXml, 0, xmlColdData)
	sendStream(t, acC, model.StreamTrace, 0, fileC)
	sendStream(t, acC, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callsC))
	waitForIndexedCalls(t, storeB, bucketOld, keyC, 2)
	require.Eventually(t, func() bool { return len(prC.SuspendPauses()) == 3 },
		5*time.Second, 10*time.Millisecond, "the pauses must reach suspend.wal before the close")
	require.NoError(t, acC.CommandClose())
	_ = acC.Close()
	require.Eventually(t, prC.Finalized, 5*time.Second, 10*time.Millisecond)

	_, err := storeB.Seal(ctx, keyC, bucketOld)
	require.NoError(t, err)
	_, err = hotstore.NewUploader(storeB, fake).Pass(ctx)
	require.NoError(t, err)
	cancelB()
	waitForCollectorStop(t, svcB)

	// --- Phase 2: the hot pod on a live collector, never sealed.
	svcA := startCollector(t, ctx, t.TempDir())
	storeA := svcA.Store()

	sqlHotData, sqlHotOffs := wire.ValueStream([]string{hotSqlValue})
	fileH, offH := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 81, StartMs: baseMs + 5, Events: []wire.TraceEvent{
			wire.Enter(5, sealMethodHandle), // root: enter at timer+5
			wire.Tag(1, sealDictRequestId, "req-hot"),
			wire.BigTag(0, treeDictXml, false, 7, 0), // segment 7 never exists: unresolved
			wire.Enter(2, sealMethodQuery),           // child: enter at +8
			wire.BigTag(1, treeDictSql, true, 1, int(sqlHotOffs[0])),
			wire.Exit(4), // child exits at +13
			wire.Exit(7), // root exits at +20
		}},
	})
	callsH := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 15, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(offH[0]), RecordIndex: 0},
	}

	acH := connectAgentAs(t, ctx, treePodHot)
	keyH := waitForPodNamed(t, storeA, treePodHot, 1)
	prH, ok := storeA.PodRestart(keyH)
	require.True(t, ok)
	sendStream(t, acH, model.StreamDictionary, 0, wire.DictionaryStream(treeDictWords))
	// The R7 timeline against the hot windows (root [5, 20), q [8, 13)):
	// [6, 8) is root self time, [9, 10) falls inside q. DeltaMs is the delta to
	// the pause END, so a pause spans [end−amount, end] (№4): end 8 → [6, 8),
	// end 10 → [9, 10).
	sendStream(t, acH, model.StreamSuspend, 0, wire.SuspendStream(timerStartMs, []wire.SuspendEvent{
		{DeltaMs: 8, AmountMs: 2}, {DeltaMs: 2, AmountMs: 1},
	}))
	sendStream(t, acH, model.StreamSql, 0, sqlHotData)
	sendStream(t, acH, model.StreamTrace, 0, fileH)
	sendStream(t, acH, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, callsH))
	waitForIndexedCalls(t, storeA, bucketOld, keyH, 1)
	require.Eventually(t, func() bool { return len(prH.SuspendPauses()) == 2 },
		5*time.Second, 10*time.Millisecond, "the pauses must land before /tree is queried")

	hotSrv := httptest.NewServer(hotread.New(storeA).Handler())
	t.Cleanup(hotSrv.Close)
	disco := &scriptedDiscovery{}
	disco.set(hotSrv.URL)
	api := httptest.NewServer(query.New(query.Options{
		Config:       query.Config{WideRangeLimit: 30 * 24 * time.Hour},
		ColdStore:    fake,
		HotDiscovery: disco,
	}).Handler())
	t.Cleanup(api.Close)

	pkHot := fmt.Sprintf("%s:%s:%s:%d:1:%d:0", hotstoreNs, hotstoreSvc, treePodHot, keyH.RestartTimeMs, offH[0])
	pkCold := fmt.Sprintf("%s:%s:%s:%d:1:%d:2", hotstoreNs, hotstoreSvc, treePodCold, keyC.RestartTimeMs, offC[0])
	pkColdNoise := fmt.Sprintf("%s:%s:%s:%d:1:%d:0", hotstoreNs, hotstoreSvc, treePodCold, keyC.RestartTimeMs, offC[0])
	coldHints := url.Values{"ts_ms": {fmt.Sprint(baseMs + 45)}, "retention_class": {hotstore.RetentionShortClean}}

	t.Run("hot /tree: names, times, inlined and unresolved big params", func(t *testing.T) {
		tree, version, headers := getTree(t, api, pkHot, nil, "")
		assert.Equal(t, int64(1), version, "the §2.5.2 envelope carries v = 1")
		assert.Equal(t, "application/x-msgpack", headers.Get("Content-Type"))
		assert.NotEmpty(t, headers.Get("ETag"))

		require.NotNil(t, tree.Root)
		assert.Equal(t, []string{"com.example.Service.handle", "com.example.Db.query"}, tree.Methods,
			"the per-tree dictionary carries only referenced strings, in first-use order (02 §2.5.2)")
		assert.Equal(t, []string{"request.id", "xml", "sql"}, tree.Params)

		root := tree.Root
		assert.Equal(t, int64(15), root.DurationMs)
		assert.Equal(t, int64(10), root.SelfDurationMs, "15 total minus the child's 5")
		assert.Equal(t, int64(2), root.Executions, "itself plus the one child invocation")
		assert.Equal(t, int64(1), root.SelfExecutions)
		assert.Equal(t, int64(3), root.SuspensionMs, "[6, 8) and [9, 10) fall inside [5, 20)")
		assert.Equal(t, int64(2), root.SelfSuspensionMs, "[9, 10) belongs to the child")
		require.Len(t, root.Params, 2)
		assert.Equal(t, []calltree.ParamGroup{{Value: "req-hot", DurationMs: 15, Executions: 1}},
			root.Params[0].Groups)
		require.Len(t, root.Params[1].Groups, 1)
		assert.Equal(t, "xml:7:0", root.Params[1].Groups[0].Value,
			"an unresolvable reference is marked with its ref text, not dropped")
		assert.True(t, root.Params[1].Groups[0].Unresolved)

		require.Len(t, root.Children, 1)
		child := root.Children[0]
		assert.Equal(t, "com.example.Db.query", tree.Methods[child.MethodIdx])
		assert.Equal(t, int64(5), child.DurationMs)
		assert.Equal(t, int64(5), child.SelfDurationMs, "a leaf's self equals its total")
		assert.Equal(t, int64(1), child.SuspensionMs, "the replica's live timeline attributes [9, 10)")
		require.Len(t, child.Params, 1)
		assert.Equal(t, "sql", tree.Params[child.Params[0].ParamIdx])
		assert.Equal(t, []calltree.ParamGroup{{Value: hotSqlValue, DurationMs: 5, Executions: 1}},
			child.Params[0].Groups, "hot big params resolve from the replica's value segments")
	})

	t.Run("hot /tree: a values transport error fails the tree, not degrades it", func(t *testing.T) {
		// §2.5.3: a failed /values fetch must not serve a 200 with SQL silently
		// downgraded to unresolved groups. It fails like the dictionary and
		// suspend transport paths — 504 — so the client never trusts corrupted
		// R11 aggregation. Absent references (a successful response) still
		// render as unresolved; only the transport failure is fatal.
		base := hotread.New(storeA).Handler()
		faulty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(r.URL.Path, "/values") {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			base.ServeHTTP(w, r)
		}))
		t.Cleanup(faulty.Close)
		fdisco := &scriptedDiscovery{}
		fdisco.set(faulty.URL)
		fapi := httptest.NewServer(query.New(query.Options{
			Config:       query.Config{WideRangeLimit: 30 * 24 * time.Hour},
			ColdStore:    fake,
			HotDiscovery: fdisco,
		}).Handler())
		t.Cleanup(fapi.Close)

		getProblem(t, fapi, "/api/v1/calls/"+pkHot+"/tree", nil, http.StatusGatewayTimeout)
	})

	t.Run("cold /tree: self-contained row, sealed big params, record_index noise", func(t *testing.T) {
		// №3/№23 acceptance: no dictionary or suspend snapshot exists in S3 —
		// the uploader never writes one any more — yet the tree below still
		// resolves every name and every per-node suspension, from the sealed
		// row's own dict_words_json and suspend_json columns.
		for _, objKey := range fake.allKeys() {
			assert.False(t, strings.HasPrefix(objKey, "dictionaries/") || strings.HasPrefix(objKey, "suspend/"),
				"no snapshot object may exist: %s", objKey)
		}

		tree, version, _ := getTree(t, api, pkCold, coldHints, "")
		assert.Equal(t, int64(1), version)

		assert.Equal(t, []string{
			"com.example.Service.handle", "com.example.Db.query", "com.example.Service.process",
		}, tree.Methods, "names resolve from the row's dict_words_json column (01 §3.6, №3)")
		assert.Equal(t, []string{"request.id", "sql", "xml"}, tree.Params)

		root := tree.Root
		assert.Equal(t, int64(19), root.DurationMs, "tail noise before record_index only advances the clock (01 §4.5)")
		assert.Equal(t, int64(11), root.SelfDurationMs, "19 total minus 5+3 in children")
		assert.Equal(t, int64(5), root.SuspensionMs, "all three pauses fall inside [5, 24)")
		assert.Equal(t, int64(2), root.SelfSuspensionMs, "[9, 11) and [15, 16) belong to the children")
		require.Len(t, root.Params, 1)
		assert.Equal(t, []calltree.ParamGroup{{Value: "req-cold", DurationMs: 19, Executions: 1}},
			root.Params[0].Groups)

		require.Len(t, root.Children, 2)
		q, p := root.Children[0], root.Children[1]
		assert.Equal(t, "com.example.Db.query", tree.Methods[q.MethodIdx])
		assert.Equal(t, int64(5), q.DurationMs)
		assert.Equal(t, int64(2), q.SuspensionMs, "the row's suspend_json attributes [9, 11)")
		require.Len(t, q.Params, 1)
		assert.Equal(t, []calltree.ParamGroup{{Value: coldSqlValue, DurationMs: 5, Executions: 1}},
			q.Params[0].Groups, "cold big params inline from the sealed big_params_json column")

		assert.Equal(t, "com.example.Service.process", tree.Methods[p.MethodIdx])
		assert.Equal(t, int64(3), p.DurationMs)
		assert.Equal(t, int64(1), p.SuspensionMs, "[15, 16) falls inside [14, 17)")
		require.Len(t, p.Params, 1)
		assert.Equal(t, []calltree.ParamGroup{{Value: coldXmlValue, DurationMs: 3, Executions: 1}},
			p.Params[0].Groups)
	})

	t.Run("cold /tree without the class hint discovers across classes", func(t *testing.T) {
		tree, _, _ := getTree(t, api, pkColdNoise, url.Values{"ts_ms": {fmt.Sprint(baseMs + 40)}}, "")
		assert.Equal(t, []string{"com.example.Db.query"}, tree.Methods)
		assert.Equal(t, int64(1), tree.Root.DurationMs)
		assert.Empty(t, tree.Root.Children, "C1 stops at its own depth-0 exit; C2 behind it is head noise")
		assert.Equal(t, int64(0), tree.Root.SuspensionMs, "no pause overlaps C1's [0, 1)")
	})

	t.Run("tree envelope: Accept-Version and gzip", func(t *testing.T) {
		_, version, _ := getTree(t, api, pkHot, nil, "1")
		assert.Equal(t, int64(1), version, "Accept-Version: 1 answers with the declared version (02 §2.5.4)")

		req, err := http.NewRequest(http.MethodGet, api.URL+"/api/v1/calls/"+pkHot+"/tree", nil)
		require.NoError(t, err)
		req.Header.Set("Accept-Version", "2")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "no v2 exists to negotiate down from")

		// gzip rides on Accept-Encoding (02 §2.5.5). A raw transport sees the
		// header; the decompressed body must equal the identity response.
		identity, _, _ := getTreeRaw(t, api, pkHot, nil, "", false)
		raw, headers, status := getTreeRaw(t, api, pkHot, nil, "", true)
		require.Equal(t, http.StatusOK, status)
		assert.Equal(t, "gzip", headers.Get("Content-Encoding"))
		gz, err := gzip.NewReader(bytes.NewReader(raw))
		require.NoError(t, err)
		unzipped, err := io.ReadAll(gz)
		require.NoError(t, err)
		assert.Equal(t, identity, unzipped)
	})

	t.Run("/trace: raw blob byte-for-byte on both tiers", func(t *testing.T) {
		// Hot: the external endpoint must return exactly what the replica's
		// seal-shared assembler produces.
		wantHot, err := storeA.AssembleTraceBlob(ctx, keyH, 1, offH[0], 0)
		require.NoError(t, err)
		gotHot, headers := getTrace(t, api, pkHot, nil)
		assert.Equal(t, wantHot, gotHot)
		assert.Equal(t, "application/octet-stream", headers.Get("Content-Type"))
		assert.NotEmpty(t, headers.Get("ETag"))
		assert.Equal(t, "bytes", headers.Get("Accept-Ranges"), "02 §2.4: partial reads supported")

		// Cold: byte-for-byte the sealed trace_blob column.
		row := sealedCallRow(t, fake, keyC, 2)
		require.NotNil(t, row.TraceBlob)
		gotCold, _ := getTrace(t, api, pkCold, coldHints)
		assert.Equal(t, row.TraceBlob, gotCold)
		require.NotNil(t, row.BigParamsJson, "the seal inlined the call's big params next to the blob")
		var sealed map[string]string
		require.NoError(t, json.Unmarshal([]byte(*row.BigParamsJson), &sealed))
		assert.Equal(t, map[string]string{
			fmt.Sprintf("sql:1:%d", sqlColdOffs[0]): coldSqlValue,
			fmt.Sprintf("xml:1:%d", xmlColdOffs[0]): coldXmlValue,
		}, sealed)

		// Range: the first 8 bytes are the timer epoch prefix (01 §4.5).
		req, err := http.NewRequest(http.MethodGet, api.URL+"/api/v1/calls/"+pkHot+"/trace", nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=0-7")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		part, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		assert.Equal(t, http.StatusPartialContent, resp.StatusCode)
		assert.Equal(t, wantHot[:8], part)
	})

	t.Run("a cold pk without ts_ms is a guided 404, not a scan", func(t *testing.T) {
		problem := getProblem(t, api, "/api/v1/calls/"+pkCold+"/tree", url.Values{}, http.StatusNotFound)
		assert.Contains(t, problem.Detail, "ts_ms", "the detail points at the §2.2 hints")
		problem = getProblem(t, api, "/api/v1/calls/"+pkCold+"/trace", url.Values{}, http.StatusNotFound)
		assert.Contains(t, problem.Detail, "ts_ms")
	})

	// Close the hot agent before the collector's shutdown wait: Stop() blocks
	// on live connections up to the read timeout (stage1-progress.md open
	// issue), which overruns the fixture's 10 s stop budget.
	require.NoError(t, acH.CommandClose())
	_ = acH.Close()
}

// getTree fetches and decodes one /tree response with the §2.5.1 reference
// decoder.
func getTree(t *testing.T, srv *httptest.Server, pk string, params url.Values, acceptVersion string) (*calltree.Tree, int64, http.Header) {
	t.Helper()
	body, headers, status := getTreeRaw(t, srv, pk, params, acceptVersion, false)
	require.Equal(t, http.StatusOK, status, "body: %s", body)
	tree, version, err := calltree.Decode(body)
	require.NoError(t, err)
	return tree, version, headers
}

func getTreeRaw(t *testing.T, srv *httptest.Server, pk string, params url.Values, acceptVersion string, gzipEncoding bool) ([]byte, http.Header, int) {
	t.Helper()
	u := srv.URL + "/api/v1/calls/" + pk + "/tree"
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	req, err := http.NewRequest(http.MethodGet, u, nil)
	require.NoError(t, err)
	if acceptVersion != "" {
		req.Header.Set("Accept-Version", acceptVersion)
	}
	client := http.DefaultClient
	if gzipEncoding {
		// A raw transport so the Go client does not hide Content-Encoding.
		client = &http.Client{Transport: &http.Transport{DisableCompression: true}}
		req.Header.Set("Accept-Encoding", "gzip")
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return body, resp.Header, resp.StatusCode
}

// getTrace fetches one /trace body and requires a 200.
func getTrace(t *testing.T, srv *httptest.Server, pk string, params url.Values) ([]byte, http.Header) {
	t.Helper()
	u := srv.URL + "/api/v1/calls/" + pk + "/trace"
	if len(params) > 0 {
		u += "?" + params.Encode()
	}
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	return body, resp.Header
}

// sealedCallRow reads the uploaded parquet rows of one pod-restart and picks
// the call by record_index.
func sealedCallRow(t *testing.T, fake *coldFakeStore, key hotstore.PodRestartKey, recordIndex int32) *storageparquet.CallV2 {
	t.Helper()
	hash := hotstore.PodRestartHash(key)
	for _, objKey := range fake.allKeys() {
		if !strings.HasSuffix(objKey, ".parquet") || !strings.Contains(objKey, "-"+hash+"-") {
			continue
		}
		obj, err := fake.Open(context.Background(), objKey)
		require.NoError(t, err)
		rows := readParquetRows[storageparquet.CallV2](t, obj)
		_ = obj.Close()
		for i := range rows {
			if rows[i].RecordIndex == recordIndex {
				return &rows[i]
			}
		}
	}
	t.Fatalf("no uploaded parquet row of %v has record_index %d", key, recordIndex)
	return nil
}
