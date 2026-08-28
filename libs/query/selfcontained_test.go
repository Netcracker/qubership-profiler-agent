package query

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColdTreeIsSelfContained is the №3/№23 acceptance at the handler level:
// the store holds ONE object — the sealed parquet file — and no snapshot of
// any kind, yet /tree resolves the method and param-key names from the row's
// dict_words_json (not "#<id>" placeholders) and the per-node suspension
// from its suspend_json (not zero). Nothing is left for a snapshot TTL to
// dangle.
func TestColdTreeIsSelfContained(t *testing.T) {
	const restartMs = int64(1_700_000_000_000)
	tuple := model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod", RestartTimeMs: restartMs}
	pk := model.PK{
		PodNamespace: "ns", PodService: "svc", PodName: "pod", RestartTimeMs: restartMs,
		TraceFileIndex: 1, BufferOffset: 100, RecordIndex: 0,
	}
	tsMs := restartMs + 42

	// Root [5, 15) with a request.id tag; the pause [7, 9) (end 9, 2 ms) falls
	// inside the root window on the trace timer axis.
	blob, _ := wire.TraceStream(restartMs, []wire.TraceChunk{
		{ThreadId: 7, StartMs: restartMs, Events: []wire.TraceEvent{
			wire.Enter(5, 1), wire.Tag(0, 2, "req-42"), wire.Exit(10),
		}},
	})
	dictJSON := `{"1":"com.example.Service.handle","2":"request.id"}`
	suspendJSON, err := json.Marshal([]storageparquet.SuspendEvent{{EndMs: restartMs + 9, DurationMs: 2}})
	require.NoError(t, err)
	suspendStr := string(suspendJSON)

	row := storageparquet.CallV2{
		TsMs:           tsMs,
		PodId:          "ns/svc/pod",
		RestartTimeMs:  restartMs,
		TraceFileIndex: pk.TraceFileIndex,
		BufferOffset:   pk.BufferOffset,
		RecordIndex:    pk.RecordIndex,
		Namespace:      "ns", ServiceName: "svc", PodName: "pod",
		Method:         "com.example.Service.handle",
		DurationMs:     10,
		RetentionClass: model.RetentionNormalClean,
		TraceBlob:      blob,
		DictWordsJson:  &dictJSON,
		SuspendJson:    &suspendStr,
	}

	store := newMemColdStore()
	store.put(sealedKey(model.RetentionNormalClean, tuple, tsMs), writeCallParquet(t, row))

	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()

	resp, err := http.Get(api.URL + "/api/v1/calls/" + url.PathEscape(pk.PathString()) + "/tree?" +
		url.Values{"ts_ms": {strconv.FormatInt(tsMs, 10)}}.Encode())
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	tree, _, err := calltree.Decode(body)
	require.NoError(t, err)
	require.NotNil(t, tree.Root)
	assert.Equal(t, []string{"com.example.Service.handle"}, tree.Methods,
		"the method name resolves from dict_words_json, not a #<id> placeholder")
	assert.Equal(t, []string{"request.id"}, tree.Params,
		"the param-key name resolves from dict_words_json")
	assert.EqualValues(t, 10, tree.Root.DurationMs)
	assert.EqualValues(t, 2, tree.Root.SuspensionMs,
		"the per-node suspension comes from suspend_json, not zero")
}

// TestColdCallsKeepTierMatchedRows is the №10 regression at the /calls
// handler level: a 5-second call seals into long_clean under the
// [100ms, 1s, 10s] tier table, and a query with duration_min_ms=2000 must
// return it — before the shared tier table the read side pruned classes
// against its own hardcoded bounds and silently dropped the row.
func TestColdCallsKeepTierMatchedRows(t *testing.T) {
	const restartMs = int64(1_700_000_000_000)
	tuple := model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod", RestartTimeMs: restartMs}
	tsMs := restartMs + 42

	class := model.ClassifyDuration(5*time.Second, false, nil)
	require.Equal(t, model.RetentionLongClean, class, "a 5s call is long_clean under [100ms, 1s, 10s)")

	row := storageparquet.CallV2{
		TsMs:           tsMs,
		PodId:          "ns/svc/pod",
		RestartTimeMs:  restartMs,
		TraceFileIndex: 1, BufferOffset: 100, RecordIndex: 0,
		Namespace: "ns", ServiceName: "svc", PodName: "pod",
		Method:         "com.example.Service.handle",
		DurationMs:     5000,
		RetentionClass: class,
	}
	store := newMemColdStore()
	store.put(sealedKey(class, tuple, tsMs), writeCallParquet(t, row))

	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()

	resp, err := http.Get(api.URL + "/api/v1/calls?" + url.Values{
		"from":            {strconv.FormatInt(tsMs-1000, 10)},
		"to":              {strconv.FormatInt(tsMs+1000, 10)},
		"duration_min_ms": {"2000"},
	}.Encode())
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var page struct {
		Calls []struct {
			DurationMs     int32  `json:"duration_ms"`
			RetentionClass string `json:"retention_class"`
		} `json:"calls"`
	}
	require.NoError(t, json.Unmarshal(body, &page))
	require.Len(t, page.Calls, 1, "the 5s call must survive the duration_min_ms=2000 class pruning")
	assert.EqualValues(t, 5000, page.Calls[0].DurationMs)
	assert.Equal(t, model.RetentionLongClean, page.Calls[0].RetentionClass)
}
