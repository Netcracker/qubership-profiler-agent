package query

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	storageparquet "github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// budgetTestFixture plants a small cold data set and returns the service
// (with its internal budget reachable — same package) and the API server.
func budgetTestFixture(t *testing.T, cfg Config) (*Service, *httptest.Server, int64) {
	t.Helper()
	const restartMs = int64(1_700_000_000_000)
	tuple := model.PodTuple{Namespace: "ns", Service: "svc", Pod: "pod", RestartTimeMs: restartMs}
	store := newMemColdStore()
	// One sealed file with five rows in the (ts_ms DESC, pk ASC) file order —
	// the sealed-key stamps are second-precision, so per-row files with
	// millisecond-apart timestamps would silently overwrite each other.
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[storageparquet.CallV2](&buf,
		parquet.Compression(&parquet.Zstd),
		parquet.KeyValueMetadata(storageparquet.SchemaVersionKey, storageparquet.SchemaVersion),
	)
	for i := 4; i >= 0; i-- {
		_, err := w.Write([]storageparquet.CallV2{{
			TsMs:  restartMs + int64(1000+i),
			PodId: "ns/svc/pod", RestartTimeMs: restartMs,
			TraceFileIndex: 1, BufferOffset: int32(i), RecordIndex: 0,
			Namespace: "ns", ServiceName: "svc", PodName: "pod",
			Method: "com.example.Service.handle", DurationMs: 10,
			RetentionClass: model.RetentionShortClean,
		}})
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	store.put(sealedKey(model.RetentionShortClean, tuple, restartMs+1000), buf.Bytes())
	svc := New(Options{Config: cfg, ColdStore: store})
	api := httptest.NewServer(svc.Handler())
	t.Cleanup(api.Close)
	return svc, api, restartMs
}

func getJSONBody(t *testing.T, u string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(u)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp, body
}

// TestCallsBudget503Shape pins the §7.5 rejection contract on the wire: a
// budget no scan batch can ever fit answers an atomic 503 in the guard's
// problem dialect, with Retry-After, the never-fits detail, the narrowing
// filters, and the discovery estimate — and the ledger settles to zero.
func TestCallsBudget503Shape(t *testing.T) {
	svc, api, restartMs := budgetTestFixture(t, Config{
		ReadMemoryBudget: 4 << 10, // smaller than any batch charge
		ReadBudgetWait:   30 * time.Millisecond,
	})

	resp, body := getJSONBody(t, api.URL+"/api/v1/calls?"+url.Values{
		"from": {strconv.FormatInt(restartMs, 10)},
		"to":   {strconv.FormatInt(restartMs+10_000, 10)},
	}.Encode())
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "body: %s", body)
	assert.Equal(t, "application/problem+json", resp.Header.Get("Content-Type"))
	retry, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	require.NoError(t, err, "Retry-After must be set")
	assert.GreaterOrEqual(t, retry, 1)

	var p struct {
		Title            string   `json:"title"`
		Detail           string   `json:"detail"`
		SuggestedFilters []string `json:"suggested_filters"`
		EstimatedBytes   *int64   `json:"estimated_bytes"`
	}
	require.NoError(t, json.Unmarshal(body, &p))
	assert.Equal(t, "read memory budget exhausted", p.Title)
	assert.Contains(t, p.Detail, "exceeds the whole read memory budget", "never-fits must be named")
	assert.NotEmpty(t, p.SuggestedFilters)
	require.NotNil(t, p.EstimatedBytes, "discovery ran, so the estimate rides along")
	assert.Positive(t, *p.EstimatedBytes)
	assert.Equal(t, int64(0), svc.budget.Used())
}

// TestCallsBudgetRetrySameCursor pins the atomic-503 pagination property: a
// page denied under contention is retried with the SAME cursor and returns
// the full page — no rows were skipped by the failed attempt.
func TestCallsBudgetRetrySameCursor(t *testing.T) {
	svc, api, restartMs := budgetTestFixture(t, Config{
		ReadBudgetWait: 30 * time.Millisecond,
	})
	base := api.URL + "/api/v1/calls?" + url.Values{
		"from":  {strconv.FormatInt(restartMs, 10)},
		"to":    {strconv.FormatInt(restartMs+10_000, 10)},
		"limit": {"2"},
	}.Encode()

	resp, body := getJSONBody(t, base)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var page1 callsResponse
	require.NoError(t, json.Unmarshal(body, &page1))
	require.Len(t, page1.Calls, 2)
	require.NotNil(t, page1.NextCursor)
	// limit is a per-request parameter, not part of the frozen query — resend it.
	cursorURL := api.URL + "/api/v1/calls?limit=2&cursor=" + url.QueryEscape(*page1.NextCursor)

	// Saturate the budget: the page-2 attempt must 503 without consuming the
	// cursor position.
	blocker, err := svc.budget.Acquire(context.Background(), svc.budget.Limit())
	require.NoError(t, err)
	resp, body = getJSONBody(t, cursorURL)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "body: %s", body)

	// The same cursor after the pressure clears serves the full page.
	blocker.Release()
	resp, body = getJSONBody(t, cursorURL)
	require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	var page2 callsResponse
	require.NoError(t, json.Unmarshal(body, &page2))
	assert.Len(t, page2.Calls, 2, "the retried page is complete; nothing was skipped by the 503")
	for _, call := range page2.Calls {
		assert.NotEqual(t, page1.Calls[0].PK, call.PK, "page 2 continues past page 1")
	}
	assert.Equal(t, int64(0), svc.budget.Used())
}

// TestPointBudget503AndLedger pins the point endpoints: a saturated budget
// answers 503 on /tree, and every completed response leaves the ledger at
// zero (the point lease released after the write).
func TestPointBudget503AndLedger(t *testing.T) {
	svc, api, restartMs := budgetTestFixture(t, Config{
		ReadBudgetWait: 30 * time.Millisecond,
	})
	pk := model.PK{
		PodNamespace: "ns", PodService: "svc", PodName: "pod", RestartTimeMs: restartMs,
		TraceFileIndex: 1, BufferOffset: 0, RecordIndex: 0,
	}
	u := api.URL + "/api/v1/calls/" + url.PathEscape(pk.PathString()) + "/trace?" +
		url.Values{"ts_ms": {strconv.FormatInt(restartMs+1000, 10)}}.Encode()

	blocker, err := svc.budget.Acquire(context.Background(), svc.budget.Limit())
	require.NoError(t, err)
	resp, body := getJSONBody(t, u)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode, "body: %s", body)
	assert.NotEmpty(t, resp.Header.Get("Retry-After"))
	blocker.Release()

	// The same fetch succeeds once the budget clears (the planted row has no
	// blob, so a 404 is the honest outcome — what matters is the ledger).
	resp, body = getJSONBody(t, u)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode, "body: %s", body)
	assert.Equal(t, int64(0), svc.budget.Used(), "the point lease is released after the response")
}
