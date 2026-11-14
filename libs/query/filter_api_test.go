package query

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// legacyReplica is a collector that predates the filter model: it ignores
// `filter` and pages through every call it holds, as an old replica does
// during a rolling upgrade.
type legacyReplica struct {
	rows []model.CallRow // in (ts_ms DESC, pk ASC) order
}

func (r *legacyReplica) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch req.URL.Path {
	case "/internal/v1/health/hot-window":
		_ = json.NewEncoder(w).Encode(map[string]int64{"hot_window_oldest_ms": 0, "hot_window_now_ms": legacyBaseMs + 1000})
	case "/internal/v1/calls":
		q := req.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		var after *model.Position
		if raw := q.Get("after_pk"); raw != "" {
			pk, err := model.ParsePKPath(raw)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ts, _ := strconv.ParseInt(q.Get("after_ts_ms"), 10, 64)
			after = &model.Position{TsMs: ts, PK: pk}
		}
		calls := []model.CallJSON{}
		for _, row := range r.rows {
			if after != nil && !row.Position().After(*after) {
				continue
			}
			if len(calls) == limit {
				break
			}
			calls = append(calls, row.JSON())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"calls": calls, "partial_reasons": []string{}})
	default:
		http.NotFound(w, req)
	}
}

// legacyBaseMs puts the rows far past the overlap margin above the replica's
// hot-window start, so the query never reaches for the (absent) cold tier.
const legacyBaseMs = int64(1_700_000_000_000)

func legacyRow(service string, offsetMs int64) model.CallRow {
	tsMs := legacyBaseMs + offsetMs
	return model.CallRow{
		PK: model.PK{
			PodNamespace: "prod", PodService: service, PodName: service + "-1", RestartTimeMs: 1,
			TraceFileIndex: 1, BufferOffset: int32(offsetMs), RecordIndex: 0,
		},
		TsMs: tsMs, DurationMs: 10, Method: "com.example.Service.handle",
		RetentionClass: model.RetentionNormalClean, Tier: model.TierHot,
	}
}

// TestFilterOverALegacyReplica pages through `service=billing` while the only
// replica ignores the filter. query re-checks every hot row, and a page that
// the re-check empties still carries a cursor, so no billing row is lost and
// none repeats.
func TestFilterOverALegacyReplica(t *testing.T) {
	replica := &legacyReplica{rows: []model.CallRow{
		legacyRow("orders", 100), legacyRow("orders", 90),
		legacyRow("billing", 80), legacyRow("orders", 70),
		legacyRow("billing", 60), legacyRow("billing", 50),
	}}
	srv := httptest.NewServer(replica)
	defer srv.Close()
	api := httptest.NewServer(New(Options{HotDiscovery: staticDiscovery{srv.URL}}).Handler())
	defer api.Close()

	params := url.Values{
		"from": {strconv.FormatInt(legacyBaseMs, 10)}, "to": {strconv.FormatInt(legacyBaseMs+1000, 10)},
		"filter": {"service=billing"}, "limit": {"2"},
	}
	var got []int64
	for page := 0; page < 10; page++ {
		resp, err := http.Get(api.URL + "/api/v1/calls?" + params.Encode())
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
		var out struct {
			Calls      []model.CallJSON `json:"calls"`
			NextCursor *string          `json:"next_cursor"`
		}
		require.NoError(t, json.Unmarshal(body, &out))
		for _, c := range out.Calls {
			assert.Equal(t, "billing", c.PK.PodService)
			got = append(got, c.TsMs-legacyBaseMs)
		}
		if out.NextCursor == nil {
			break
		}
		params = url.Values{"cursor": {*out.NextCursor}, "limit": {"2"}}
	}
	assert.Equal(t, []int64{80, 60, 50}, got)
}
