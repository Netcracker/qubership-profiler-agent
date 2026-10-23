package integration

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	qmodel "github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStageBOrdersOnBothTiers is the acceptance check of agent-access task
// 4.2: every order of 02 §2.3.5 pages through the same calls in the same
// order on the hot tier and, after the seal, on the cold tier.
func TestStageBOrdersOnBothTiers(t *testing.T) {
	f := ingestSharedFixture(t)
	want := map[string]string{
		"start_desc":    "c5 c4 c3 c2 c1",
		"start_asc":     "c1 c2 c3 c4 c5",
		"duration_desc": "c4 c2 c3 c1 c5",
		"duration_asc":  "c5 c1 c3 c2 c4",
		"cpu_desc":      "c2 c1 c3 c4 c5",
		"cpu_asc":       "c5 c4 c3 c1 c2",
		"queue_desc":    "c3 c4 c1 c5 c2",
		"queue_asc":     "c2 c5 c1 c4 c3",
		"calls_desc":    "c3 c1 c2 c5 c4",
		"calls_asc":     "c4 c5 c2 c1 c3",
		"net_desc":      "c2 c1 c4 c5 c3",
		"net_asc":       "c3 c5 c4 c1 c2",
	}
	require.Len(t, want, len(qmodel.ImplementedOrders))
	run := func(tier, api string) {
		for order, names := range want {
			params := f.window()
			params.Set("order", order)
			params.Set("limit", "2")
			assert.Equal(t, strings.Fields(names), pageThreadNames(t, api, params), "%s: %s", tier, order)
		}
	}
	run("hot", f.hotURL)
	f.seal()
	run("cold", f.coldURL)
}

// pageThreadNames pages a /calls query to the end and returns the thread
// names in page order.
func pageThreadNames(t *testing.T, api string, params url.Values) []string {
	t.Helper()
	var names []string
	for range 20 {
		resp, err := http.Get(api + "/api/v1/calls?" + params.Encode())
		require.NoError(t, err)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		_ = resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
		var page struct {
			Calls      []qmodel.CallJSON `json:"calls"`
			NextCursor *string           `json:"next_cursor"`
		}
		require.NoError(t, json.Unmarshal(body, &page))
		for _, c := range page.Calls {
			names = append(names, c.ThreadName)
		}
		if page.NextCursor == nil {
			return names
		}
		params = url.Values{"cursor": {*page.NextCursor}, "limit": params["limit"]}
	}
	t.Fatal("paging did not end")
	return nil
}
