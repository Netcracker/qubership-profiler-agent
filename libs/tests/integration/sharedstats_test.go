package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStageBGroupingsOnBothTiers is the acceptance check of agent-access task
// 4.3: /stats groups the same calls by params, the derived fields, error,
// retention_class, and param names, with the same exact counts on the hot
// tier and, after the seal, on the cold tier (02 §2.9).
func TestStageBGroupingsOnBothTiers(t *testing.T) {
	f := ingestSharedFixture(t)
	want := map[string]map[string]int64{
		"class":           {"com.example.OrderService": 2, "com.example.billing.Invoice": 2},
		"endpoint":        {"GET /orders/{id}": 2, "POST /invoices/{id}/pdf": 1},
		"error":           {"false": 5},
		"retention_class": {"short_clean": 2, "normal_clean": 1, "long_clean": 1, "huge_clean": 1},
		"user.tenant":     {"acme": 2, "globex": 2, "initech": 1},
		"trace_id":        {"t1": 1, "t5": 1},
		"param_name":      {"web.url": 3, "web.method": 3, "user.tenant": 4, "trace.id": 2},
	}
	run := func(tier, api string) {
		for groupBy, counts := range want {
			params := f.window()
			params.Set("group_by", groupBy)
			resp, err := http.Get(api + "/api/v1/stats?" + params.Encode())
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			_ = resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s %s: %s", tier, groupBy, body)
			var out stats.Response
			require.NoError(t, json.Unmarshal(body, &out))
			got := map[string]int64{}
			for _, r := range out.Rows {
				got[fmt.Sprint(r.Key)] = r.Count
			}
			assert.Equal(t, counts, got, "%s: group_by=%s", tier, groupBy)
			assert.Equal(t, stats.Totals{Count: 5}, out.Totals, "%s: group_by=%s", tier, groupBy)
			assert.Equal(t, len(counts), out.Distinct, "%s: group_by=%s", tier, groupBy)
			assert.False(t, out.Approximate)
		}
	}
	run("hot", f.hotURL)
	f.seal()
	run("cold", f.coldURL)
}
