//go:build e2e

package cmd

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestE2ERequestInvestigation is task 1.7 of the agent-access plan: against
// a running query service seeded by tools/ui-seed (make e2e), find one
// request's call by its request ID and explain it through the CLI.
//
// ui-seed writes e2e/shop/shop-{1,2}, 30 calls each, with request IDs
// req-<pod>-<nnnn> under the param request.id, and call 6 of each pod lasts
// 5200 ms. Each tree nests Api.handle, CartService.validate, and
// OrderDao.query, which runs one SQL statement.
func TestE2ERequestInvestigation(t *testing.T) {
	url := os.Getenv(urlEnv)
	require.NotEmpty(t, url, "set %s to the seeded query service", urlEnv)

	out, err := run(t, "services", "--since", "1h", "--json")
	require.NoError(t, err)
	var services insight.ServicesResult
	require.NoError(t, json.Unmarshal([]byte(out), &services))
	assert.True(t, hasService(services, "e2e", "shop"), "services lists e2e/shop: %s", out)

	out, err = run(t, "calls", "--since", "1h", "--service", "e2e/shop", "--filter", "request.id=req-shop-1-0006", "--json")
	require.NoError(t, err)
	var calls insight.CallsResult
	require.NoError(t, json.Unmarshal([]byte(out), &calls))
	require.Len(t, calls.Calls, 1, "one call carries the request ID")
	call := calls.Calls[0]
	assert.Equal(t, int32(5200), call.DurationMs)
	assert.Equal(t, "shop-1", call.Pod)

	out, err = run(t, "explain", call.Ref, "--json")
	require.NoError(t, err)
	var explained insight.ExplainResult
	require.NoError(t, json.Unmarshal([]byte(out), &explained))
	require.NotNil(t, explained.Call, "the header found the call's row")
	assert.Equal(t, call.Ref, explained.Call.Ref)
	var methods []string
	for _, m := range explained.TopSelf {
		methods = append(methods, m.Method)
	}
	assert.ElementsMatch(t, []string{
		"com.example.shop.Api.handle(HttpRequest)",
		"com.example.shop.CartService.validate(Cart)",
		"com.example.shop.OrderDao.query(String)",
	}, methods)
	require.NotNil(t, explained.HotPath)
	assert.Contains(t, explained.HotPath.Method, "Api.handle")
	for n := explained.HotPath; n != nil; {
		assert.Equal(t, int64(1), n.Executions, "%s ran once in this call", n.Method)
		if len(n.Children) == 0 {
			break
		}
		n = n.Children[0]
	}
	require.NotEmpty(t, explained.TopSQL)
	assert.Contains(t, explained.TopSQL[0].Statement, "update inventory")

	text, err := run(t, "explain", call.Ref)
	require.NoError(t, err)
	assert.Contains(t, text, "OrderDao.query")
	t.Log("\n" + text)

	out, err = run(t, "calls", "--since", "1h", "--service", "e2e/shop", "--filter", "duration>5s", "--json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal([]byte(out), &calls))
	assert.Len(t, calls.Calls, 6, "three 5200 ms calls per pod")
	for _, c := range calls.Calls {
		assert.True(t, strings.HasPrefix(c.Pod, "shop-"))
	}
}

func hasService(res insight.ServicesResult, namespace, service string) bool {
	for _, ns := range res.Namespaces {
		for _, s := range ns.Services {
			if ns.Name == namespace && s.Name == service {
				return true
			}
		}
	}
	return false
}
