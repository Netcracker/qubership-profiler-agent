package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestStageBFiltersOnBothTiers is the acceptance check of agent-access task
// 4.1: every stage B operator, the method, class, and endpoint fields, and
// the None and Any values select the same calls from the hot tier and, after
// the seal, from the cold tier (02 §2.3.3).
func TestStageBFiltersOnBothTiers(t *testing.T) {
	f := ingestSharedFixture(t)
	place := sharedDictWords[sharedMethodPlace]
	cases := []struct {
		filter string
		want   string
	}{
		{"pod=~^pod-sh", "c1 c2 c3 c4 c5"},
		{"pod!~shared", ""},
		{"namespace not in other", "c1 c2 c3 c4 c5"},
		{"service!=" + sharedPod, "c1 c2 c3 c4 c5"},
		{"method=" + place, "c1 c2"},
		{"method!=" + place, "c3 c4 c5"},
		{"method!~Invoice", "c1 c2 c4"},
		{"class=com.example.OrderService", "c1 c2"},
		{"class!=com.example.OrderService", "c3 c4 c5"},
		{"class=~billing", "c3 c5"},
		{"class!~billing", "c1 c2 c4"},
		{"class in com.example.billing.Invoice,x.Y", "c3 c5"},
		{"class not in com.example.billing.Invoice", "c1 c2 c4"},
		{"endpoint=GET /orders/{id}", "c1 c2"},
		{"endpoint!=GET /orders/{id}", "c3 c4 c5"},
		{"endpoint=~^POST", "c3"},
		{"endpoint!~^POST", "c1 c2 c4 c5"},
		{"endpoint in POST /invoices/{id}/pdf,GET /x", "c3"},
		{"endpoint not in GET /orders/{id}", "c3 c4 c5"},
		{"endpoint=None", "c4 c5"},
		{"endpoint=Any", "c1 c2 c3"},
		{"user.tenant!=acme", "c3 c4 c5"},
		{"user.tenant in globex,initech", "c2 c3 c5"},
		{"user.tenant not in acme,initech", "c3 c4"},
		{"user.tenant=~^glo", "c2 c3"},
		{"user.tenant!~^glo", "c1 c4 c5"},
		{"user.tenant=None", "c4"},
		{"user.tenant=Any", "c1 c2 c3 c5"},
		{"user.tenant!=None", "c1 c2 c3 c5"},
		{"user.tenant!=Any", "c4"},
		{"trace_id!=t1", "c2 c3 c4 c5"},
		{"trace_id=~^t", "c1 c5"},
		{"trace_id=None", "c2 c3 c4"},
		{"request_id=None", "c1 c2 c3 c4 c5"},
		{"request_id!=Any", "c1 c2 c3 c4 c5"},
		{"retention_class!=short_clean", "c2 c3 c4"},
		{"retention_class not in short_clean,huge_clean", "c2 c3"},
	}
	run := func(tier, api string) {
		for _, tc := range cases {
			params := f.window()
			params.Set("filter", tc.filter)
			want := strings.Fields(tc.want)
			if want == nil {
				want = []string{}
			}
			assert.Equal(t, want, threadNames(t, api+"/api/v1/calls?"+params.Encode()), "%s: %s", tier, tc.filter)
		}
	}
	run("hot", f.hotURL)
	f.seal()
	run("cold", f.coldURL)
}
