package query

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectedOrder sorts the calls by the order's key and then by offset, which
// is the PK order of calls that share a pod-restart. It reads the keys
// itself rather than through the model comparator it checks.
func expectedOrder(order string, calls []orderCall) []string {
	field, dir, _ := cutOrder(order)
	key := func(c orderCall) int64 {
		switch field {
		case "duration":
			return int64(c.durMs)
		case "cpu":
			return c.cpuMs()
		case "queue":
			return int64(c.queueMs())
		case "calls":
			return int64(c.childCalls())
		case "net":
			return c.netRead() + c.netWritten()
		}
		return c.tsMs()
	}
	sorted := slices.Clone(calls)
	sort.SliceStable(sorted, func(i, j int) bool {
		ki, kj := key(sorted[i]), key(sorted[j])
		if ki != kj {
			return (ki > kj) == (dir == "desc")
		}
		return sorted[i].off < sorted[j].off
	})
	out := make([]string, len(sorted))
	for i, c := range sorted {
		out[i] = fmt.Sprintf("%d/%d", c.durMs, c.off)
	}
	return out
}

func cutOrder(order string) (field, dir string, ok bool) {
	i := len(order) - 1
	for i >= 0 && order[i] != '_' {
		i--
	}
	if i < 0 {
		return order, "", false
	}
	return order[:i], order[i+1:], true
}

func orderQuery(order string, limit int) url.Values {
	q := durationQuery(limit)
	q.Set("order", order)
	return q
}

// TestDurationAscWalksTheBands reads the bands shortest first (02 §2.3.5):
// a full first page opens short_clean and the error classes only.
func TestDurationAscWalksTheBands(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()

	first := getCallsPage(t, api.URL, orderQuery("duration_asc", 3))
	assert.Equal(t, []string{model.RetentionAnyError, model.RetentionCorrupted, model.RetentionShortClean},
		store.listedClasses(), "a full first page opens the shortest band and the error classes only")
	require.NotNil(t, first.NextCursor)
	tok, err := decodeCursor(*first.NextCursor, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, model.RetentionShortClean, tok.Band)

	want := slices.Clone(bandOrder())
	slices.Reverse(want)
	want[5], want[6], want[7] = "1000/10", "1000/20", "1000/30" // ties stay in PK order
	assert.Equal(t, expectedOrder("duration_asc", bandCalls), want)
	assert.Equal(t, want, slices.Concat(pageIDs(t, api.URL, orderQuery("duration_asc", 3), nil)...),
		"pages concatenate into (duration_ms ASC, pk ASC), ties at a band bound included")

	t.Run("conditions skip bands", func(t *testing.T) {
		q := orderQuery("duration_asc", 1)
		q.Set("filter", "duration>=100ms")
		q.Add("filter", "retention_class not in long_clean,any_error")
		assert.Equal(t, []string{"100/6", "999/5", "15000/2", "20000/1"}, slices.Concat(pageIDs(t, api.URL, q, nil)...))
	})
}

func TestDurationAscStopsAtABandTheGuardRejects(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	// The three error files plus any one band fit; two bands never do.
	api := httptest.NewServer(New(Options{Config: Config{MaxScanFiles: 6}, ColdStore: store}).Handler())
	defer api.Close()

	pages := pageIDs(t, api.URL, orderQuery("duration_asc", 3), nil)
	assert.Equal(t, expectedOrder("duration_asc", bandCalls), slices.Concat(pages...), "stopped pages lose and repeat nothing")

	t.Run("a stop keeps the rows below the bound and resumes above it", func(t *testing.T) {
		after := model.Position{DurationMs: 50, PK: bandCalls[10].row(model.TierCold).PK}
		params := url.Values{"cursor": {encodeCursor(orderQuery("duration_asc", 3), after, model.RetentionShortClean)},
			"limit": {"3"}}
		p := getCallsPage(t, api.URL, params)
		assert.Equal(t, []string{"99/7"}, idsOf(p), "the normal band did not fit; only the short rows are final")
		require.NotNil(t, p.NextCursor)
		tok, err := decodeCursor(*p.NextCursor, time.Minute)
		require.NoError(t, err)
		assert.Equal(t, model.Position{DurationMs: 99, NoPK: true}, tok.Pos)
		assert.Equal(t, model.RetentionNormalClean, tok.Band, "the next page starts at the band that did not fit")
	})
}

func idsOf(p callsPage) []string {
	ids := []string{}
	for _, c := range p.Calls {
		ids = append(ids, fmt.Sprintf("%d/%d", c.DurationMs, c.PK.BufferOffset))
	}
	return ids
}

// TestStageBOrderGuard: no stage B order is a narrowing filter. duration_asc
// opens short_clean first, the largest class, and the other orders read
// every file of the window (02 §2.3.5).
func TestStageBOrderGuard(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()
	for _, order := range []string{"duration_asc", "start_asc", "cpu_desc", "queue_asc", "calls_desc", "net_asc"} {
		wide := orderQuery(order, 3)
		wide.Set("to", strconv.FormatInt(orderBaseMs+7*3_600_000, 10))
		requestProblem(t, api, http.MethodGet, "/api/v1/calls?"+wide.Encode(), http.StatusBadRequest, "query_too_wide")
		wide.Set("filter", "duration>=1s")
		assert.Len(t, getCallsPage(t, api.URL, wide).Calls, 3, "%s works inside the guard", order)
	}

	tight := httptest.NewServer(New(Options{Config: Config{MaxScanFiles: 4}, ColdStore: store}).Handler())
	defer tight.Close()
	requestProblem(t, tight, http.MethodGet, "/api/v1/calls?"+orderQuery("cpu_desc", 3).Encode(),
		http.StatusBadRequest, "query_too_wide")
	asc := requestProblem(t, tight, http.MethodGet, "/api/v1/calls?"+orderQuery("duration_asc", 3).Encode(),
		http.StatusBadRequest, "query_too_wide")
	assert.Contains(t, asc.Detail, "5 files", "duration_asc charges its first band and the error classes")
}

// TestStageBOrdersPageTheColdTier pages every order through the cold tier: the
// pages concatenate into the order, ties broken by PK.
func TestStageBOrdersPageTheColdTier(t *testing.T) {
	store := newMemColdStore()
	plantCold(t, store, bandCalls...)
	api := httptest.NewServer(New(Options{ColdStore: store}).Handler())
	defer api.Close()
	for _, order := range model.ImplementedOrders {
		pages := pageIDs(t, api.URL, orderQuery(order.String(), 2), nil)
		assert.Equal(t, expectedOrder(order.String(), bandCalls), slices.Concat(pages...), order)
	}
}

// TestStageBOrdersAcrossTheHotToColdMigration moves a call from the hot tier
// to the cold tier between two pages, and keeps a duplicate in both tiers,
// under every order: the merge places each call once, in order, whichever
// tier holds it, and the cursor keeps its place across the move.
func TestStageBOrdersAcrossTheHotToColdMigration(t *testing.T) {
	coldCalls := []orderCall{{model.RetentionLongClean, 4000, 2}, {model.RetentionLongClean, 3000, 3}}
	hotCalls := []orderCall{
		{model.RetentionLongClean, 5000, 1}, {model.RetentionLongClean, 3000, 3},
		{model.RetentionLongClean, 2000, 4}, {model.RetentionNormalClean, 500, 5},
		{model.RetentionNormalClean, 700, 6}, {model.RetentionShortClean, 20, 7},
	}
	all := []orderCall{hotCalls[0], coldCalls[0], hotCalls[1], hotCalls[2], hotCalls[3], hotCalls[4], hotCalls[5]}
	for _, order := range model.ImplementedOrders {
		t.Run(order.String(), func(t *testing.T) {
			store := newMemColdStore()
			plantCold(t, store, coldCalls...)
			replica := &durationReplica{oldestMs: orderBaseMs + 10_000, calls: slices.Clone(hotCalls)}
			hotSrv := httptest.NewServer(replica)
			defer hotSrv.Close()
			api := httptest.NewServer(New(Options{ColdStore: store, HotDiscovery: staticDiscovery{hotSrv.URL}}).Handler())
			defer api.Close()

			pages := pageIDs(t, api.URL, orderQuery(order.String(), 2), func(page int) {
				if page == 0 {
					replica.remove(4)
					plantCold(t, store, orderCall{model.RetentionLongClean, 2000, 4})
				}
			})
			assert.Equal(t, expectedOrder(order.String(), all), slices.Concat(pages...))
		})
	}
}
