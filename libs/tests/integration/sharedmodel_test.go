package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/require"
)

// The shared-model fixture: five calls with distinct methods, params, and
// metrics, read through a query service first from the collector's hot
// store and then, after a seal and an upload, from the cold tier alone. The
// stage B tests of the filter model, the orders, and the /stats groupings
// (agent-access phase 4) run the same expectations on both tiers.

const sharedPod = "pod-shared"

// Dictionary word ids follow arrival order.
const (
	sharedMethodPlace = iota
	sharedMethodRender
	sharedMethodRun
	sharedDictURL
	sharedDictHTTPMethod
	sharedDictTenant
	sharedDictTrace
)

var sharedDictWords = []string{
	"void com.example.OrderService.place(long) (OrderService.java) [orders.jar]",
	"com.example.billing.Invoice.render",
	"run()",
	"web.url", "web.method", "user.tenant", "trace.id",
}

// sharedCalls are named by their thread names, c1 to c5, so an expectation
// reads as a set of names.
var sharedCalls = []wire.CallRecord{
	{DeltaMs: 5, Method: sharedMethodPlace, DurationMs: 50, ChildCalls: 10, ThreadName: "c1",
		CpuTimeMs: 40, QueueWaitMs: 5, NetRead: 100,
		Params: map[int][]string{
			sharedDictURL: {"http://orders/orders/123?x=1"}, sharedDictHTTPMethod: {"GET"},
			sharedDictTenant: {"acme"}, sharedDictTrace: {"t1"},
		}},
	{DeltaMs: 10, Method: sharedMethodPlace, DurationMs: 1500, ChildCalls: 3, ThreadName: "c2",
		CpuTimeMs: 1000, NetRead: 1000, NetWritten: 1000,
		Params: map[int][]string{
			sharedDictURL: {"http://orders/orders/456"}, sharedDictHTTPMethod: {"GET"},
			sharedDictTenant: {"acme", "globex"},
		}},
	{DeltaMs: 10, Method: sharedMethodRender, DurationMs: 300, ChildCalls: 50, ThreadName: "c3",
		CpuTimeMs: 10, QueueWaitMs: 200,
		Params: map[int][]string{
			sharedDictURL:        {"/invoices/550e8400-e29b-41d4-a716-446655440000/pdf"},
			sharedDictHTTPMethod: {"POST"}, sharedDictTenant: {"globex"},
		}},
	{DeltaMs: 10, Method: sharedMethodRun, DurationMs: 20_000, ThreadName: "c4",
		CpuTimeMs: 5, QueueWaitMs: 7, NetRead: 5, NetWritten: 5},
	{DeltaMs: 10, Method: sharedMethodRender, DurationMs: 5, ChildCalls: 1, ThreadName: "c5",
		CpuTimeMs: 1, QueueWaitMs: 1, NetRead: 1,
		Params: map[int][]string{sharedDictTenant: {"initech"}, sharedDictTrace: {"t5"}}},
}

// sharedFixture is the ingested fixture. hotURL and coldURL are query
// services; coldURL answers only after seal.
type sharedFixture struct {
	hotURL  string
	coldURL string
	seal    func()
	from    int64
	to      int64
}

// window returns the fixture's window as /calls or /stats parameters.
func (f sharedFixture) window() url.Values {
	return url.Values{"from": {fmt.Sprint(f.from)}, "to": {fmt.Sprint(f.to)}}
}

func ingestSharedFixture(t *testing.T) sharedFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	fake := newColdFakeStore()
	svc := startCollector(t, ctx, t.TempDir())
	// Cleanups run last-in first-out: the collector stops only once ctx ends.
	t.Cleanup(cancel)
	store := svc.Store()
	bucket := store.Config().Bucket(baseMs + 5)

	chunks := make([]wire.TraceChunk, len(sharedCalls))
	startMs := baseMs
	for i, c := range sharedCalls {
		startMs += c.DeltaMs
		events := []wire.TraceEvent{wire.Enter(0, c.Method)}
		ids := make([]int, 0, len(c.Params))
		for id := range c.Params {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		for _, id := range ids {
			for _, v := range c.Params[id] {
				events = append(events, wire.Tag(0, id, v))
			}
		}
		chunks[i] = wire.TraceChunk{ThreadId: uint64(90 + i), StartMs: startMs, Events: append(events, wire.Exit(1))}
	}
	traceFile, offs := wire.TraceStream(timerStartMs, chunks)
	calls := make([]wire.CallRecord, len(sharedCalls))
	for i, c := range sharedCalls {
		c.TraceFileIndex, c.BufferOffset = 1, int(offs[i])
		calls[i] = c
	}

	ac := connectAgentAs(t, ctx, sharedPod)
	key := waitForPodNamed(t, store, sharedPod, 1)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sharedDictWords))
	sendStream(t, ac, model.StreamTrace, 0, traceFile)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	waitForIndexedCalls(t, store, bucket, key, len(calls))

	queryAPI := func(discovery *scriptedDiscovery) string {
		api := httptest.NewServer(query.New(query.Options{
			Config:       query.Config{OverlapMargin: 5 * time.Minute, WideRangeLimit: 30 * 24 * time.Hour},
			ColdStore:    fake,
			HotDiscovery: discovery,
		}).Handler())
		t.Cleanup(api.Close)
		return api.URL
	}
	hotSrv := httptest.NewServer(hotread.New(store).Handler())
	t.Cleanup(hotSrv.Close)

	return sharedFixture{
		hotURL:  queryAPI(&scriptedDiscovery{urls: []string{hotSrv.URL}}),
		coldURL: queryAPI(&scriptedDiscovery{}),
		from:    baseMs - 60_000,
		to:      baseMs + 60_000,
		seal: func() {
			require.NoError(t, ac.CommandClose())
			_ = ac.Close()
			require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)
			_, err := store.Seal(ctx, key, bucket)
			require.NoError(t, err)
			_, err = hotstore.NewUploader(store, fake).Pass(ctx)
			require.NoError(t, err)
		},
	}
}

// threadNames returns the thread names of a /calls page, sorted.
func threadNames(t *testing.T, callsURL string) []string {
	t.Helper()
	names := []string{}
	for _, c := range decodeCallsJSON(t, callsURL) {
		names = append(names, c.ThreadName)
	}
	sort.Strings(names)
	return names
}
