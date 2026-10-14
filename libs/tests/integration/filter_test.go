package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotread"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const filterPod = "pod-filter"

// Dictionary word ids follow arrival order.
const (
	filterMethodHandle = 0
	filterDictRequest  = 1
)

var filterDictWords = []string{"com.example.Service.handle", "x-request-id"}

// TestFilterByRequestIdOnBothTiers is the acceptance check of agent-access
// task 1.2: a call is found by its x-request-id through `filter=request_id=`
// (02 §2.3.3), first while it is hot and again after it is sealed to the cold
// tier.
func TestFilterByRequestIdOnBothTiers(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()

	fake := newColdFakeStore()
	svc := startCollector(t, ctx, t.TempDir())
	store := svc.Store()
	bucket := store.Config().Bucket(baseMs + 5)

	traceFile, offs := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: 81, StartMs: baseMs + 5, Events: []wire.TraceEvent{
			wire.Enter(0, filterMethodHandle), wire.Tag(1, filterDictRequest, "abc123"), wire.Exit(2),
		}},
		{ThreadId: 82, StartMs: baseMs + 30, Events: []wire.TraceEvent{
			wire.Enter(0, filterMethodHandle), wire.Tag(1, filterDictRequest, "def456"), wire.Exit(2),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: filterMethodHandle, DurationMs: 50, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(offs[0]), RecordIndex: 0,
			Params: map[int][]string{filterDictRequest: {"abc123"}}},
		{DeltaMs: 25, Method: filterMethodHandle, DurationMs: 50, ThreadName: "exec-2",
			TraceFileIndex: 1, BufferOffset: int(offs[1]), RecordIndex: 0,
			Params: map[int][]string{filterDictRequest: {"def456"}}},
	}

	ac := connectAgentAs(t, ctx, filterPod)
	key := waitForPodNamed(t, store, filterPod, 1)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(filterDictWords))
	sendStream(t, ac, model.StreamTrace, 0, traceFile)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	waitForIndexedCalls(t, store, bucket, key, 2)

	params := url.Values{
		"from":   {fmt.Sprint(baseMs - 60_000)},
		"to":     {fmt.Sprint(baseMs + 60_000)},
		"filter": {"request_id=abc123"},
	}
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
	hotURL := queryAPI(&scriptedDiscovery{urls: []string{hotSrv.URL}})
	hot := decodeCallsJSON(t, hotURL+"/api/v1/calls?"+params.Encode())
	require.Len(t, hot, 1, "the hot tier finds the call by its request ID")
	assert.Equal(t, []string{"abc123"}, hot[0].Params["x-request-id"])
	assert.Equal(t, filterPod, hot[0].PK.PodName)

	all := decodeCallsJSON(t, hotURL+"/api/v1/calls?"+url.Values{"from": params["from"], "to": params["to"]}.Encode())
	require.Len(t, all, 2, "without the filter both calls come back")

	// Seal and upload, then read through a query service with no replicas.
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)
	_, err := store.Seal(ctx, key, bucket)
	require.NoError(t, err)
	_, err = hotstore.NewUploader(store, fake).Pass(ctx)
	require.NoError(t, err)

	cold := decodeCallsJSON(t, queryAPI(&scriptedDiscovery{})+"/api/v1/calls?"+params.Encode())
	require.Len(t, cold, 1, "the cold tier finds the call by its request ID")
	assert.Equal(t, hot[0].PK, cold[0].PK, "both tiers return the same call")
}
