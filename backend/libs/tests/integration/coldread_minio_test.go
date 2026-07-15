//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/collector"
	"github.com/Netcracker/qubership-profiler-backend/libs/collector/hotstore"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColdReadMinio proves the S3ObjectReader wiring against a real MinIO:
// discovery LISTs the sealed key, the projected scan reads it over ranged
// ReadAt, and cold /pods resolves the identity from the uploaded manifest —
// all under a shared-bucket S3_PATH_PREFIX applied on both sides. The
// behavioural scenarios live in TestColdReadPath on the in-test fake.
func TestColdReadMinio(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()
	mc := helpers.CreateMinioContainer(ctx)
	defer func() { _ = mc.Terminate(ctx) }()

	svc := startCollector(t, ctx, t.TempDir())
	store := svc.Store()

	file1, off1 := wire.TraceStream(timerStartMs, []wire.TraceChunk{
		{ThreadId: sealThread1, StartMs: baseMs, Events: []wire.TraceEvent{
			wire.Enter(0, sealMethodHandle), wire.Exit(1),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: sealMethodHandle, DurationMs: 10, ThreadName: "exec-1",
			TraceFileIndex: 1, BufferOffset: int(off1[0]), RecordIndex: 0},
	}

	ac := connectAgent(t, ctx)
	key := waitForPodRestart(t, store)
	pr, ok := store.PodRestart(key)
	require.True(t, ok)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(sealDictWords))
	sendStream(t, ac, model.StreamTrace, 0, file1)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(baseMs, calls))
	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()
	require.Eventually(t, pr.Finalized, 5*time.Second, 10*time.Millisecond)

	res, err := store.Seal(ctx, key, store.Config().Bucket(baseMs+5))
	require.NoError(t, err)
	require.Len(t, res.Files, 1)
	const pathPrefix = "team-a"
	_, err = hotstore.NewUploader(store, collector.NewS3ObjectStore(mc.Client, pathPrefix)).Pass(ctx)
	require.NoError(t, err)

	api := httptest.NewServer(query.New(query.Options{
		ColdStore: query.NewS3ObjectReader(mc.Client, pathPrefix),
	}).Handler())
	defer api.Close()

	page := getCalls(t, api, url.Values{
		"from": {fmt.Sprint(baseMs)}, "to": {fmt.Sprint(baseMs + 60_000)},
	})
	require.Len(t, page.Calls, 1)
	call := page.Calls[0]
	assert.Equal(t, baseMs+5, call.TsMs)
	assert.Equal(t, "com.example.Service.handle", call.Method)
	assert.Equal(t, hotstorePod, call.PK.PodName)
	assert.Equal(t, key.RestartTimeMs, call.PK.RestartTimeMs)
	assert.False(t, page.Partial)

	pods := getPods(t, api, url.Values{
		"from": {fmt.Sprint(baseMs)}, "to": {fmt.Sprint(baseMs + 60_000)},
	})
	require.Len(t, pods.Pods, 1)
	assert.Equal(t, hotstoreNs, pods.Pods[0].Namespace)
	assert.Equal(t, hotstoreSvc, pods.Pods[0].Service)
	assert.Equal(t, hotstorePod, pods.Pods[0].Pod)
	assert.Equal(t, key.RestartTimeMs, pods.Pods[0].RestartTimeMs)
}
