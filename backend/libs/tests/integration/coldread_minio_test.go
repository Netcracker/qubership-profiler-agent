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
	"github.com/Netcracker/qubership-profiler-backend/libs/query/cold"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColdReadMinio proves the S3ObjectReader wiring against a real MinIO:
// discovery LISTs the sealed key, the projected scan reads it over ranged
// ReadAt, and cold /pods resolves the identity from the uploaded manifest —
// all under a shared-bucket S3_PATH_PREFIX applied on both sides. Every GET
// of that read is counted in the cdt_minio_* "get" series, while a missing
// key and a cancelled read are not counted as errors. The behavioural
// scenarios live in TestColdReadPath on the in-test fake.
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

	reader := query.NewS3ObjectReader(mc.Client, pathPrefix)
	api := httptest.NewServer(query.New(query.Options{
		ColdStore: reader,
	}).Handler())
	defer api.Close()

	metrics := newMinioMetrics()
	getsBefore := readMinioCounts(t, metrics, s3.OperationGet)
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

	// Opening the parquet object and fetching the manifest count one object
	// each; the ranged reads of the parquet scan add requests on top.
	gets := readMinioCounts(t, metrics, s3.OperationGet).minus(getsBefore)
	assert.Positive(t, gets.objects, "objects opened or fetched")
	assert.Greater(t, gets.requests, gets.objects, "the ranged ReadAt GETs are counted as requests")
	assert.Zero(t, gets.errors)

	t.Run("a missing key is not an S3 error", func(t *testing.T) {
		before := readMinioCounts(t, metrics, s3.OperationGet)
		_, err := reader.Open(ctx, "parquet/v1/no-such-key.parquet")
		assert.ErrorIs(t, err, cold.ErrNotFound, "Open")
		_, err = reader.Get(ctx, "pods/v1/no-such-key.json")
		assert.ErrorIs(t, err, cold.ErrNotFound, "Get")
		assert.Equal(t, minioCounts{requests: 2}, readMinioCounts(t, metrics, s3.OperationGet).minus(before))
	})

	t.Run("a read the caller cancelled is not an S3 error", func(t *testing.T) {
		before := readMinioCounts(t, metrics, s3.OperationGet)
		readCtx, cancelRead := context.WithCancel(ctx)
		obj, err := reader.Open(readCtx, res.Files[0].S3Key)
		require.NoError(t, err)
		defer func() { _ = obj.Close() }()
		cancelRead()
		_, err = obj.ReadAt(make([]byte, 4), 0)
		require.ErrorIs(t, err, context.Canceled, "ReadAt after the caller cancelled")

		_, err = reader.Get(readCtx, res.Files[0].S3Key)
		require.ErrorIs(t, err, context.Canceled, "Get under a cancelled caller")

		assert.Equal(t, minioCounts{requests: 1, objects: 1},
			readMinioCounts(t, metrics, s3.OperationGet).minus(before), "only the Open completed")
	})
}

// minioCounts is one reading of the cdt_minio_* series of one operation.
type minioCounts struct {
	requests float64 // cdt_minio_operation_latency_seconds sample count
	objects  float64
	errors   float64
}

func (c minioCounts) minus(before minioCounts) minioCounts {
	return minioCounts{c.requests - before.requests, c.objects - before.objects, c.errors - before.errors}
}

// newMinioMetrics returns a registry that exposes the cdt_minio_* series.
// The series are process-wide, so a test compares two readings rather than
// absolute values.
func newMinioMetrics() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	s3.RegisterMetrics(reg)
	return reg
}

func readMinioCounts(t *testing.T, reg *prometheus.Registry, op string) minioCounts {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	var c minioCounts
	for _, f := range families {
		for _, m := range f.GetMetric() {
			matches := false
			for _, l := range m.GetLabel() {
				matches = matches || (l.GetName() == s3.OperationLabel && l.GetValue() == op)
			}
			if !matches {
				continue
			}
			switch f.GetName() {
			case "cdt_minio_operation_latency_seconds":
				c.requests = float64(m.GetHistogram().GetSampleCount())
			case "cdt_minio_operation_objects_count":
				c.objects = m.GetCounter().GetValue()
			case "cdt_minio_operation_errors_count":
				c.errors = m.GetCounter().GetValue()
			}
		}
	}
	return c
}
