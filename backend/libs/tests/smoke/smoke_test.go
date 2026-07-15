//go:build smoke

// Package smoke proves the Stage 1 stack end to end against the running
// docker-compose services (backend/docker-compose.yaml): a synthetic agent
// feeds the collector over a real TCP socket, the seal and upload loops move
// the aged bucket into MinIO, and the query service answers /api/v1 from the
// hot tier, then — with the collector stopped — from S3 alone.
//
// Run `make smoke` from backend/, or bring the stack up yourself and run
// `go test -tags smoke -count=1 ./libs/tests/smoke/...`. The test expects a
// FRESH stack: a bucket holding parquet from an earlier run fails the
// hot-phase "nothing sealed yet" assertion.
package smoke

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/calltree"
	"github.com/Netcracker/qubership-profiler-backend/libs/emulator"
	profio "github.com/Netcracker/qubership-profiler-backend/libs/io"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	model "github.com/Netcracker/qubership-profiler-backend/libs/protocol"
	querymodel "github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers/wire"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nsSmoke  = "smoke"
	svcSmoke = "smoke-svc"
	podHot   = "pod-smoke-hot"
	podCold  = "pod-smoke-cold"

	methodHandle = 0 // "com.example.Api.handle"
	methodQuery  = 1 // "com.example.Db.query"
	tagRequestID = 2 // "request.id"
)

var dictWords = []string{"com.example.Api.handle", "com.example.Db.query", "request.id"}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

var (
	agentAddr   = envOr("SMOKE_AGENT_ADDR", "localhost:1715")
	queryURL    = envOr("SMOKE_QUERY_URL", "http://localhost:8080")
	internalURL = envOr("SMOKE_INTERNAL_URL", "http://localhost:8081")
	s3Endpoint  = envOr("SMOKE_S3_ENDPOINT", "localhost:9000")
	s3Access    = envOr("SMOKE_S3_ACCESS_KEY", "minioadmin")
	s3Secret    = envOr("SMOKE_S3_SECRET_KEY", "minioadmin")
	s3Bucket    = envOr("SMOKE_S3_BUCKET", "profiler-data")
)

// timeBucket must match the collector's PROFILER_TIME_BUCKET.
const timeBucket = 5 * time.Minute

// composeDir locates backend/docker-compose.yaml for the mid-test
// stop/start of the collector container.
func composeDir(t *testing.T) string {
	if dir := os.Getenv("SMOKE_COMPOSE_DIR"); dir != "" {
		return dir
	}
	_, self, _, ok := runtime.Caller(0)
	require.True(t, ok, "cannot locate the smoke test source for SMOKE_COMPOSE_DIR")
	return filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(self)))) // libs/tests/smoke → backend
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = composeDir(t)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "docker compose %v: %s", args, out)
}

// stopCollector / startCollector take the collector away out-of-band for the
// cold phase. Default: the compose container. The SMOKE_COLLECTOR_{STOP,START}_CMD
// hooks switch the mechanism without forking the test — the kind smoke
// (deploy/kind-smoke.sh) scales the StatefulSet to zero instead.
func stopCollector(t *testing.T) {
	t.Helper()
	if cmd := os.Getenv("SMOKE_COLLECTOR_STOP_CMD"); cmd != "" {
		runShell(t, cmd)
		return
	}
	compose(t, "stop", "collector")
}

func startCollector(t *testing.T) {
	t.Helper()
	if cmd := os.Getenv("SMOKE_COLLECTOR_START_CMD"); cmd != "" {
		runShell(t, cmd)
		return
	}
	compose(t, "start", "collector")
}

func runShell(t *testing.T, script string) {
	t.Helper()
	out, err := exec.Command("sh", "-c", script).CombinedOutput()
	require.NoError(t, err, "sh -c %q: %s", script, out)
}

func TestStage1EndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(log.SetLevel(context.Background(), log.INFO))
	defer cancel()

	mc, err := minio.New(s3Endpoint, &minio.Options{
		Creds: credentials.NewStaticV4(s3Access, s3Secret, ""),
	})
	require.NoError(t, err)

	waitReady(t, internalURL+"/internal/v1/health/ready", 2*time.Minute)
	waitReady(t, queryURL+"/api/v1/health/ready", time.Minute)

	// --- Hot phase: recent calls stay un-sealed and serve via the fan-out.
	//
	// The bucket of a call seals TimeBucketGrace after the bucket ends
	// (01 §6.1), so start clear of a boundary: with `now % bucket` in
	// [45 s, bucket-90 s], a call stamped now-30 s sits in the current bucket
	// and stays hot for at least another minute.
	nowMs := waitForBucketHeadroom(t)
	hotBase := nowMs - 30_000

	trace, offs := wire.TraceStream(hotBase-1_000, []wire.TraceChunk{
		{ThreadId: 11, StartMs: hotBase + 5, Events: []wire.TraceEvent{
			wire.Enter(0, methodHandle),
			wire.Tag(1, tagRequestID, "req-hot-1"),
			wire.Enter(2, methodQuery), wire.Exit(10),
			wire.Exit(20),
		}},
		{ThreadId: 22, StartMs: hotBase + 40, Events: []wire.TraceEvent{
			wire.Enter(0, methodQuery), wire.Exit(7),
		}},
	})
	calls := []wire.CallRecord{
		{DeltaMs: 5, Method: methodHandle, DurationMs: 33, ChildCalls: 1, ThreadName: "http-1",
			TraceFileIndex: 1, BufferOffset: int(offs[0]), RecordIndex: 0,
			Params: map[int][]string{tagRequestID: {"req-hot-1"}}},
		{DeltaMs: 35, Method: methodQuery, DurationMs: 7, ThreadName: "http-2",
			TraceFileIndex: 1, BufferOffset: int(offs[1]), RecordIndex: 0},
	}

	ac := connectAgent(t, ctx, podHot)
	sendStream(t, ac, model.StreamDictionary, 0, wire.DictionaryStream(dictWords))
	sendStream(t, ac, model.StreamTrace, 0, trace)
	sendStream(t, ac, model.StreamCalls, 0, wire.CallsStreamRecords(hotBase, calls))
	sendStream(t, ac, model.StreamSuspend, 0, wire.SuspendStream(hotBase, []wire.SuspendEvent{
		{DeltaMs: 10, AmountMs: 3},
	}))

	hotRows := pollCalls(t, podHot, hotBase-60_000, nowMs+60_000, 2, time.Minute)
	byMethod := map[string]callRow{}
	for _, r := range hotRows {
		byMethod[r.Method] = r
	}
	require.Contains(t, byMethod, "com.example.Api.handle",
		"the hot list must resolve method names from the live dictionary")
	require.Contains(t, byMethod, "com.example.Db.query")
	handle := byMethod["com.example.Api.handle"]
	assert.Equal(t, hotBase+5, handle.TsMs)
	assert.Equal(t, int32(33), handle.DurationMs)
	assert.Equal(t, "short_clean", handle.RetentionClass)

	// Nothing is sealed yet, so the rows above came from the hot tier alone.
	assertPrefixEmpty(t, ctx, mc, "parquet/v1/")

	tree := fetchTree(t, handle.PK, nil)
	assertHotTree(t, tree)

	// /metrics is part of the deploy acceptance: the key series must exist on
	// both services (the names are the dashboard/alert contract).
	assertMetricsContain(t, internalURL+"/metrics",
		"profiler_seal_rows_total", "profiler_hotstore_quarantine_objects")
	assertMetricsContain(t, queryURL+"/metrics",
		"profiler_query_cold_lists_total", "profiler_query_fanout_replica_request_seconds")

	// A clean close finalizes the pod-restart; its bucket still seals only
	// when it ages out, which keeps the hot rows hot for the rest of the run.
	require.NoError(t, ac.CommandClose())
	_ = ac.Close()

	// --- Cold phase: a two-hour-old bucket seals and uploads, then the
	// collector goes away and the wide range answers from S3 alone.
	coldBase := nowMs - 2*time.Hour.Milliseconds()
	coldTrace, coldOffs := wire.TraceStream(coldBase-1_000, []wire.TraceChunk{
		{ThreadId: 31, StartMs: coldBase + 5, Events: []wire.TraceEvent{
			wire.Enter(0, methodHandle),
			wire.Tag(1, tagRequestID, "req-cold-1"),
			wire.Exit(3),
		}},
		{ThreadId: 32, StartMs: coldBase + 20, Events: []wire.TraceEvent{
			wire.Enter(0, methodQuery), wire.Exit(2),
		}},
	})
	coldCalls := []wire.CallRecord{
		{DeltaMs: 5, Method: methodHandle, DurationMs: 4, ThreadName: "worker-1",
			TraceFileIndex: 1, BufferOffset: int(coldOffs[0]), RecordIndex: 0,
			Params: map[int][]string{tagRequestID: {"req-cold-1"}}},
		{DeltaMs: 15, Method: methodQuery, DurationMs: 1_500, ThreadName: "worker-2",
			TraceFileIndex: 1, BufferOffset: int(coldOffs[1]), RecordIndex: 0},
	}

	acCold := connectAgent(t, ctx, podCold)
	sendStream(t, acCold, model.StreamDictionary, 0, wire.DictionaryStream(dictWords))
	sendStream(t, acCold, model.StreamTrace, 0, coldTrace)
	sendStream(t, acCold, model.StreamCalls, 0, wire.CallsStreamRecords(coldBase, coldCalls))
	sendStream(t, acCold, model.StreamSuspend, 0, wire.SuspendStream(coldBase, []wire.SuspendEvent{
		{DeltaMs: 8, AmountMs: 2},
	}))
	require.NoError(t, acCold.CommandClose())
	_ = acCold.Close()

	// The aged bucket is due immediately; the compose loops run every 2 s.
	waitPrefixNonEmpty(t, ctx, mc, "parquet/v1/", 90*time.Second)
	waitPrefixNonEmpty(t, ctx, mc, "dictionaries/v1/", 90*time.Second)

	stopCollector(t)
	t.Cleanup(func() { startCollector(t) })

	coldRows := pollCalls(t, podCold, coldBase-60_000, nowMs, 2, time.Minute)
	coldByMethod := map[string]callRow{}
	for _, r := range coldRows {
		coldByMethod[r.Method] = r
	}
	require.Contains(t, coldByMethod, "com.example.Db.query",
		"the wide range must answer from S3 with the collector down")
	longCall := coldByMethod["com.example.Db.query"]
	assert.Equal(t, "long_clean", longCall.RetentionClass, "1500 ms lands past the 1 s threshold")
	assert.Equal(t, coldBase+20, longCall.TsMs)

	// The cold point fetch needs the §2.2 hints; the per-tree dictionary
	// comes from the pod-restart's S3 snapshot.
	coldTree := fetchTree(t, longCall.PK, map[string]string{
		"ts_ms":           fmt.Sprint(longCall.TsMs),
		"retention_class": longCall.RetentionClass,
	})
	require.NotNil(t, coldTree.Root)
	assert.Equal(t, "com.example.Db.query", coldTree.Methods[coldTree.Root.MethodIdx],
		"cold /tree must resolve names from the dictionary snapshot")
	assert.EqualValues(t, 2, coldTree.Root.DurationMs)

	// --- Recovery: the restarted collector recovers the PV and the hot rows
	// come back (their bucket may have sealed meanwhile; either tier serves).
	startCollector(t)
	waitReady(t, internalURL+"/internal/v1/health/ready", 2*time.Minute)
	pollCalls(t, podHot, hotBase-60_000, nowMs+60_000, 2, time.Minute)
}

// waitForBucketHeadroom blocks until `now % timeBucket` falls into
// [45 s, timeBucket-90 s] and returns the reached Unix ms.
func waitForBucketHeadroom(t *testing.T) int64 {
	t.Helper()
	for {
		now := time.Now()
		offset := now.UnixMilli() % timeBucket.Milliseconds()
		if offset >= 45_000 && offset <= (timeBucket-90*time.Second).Milliseconds() {
			return now.UnixMilli()
		}
		wait := time.Duration(45_000-offset) * time.Millisecond
		if wait < 0 {
			wait += timeBucket
		}
		t.Logf("waiting %s for bucket headroom", wait.Round(time.Second))
		time.Sleep(wait)
	}
}

func waitReady(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode == http.StatusOK
	}, timeout, 500*time.Millisecond, "service at %s must report READY", url)
}

// connectAgent dials and initializes with a retry: behind a kubectl
// port-forward the local accept succeeds before the backend stream exists, so
// the first connection can die with an EOF while the tunnel (or the collector
// endpoint) warms up.
func connectAgent(t *testing.T, ctx context.Context, pod string) *emulator.AgentConnection {
	t.Helper()
	var ac *emulator.AgentConnection
	require.Eventually(t, func() bool {
		ac = emulator.PrepareAgent(ctx, nil, noopListener{}, pod)
		if err := ac.Prepare(emulator.ConnectionOpts{
			ProtocolAddress: agentAddr,
			Timeout: profio.TcpTimeout{
				ConnectTimeout: 10 * time.Second,
				SessionTimeout: 60 * time.Second,
				ReadTimeout:    5 * time.Second,
				WriteTimeout:   5 * time.Second,
			},
		}).Connect(); err != nil {
			t.Logf("agent connect to %s: %s", agentAddr, err)
			return false
		}
		if err := ac.InitializeConnection(model.PROTOCOL_VERSION_V3, nsSmoke, svcSmoke, pod); err != nil {
			t.Logf("agent init on %s: %s", agentAddr, err)
			_ = ac.Close()
			return false
		}
		return true
	}, 30*time.Second, time.Second, "agent must connect and initialize against %s", agentAddr)
	require.Equal(t, model.PROTOCOL_VERSION_V2, ac.ServerVersion(),
		"the collector must reply V2 (06-wire-protocol-server.md §3)")
	return ac
}

// sendStream opens one agent stream file and feeds it in RCV_DATA payloads,
// splitting the first payload to cross a payload boundary mid-chunk
// (backend/CLAUDE.md: a logical chunk spans many payloads).
func sendStream(t *testing.T, ac *emulator.AgentConnection, stream string, requestedSeq int, data []byte) {
	t.Helper()
	handle, err := ac.CommandInitStream(stream, requestedSeq, false)
	require.NoError(t, err)
	require.NotEqual(t, [16]byte{}, handle.ToBin(), "stream %s must get a non-nil handle", stream)

	split := min(10, len(data))
	require.NoError(t, ac.CommandRcvData(stream, handle, data[:split]))
	for pos := split; pos < len(data); pos += emulator.MaxBufSize {
		end := min(pos+emulator.MaxBufSize, len(data))
		require.NoError(t, ac.CommandRcvData(stream, handle, data[pos:end]))
	}
	require.NoError(t, ac.Flush())
	require.NoError(t, ac.WaitForAcks())
}

// callRow is the subset of the /calls row the smoke asserts on (02 §2.3).
type callRow struct {
	PK             querymodel.PK       `json:"pk"`
	TsMs           int64               `json:"ts_ms"`
	DurationMs     int32               `json:"duration_ms"`
	Method         string              `json:"method"`
	ErrorFlag      bool                `json:"error_flag"`
	RetentionClass string              `json:"retention_class"`
	Params         map[string][]string `json:"params"`
}

type callsPage struct {
	Calls          []callRow `json:"calls"`
	Partial        bool      `json:"partial"`
	PartialReasons []string  `json:"partial_reasons"`
}

// pollCalls queries /api/v1/calls for one pod's window until it holds `want`
// rows.
func pollCalls(t *testing.T, pod string, fromMs, toMs int64, want int, timeout time.Duration) []callRow {
	t.Helper()
	var rows []callRow
	require.Eventually(t, func() bool {
		q := url.Values{}
		q.Set("from", fmt.Sprint(fromMs))
		q.Set("to", fmt.Sprint(toMs))
		q.Set("pod", nsSmoke+"/"+svcSmoke+"/"+pod)
		q.Set("limit", "100")
		resp, err := http.Get(queryURL + "/api/v1/calls?" + q.Encode())
		if err != nil {
			t.Logf("/calls: %s", err)
			return false
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Logf("/calls: %d %s", resp.StatusCode, body)
			return false
		}
		var page callsPage
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			t.Logf("/calls decode: %s", err)
			return false
		}
		if page.Partial {
			t.Logf("/calls partial: %v", page.PartialReasons)
		}
		rows = page.Calls
		return len(rows) >= want
	}, timeout, time.Second, "pod %s must show %d calls in [%d, %d]", pod, want, fromMs, toMs)
	require.Len(t, rows, want, "duplicate rows must not survive the PK dedup")
	return rows
}

// fetchTree GETs /api/v1/calls/{pk}/tree and decodes the §2.5 MessagePack.
func fetchTree(t *testing.T, pk querymodel.PK, params map[string]string) *calltree.Tree {
	t.Helper()
	u := queryURL + "/api/v1/calls/" + url.PathEscape(pk.PathString()) + "/tree"
	if len(params) > 0 {
		q := url.Values{}
		for k, v := range params {
			q.Set(k, v)
		}
		u += "?" + q.Encode()
	}
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "/tree: %s", body)
	require.Equal(t, "application/x-msgpack", resp.Header.Get("Content-Type"))
	tree, _, err := calltree.Decode(body)
	require.NoError(t, err)
	return tree
}

// assertHotTree pins the hot-tier tree: handle → query nesting with the tag
// value resolved from the live dictionary.
func assertHotTree(t *testing.T, tree *calltree.Tree) {
	t.Helper()
	require.NotNil(t, tree.Root)
	assert.Equal(t, "com.example.Api.handle", tree.Methods[tree.Root.MethodIdx])
	assert.EqualValues(t, 33, tree.Root.DurationMs)
	require.Len(t, tree.Root.Children, 1)
	child := tree.Root.Children[0]
	assert.Equal(t, "com.example.Db.query", tree.Methods[child.MethodIdx])
	assert.EqualValues(t, 10, child.DurationMs)
	require.Len(t, tree.Root.Params, 1)
	assert.Equal(t, "request.id", tree.Params[tree.Root.Params[0].ParamIdx])
	assert.Equal(t, []string{"req-hot-1"}, tree.Root.Params[0].Values)
}

// assertMetricsContain scrapes a /metrics endpoint and requires the named
// series (or their HELP lines) to be present.
func assertMetricsContain(t *testing.T, url string, series ...string) {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "/metrics: %s", body)
	for _, s := range series {
		assert.Contains(t, string(body), s, "series %s missing from %s", s, url)
	}
}

func assertPrefixEmpty(t *testing.T, ctx context.Context, mc *minio.Client, prefix string) {
	t.Helper()
	for obj := range mc.ListObjects(ctx, s3Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		require.NoError(t, obj.Err)
		t.Fatalf("expected no objects under %s yet, found %s — is the stack fresh?", prefix, obj.Key)
	}
}

func waitPrefixNonEmpty(t *testing.T, ctx context.Context, mc *minio.Client, prefix string, timeout time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		for obj := range mc.ListObjects(ctx, s3Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err == nil {
				t.Logf("found %s", obj.Key)
				return true
			}
		}
		return false
	}, timeout, time.Second, "an object must appear under %s", prefix)
}

// noopListener satisfies the emulator's observer interface.
type noopListener struct{}

func (noopListener) Command(model.Command, time.Duration, error) {}
func (noopListener) Read(int, time.Duration, error)              {}
func (noopListener) Write(int, time.Duration, error)             {}
func (noopListener) Error(error)                                 {}
func (noopListener) IsAlive() (bool, error)                      { return true, nil }
