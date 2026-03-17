////go:build integration

package server_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client/sqlite"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/server"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	testService1  = "svc-one"
	testService2  = "svc-two"
	testPodHash   = "abc123"
	testPodSuffix = "x1y2z"
	restartTsMs   = int64(1719318147399)
)

func testPodName(service string) string {
	return fmt.Sprintf("%s-%s-%s_%d", service, testPodHash, testPodSuffix, restartTsMs)
}

type E2ETestSuite struct {
	suite.Suite
	ctx      context.Context
	cancel   context.CancelFunc
	baseDir  string
	baseURL  string
	dbClient db.DumpDbClient
}

func (s *E2ETestSuite) SetupSuite() {
	s.ctx = log.SetLevel(log.Context("e2e"), log.DEBUG)

	tmpDir, err := os.MkdirTemp("", "dumps-e2e-*")
	require.NoError(s.T(), err)
	s.baseDir = tmpDir

	s.dbClient, err = sqlite.NewClient(s.ctx, db.DBParams{
		DBPath: "file:e2e-suite?mode=memory&cache=shared",
	})
	require.NoError(s.T(), err)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(s.T(), err)
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()

	bindAddr := fmt.Sprintf("127.0.0.1:%d", port)
	s.baseURL = fmt.Sprintf("http://%s", bindAddr)

	rp, err := task.NewRequestProcessor(s.baseDir, s.dbClient, false)
	require.NoError(s.T(), err)

	s.ctx, s.cancel = context.WithCancel(s.ctx)
	dbClient := s.dbClient
	baseDir := s.baseDir
	go func() {
		_ = server.StartHttpServer(s.ctx, rp, bindAddr, func(e *echo.Echo) {
			e.POST("/esc/rescan", func(c echo.Context) error {
				rt, err := task.NewRescanTask(baseDir, dbClient)
				if err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				if err := rt.Execute(c.Request().Context()); err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				return c.JSON(200, map[string]string{"status": "ok"})
			})
			e.POST("/esc/insert", func(c echo.Context) error {
				dateFromMs, err := getQueryInt64Param(c, "dateFrom")
				if err != nil {
					return c.JSON(400, map[string]string{"error": err.Error()})
				}
				dateToMs, err := getQueryInt64Param(c, "dateTo")
				if err != nil {
					return c.JSON(400, map[string]string{"error": err.Error()})
				}
				it, err := task.NewInsertTask(baseDir, dbClient)
				if err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				if err := it.Execute(c.Request().Context(), time.UnixMilli(dateFromMs).UTC(), time.UnixMilli(dateToMs).UTC()); err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				return c.JSON(200, map[string]string{"status": "ok"})
			})
			e.POST("/esc/pack", func(c echo.Context) error {
				beforeMs, err := getQueryInt64Param(c, "before")
				if err != nil {
					return c.JSON(400, map[string]string{"error": err.Error()})
				}
				pt, err := task.NewPackTask(baseDir, dbClient)
				if err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				if err := pt.Execute(c.Request().Context(), time.UnixMilli(beforeMs).UTC()); err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				return c.JSON(200, map[string]string{"status": "ok"})
			})
			e.GET("/esc/statistics", func(c echo.Context) error {
				dateFromMs, err := getQueryInt64Param(c, "dateFrom")
				if err != nil {
					return c.JSON(400, map[string]string{"error": err.Error()})
				}
				dateToMs, err := getQueryInt64Param(c, "dateTo")
				if err != nil {
					return c.JSON(400, map[string]string{"error": err.Error()})
				}
				dateFrom := time.UnixMilli(dateFromMs).UTC()
				dateTo := time.UnixMilli(dateToMs).UTC()
				stats, err := rp.StatisticRequest(c.Request().Context(), dateFrom, dateTo, model.EmptyPodFilter{})
				if err != nil {
					return c.JSON(500, map[string]string{"error": err.Error()})
				}
				return c.JSON(200, stats)
			})
		})
	}()

	require.Eventually(s.T(), func() bool {
		resp, err := http.Get(s.baseURL + "/esc/health")
		if err != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusNoContent
	}, 3*time.Second, 50*time.Millisecond, "server did not become ready")
}

func (s *E2ETestSuite) TearDownSuite() {
	s.cancel()
	if s.dbClient != nil {
		_ = s.dbClient.CloseConnection(s.ctx)
	}
	if s.baseDir != "" {
		_ = os.RemoveAll(s.baseDir)
	}
}

// createDumpFile writes a dump file into the PV directory structure.
// Path: baseDir/namespace/YYYY/MM/DD/HH/mm/ss/podNameWithTs/filename
func (s *E2ETestSuite) createDumpFile(namespace string, t time.Time, podName string, dumpType model.DumpType, content []byte) {
	dir := filepath.Join(s.baseDir, namespace, task.FileSecondDirInPV(t), podName)
	require.NoError(s.T(), os.MkdirAll(dir, 0o755))
	filename := task.FileNameInPV(t) + string(dumpType.GetFileSuffix())
	require.NoError(s.T(), os.WriteFile(filepath.Join(dir, filename), content, 0o644))
}

func (s *E2ETestSuite) rescan() {
	resp, err := http.Post(s.baseURL+"/esc/rescan", "", nil)
	require.NoError(s.T(), err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(s.T(), 200, resp.StatusCode)
}

func (s *E2ETestSuite) insert(dateFrom, dateTo time.Time) {
	url := fmt.Sprintf("%s/esc/insert?dateFrom=%d&dateTo=%d", s.baseURL, dateFrom.UnixMilli(), dateTo.UnixMilli())
	resp, err := http.Post(url, "", nil)
	require.NoError(s.T(), err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(s.T(), 200, resp.StatusCode)
}

func (s *E2ETestSuite) pack(before time.Time) {
	url := fmt.Sprintf("%s/esc/pack?before=%d", s.baseURL, before.UnixMilli())
	resp, err := http.Post(url, "", nil)
	require.NoError(s.T(), err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(s.T(), 200, resp.StatusCode)
}

func (s *E2ETestSuite) getStatistics(dateFrom, dateTo time.Time) []*model.StatisticItem {
	url := fmt.Sprintf("%s/esc/statistics?dateFrom=%d&dateTo=%d", s.baseURL, dateFrom.UnixMilli(), dateTo.UnixMilli())
	resp, err := http.Get(url)
	require.NoError(s.T(), err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(s.T(), 200, resp.StatusCode)

	var stats []*model.StatisticItem
	require.NoError(s.T(), json.NewDecoder(resp.Body).Decode(&stats))
	return stats
}

// Each test uses a unique namespace to avoid cross-test interference,
// since all tests share the same server, DB, and PV directory.

func (s *E2ETestSuite) TestSinglePodThreadDumps() {
	t := s.T()
	ns := "ns-td"
	pod := testPodName(testService1)
	t1 := time.Date(2024, 8, 1, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2024, 8, 1, 10, 1, 0, 0, time.UTC)
	t3 := time.Date(2024, 8, 1, 10, 2, 0, 0, time.UTC)

	s.createDumpFile(ns, t1, pod, model.TdDumpType, []byte("thread-dump-1"))   // 13 bytes
	s.createDumpFile(ns, t2, pod, model.TdDumpType, []byte("thread-dump-22"))  // 14 bytes
	s.createDumpFile(ns, t3, pod, model.TdDumpType, []byte("thread-dump-333")) // 15 bytes
	s.rescan()

	stats := s.getStatistics(
		time.Date(2024, 8, 1, 10, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 10, 3, 0, 0, time.UTC))

	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat, "expected stats for namespace %s", ns)
	require.Equal(t, testService1, stat.ServiceName)
	require.Equal(t, pod, stat.PodName)
	require.Equal(t, restartTsMs, stat.ActiveSinceMillis)
	require.Equal(t, t1.UnixMilli(), stat.FirstSamleMillis)
	require.Equal(t, t3.UnixMilli(), stat.LastSampleMillis)
	require.Equal(t, int64(13+14+15), stat.DataAtEnd)
	require.Empty(t, stat.HeapDumps)
}

func (s *E2ETestSuite) TestSinglePodMixedDumpTypes() {
	t := s.T()
	ns := "ns-mixed"
	pod := testPodName(testService1)
	ts := time.Date(2024, 8, 1, 14, 30, 0, 0, time.UTC)

	tdContent := []byte("td-content-here")
	topContent := []byte("top-content-here!")
	heapContent := []byte("fake-heap-dump-binary-data-here")

	s.createDumpFile(ns, ts, pod, model.TdDumpType, tdContent)
	s.createDumpFile(ns, ts, pod, model.TopDumpType, topContent)
	s.createDumpFile(ns, ts, pod, model.HeapDumpType, heapContent)
	s.rescan()

	stats := s.getStatistics(
		time.Date(2024, 8, 1, 14, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 15, 0, 0, 0, time.UTC))

	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat)
	require.Equal(t, int64(len(tdContent)+len(topContent)), stat.DataAtEnd)
	require.Equal(t, 1, len(stat.HeapDumps))
	require.Equal(t, int64(len(heapContent)), stat.HeapDumps[0].Bytes)
	require.Equal(t, ts.UnixMilli(), stat.HeapDumps[0].Date)
	require.NotEmpty(t, stat.HeapDumps[0].Handle)
}

func (s *E2ETestSuite) TestMultiplePodsSameHour() {
	t := s.T()
	ns := "ns-multi"
	pod1 := testPodName(testService1)
	pod2 := testPodName(testService2)
	ts := time.Date(2024, 8, 1, 16, 0, 0, 0, time.UTC)

	s.createDumpFile(ns, ts, pod1, model.TdDumpType, []byte("aaa"))
	s.createDumpFile(ns, ts, pod2, model.TdDumpType, []byte("bbbbb"))
	s.rescan()

	stats := s.getStatistics(
		time.Date(2024, 8, 1, 16, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 17, 0, 0, 0, time.UTC))

	nsStats := filterByNamespace(stats, ns)
	require.Equal(t, 2, len(nsStats))

	byService := map[string]*model.StatisticItem{}
	for _, stat := range nsStats {
		byService[stat.ServiceName] = stat
	}

	require.Contains(t, byService, testService1)
	require.Contains(t, byService, testService2)
	require.Equal(t, int64(3), byService[testService1].DataAtEnd)
	require.Equal(t, int64(5), byService[testService2].DataAtEnd)
}

func (s *E2ETestSuite) TestCrossHourPartitioning() {
	t := s.T()
	ns := "ns-cross"
	pod := testPodName(testService1)

	t1 := time.Date(2024, 8, 1, 22, 58, 0, 0, time.UTC)
	t2 := time.Date(2024, 8, 1, 22, 59, 0, 0, time.UTC)
	t3 := time.Date(2024, 8, 1, 23, 0, 0, 0, time.UTC)
	t4 := time.Date(2024, 8, 1, 23, 1, 0, 0, time.UTC)

	s.createDumpFile(ns, t1, pod, model.TdDumpType, []byte("h22-a"))    // 5
	s.createDumpFile(ns, t2, pod, model.TdDumpType, []byte("h22-bb"))   // 6
	s.createDumpFile(ns, t3, pod, model.TdDumpType, []byte("h23-ccc"))  // 7
	s.createDumpFile(ns, t4, pod, model.TdDumpType, []byte("h23-dddd")) // 8
	s.rescan()

	stats := s.getStatistics(
		time.Date(2024, 8, 1, 22, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 2, 0, 0, 0, 0, time.UTC))

	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat)
	require.Equal(t, int64(5+6+7+8), stat.DataAtEnd)
	require.Equal(t, t1.UnixMilli(), stat.FirstSamleMillis)
	require.Equal(t, t4.UnixMilli(), stat.LastSampleMillis)

	// Query only hour 23
	stats23 := s.getStatistics(
		time.Date(2024, 8, 1, 23, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 2, 0, 0, 0, 0, time.UTC))

	stat23 := findByNamespace(stats23, ns)
	require.NotNil(t, stat23)
	require.Equal(t, int64(7+8), stat23.DataAtEnd)
}

func (s *E2ETestSuite) TestDownloadTdTopDumps() {
	t := s.T()
	ns := "ns-dl-td"
	pod := testPodName(testService1)
	ts := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	content := []byte("downloadable-thread-dump")

	s.createDumpFile(ns, ts, pod, model.TdDumpType, content)
	s.rescan()

	url := fmt.Sprintf("%s/cdt/v2/download?dateFrom=%d&dateTo=%d&type=td&namespace=%s",
		s.baseURL,
		time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC).UnixMilli(),
		time.Date(2024, 8, 1, 13, 0, 0, 0, time.UTC).UnixMilli(),
		ns)

	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)
	require.Contains(t, resp.Header.Get("Content-Type"), "application/zip")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.Equal(t, 1, len(zr.File), "ZIP should contain exactly 1 td dump file")

	// Verify the ZIP entry is under the correct pod directory and has .td.txt suffix
	require.Contains(t, zr.File[0].Name, pod+"/")
	require.Contains(t, zr.File[0].Name, ".td.txt")

	zf, err := zr.File[0].Open()
	require.NoError(t, err)
	defer func() { _ = zf.Close() }()
	fileContent, err := io.ReadAll(zf)
	require.NoError(t, err)
	require.Equal(t, content, fileContent)
}

func (s *E2ETestSuite) TestDownloadHeapDump() {
	t := s.T()
	ns := "ns-dl-heap"
	pod := testPodName(testService1)
	ts := time.Date(2024, 8, 1, 18, 30, 0, 0, time.UTC)
	heapContent := []byte("heap-dump-binary-payload")

	// A td dump is needed so the pod appears in statistics (heap-only pods are not listed)
	s.createDumpFile(ns, ts, pod, model.TdDumpType, []byte("td"))
	s.createDumpFile(ns, ts, pod, model.HeapDumpType, heapContent)
	s.rescan()

	stats := s.getStatistics(
		time.Date(2024, 8, 1, 18, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 19, 0, 0, 0, time.UTC))

	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat)
	require.Equal(t, 1, len(stat.HeapDumps))
	handle := stat.HeapDumps[0].Handle

	// Verify deterministic handle format: {podName}-heap-{creationTimeMs}
	expectedHandle := fmt.Sprintf("%s-heap-%d", pod, ts.UnixMilli())
	require.Equal(t, expectedHandle, handle)

	resp, err := http.Get(fmt.Sprintf("%s/cdt/v2/heaps/download/%s", s.baseURL, handle))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, heapContent, body)
}

func (s *E2ETestSuite) TestIncrementalInsert() {
	t := s.T()
	ns := "ns-incr"
	pod := testPodName(testService1)

	// Phase 1: rescan picks up initial dump at minute 0
	t1 := time.Date(2024, 8, 1, 20, 0, 0, 0, time.UTC)
	s.createDumpFile(ns, t1, pod, model.TdDumpType, []byte("initial")) // 7 bytes
	s.rescan()

	stats := s.getStatistics(
		time.Date(2024, 8, 1, 20, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 21, 0, 0, 0, time.UTC))
	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat)
	require.Equal(t, int64(7), stat.DataAtEnd)

	// Phase 2: write new dump at minute 3 and run insert over the FULL range 00-05.
	// This deliberately overlaps with the already-rescanned minute 0.
	// The unique constraint on (pod_id, creation_time, dump_type) prevents duplicates.
	t2 := time.Date(2024, 8, 1, 20, 3, 0, 0, time.UTC)
	s.createDumpFile(ns, t2, pod, model.TdDumpType, []byte("incremental-add")) // 15 bytes
	s.insert(
		time.Date(2024, 8, 1, 20, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 20, 5, 0, 0, time.UTC))

	stats2 := s.getStatistics(
		time.Date(2024, 8, 1, 20, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 21, 0, 0, 0, time.UTC))
	stat2 := findByNamespace(stats2, ns)
	require.NotNil(t, stat2)
	require.Equal(t, int64(7+15), stat2.DataAtEnd)
	require.Equal(t, t1.UnixMilli(), stat2.FirstSamleMillis)
	require.Equal(t, t2.UnixMilli(), stat2.LastSampleMillis)
}

func (s *E2ETestSuite) TestPodOnlineStatus() {
	t := s.T()
	ns := "ns-online"
	pod := testPodName(testService1)

	// Use time.Now to create a "recent" dump so the pod registers as online
	now := time.Now().UTC().Truncate(time.Second)
	s.createDumpFile(ns, now, pod, model.TdDumpType, []byte("live"))
	s.rescan()

	stats := s.getStatistics(now.Add(-time.Hour), now.Add(time.Hour))
	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat)
	require.True(t, stat.OnlineNow, "pod with dump at %v should be online", now)

	// Old dump: pod should be offline
	nsOld := "ns-offline"
	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	s.createDumpFile(nsOld, oldTime, pod, model.TdDumpType, []byte("old"))
	s.rescan()

	statsOld := s.getStatistics(
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC))
	statOld := findByNamespace(statsOld, nsOld)
	require.NotNil(t, statOld)
	require.False(t, statOld.OnlineNow, "pod with dump at %v should be offline", oldTime)
}

func (s *E2ETestSuite) TestEmptyState() {
	t := s.T()

	// Query a time range with no data at all — should return empty, no errors
	stats := s.getStatistics(
		time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2099, 1, 2, 0, 0, 0, 0, time.UTC))

	require.Equal(t, 0, len(stats))
}

func (s *E2ETestSuite) TestPackPreservesQueryability() {
	t := s.T()
	ns := "ns-pack"
	pod := testPodName(testService1)
	ts := time.Date(2024, 7, 1, 8, 0, 0, 0, time.UTC)
	content := []byte("packable-td-dump")

	s.createDumpFile(ns, ts, pod, model.TdDumpType, content)
	s.rescan()

	// Verify data is visible before pack
	stats := s.getStatistics(
		time.Date(2024, 7, 1, 8, 0, 0, 0, time.UTC),
		time.Date(2024, 7, 1, 9, 0, 0, 0, time.UTC))
	stat := findByNamespace(stats, ns)
	require.NotNil(t, stat)
	require.Equal(t, int64(len(content)), stat.DataAtEnd)

	// Pack the hour (archive dumps older than the given time)
	s.pack(time.Date(2024, 7, 1, 9, 0, 0, 0, time.UTC))

	// Verify ZIP was created
	zipPath := filepath.Join(s.baseDir, ns, task.FileHourZipInPV(ts))
	_, err := os.Stat(zipPath)
	require.NoError(t, err, "ZIP archive should exist at %s", zipPath)

	// Verify statistics still work after pack
	statsAfter := s.getStatistics(
		time.Date(2024, 7, 1, 8, 0, 0, 0, time.UTC),
		time.Date(2024, 7, 1, 9, 0, 0, 0, time.UTC))
	statAfter := findByNamespace(statsAfter, ns)
	require.NotNil(t, statAfter)
	require.Equal(t, int64(len(content)), statAfter.DataAtEnd)

	// Verify download still works from the ZIP archive
	url := fmt.Sprintf("%s/cdt/v2/download?dateFrom=%d&dateTo=%d&type=td&namespace=%s",
		s.baseURL,
		time.Date(2024, 7, 1, 8, 0, 0, 0, time.UTC).UnixMilli(),
		time.Date(2024, 7, 1, 9, 0, 0, 0, time.UTC).UnixMilli(),
		ns)
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, 200, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	zr, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	require.NoError(t, err)
	require.Equal(t, 1, len(zr.File), "ZIP should contain exactly 1 td dump file")

	zf, err := zr.File[0].Open()
	require.NoError(t, err)
	defer func() { _ = zf.Close() }()
	fileContent, err := io.ReadAll(zf)
	require.NoError(t, err)
	require.Equal(t, content, fileContent)
}

// --- Helpers ---

func findByNamespace(stats []*model.StatisticItem, ns string) *model.StatisticItem {
	for _, s := range stats {
		if s.Namespace == ns {
			return s
		}
	}
	return nil
}

func filterByNamespace(stats []*model.StatisticItem, ns string) []*model.StatisticItem {
	var result []*model.StatisticItem
	for _, s := range stats {
		if s.Namespace == ns {
			result = append(result, s)
		}
	}
	return result
}

func getQueryInt64Param(c echo.Context, name string) (int64, error) {
	s := c.QueryParam(name)
	if s == "" {
		return 0, fmt.Errorf("missing required parameter: %s", name)
	}
	var v int64
	_, err := fmt.Sscanf(s, "%d", &v)
	return v, err
}

func TestE2ETestSuite(t *testing.T) {
	suite.Run(t, new(E2ETestSuite))
}
