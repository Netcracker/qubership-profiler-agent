////go:build integration

package task_test

import (
	"context"
	"os"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RequestProcessorTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *RequestProcessorTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RequestProcessorTestSuite) SetupTest() {
	helpers.CopyPVDataToTestDir(suite.ctx)
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *RequestProcessorTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RequestProcessorTestSuite) TestWrongParameters() {
	t := suite.T()

	requestProcessor, err := task.NewRequestProcessor(helpers.TestBaseDir, nil, true)
	require.ErrorContains(t, err, "nil db client provided")
	require.Nil(t, requestProcessor)

	requestProcessor, err = task.NewRequestProcessor("unexist-dir", suite.db, true)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, requestProcessor)

	requestProcessor, err = task.NewRequestProcessor("insert_task_test.go", suite.db, true)
	require.ErrorContains(t, err, "is not a directory")
	require.Nil(t, requestProcessor)

	requestProcessor, err = task.NewRequestProcessor(helpers.TestBaseDir, suite.db, true)
	require.NoError(t, err)
	require.NotNil(t, requestProcessor)
}

// TestStatisticCalculationFromOneHourTable verifies that statistics are correctly calculated
// for a time range within a single hour.
// It runs a rescan task to populate the database, then executes a statistics request and checks that:
//   - 1 pod matching the filter is returned with correct metadata.
//   - ActiveSinceMillis is parsed correctly from the database.
//   - First and last sample timestamps match the first and last top/thread dumps found in PV for the time range.
//   - 3 top/thread dumps are accounted for, with a total size of 37 bytes.
//   - No heap dumps are present.
func (suite *RequestProcessorTestSuite) TestStatisticCalculationFromOneHourTable() {
	t := suite.T()

	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	requestProcessor, err := task.NewRequestProcessor(helpers.TestBaseDir, suite.db, true)
	require.NoError(t, err)

	// Time range across one hour
	statistics, err := requestProcessor.StatisticRequest(suite.ctx,
		time.Date(2024, 07, 31, 23, 58, 00, 00, time.UTC),
		time.Date(2024, 07, 31, 23, 59, 00, 00, time.UTC),
		model.NewPodFilterComparator("service_name", model.ComparatorEqual, "test-service-1"))

	require.NoError(t, err)
	require.Equal(t, 1, len(statistics))

	// General information about pod
	require.Equal(t, "test-namespace-1", statistics[0].Namespace)
	require.Equal(t, "test-service-1", statistics[0].ServiceName)
	require.Equal(t, "test-service-1-5cbcd847d-l2t7t_1719318147399", statistics[0].PodName)

	// Verify that ActiveSinceMillis matches the restart time from the pod name (1719318147399), truncated to seconds
	require.Equal(t, int64(1719318147000), statistics[0].ActiveSinceMillis)

	// The first top / thread dump for test-service-1 in the given time range is at 2024-07-31 23:58:00
	require.Equal(t, time.Date(2024, 07, 31, 23, 58, 00, 00, time.UTC).UnixMilli(), statistics[0].FirstSamleMillis)

	// The last top / thread dump for test-service-1 in the given time range is at 2024-07-31 23:59:00
	require.Equal(t, time.Date(2024, 07, 31, 23, 59, 00, 00, time.UTC).UnixMilli(), statistics[0].LastSampleMillis)

	require.Equal(t, int64(0), statistics[0].DataAtStart)

	// In the given time range for test-service-1, there should be 3 dumps:
	// - 2024-07-31 23:58:00 -> 1 thread dump (12 bytes)
	// - 2024-07-31 23:58:01 -> 1 top dump (13 bytes)
	// - 2024-07-31 23:59:00 -> 1 thread dump (12 bytes)
	require.Equal(t, int64(12*2+13), statistics[0].DataAtEnd)

	// No heap dumps should be present
	require.Equal(t, 0, len(statistics[0].HeapDumps))
}

// TestStatisticCalculationBetweenHourTables verifies the full execution flow of StatisticRequest.
// It runs StatisticRequest over a time range that covers two adjacent hours (22:58:30–23:59:30) and checks that:
//   - 1 pod matching the filter is returned with correct metadata.
//   - ActiveSinceMillis is parsed correctly from the database.
//   - First and last sample timestamps match the first and last dumps in the specified range.
//   - DataAtEnd reflects the total size of all 6 top/thread dumps (2 in hour 22 and 4 in hour 23).
//   - 1 heap dump is found and properly described.
func (suite *RequestProcessorTestSuite) TestStatisticCalculationBetweenHourTables() {
	t := suite.T()

	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	requestProcessor, err := task.NewRequestProcessor(helpers.TestBaseDir, suite.db, true)
	require.NoError(t, err)

	// Time range across two hours
	statistics, err := requestProcessor.StatisticRequest(suite.ctx,
		time.Date(2024, 07, 31, 22, 58, 30, 00, time.UTC),
		time.Date(2024, 07, 31, 23, 59, 30, 00, time.UTC),
		model.NewPodFilterComparator("service_name", model.ComparatorEqual, "test-service-1"))

	require.NoError(t, err)

	// General information about pod
	require.Equal(t, 1, len(statistics))
	require.Equal(t, "test-namespace-1", statistics[0].Namespace)
	require.Equal(t, "test-service-1", statistics[0].ServiceName)
	require.Equal(t, "test-service-1-5cbcd847d-l2t7t_1719318147399", statistics[0].PodName)

	// Verify that ActiveSinceMillis matches the restart time from the pod name (1719318147399), truncated to seconds
	require.Equal(t, int64(1719318147000), statistics[0].ActiveSinceMillis)

	// The first top / thread dump for test-service-1 in the given time range is at 2024-07-31 22:59:00
	require.Equal(t, time.Date(2024, 07, 31, 22, 59, 00, 00, time.UTC).UnixMilli(), statistics[0].FirstSamleMillis)

	// The last top / thread dump for test-service-1 in the given time range is at 2024-07-31 23:59:01
	require.Equal(t, time.Date(2024, 07, 31, 23, 59, 01, 00, time.UTC).UnixMilli(), statistics[0].LastSampleMillis)

	require.Equal(t, int64(0), statistics[0].DataAtStart)

	// In the given time range for test-service-1, there should be 6 dumps:
	// - 2024-07-31 22:59:00 -> 1 thread dump (12 bytes) (zipped)
	// - 2024-07-31 22:59:01 -> 1 top dump (13 bytes) (zipped)
	// - 2024-07-31 23:58:00 -> 1 thread dump (12 bytes)
	// - 2024-07-31 23:58:01 -> 1 top dump (13 bytes)
	// - 2024-07-31 23:59:00 -> 1 thread dump (12 bytes)
	// - 2024-07-31 23:59:01 -> 1 top dump (13 bytes)
	require.Equal(t, int64(12+13)*3, statistics[0].DataAtEnd)

	// There should be 1 heap dump for 2024-07-31 22:59:35 (169 bytes)
	require.Equal(t, 1, len(statistics[0].HeapDumps))
	require.Equal(t, "test-service-1-5cbcd847d-l2t7t_1719318147399-heap-1722466775000", statistics[0].HeapDumps[0].Handle)
	require.Equal(t, time.Date(2024, 07, 31, 22, 59, 35, 00, time.UTC).UnixMilli(), statistics[0].HeapDumps[0].Date)
	require.Equal(t, int64(169), statistics[0].HeapDumps[0].Bytes)
}

// TestStatisticCalculationFull verifies statistic calculation over all available hourly partitions.
// It runs RequestProcessor.StatisticRequest across two days and checks that:
//
//   - 2 pods are returned (test-service-1 and test-service-2), each with correct metadata.
//
// Example test-service-1:
//   - First and last top/thread dumps at 2024-07-31 22:58:00 and 2024-08-01 00:01:01 respectively.
//   - 150 bytes of data accumulated from top/thread dumps.
//   - 3 heap dumps present.
//
// Example test-service-2:
//   - First and last top/thread dumps at 2024-07-31 22:58:59 and 2024-08-01 00:01:59 respectively.
//   - 150 bytes of data accumulated from top/thread dumps.
//   - No heap dumps present.
func (suite *RequestProcessorTestSuite) TestStatisticCalculationFull() {
	t := suite.T()

	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	requestProcessor, err := task.NewRequestProcessor(helpers.TestBaseDir, suite.db, true)
	require.NoError(t, err)

	// Time range across all hours
	statistics, err := requestProcessor.StatisticRequest(suite.ctx,
		time.Date(2024, 07, 31, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 02, 00, 00, 00, 00, time.UTC),
		model.EmptyPodFilter{})

	require.NoError(t, err)

	// General information about pods (test-service-1 & test-service-2)
	require.Equal(t, 2, len(statistics))
	var svc1 *model.StatisticItem
	var svc2 *model.StatisticItem
	for _, statistic := range statistics {
		if statistic.ServiceName == "test-service-1" {
			svc1 = statistic
		} else if statistic.ServiceName == "test-service-2" {
			svc2 = statistic
		}
	}

	require.NotNil(t, svc1)
	require.Equal(t, "test-namespace-1", svc1.Namespace)
	require.Equal(t, "test-service-1-5cbcd847d-l2t7t_1719318147399", svc1.PodName)
	require.Equal(t, int64(1719318147000), svc1.ActiveSinceMillis)
	require.Equal(t, time.Date(2024, 07, 31, 22, 58, 00, 00, time.UTC).UnixMilli(), svc1.FirstSamleMillis)
	require.Equal(t, time.Date(2024, 8, 01, 00, 01, 01, 00, time.UTC).UnixMilli(), svc1.LastSampleMillis)
	require.Equal(t, int64(0), svc1.DataAtStart)
	require.Equal(t, int64(150), svc1.DataAtEnd)
	require.Equal(t, 3, len(svc1.HeapDumps))

	require.NotNil(t, svc2)
	require.Equal(t, "test-namespace-1", svc2.Namespace)
	require.Equal(t, "test-service-2-5cbcd847d-l2t7t_1719318147399", svc2.PodName)
	require.Equal(t, int64(1719318147000), svc2.ActiveSinceMillis)
	require.Equal(t, time.Date(2024, 07, 31, 22, 58, 59, 00, time.UTC).UnixMilli(), svc2.FirstSamleMillis)
	require.Equal(t, time.Date(2024, 8, 01, 00, 01, 59, 00, time.UTC).UnixMilli(), svc2.LastSampleMillis)
	require.Equal(t, int64(0), svc2.DataAtStart)
	require.Equal(t, int64(150), svc2.DataAtEnd)
	require.Equal(t, 0, len(svc2.HeapDumps))
}

func TestRequestProcessorTestSuite(t *testing.T) {
	suite.Run(t, new(RequestProcessorTestSuite))
}
