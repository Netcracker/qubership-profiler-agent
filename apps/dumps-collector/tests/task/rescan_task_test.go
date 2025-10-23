//go:build integration

package task_test

import (
	"context"
	"os"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/utils"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RescanTaskTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *RescanTaskTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RescanTaskTestSuite) SetupTest() {
	helpers.CopyPVDataToTestDir(suite.ctx)
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *RescanTaskTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
	helpers.RemoveTestDir(suite.ctx)
}

// TestWrongParameters checks that RescanTask correctly handles various invalid constructor arguments.
func (suite *RescanTaskTestSuite) TestWrongParameters() {
	t := suite.T()

	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, nil)
	require.ErrorContains(t, err, "nil db client provided")
	require.Nil(t, rescanTask)

	rescanTask, err = task.NewRescanTask("unexist-dir", suite.db)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, rescanTask)

	rescanTask, err = task.NewRescanTask("insert_task_test.go", suite.db)
	require.ErrorContains(t, err, "is not a directory")
	require.Nil(t, rescanTask)

	rescanTask, err = task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, rescanTask)
}

// TestFullRun verifies the full execution flow of a rescan task.
// It runs RescanTask.Execute and checks that:
//   - 3 timelines are inserted (22:00 zipped, 23:00 raw, and 00:00 raw).
//   - 2 pods are added to the database.
//   - 3 heap dumps are inserted and associated with corresponding pods.
//   - 8 top/thread dumps are inserted for the 2024-08-01 00-hour timeline.
func (suite *RescanTaskTestSuite) TestFullRun() {
	t := suite.T()

	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	expectedTimelines := []model.Timeline{
		{
			Status: model.ZippedStatus,
			TsHour: time.Date(2024, 7, 31, 22, 00, 00, 00, time.UTC),
		},
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC),
		},
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC),
		},
	}

	// Check the database for timelines created by the rescan task
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 3, len(timelines))
	for _, timeline := range expectedTimelines {
		require.Contains(t, timelines, timeline)
	}

	expectedPods := []model.Pod{
		{
			Namespace:   "test-namespace-1",
			ServiceName: "test-service-1",
			PodName:     "test-service-1-5cbcd847d-l2t7t_1719318147399",
			RestartTime: time.Date(2024, 6, 25, 12, 22, 27, 00, time.UTC),
			LastActive:  utils.Ref(time.Date(2024, 8, 1, 0, 1, 42, 00, time.UTC)),
		},
		{
			Namespace:   "test-namespace-1",
			ServiceName: "test-service-2",
			PodName:     "test-service-2-5cbcd847d-l2t7t_1719318147399",
			RestartTime: time.Date(2024, 6, 25, 12, 22, 27, 00, time.UTC),
			LastActive:  utils.Ref(time.Date(2024, 8, 1, 0, 1, 59, 00, time.UTC)),
		},
	}

	// Check the database for pods created by the rescan task
	pods, err := suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))
	podIds := make([]uuid.UUID, 2)
	for i, pod := range pods {
		podIds[i] = pod.Id
		pod.Id = uuid.UUID{} // to compare
		require.Contains(t, expectedPods, pod)
	}

	// 3 heap dumps should be inserted:
	// one for 2024-07-31 22:59_35, one for 2024-07-31 23:59:35 and one for 2024-08-01 00:01:42
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 3, len(heapDumps))

	// 8 top and thread dumps should be inserted for the 2024-08-01 00 hour
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[2].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)
}

// TestNotFinishedRun verifies that the rescan task skips existing timelines.
//
// Expected results:
//   - 3 timelines total (1 pre-inserted)
//   - 2 pods
//   - 2 heap dumps (22:00 skipped)
//   - 0 top/thread dumps for 22:00 hour
//   - 8 top/thread dumps for 23:00 hour
//   - 8 top/thread dumps for 00:00 hour
func (suite *RescanTaskTestSuite) TestNotFinishedRun() {
	t := suite.T()

	// Add a timeline that should be ignored by the rescan task
	_, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx,
		time.Date(2024, 7, 31, 22, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	expectedTimelines := []model.Timeline{
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 7, 31, 22, 00, 00, 00, time.UTC),
		},
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC),
		},
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC),
		},
	}
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 3, len(timelines))
	for _, timeline := range expectedTimelines {
		require.Contains(t, timelines, timeline)
	}

	expectedPods := []model.Pod{
		{
			Namespace:   "test-namespace-1",
			ServiceName: "test-service-1",
			PodName:     "test-service-1-5cbcd847d-l2t7t_1719318147399",
			RestartTime: time.Date(2024, 6, 25, 12, 22, 27, 00, time.UTC),
			LastActive:  utils.Ref(time.Date(2024, 8, 1, 0, 1, 42, 00, time.UTC)),
		},
		{
			Namespace:   "test-namespace-1",
			ServiceName: "test-service-2",
			PodName:     "test-service-2-5cbcd847d-l2t7t_1719318147399",
			RestartTime: time.Date(2024, 6, 25, 12, 22, 27, 00, time.UTC),
			LastActive:  utils.Ref(time.Date(2024, 8, 1, 0, 1, 59, 00, time.UTC)),
		},
	}

	pods, err := suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))
	podIds := make([]uuid.UUID, 2)
	for i, pod := range pods {
		podIds[i] = pod.Id
		pod.Id = uuid.UUID{} // to compare
		require.Contains(t, expectedPods, pod)
	}

	// 2 heap dumps should be inserted: one for 2024-07-31 23:59:35 and one for 2024-08-01 00:01:42
	// (expectedTimelines[0] should be ignored)
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(heapDumps))

	// expectedTimelines[0] should be ignored
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(0), tdTopDumpsCount)

	// 8 top and thread dumps should be inserted for 2024-07-31 hour 23
	tdTopDumpsCount, err = suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[1].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)

	// 8 top and thread dumps should be inserted for the 2024-08-01 00 hour
	tdTopDumpsCount, err = suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[2].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)
}

func TestRescanTaskTestSuite(t *testing.T) {
	suite.Run(t, new(RescanTaskTestSuite))
}
