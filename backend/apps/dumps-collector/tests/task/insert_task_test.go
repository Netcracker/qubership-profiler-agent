//go:build integration

package task_test

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/envconfig"
	"os"
	"path/filepath"
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

type InsertTaskTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *InsertTaskTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
}

func (suite *InsertTaskTestSuite) SetupTest() {
	helpers.CopyPVDataToTestDir(suite.ctx)
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *InsertTaskTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *InsertTaskTestSuite) TestWrongParameters() {
	t := suite.T()

	insertTask, err := task.NewInsertTask(helpers.TestBaseDir, nil)
	require.ErrorContains(t, err, "nil db client provided")
	require.Nil(t, insertTask)

	insertTask, err = task.NewInsertTask("unexist-dir", suite.db)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, insertTask)

	insertTask, err = task.NewInsertTask("insert_task_test.go", suite.db)
	require.ErrorContains(t, err, "is not a directory")
	require.Nil(t, insertTask)

	insertTask, err = task.NewInsertTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, insertTask)
}

// TestEmptyRun verifies that the insert task performs no operations when run over a time range
// where no dumps are present in the PV. It ensures that:
//   - No timelines are inserted.
//   - No pods are added to the database.
func (suite *InsertTaskTestSuite) TestEmptyRun() {
	t := suite.T()

	insertTask, err := task.NewInsertTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	// Run the insert task for a time interval where no dumps are present in the PV
	err = insertTask.Execute(suite.ctx,
		time.Date(2024, 07, 30, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 07, 31, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// No timelines should be present
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 07, 31, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 0, len(timelines))

	// No pods should be present
	pods, err := suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 0, len(pods))
}

// TestFullRun verifies the full execution flow of an insert task.
// It runs an insert task over a specific time range and checks that:
//   - 2 timelines are inserted (one for each hour in the range).
//   - 2 expected pods are added to the database.
//   - 2 heap dumps are created, each associated with a pod.
//   - 8 top/thread dumps are inserted for each timeline hour (total of 16).
func (suite *InsertTaskTestSuite) TestFullRun() {
	t := suite.T()

	insertTask, err := task.NewInsertTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	// Process dumps for each minute within the specified time range
	err = insertTask.Execute(suite.ctx,
		time.Date(2024, 07, 31, 23, 56, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 00, 02, 00, 00, time.UTC))
	require.NoError(t, err)

	expectedTimelines := []model.Timeline{
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC),
		},
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC),
		},
	}

	// Check the database for timelines created by the insert task
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(timelines))
	require.Contains(t, expectedTimelines, timelines[0])
	require.Contains(t, expectedTimelines, timelines[1])

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

	// Check the database for pods created by the insert task
	pods, err := suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))
	podIds := make([]uuid.UUID, 2)
	for i, pod := range pods {
		podIds[i] = pod.Id
		pod.Id = uuid.UUID{} // to compare
		require.Contains(t, expectedPods, pod)
	}

	// 2 heap dumps should be inserted: one for 2024-07-31 23:59:35 and one for 2024-08-01 00:01:42.
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(heapDumps))

	// 8 top and thread dumps should be inserted for 2024-07-31 hour 23
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)

	// 8 top and thread dumps should be inserted for 2024-08-01 hour 00
	tdTopDumpsCount, err = suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[1].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)
}

// TestRepeatedRun verifies how the insert task behaves when executed multiple times.
// It runs the insert task twice: the first time with one dump file renamed (excluded),
// and the second time with that dump restored. The test checks that:
//   - Timelines, pods, and heap dumps are not inserted or modified during the second run.
//   - The first run inserts 7 top/thread dumps.
//   - The second run inserts 8 top/thread dumps (including the previously skipped one),
//     resulting in 15 total top/thread dumps in the database.
func (suite *InsertTaskTestSuite) TestRepeatedRun() {
	t := suite.T()

	insertTask, err := task.NewInsertTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	// Temporarily rename the dump so that it is not processed by an insert task
	dumpFile := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024", "07", "31", "23", "59", "01", "test-service-1-5cbcd847d-l2t7t_1719318147399", "20240731T235901.top.txt")
	dumpFileRenamed := filepath.Join(helpers.TestBaseDir, "tmp.txt")

	err = os.Rename(dumpFile, dumpFileRenamed)
	require.NoError(t, err)

	err = insertTask.Execute(suite.ctx,
		time.Date(2024, 07, 31, 23, 56, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 00, 02, 00, 00, time.UTC))
	require.NoError(t, err)

	expectedTimelines := []model.Timeline{
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC),
		},
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC),
		},
	}

	// Check the database for timelines created by the insert task
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(timelines))
	require.Contains(t, expectedTimelines, timelines[0])
	require.Contains(t, expectedTimelines, timelines[1])

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

	// Check the database for pods created by the insert task
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
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(heapDumps))

	// 7 top and thread dumps should be inserted for 2024-07-31 hour 23
	// (1 dump we renamed, so it shouldn't be processed)
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(7), tdTopDumpsCount)

	// Rename the dump back
	err = os.Rename(dumpFileRenamed, dumpFile)
	require.NoError(t, err)

	err = insertTask.Execute(suite.ctx,
		time.Date(2024, 07, 31, 23, 56, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 00, 02, 00, 00, time.UTC))
	require.NoError(t, err)

	// A second run of an insert task should not insert new timelines or modify existing ones
	timelines, err = suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(timelines))
	require.Contains(t, expectedTimelines, timelines[0])
	require.Contains(t, expectedTimelines, timelines[1])

	// A second run of an insert task should not insert new pods or modify existing ones
	pods, err = suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))
	podIds = make([]uuid.UUID, 2)
	for i, pod := range pods {
		podIds[i] = pod.Id
		pod.Id = uuid.UUID{} // to compare
		require.Contains(t, expectedPods, pod)
	}

	// A second run of an insert task should not insert new heap dumps or modify existing ones
	heapDumps, err = suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 2, len(heapDumps))

	// The second run of the insert task should add 8 top/thread dumps (1 was previously renamed)
	// Total: 7 from the first run + 8 new = 15 dumps
	tdTopDumpsCount, err = suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(15), tdTopDumpsCount)
}

// TestPartRun verifies the behavior of the insert task when executed over a limited time range.
// It runs the insert task only for minute 23:59 of 2024-07-31 (month 07), excluding any data from month 08.
// The test checks that:
//   - 1 timeline is inserted for the 23rd hour of 2024-07-31.
//   - 2 expected pods are added to the database.
//   - 1 heap dump is inserted, associated with one of the pods.
//   - 4 top/thread dumps are inserted for the corresponding timeline hour.
func (suite *InsertTaskTestSuite) TestPartRun() {
	t := suite.T()

	insertTask, err := task.NewInsertTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	// Process only part of month 07 (2024-07-31 23:59 minute) and skip month 08 entirely
	err = insertTask.Execute(suite.ctx,
		time.Date(2024, 07, 31, 23, 59, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	expectedTimelines := []model.Timeline{
		{
			Status: model.RawStatus,
			TsHour: time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC),
		},
	}

	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(timelines))
	require.Contains(t, expectedTimelines, timelines[0])

	expectedPods := []model.Pod{
		{
			Namespace:   "test-namespace-1",
			ServiceName: "test-service-1",
			PodName:     "test-service-1-5cbcd847d-l2t7t_1719318147399",
			RestartTime: time.Date(2024, 6, 25, 12, 22, 27, 00, time.UTC),
			LastActive:  utils.Ref(time.Date(2024, 7, 31, 23, 59, 35, 00, time.UTC)),
		},
		{
			Namespace:   "test-namespace-1",
			ServiceName: "test-service-2",
			PodName:     "test-service-2-5cbcd847d-l2t7t_1719318147399",
			RestartTime: time.Date(2024, 6, 25, 12, 22, 27, 00, time.UTC),
			LastActive:  utils.Ref(time.Date(2024, 7, 31, 23, 59, 59, 00, time.UTC)),
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

	// 1 heap dump should be inserted for 2024-07-31 23:59:35
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(heapDumps))

	// 4 top and thread dumps should be inserted for 2024-07-31 hour 23
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, expectedTimelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(4), tdTopDumpsCount)
}

func (suite *InsertTaskTestSuite) TestHeapDumpsTrimming() {
	t := suite.T()

	// Initialize InsertTask with the test base directory and DB client
	insertTask, err := task.NewInsertTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	// Execute InsertTask for a specific 1-hour window
	// This triggers collection, storage, and trimming logic
	// tests/resources/test-data/test-namespace-1/2024/07/31/22/59/35/test-service-1-5cbcd847d-l2t7t_1719318147399/20240731T225935.hprof.zip
	// tests/resources/test-data/test-namespace-1/2024/07/31/23/59/35/test-service-1-5cbcd847d-l2t7t_1719318147399/20240731T235935.hprof.zip
	// tests/resources/test-data/test-namespace-1/2024/08/01/00/01/42/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000142.hprof.zip
	err = insertTask.Execute(suite.ctx,
		time.Date(2024, 7, 31, 22, 59, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 00, 03, 00, 00, time.UTC))
	require.NoError(t, err)

	// Case 1: the 3rd heap dump for this pod should be deleted,
	// This dump is the oldest and exceeds the per-pod limit of 10 (by default)
	yearDir := filepath.Join(
		helpers.TestBaseDir,
		"test-namespace-1", "2024", "07", "31", "22", "59", "35",
		"test-service-1-5cbcd847d-l2t7t_1719318147399",
	)
	entries, err := os.ReadDir(yearDir)
	require.NoError(t, err)
	require.Equal(t, 0, len(entries))

	// Case 2: the newest (latest) heap dump for this pod
	// Should remain - used to verify that trimming didn't delete the newest dump
	yearDir = filepath.Join(
		helpers.TestBaseDir,
		"test-namespace-1", "2024", "08", "01", "00", "01", "42",
		"test-service-1-5cbcd847d-l2t7t_1719318147399",
	)
	entries, err = os.ReadDir(yearDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
}

func TestInsertTaskTestSuite(t *testing.T) {

	err := envconfig.InitConfig()
	envconfig.EnvConfig.MaxHeapDumps = 2
	require.NoError(t, err)

	suite.Run(t, new(InsertTaskTestSuite))
}
