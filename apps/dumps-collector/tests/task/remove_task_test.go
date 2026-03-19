//go:build integration

package task_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RemoveTaskTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *RemoveTaskTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RemoveTaskTestSuite) SetupTest() {
	helpers.CopyPVDataToTestDir(suite.ctx)
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *RemoveTaskTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *RemoveTaskTestSuite) TestWrongParameters() {
	t := suite.T()

	removeTask, err := task.NewRemoveTask(helpers.TestBaseDir, nil)
	require.ErrorContains(t, err, "nil db client provided")
	require.Nil(t, removeTask)

	removeTask, err = task.NewRemoveTask("unexist-dir", suite.db)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, removeTask)

	removeTask, err = task.NewRemoveTask("insert_task_test.go", suite.db)
	require.ErrorContains(t, err, "is not a directory")
	require.Nil(t, removeTask)

	removeTask, err = task.NewRemoveTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, removeTask)
}

// TestFullRun verifies the full execution flow of a remove task.
// It first runs RescanTask to populate the database, then RemoveTask with a cutoff at 2024-07-31 23:00.
// It checks that:
//   - only the 2024-08 directory remains in PV
//   - only 1 timeline (2024-08-01 00:00) remains in the database
//   - 2 pods are preserved
//   - 1 heap dump is present for 2024-08-01 00:01:42 time
//   - 8 top/thread dumps are preserved for the 2024-08-01 00 hour
func (suite *RemoveTaskTestSuite) TestFullRun() {
	t := suite.T()

	// Rescan removeTask to add entities to db
	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	removeTask, err := task.NewRemoveTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = removeTask.Execute(suite.ctx,
		time.Date(2024, 07, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// Check that only the 2024-08 directory exists in PV
	yearDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024")
	entries, err := os.ReadDir(yearDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	require.Equal(t, "08", entries[0].Name())

	expectedTimeline := model.Timeline{
		Status: model.RawStatus,
		TsHour: time.Date(2024, 8, 01, 00, 00, 00, 00, time.UTC),
	}

	// There should be only 1 timeline for 2024-08-01 00 hour
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(timelines))
	require.Contains(t, timelines, expectedTimeline)

	// There should be 2 pods in 2024-08 month:
	// test-service-1-5cbcd847d-l2t7t_1719318147399 and test-service-2-5cbcd847d-l2t7t_1719318147399
	pods, err := suite.db.SearchPods(suite.ctx, &model.EmptyPodFilter{})
	require.NoError(t, err)
	require.Equal(t, 2, len(pods))

	podIds := make([]uuid.UUID, 2)
	for i, pod := range pods {
		podIds[i] = pod.Id
	}

	// There should be 1 heap dump for 2024-08-01 00:01:42
	heapDumps, err := suite.db.SearchHeapDumps(suite.ctx, podIds,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(heapDumps))

	// There should be 8 thread/top dumps for 2024-08 month
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, timelines[0].TsHour,
		time.Date(2024, 07, 29, 00, 00, 00, 00, time.UTC),
		time.Date(2024, 8, 01, 01, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount)
}

// TestRescanThenRemoveAtStartup simulates the application startup flow:
// rescan populates the database from existing PV data, then initial remove
// deletes everything older than the configured threshold.
//
// Test data has 3 hours on PV:
//   - 2024-07-31 22:00 (zipped: 22.zip + raw heap dump in 22/)
//   - 2024-07-31 23:00 (raw files in 23/)
//   - 2024-08-01 00:00 (raw files in 00/)
//
// It verifies that after rescan + remove with cutoff 2024-07-31 23:30:
//   - old zip archives and raw directories are deleted from PV
//   - old month directory (07) is cleaned up entirely
//   - new month directory (08) with hour 00:00 remains intact
//   - only 1 timeline remains in the database
func (suite *RemoveTaskTestSuite) TestRescanThenRemoveAtStartup() {
	t := suite.T()

	nsDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1")
	yearDir := filepath.Join(nsDir, "2024")

	// Verify test data is in place before rescan
	oldMonthDir := filepath.Join(yearDir, "07")
	_, err := os.Stat(oldMonthDir)
	require.NoError(t, err, "test data: old month directory 07 should exist before cleanup")

	newMonthDir := filepath.Join(yearDir, "08")
	_, err = os.Stat(newMonthDir)
	require.NoError(t, err, "test data: new month directory 08 should exist before cleanup")

	// Step 1: Rescan populates the database from PV (simulates startup)
	rescanTask, err := task.NewRescanTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = rescanTask.Execute(suite.ctx)
	require.NoError(t, err)

	// Verify rescan found all 3 timelines
	timelines, err := suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 01, 1, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 3, len(timelines), "rescan should find 3 timelines (hours 22, 23, 00)")

	// Step 2: Remove task runs right after rescan (simulates initial remove at startup)
	// Cutoff 2024-07-31 23:30 removes hours 22:00 and 23:00 (SearchTimelines uses BETWEEN), keeps 00:00
	removeTask, err := task.NewRemoveTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)

	err = removeTask.Execute(suite.ctx,
		time.Date(2024, 7, 31, 23, 30, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify old month directory (07) is completely removed from PV
	_, err = os.Stat(oldMonthDir)
	require.True(t, os.IsNotExist(err), "old month directory 07 should be removed")

	// Verify new month directory (08) still exists with data
	_, err = os.Stat(newMonthDir)
	require.NoError(t, err, "new month directory 08 should still exist")

	// Verify only month 08 remains under the year directory
	entries, err := os.ReadDir(yearDir)
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	require.Equal(t, "08", entries[0].Name())

	// Verify only 1 timeline (2024-08-01 00:00) remains in the database
	timelines, err = suite.db.SearchTimelines(suite.ctx,
		time.Date(2024, 07, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 01, 1, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, 1, len(timelines), "only hour 00:00 timeline should remain")
	require.Equal(t, time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC), timelines[0].TsHour)

	// Verify td/top dumps for the remaining hour are intact
	tdTopDumpsCount, err := suite.db.GetTdTopDumpsCount(suite.ctx, timelines[0].TsHour,
		time.Date(2024, 07, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 01, 1, 0, 0, 0, time.UTC))
	require.NoError(t, err)
	require.Equal(t, int64(8), tdTopDumpsCount, "8 td/top dumps should remain for hour 00:00")
}

func TestRemoveTaskTestSuite(t *testing.T) {
	suite.Run(t, new(RemoveTaskTestSuite))
}
