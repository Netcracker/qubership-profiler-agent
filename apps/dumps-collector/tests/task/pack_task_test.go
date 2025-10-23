//go:build integration

package task_test

import (
	"archive/zip"
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

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type PackTaskTestSuite struct {
	suite.Suite

	ctx context.Context
	db  db.DumpDbClient
}

func (suite *PackTaskTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *PackTaskTestSuite) SetupTest() {
	helpers.CopyPVDataToTestDir(suite.ctx)
	suite.db = helpers.CreateDbClient(suite.ctx)
}

func (suite *PackTaskTestSuite) TearDownTest() {
	if err := suite.db.CloseConnection(suite.ctx); err != nil {
		log.Fatal(suite.ctx, err, "error closing connection")
	}
	helpers.StopTestDb(suite.ctx)
	helpers.RemoveTestDir(suite.ctx)
}

func (suite *PackTaskTestSuite) TestWrongParameters() {
	t := suite.T()

	packTask, err := task.NewPackTask(helpers.TestBaseDir, nil)
	require.ErrorContains(t, err, "nil db client provided")
	require.Nil(t, packTask)

	packTask, err = task.NewPackTask("unexist-dir", suite.db)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
	require.Nil(t, packTask)

	packTask, err = task.NewPackTask("insert_task_test.go", suite.db)
	require.ErrorContains(t, err, "is not a directory")
	require.Nil(t, packTask)

	packTask, err = task.NewPackTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, packTask)
}

// TestProcessingRawStatus verifies that pack task correctly processes a timeline with raw status.
// It creates a timeline for 2024-07-31 23:00:00, runs pack task, and checks that:
//
// - Timeline status is updated to ZippedStatus.
// - Corresponding hour archive (23.zip) is created.
// - All top/thread dumps are compressed and removed from the raw folder.
// - The only remaining file in the hour folder is the heap dump.
// - The archive contains exactly 8 top/thread dump files.
func (suite *PackTaskTestSuite) TestProcessingRawStatus() {
	t := suite.T()

	packTask, err := task.NewPackTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, packTask)

	// Create a timeline that the pack task will process
	curTimeline, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// Zip all top/thread dumps for the specified hour
	err = packTask.Execute(suite.ctx, curTimeline.TsHour)
	require.NoError(t, err)

	// Timeline status should be set to ZippedStatus
	timeline, err := suite.db.FindTimeline(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, model.ZippedStatus, timeline.Status)

	// There should be dump archive for our timeline.
	dayDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024", "07", "31")
	_, err = os.Stat(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)

	// After compressing top/thread dumps, only the heap dump should remain in the hour folder
	pattern := filepath.Join(dayDir, "23", "*", "*", "*", "*")
	files, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, filepath.Join(dayDir, "23", "59", "35",
		"test-service-1-5cbcd847d-l2t7t_1719318147399", "20240731T235935.hprof.zip"), files[0])

	hourZip, err := zip.OpenReader(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)
	defer hourZip.Close()

	// Archive should contain 8 top/thread dump files
	zipFiles := hourZip.File
	require.Equal(t, 8, len(zipFiles))
}

// TestProcessingZippingStatus verifies that PackTask correctly processes a timeline
// that is already marked with ZippingStatus. This status indicates that zipping has
// started but may not have finished due to an interruption or failure.
//
// The test ensures that:
//   - The timeline is processed in the same way as with RawStatus.
//   - All top/thread dumps are zipped into a single archive.
//   - The timeline status is updated to ZippedStatus.
//   - Only the heap dump remains in the hourly folder after zipping.
//   - The archive contains the expected number of top/thread dump files.
func (suite *PackTaskTestSuite) TestProcessingZippingStatus() {
	t := suite.T()

	packTask, err := task.NewPackTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, packTask)

	// Create a timeline that the pack task will process
	curTimeline, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// Pack task should handle ZippingStatus the same way as RawStatus
	curTimeline, err = suite.db.UpdateTimelineStatus(suite.ctx, curTimeline.TsHour, model.ZippingStatus)
	require.NoError(t, err)

	// Zip all top/thread dumps for the specified hour
	err = packTask.Execute(suite.ctx, curTimeline.TsHour)
	require.NoError(t, err)

	// Timeline status should be set to ZippedStatus
	timeline, err := suite.db.FindTimeline(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, model.ZippedStatus, timeline.Status)

	// There should be dump archive for our timeline.
	dayDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024", "07", "31")
	_, err = os.Stat(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)

	// After compressing top/thread dumps, only the heap dump should remain in the hour folder
	pattern := filepath.Join(dayDir, "23", "*", "*", "*", "*")
	files, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, filepath.Join(dayDir, "23", "59", "35",
		"test-service-1-5cbcd847d-l2t7t_1719318147399", "20240731T235935.hprof.zip"), files[0])

	hourZip, err := zip.OpenReader(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)
	defer hourZip.Close()

	// Archive should contain 8 top/thread dump files
	zipFiles := hourZip.File
	require.Equal(t, 8, len(zipFiles))
}

// TestProcessingZippingStatusWithExistArchive verifies that the pack task correctly overwrites
// an existing archive when processing a timeline.
//
// The test ensures that:
//   - If a zip archive for the hour already exists, it is replaced.
//   - The pack task still compresses the top/thread dumps as expected.
//   - The timeline status is updated to ZippedStatus.
//   - Only the heap dump remains in the hour folder after compression.
//   - The resulting archive contains all expected dump files.
func (suite *PackTaskTestSuite) TestProcessingZippingStatusWithExistArchive() {
	t := suite.T()

	packTask, err := task.NewPackTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, packTask)

	// Create a timeline that the pack task will process
	curTimeline, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// Pack task should handle ZippingStatus the same way as RawStatus
	curTimeline, err = suite.db.UpdateTimelineStatus(suite.ctx, curTimeline.TsHour, model.ZippingStatus)
	require.NoError(t, err)

	// Create an empty archive to simulate a pre-existing zip file for the given hour.
	dayDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024", "07", "31")
	file, err := os.Create(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)
	w := zip.NewWriter(file)
	w.Close()
	file.Close()

	// Zip all top/thread dumps for the specified hour
	err = packTask.Execute(suite.ctx, curTimeline.TsHour)
	require.NoError(t, err)

	// Timeline status should be set to ZippedStatus
	timeline, err := suite.db.FindTimeline(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)
	require.Equal(t, model.ZippedStatus, timeline.Status)

	_, err = os.Stat(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)

	// After compressing top/thread dumps, only the heap dump should remain in the hour folder
	pattern := filepath.Join(dayDir, "23", "*", "*", "*", "*")
	files, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Equal(t, 1, len(files))
	require.Equal(t, filepath.Join(dayDir, "23", "59", "35",
		"test-service-1-5cbcd847d-l2t7t_1719318147399", "20240731T235935.hprof.zip"), files[0])

	hourZip, err := zip.OpenReader(filepath.Join(dayDir, "23.zip"))
	require.NoError(t, err)
	defer hourZip.Close()

	// Archive should contain 8 top/thread dump files
	zipFiles := hourZip.File
	require.Equal(t, 8, len(zipFiles))
}

// TestProcessingUnexpectedStatus verifies that the pack task skips processing
// timelines with statuses other than RawStatus or ZippingStatus.
func (suite *PackTaskTestSuite) TestProcessingUnexpectedStatus() {
	t := suite.T()

	packTask, err := task.NewPackTask(helpers.TestBaseDir, suite.db)
	require.NoError(t, err)
	require.NotNil(t, packTask)

	curTimeline, _, err := suite.db.CreateTimelineIfNotExist(suite.ctx,
		time.Date(2024, 7, 31, 23, 00, 00, 00, time.UTC))
	require.NoError(t, err)

	// The pack task should skip timelines that are not in RawStatus or ZippingStatus.
	curTimeline, err = suite.db.UpdateTimelineStatus(suite.ctx, curTimeline.TsHour, model.RemovingStatus)
	require.NoError(t, err)

	err = packTask.Execute(suite.ctx, curTimeline.TsHour)
	require.NoError(t, err)

	dayDir := filepath.Join(helpers.TestBaseDir, "test-namespace-1", "2024", "07", "31")
	entries, err := os.ReadDir(dayDir)
	require.NoError(t, err)

	fileNames := make([]string, len(entries))
	for i, entry := range entries {
		fileNames[i] = entry.Name()
	}

	// Ensure that no archive was created for the hour
	require.NotContains(t, fileNames, "23.zip")
}

func TestPackTaskTestSuite(t *testing.T) {
	suite.Run(t, new(PackTaskTestSuite))
}
