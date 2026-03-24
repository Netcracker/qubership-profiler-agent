//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/maintenance"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/tests/helpers"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

const (
	stubFileName = "calls.parquet"
)

type S3RemoveTestSuite struct {
	suite.Suite
	pg    *helpers.PostgresContainer
	minio *helpers.MinioContainer
	job   maintenance.MaintenanceJob
	ctx   context.Context

	fromTs       time.Time
	toTs         time.Time
	drToCheck    model.DurationRange
	dtToCheck    model.DumpType
	existS3Files map[string]*inventory.S3FileInfo
}

func (suite *S3RemoveTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)

	suite.fromTs = time.Date(2024, 5, 23, 0, 0, 0, 0, time.UTC)
	suite.toTs = time.Date(2024, 5, 23, 2, 0, 0, 0, time.UTC)

	suite.pg = helpers.CreatePgContainer(suite.ctx)
	suite.minio = helpers.CreateMinioContainer(suite.ctx)

	suite.drToCheck = *model.Durations.GetByName("0ms")
	suite.dtToCheck = model.DumpTypeGc

	suite.job = maintenance.MaintenanceJob{
		Postgres:    suite.pg.Client,
		MinioClient: suite.minio.Client,
		JobConfig: &config.JobConfig{
			S3FileRemoval: config.S3RemoveJobConfig{
				Calls: config.CallsS3RemoveJobConfig{Map: map[model.DurationRange]config.TimeHours{suite.drToCheck: 1}},
				Dumps: config.DumpsS3RemoveJobConfig{Map: map[model.DumpType]config.TimeHours{suite.dtToCheck: 1}},
				Heaps: 1,
			},
		},
	}
}

func (suite *S3RemoveTestSuite) SetupTest() {
	var err error
	if suite.existS3Files, err = suite.pg.AddS3Files(suite.ctx, suite.fromTs, suite.toTs, model.FileCompleted); err != nil {
		log.Error(suite.ctx, err, "error creating initial s3 files")
		suite.FailNow("setup sub test")
	}
	if err := suite.minio.UploadStubS3Files(suite.ctx, suite.existS3Files); err != nil {
		log.Error(suite.ctx, err, "error uploading initial s3 files")
		suite.FailNow("setup sub test")
	}
}

func (suite *S3RemoveTestSuite) TestRemoveS3FilesForSpecifiedTimeRange() {
	// Data generated from 00:00 to 02:00
	// Now is 02:00
	// Data from 00:00 to 01:00 should be removed
	t := suite.T()

	ts := time.Date(2024, 5, 23, 1, 0, 0, 0, time.UTC) // Time, before that data should be removed
	expectedS3FilesToExist := []string{}
	for _, s3File := range suite.existS3Files {
		if s3File.StartTime.After(ts) || s3File.DurationRange != model.DurationAsInt(&suite.drToCheck) && s3File.DumpType != suite.dtToCheck && s3File.Type != model.FileHeap {
			expectedS3FilesToExist = append(expectedS3FilesToExist, s3File.RemoteStoragePath)
		}
	}

	removeJob, err := maintenance.NewS3FileRemoveJob(suite.ctx, &suite.job, suite.toTs)
	require.NoError(t, err)

	err = removeJob.Execute(suite.ctx)
	require.NoError(t, err)

	list, err := suite.minio.Client.ListObjects(suite.ctx)
	require.NoError(t, err)
	require.Equal(t, len(expectedS3FilesToExist), len(list))
	for _, obj := range list {
		require.Contains(t, expectedS3FilesToExist, obj.Key)
	}

	s3Files, err := suite.pg.Client.GetS3FilesByStartTimeBetween(suite.ctx, suite.fromTs, suite.toTs)
	require.NoError(t, err)
	require.Equal(t, len(expectedS3FilesToExist), len(s3Files))
	for _, remotePath := range expectedS3FilesToExist {
		require.Contains(t, s3Files, remotePath)
	}
}

func (suite *S3RemoveTestSuite) TearDownTest() {
	if err := suite.minio.Cleanup(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error cleaning up s3 files from s3 storage")
		suite.FailNow("tear down test")
	}
	if err := suite.pg.CleanUpS3Files(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error cleaning up s3 files from pg")
		suite.FailNow("tear down test")
	}
}

func (suite *S3RemoveTestSuite) TearDownSuite() {
	if err := suite.pg.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating pg container")
		suite.FailNow("tear down")
	}
	if err := suite.minio.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating minio container")
		suite.FailNow("tear down")
	}
}

func TestS3RemoveTestSuite(t *testing.T) {
	suite.Run(t, new(S3RemoveTestSuite))
}

