//go:build integration

package integration

import (
	"context"
	"path"
	"path/filepath"
	"testing"

	"github.com/Netcracker/qubership-profiler-backend/libs/tests/helpers"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	callsObjectName = "calls.parquet"
)

var (
	testObjectFile = filepath.Join("../resources", "data", callsObjectName)
	outputPath     = filepath.Join("../..", "output")
)

type MinioTestSuite struct {
	suite.Suite
	ctx           context.Context
	minio         *helpers.MinioContainer
	fileVersionId string
}

func (suite *MinioTestSuite) SetupSuite() {
	suite.ctx = log.SetLevel(log.Context("itest"), log.DEBUG)
	suite.minio = helpers.CreateMinioContainer(suite.ctx)
}

func (suite *MinioTestSuite) TestBuckets() {
	t := suite.T()

	err := suite.minio.Client.MakeBucket(suite.ctx, suite.minio.Params.BucketName)
	assert.NoError(t, err)

	err = suite.minio.Client.MakeBucket(suite.ctx, "another-bucket")
	assert.NoError(t, err)

	err = suite.minio.Client.RemoveBucket(suite.ctx, "another-bucket")
	assert.NoError(t, err)
}

func (suite *MinioTestSuite) TestObject() {
	t := suite.T()

	absPath, _ := filepath.Abs(testObjectFile)
	log.Debug(suite.ctx, "test file: %s", absPath)
	info, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, callsObjectName)
	assert.NoError(t, err)
	suite.fileVersionId = info.VersionID
	log.Debug(suite.ctx, "versionId: %s", suite.fileVersionId)

	info, err = suite.minio.Client.PutObject(suite.ctx, testObjectFile, callsObjectName)
	assert.NoError(t, err)
	suite.fileVersionId = info.VersionID

	log.Debug(suite.ctx, "versionId: %s", suite.fileVersionId)
	output, _ := filepath.Abs(outputPath)
	log.Debug(suite.ctx, "output path: %s", output)
	err = suite.minio.Client.GetObject(suite.ctx, callsObjectName, outputPath)
	assert.NoError(t, err)

	err = suite.minio.Client.RemoveObject(suite.ctx, callsObjectName, suite.fileVersionId)
	assert.NoError(t, err)
}

func (suite *MinioTestSuite) TestListObjects() {
	t := suite.T()

	list, err := suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(list))

	absPath, _ := filepath.Abs(testObjectFile)
	log.Debug(suite.ctx, "test file: %s", absPath)
	info1, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("dir1", callsObjectName))
	assert.NoError(t, err)

	info2, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("dir2", callsObjectName))
	assert.NoError(t, err)

	expectedRemoteFiles := []string{info1.Key, info2.Key}
	list, err = suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	for _, obj := range list {
		assert.Contains(t, expectedRemoteFiles, obj.Key)
	}

	err = suite.minio.Client.RemoveObject(suite.ctx, info1.Key, info1.VersionID)
	assert.NoError(t, err)

	err = suite.minio.Client.RemoveObject(suite.ctx, info2.Key, info2.VersionID)
	assert.NoError(t, err)

	list, err = suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(list))
}

func (suite *MinioTestSuite) TestListObjectsWirhPreix() {
	t := suite.T()

	list, err := suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(list))

	absPath, _ := filepath.Abs(testObjectFile)
	log.Debug(suite.ctx, "test file: %s", absPath)
	info1, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir1", callsObjectName))
	assert.NoError(t, err)

	info2, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir2", callsObjectName))
	assert.NoError(t, err)

	expectedRemoteFiles := []string{info1.Key, info2.Key}
	list, err = suite.minio.Client.ListObjectsWithPrefix(suite.ctx, "common")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	for _, obj := range list {
		assert.Contains(t, expectedRemoteFiles, obj.Key)
	}

	list, err = suite.minio.Client.ListObjectsWithPrefix(suite.ctx, path.Join("common", "dir1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(list))
	assert.Contains(t, info1.Key, list[0].Key)

	err = suite.minio.Client.RemoveObject(suite.ctx, info1.Key, info1.VersionID)
	assert.NoError(t, err)

	err = suite.minio.Client.RemoveObject(suite.ctx, info2.Key, info2.VersionID)
	assert.NoError(t, err)

	list, err = suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(list))
}

func (suite *MinioTestSuite) TestRemoveObjects() {
	t := suite.T()

	absPath, _ := filepath.Abs(testObjectFile)
	log.Debug(suite.ctx, "test file: %s", absPath)
	info1, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir1", callsObjectName))
	assert.NoError(t, err)

	info2, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir2", callsObjectName))
	assert.NoError(t, err)

	info3, err := suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir2", "dir3", callsObjectName))
	assert.NoError(t, err)

	expectedRemoteFiles := []string{info1.Key, info2.Key, info3.Key}
	list, err := suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(list))
	for _, obj := range list {
		assert.Contains(t, expectedRemoteFiles, obj.Key)
	}

	errs := suite.minio.Client.RemoveObjects(suite.ctx, list)
	assert.Empty(t, errs, "found unexpected errors %v", errs)

	list, err = suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(list))

	info1, err = suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir1", callsObjectName))
	assert.NoError(t, err)

	info2, err = suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir2", callsObjectName))
	assert.NoError(t, err)

	info3, err = suite.minio.Client.PutObject(suite.ctx, testObjectFile, path.Join("common", "dir2", "dir3", callsObjectName))
	assert.NoError(t, err)

	expectedRemoteFiles = []string{info2.Key, info3.Key}
	list, err = suite.minio.Client.ListObjectsWithPrefix(suite.ctx, path.Join("common", "dir2"))
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	for _, obj := range list {
		assert.Contains(t, expectedRemoteFiles, obj.Key)
	}

	errs = suite.minio.Client.RemoveObjects(suite.ctx, list)
	assert.Empty(t, errs, "found unexpected errors %s", errs)

	list, err = suite.minio.Client.ListObjects(suite.ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(list))
	assert.Equal(t, info1.Key, list[0].Key)
}

func (suite *MinioTestSuite) AfterTest(suiteName, testName string) {
	log.Debug(suite.ctx, "[%s] after test for %s", suiteName, testName)
}

func (suite *MinioTestSuite) TearDownSuite() {
	if err := suite.minio.Terminate(suite.ctx); err != nil {
		log.Error(suite.ctx, err, "error terminating minio container")
		suite.FailNow("tear down")
	}
}

func TestMinioTestSuite(t *testing.T) {
	suite.Run(t, new(MinioTestSuite))
}
