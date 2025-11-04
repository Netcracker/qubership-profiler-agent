package s3

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/data"

	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
)

type CloudStorage interface {
	Disabled() bool
	SetFileCache(callsFm *parquet.FileMap)

	UploadDir(ctx context.Context, callsStartTime time.Time, local *parquet.FileMap) (int, int, error)
	GetCallsFiles() *parquet.FileMap
	// UploadFile(file *os.File, fileName string) error
	// DownloadFile(fileName string) (*os.File, error)
}

type (
	CloudStorageImpl struct {
		cfg        data.Config
		client     *s3.MinioClient
		db         pg.DbClient
		callsFiles *parquet.FileMap
	}
)

// -----------------------------------------------------------------------------

func New(ctx context.Context, cfg data.Config, db pg.DbClient) *CloudStorageImpl {
	client, err := s3.NewClient(ctx, cfg.S3)
	if err != nil {
		client = nil
	} else {
		log.Info(ctx, "Connected to storage [%v?%v]", client.Client.EndpointURL(), client.Client.IsOnline())
	}
	return &CloudStorageImpl{cfg, client, db, nil}
}

func (csi *CloudStorageImpl) Disabled() bool {
	return csi.client == nil
}

func (csi *CloudStorageImpl) SetFileCache(callsFm *parquet.FileMap) {
	csi.callsFiles = callsFm
}

func (csi *CloudStorageImpl) UploadDir(ctx context.Context, callsStartTime time.Time, local *parquet.FileMap) (c, total int, err error) {
	startTime := time.Now()
	if csi.Disabled() {
		log.Info(ctx, "Skip uploading to S3")
		return
	}

	var list []os.DirEntry
	list, err = files.List(ctx, local.CurrentDir)
	if err != nil {
		return 0, -1, err
	}

	var totalSize int64
	for _, file := range list {
		origin, has := local.Get(strings.ReplaceAll(file.Name(), ".parquet", ""))
		if !has {
			log.Info(ctx, "Don't have information in cache about file '%v'", file.Name())
			continue
		}
		fileInfo := TransferFileInfo{
			origin.S3FileInfo,
			file, local.CurrentDir,
			fmt.Sprintf("%s/%s", local.CurrentDir, file.Name()),
		}

		fileSize, ok := csi.uploadFile(ctx, callsStartTime, fileInfo)
		if ok {
			c++
			totalSize += fileSize
		}
	}

	log.Info(ctx, "Upload %d files (%d Mb) in directory '%s' [in %v]",
		len(list), totalSize/1024/1024, local.CurrentDir, time.Since(startTime))
	return c, len(list), err
}

func (csi *CloudStorageImpl) uploadFile(ctx context.Context, callsStartTime time.Time, file TransferFileInfo) (fileSize int64, ok bool) {
	var err error

	info, skip := file.PrepareInfo(ctx, callsStartTime)
	if skip {
		log.Info(ctx, "Skip file '%s' for uploading", file.localFilePath)
		return 0, false
	}

	err = csi.db.InsertS3File(ctx, *info)
	if err != nil {
		log.Error(ctx, err, "Error during updating db for '%s'", file.localFilePath)
	}

	uploadInfo, err := csi.client.PutObject(ctx, file.localFilePath, info.RemoteStoragePath)
	if err != nil {
		log.Error(ctx, err, "Error during uploading '%s'", file.localFilePath)
		return 0, false
	}
	fileSize = uploadInfo.Size

	info.UpdateRemoteInfo(fileSize)
	err = csi.db.UpdateS3File(ctx, *info)
	if err != nil {
		log.Error(ctx, err, "Error during updating db for '%s'", file.localFilePath)
	}
	return fileSize, true
}

func (csi *CloudStorageImpl) GetCallsFiles() *parquet.FileMap {
	return csi.callsFiles
}
