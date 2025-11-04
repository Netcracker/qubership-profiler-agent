package s3

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/data"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

type (
	TransferFileInfo struct {
		origin        inventory.S3FileInfo
		file          os.DirEntry
		localDir      string
		localFilePath string
	}
)

func (fi TransferFileInfo) ShortName() string {
	return fi.file.Name()
}

func (fi TransferFileInfo) PrepareInfo(ctx context.Context, t time.Time) (*inventory.S3FileInfo, bool) {
	var info *inventory.S3FileInfo
	skip := false

	localFileSize, err := files.FileSize(ctx, fi.localFilePath)
	if err != nil {
		return info, true
	}

	info, skip = fi.ParseCallsFilename(localFileSize)

	info.RowsCount = fi.origin.RowsCount
	info.StartTime = t
	info.EndTime = t.Add(data.Cfg.Parquet.S3FileLifeTime)
	info.RemoteStoragePath = inventory.CalculateRemoteStoragePath(t, info.FileName)
	info.Services = fi.origin.Services
	return info, skip
}

func (fi TransferFileInfo) ParseCallsFilename(localFileSize int64) (info *inventory.S3FileInfo, skip bool) {
	namespace, dr, ok := fi.parseCallsFilename()
	if !ok {
		return info, true
	}

	dRng := model.Durations.Get(int32(dr))
	curTime := time.Now()
	info = inventory.PrepareCallsFileInfo(
		common.RandomUuid(),
		curTime, curTime, curTime.Add(data.Cfg.Parquet.S3FileLifeTime),
		namespace, &dRng,
		fi.ShortName(), fi.localFilePath,
	)
	return info, false
}

func (fi TransferFileInfo) parseCallsFilename() (string, int, bool) {
	filename := fi.ShortName()
	if strings.Contains(filename, "origin") {
		return strings.ReplaceAll(filename, "_calls.origin.parquet", ""), -1, false
	}
	for i := len(model.Durations.List) - 1; i >= 0; i-- { // revert
		r := model.Durations.List[i]
		if strings.Contains(filename, r.Title) {
			return strings.ReplaceAll(filename, fmt.Sprintf("-%s.parquet", r.Title), ""), r.From, true
		}
	}
	return "", -1, false
}
