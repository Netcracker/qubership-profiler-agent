package inventory

import (
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

type S3FileInfo struct { // not thread-safe!
	Uuid              common.Uuid
	StartTime         time.Time // start of time range
	EndTime           time.Time // end of time range
	Type              model.FileType
	DumpType          model.DumpType
	Namespace         string
	DurationRange     int
	FileName          string
	Status            model.FileStatus
	Services          *Services
	ApiVersion        int
	CreatedTime       time.Time
	RowsCount         int
	FileSize          int64
	RemoteStoragePath string
	LocalFilePath     string
}

func CalculateRemoteStoragePath(ts time.Time, fileName string) string {
	return fmt.Sprintf("%s/%s", common.DateHour(ts), fileName)
}

func PrepareCallsFileInfo(uuid common.Uuid, ts time.Time, startTime time.Time, endTime time.Time,
	namespace string, dr *model.DurationRange, fileName, filePath string) *S3FileInfo {

	services := &Services{set: map[string]bool{}}
	return &S3FileInfo{
		Uuid:              uuid,
		StartTime:         startTime,
		EndTime:           endTime,
		Type:              model.FileCalls,
		DumpType:          model.DumpType(""),
		Namespace:         namespace,
		DurationRange:     model.DurationAsInt(dr),
		FileName:          fileName,
		Status:            model.FileCreating,
		Services:          services,
		CreatedTime:       ts,
		ApiVersion:        model.ApiVersion,
		RowsCount:         0,
		FileSize:          0,
		RemoteStoragePath: CalculateRemoteStoragePath(startTime, fileName),
		LocalFilePath:     filePath,
	}
}

func PrepareDumpsFileInfo(uuid common.Uuid, ts time.Time, startTime time.Time, endTime time.Time,
	namespace string, dumpType model.DumpType, fileName, filePath string) *S3FileInfo {

	services := &Services{set: map[string]bool{}}
	return &S3FileInfo{
		Uuid:              uuid,
		StartTime:         startTime,
		EndTime:           endTime,
		Type:              model.FileDumps,
		DumpType:          dumpType,
		Namespace:         namespace,
		DurationRange:     -1,
		FileName:          fileName,
		Status:            model.FileCreating,
		Services:          services,
		CreatedTime:       ts,
		ApiVersion:        model.ApiVersion,
		RowsCount:         0,
		FileSize:          0,
		RemoteStoragePath: CalculateRemoteStoragePath(startTime, fileName),
		LocalFilePath:     filePath,
	}
}

func (sfi *S3FileInfo) UpdateLocalInfo(services []string, fileSize int64) {
	sfi.Status = model.FileCreated
	sfi.Services.AddList(services)
	sfi.FileSize = fileSize
}

func (sfi *S3FileInfo) UpdateRemoteInfo(fileSize int64) {
	sfi.Status = model.FileCompleted
	sfi.FileSize = fileSize
}

func (sfi *S3FileInfo) AddServices(serviceSet map[string]any) {
	sfi.Services.AddMap(serviceSet)
}
