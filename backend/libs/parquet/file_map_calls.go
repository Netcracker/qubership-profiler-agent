package parquet

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"

	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

func (fm *FileMap) CallKey(ns string, duration *model.DurationRange) string {
	return fmt.Sprintf("%s-%s", ns, duration.Title)
}

func (fm *FileMap) GetCallsFile(ctx context.Context, ns string, duration *model.DurationRange, startTime time.Time) (*FileWorker, error) {
	if duration == nil {
		return nil, fmt.Errorf("invalid duration range")
	}
	log.Debug(ctx, "Get file for call with namespace: %s, duration: %s", ns, duration.Title)
	fm.mu.Lock()
	defer fm.mu.Unlock()

	key := fm.CallKey(ns, duration)

	err := files.CheckDir(ctx, fm.CurrentDir)
	if err != nil {
		return nil, err
	}

	pFile, ok := fm.Files[key]
	if !ok {
		pq, err := fm.openCallsParquet(ctx, ns, startTime, duration)
		if err == nil {
			fm.Files[key] = pq
		}

		return pq, err
	}
	return pFile, nil
}

func (fm *FileMap) CallsFilename(namespace string, dr *model.DurationRange) (fName string, fPath string) {
	fName = fmt.Sprintf("%s_calls.origin.parquet", namespace)
	if dr != nil {
		fName = fmt.Sprintf("%s-%s.parquet", namespace, dr.Title)
	}
	fPath = fmt.Sprintf("%s/%s", fm.CurrentDir, fName)
	return fName, fPath
}

func (fm *FileMap) openCallsParquet(ctx context.Context, namespace string, startTime time.Time, dr *model.DurationRange) (*FileWorker, error) {
	fName, fPath := fm.CallsFilename(namespace, dr)
	uuid := common.RandomUuid()
	info := inventory.PrepareCallsFileInfo(uuid,
		time.Now(), startTime, startTime.Add(fm.ParquetParams.S3FileLifeTime),
		namespace, dr, fName, fPath)
	var obj interface{} = new(parquet.CallParquet)

	//	don't forget to call `defer worker.Close()`
	return CreateWorker(ctx, fm.ParquetParams, info, obj)
}
