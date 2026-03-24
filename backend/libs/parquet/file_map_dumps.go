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
)

func (fm *FileMap) DumpKey(ns string, dumpType *model.DumpType) string {
	return fmt.Sprintf("%s-%s", ns, *dumpType)
}

func (fm *FileMap) GetDumpsFile(ctx context.Context, ns string, dumpType *model.DumpType, startTime time.Time) (*FileWorker, error) {
	key := fm.DumpKey(ns, dumpType)

	err := files.CheckDir(ctx, fm.CurrentDir)
	if err != nil {
		return nil, err
	}

	pFile, ok := fm.Files[key]
	if !ok {
		pq, err := fm.openDumpsParquet(ctx, ns, dumpType, startTime)
		if err == nil {
			fm.Files[key] = pq
		}
		return pq, err
	}
	return pFile, nil
}

func (fm *FileMap) DumpsFilename(namespace string, dumpType *model.DumpType) (fName string, fPath string) {
	fName = fmt.Sprintf("%s_dumps.origin.parquet", namespace)
	if dumpType != nil {
		fName = fmt.Sprintf("%s-%s.parquet", namespace, *dumpType)
	}
	fPath = fmt.Sprintf("%s/%s", fm.CurrentDir, fName)
	return fName, fPath
}

func (fm *FileMap) openDumpsParquet(ctx context.Context, namespace string, dumpType *model.DumpType, startTime time.Time) (*FileWorker, error) {
	if dumpType == nil {
		return nil, fmt.Errorf("empty dump type")
	}

	fName, fPath := fm.DumpsFilename(namespace, dumpType)
	uuid := common.RandomUuid()
	info := inventory.PrepareDumpsFileInfo(uuid,
		time.Now(), startTime, startTime.Add(fm.ParquetParams.S3FileLifeTime),
		namespace, *dumpType, fName, fPath)
	var obj interface{} = new(parquet.DumpParquet)

	//	don't forget to call `defer worker.Close()`
	return CreateWorker(ctx, fm.ParquetParams, info, obj)
}
