package parquet

import (
	"context"
	"path/filepath"

	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/inventory"
	"github.com/xitongsys/parquet-go-source/local"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type FileWorker struct {
	inventory.S3FileInfo
	LocalWriter   source.ParquetFile
	ParquetWriter *writer.ParquetWriter
	FailRowsCount int
	closed        bool // already closed
}

// CreateWorker utility to persist data to local parquet file
//
//	NB! don't forget to call `defer pq.Close()`
func CreateWorker(ctx context.Context, params Params, info *inventory.S3FileInfo, objType interface{}) (*FileWorker, error) {
	filePath := filepath.Dir(info.LocalFilePath)
	err := files.CheckDir(ctx, filePath)
	if err != nil {
		return nil, err
	}

	fw, err := local.NewLocalFileWriter(info.LocalFilePath)
	if err != nil {
		log.Error(ctx, err, "Can't create parquet writer")
		return nil, err
	}

	pw, err := writer.NewParquetWriter(fw, objType, 4)
	if err != nil {
		log.Error(ctx, err, "Can't create parquet writer")
		return nil, err
	}

	pq := &FileWorker{
		*info, fw, pw, 0, false,
	}
	pq.UpdateWriterParams(params)
	return pq, nil
}

func (fm *FileWorker) UpdateWriterParams(params Params) {
	fm.ParquetWriter.RowGroupSize = params.RowGroupSize
	fm.ParquetWriter.PageSize = params.PageSize
	fm.ParquetWriter.CompressionType = params.CompressionType
}

func (fm *FileWorker) Info() inventory.S3FileInfo {
	return fm.S3FileInfo
}

func (fm *FileWorker) Write(ctx context.Context, src interface{}) error {
	err := fm.ParquetWriter.Write(src)
	if err != nil {
		log.Error(ctx, err, "[%v:%v] Write error", fm.Uuid, fm.FileName)
	} else {
		fm.RowsCount++
	}
	return err
}

func (fm *FileWorker) WriteStop(ctx context.Context) error {
	err := fm.ParquetWriter.WriteStop()
	if err != nil {
		log.Error(ctx, err, "[%v:%v] Write error", fm.Uuid, fm.FileName)
	}
	return err
}

func (fm *FileWorker) Close(ctx context.Context) error {
	err := fm.LocalWriter.Close()
	if err != nil {
		log.Error(ctx, err, "[%v:%v] Close error", fm.Uuid, fm.FileName)
	}
	return err
}
