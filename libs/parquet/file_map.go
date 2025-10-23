package parquet

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/files"
	"os"
	"sync"
)

type FileMap struct {
	mu            sync.RWMutex
	CurrentDir    string
	Files         map[string]*FileWorker
	ParquetParams Params
}

func NewFileMap(outputDir string, parquetParams Params) *FileMap {
	return &FileMap{
		Files:         make(map[string]*FileWorker),
		CurrentDir:    outputDir,
		ParquetParams: parquetParams,
	}
}

func (fm *FileMap) ReadLocal(ctx context.Context) ([]os.DirEntry, error) {
	return files.List(ctx, fm.CurrentDir)
}

func (fm *FileMap) Count() int {
	return len(fm.Files)
}

func (fm *FileMap) CloseAllFiles() error {
	for _, file := range fm.Files {
		if file.closed {
			continue
		}

		if err := file.ParquetWriter.WriteStop(); err != nil {
			return err
		}

		if err := file.LocalWriter.Close(); err != nil {
			return err
		}
		file.closed = true
	}

	return nil
}

func (fm *FileMap) Get(key string) (*FileWorker, bool) {
	f, has := fm.Files[key]
	return f, has
}

func (fm *FileMap) ClearDirectory(ctx context.Context) error {
	return files.ClearDirectory(ctx, fm.CurrentDir)
}
