package generator

import (
	"context"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Netcracker/qubership-profiler-backend/libs/parser"

	"github.com/pkg/errors"
)

type (
	LoadedData struct { // binary from files (ready to persist to k6 as SharedArray)
		TdDumps  []*DumpFile
		TopDumps []*DumpFile
		TcpDumps []*parser.LoadedTcpData
	}

	DumpFile struct {
		Filename string
		Path     string // full path
		Size     int64
		Data     []byte
	}
)

var (
	mutex  sync.Mutex
	cached *LoadedData
)

func ClearCache(ctx context.Context) {
	mutex.Lock()
	defer mutex.Unlock()
	cached = nil
}

func LoadData(ctx context.Context, opts Options) (res *LoadedData, err error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	mutex.Lock()
	defer mutex.Unlock()

	tdDir := filepath.Join(opts.DataDirectory(), "dumps.td")
	topDir := filepath.Join(opts.DataDirectory(), "dumps.top")
	tcpDir := filepath.Join(opts.DataDirectory(), "dumps.tcp")

	if cached != nil {
		return cached, nil
	}
	res = &LoadedData{}
	res.TdDumps, err = listDumpFiles(ctx, tdDir, ".td.txt")
	if err == nil {
		res.TopDumps, err = listDumpFiles(ctx, topDir, ".top.txt")
	}
	if err == nil {
		res.TcpDumps, err = listTcpFiles(ctx, tcpDir)
	}
	if err == nil {
		err = res.Validate()
	}
	if err == nil {
		cached = res
	}
	return res, err
}

func (s *LoadedData) Validate() error {
	if len(s.TcpDumps) == 0 {
		return errors.New("no prepared tcp dumps")
	}
	if len(s.TdDumps) == 0 {
		return errors.New("no prepared thread dumps")
	}
	if len(s.TopDumps) == 0 {
		return errors.New("no prepared top dumps")
	}
	return nil
}

func listDumpFiles(ctx context.Context, path string, extension string) ([]*DumpFile, error) {
	res := []*DumpFile{}
	err := listFiles(ctx, path, func(name string, size int64, filePath string) error {
		if !strings.HasSuffix(name, extension) {
			return nil
		}
		data, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}
		res = append(res, &DumpFile{
			Filename: name,
			Path:     filePath,
			Size:     size,
			Data:     data,
		})
		return nil
	})
	return res, err
}

func listTcpFiles(ctx context.Context, path string) (res []*parser.LoadedTcpData, err error) {
	err = listFiles(ctx, path, func(name string, size int64, filePath string) error {
		if !strings.HasSuffix(name, ".protocol") {
			return nil
		}
		file := parser.TcpFile{FileName: name, FilePath: filePath}
		parsed, err := parser.ParsePodTcpDump(ctx, file)
		if err != nil {
			return err
		}
		res = append(res, parsed)
		return nil
	})
	return res, err
}

func listFiles(ctx context.Context, path string, visit func(string, int64, string) error) error {
	files, err := os.ReadDir(path) //read the files from the directory
	if err != nil {
		log.Error(ctx, err, "error reading directory: %s", path) // if directory is not read properly
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		var info fs.FileInfo
		info, err = file.Info()
		if err != nil {
			return err
		}
		filePath := path + "/" + file.Name()
		err = visit(file.Name(), info.Size(), filePath)
		if err != nil {
			return err
		}
	}
	return err
}
