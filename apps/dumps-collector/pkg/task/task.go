package task

import (
	"archive/zip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
)

type task struct {
	dbClient db.DumpDbClient
	baseDir  string
}

func newTask(baseDir string, dbClient db.DumpDbClient) (*task, error) {
	if dbClient == nil {
		return nil, fmt.Errorf("nil db client provided")
	}
	absPath, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", baseDir)
	}
	return &task{baseDir: absPath, dbClient: dbClient}, nil
}

// splitBaseDirFromPath returns the path relative to the task's base directory.
//
// For example:
//
//	baseDir: "output/"
//	path:    "output/test-namespace-1/2024/07/31/22.zip"
//
// Returns:
//
//	"test-namespace-1/2024/07/31/22.zip"
func (t *task) splitBaseDirFromPath(path string) (string, error) {
	relPath, err := filepath.Rel(t.baseDir, path)
	if err != nil {
		return "", err
	}
	return relPath, nil
}

// collectDumpsFromPattern collects information about heap dumps and thread/top dumps
// matching the given file pattern.
//
// For example:
//
//	pattern: "output/**/2024/08/01/00/**/**/**/*.*"
//
// Returns:
//
//   - tdTopDumpsInfo:  slice of DumpInfo for thread and top dumps.
//   - heapDumpsInfo:   slice of DumpInfo for heap dumps.
func (t *task) collectDumpsFromPattern(ctx context.Context, pattern string) ([]model.DumpInfo, []model.DumpInfo) {

	// e.g., output/**/2024/08/01/00/**/**/**/*.* -> [
	//		output/test-namespace-1/2024/08/01/00/00/00/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000000.td.txt",
	//		output/test-namespace-1/2024/08/01/00/00/01/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000001.top.txt",
	//		output/test-namespace-1/2024/08/01/00/01/42/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000142.hprof.zip"
	//		...
	// 	]
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Error(ctx, err, "Error getting files for %s", pattern)
		return []model.DumpInfo{}, []model.DumpInfo{}
	}

	tdTopDumpsInfo := make([]model.DumpInfo, 0, len(files))
	heapDumpsInfo := make([]model.DumpInfo, 0)
	for _, file := range files {
		fi, err := os.Stat(file)
		if err != nil {
			log.Error(ctx, err, "Error getting information about file %s from PV", file)
			continue
		}

		// e.g., output/test-namespace-1/2024/08/01/00/00/00/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000000.td.txt
		// -> test-namespace-1/2024/08/01/00/00/00/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000000.td.txt
		path, err := t.splitBaseDirFromPath(file)
		if err != nil {
			log.Error(ctx, err, "Error calculating relative path for file %s from PV", file)
		}

		dumpInfo, err := ParseDumpInfo(path)
		if err != nil {
			log.Error(ctx, err, "Error parsing file path %s from PV", file)
			continue
		}
		if dumpInfo.Pod.RestartTime.Equal(time.UnixMilli(0).UTC()) {
			log.Warning(ctx, "Parsed dump has no pod restart time in directory, path %s", path)
		}
		dumpInfo.FileSize = fi.Size()
		if dumpInfo.DumpType == model.HeapDumpType {
			heapDumpsInfo = append(heapDumpsInfo, *dumpInfo)
		} else {
			tdTopDumpsInfo = append(tdTopDumpsInfo, *dumpInfo)
		}
	}
	return tdTopDumpsInfo, heapDumpsInfo
}

// collectDumpsFromZip parses dump metadata from a zipped hour archive.
//
// It extracts information about all dump files contained in the archive and returns
// slices for td/top dumps and for heap dumps.
//
// For example:
//
//	pathToZip := "output/test-namespace-1/2024/07/31/22.zip"
//
// Returns:
//   - tdTopDumps:  slice of DumpInfo for thread and top dumps.
//   - heapDumps:   slice of DumpInfo for heap dumps.
func (t *task) collectDumpsFromZip(ctx context.Context, pathToZip string) ([]model.DumpInfo, []model.DumpInfo, error) {
	hourZip, err := zip.OpenReader(pathToZip)
	if err != nil {
		log.Error(ctx, err, "Error opening hour zip archive %s", pathToZip)
		return []model.DumpInfo{}, []model.DumpInfo{}, err
	}
	defer hourZip.Close()

	archivePath, err := t.splitBaseDirFromPath(pathToZip)
	if err != nil {
		log.Error(ctx, err, "Error calculating relative path for file %s from PV", pathToZip)
		return []model.DumpInfo{}, []model.DumpInfo{}, err
	}

	tdTopDumpsInfo := make([]model.DumpInfo, 0, len(hourZip.File))
	heapDumpsInfo := make([]model.DumpInfo, 0)

	for _, file := range hourZip.File {
		dumpInfo, err := ParseDumpInfo(file.Name)
		if err != nil {
			log.Error(ctx, err, "Error parsing file path %s from zip archive %s", file.Name, archivePath)
			continue
		}
		if dumpInfo.Pod.RestartTime.Equal(time.UnixMilli(0).UTC()) {
			log.Warning(ctx, "Parsed dump has no pod restart time in directory, path %s from zip archive %s", file.Name, archivePath)
		}
		dumpInfo.FileSize = int64(file.UncompressedSize64)
		if dumpInfo.DumpType == model.HeapDumpType {
			heapDumpsInfo = append(heapDumpsInfo, *dumpInfo)
		} else {
			tdTopDumpsInfo = append(tdTopDumpsInfo, *dumpInfo)
		}
	}

	return tdTopDumpsInfo, heapDumpsInfo, nil
}

func (t *task) collectNamespaces(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(t.baseDir)
	if err != nil {
		log.Error(ctx, err, "Error reading namespaces from PV")
		return nil, err
	}
	namespaces := make([]string, len(entries))
	for i, entry := range entries {
		namespaces[i] = entry.Name()
	}
	return namespaces, nil
}
