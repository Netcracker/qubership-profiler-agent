package task

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/model"
)

func splitDir(path string) []string {
	// Replace '\' with '/' for archive dumps to ensure tests pass on Windows
	clean := strings.ReplaceAll(path, "\\", "/")
	return strings.Split(clean, "/")
}

func parseTimeHourFromDirs(pathEntities []string) (*time.Time, error) {
	// time information
	year, err := strconv.Atoi(pathEntities[0])
	if err != nil {
		return nil, fmt.Errorf("incorrect year directory %s", pathEntities[0])
	}
	month, err := strconv.Atoi(pathEntities[1])
	if err != nil {
		return nil, fmt.Errorf("incorrect month directory %s", pathEntities[1])
	}
	day, err := strconv.Atoi(pathEntities[2])
	if err != nil {
		return nil, fmt.Errorf("incorrect day directory %s", pathEntities[2])
	}
	pathEntities[3] = strings.TrimSuffix(pathEntities[3], ".zip")
	hour, err := strconv.Atoi(pathEntities[3])
	if err != nil {
		return nil, fmt.Errorf("incorrect hour directory %s", pathEntities[3])
	}
	minute, err := strconv.Atoi(pathEntities[4])
	if err != nil {
		return nil, fmt.Errorf("incorrect minute directory %s", pathEntities[4])
	}
	second, err := strconv.Atoi(pathEntities[5])
	if err != nil {
		return nil, fmt.Errorf("incorrect second directory %s", pathEntities[5])
	}
	creationTime := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	return &creationTime, nil
}

// parseDumpInfoFromDirs extracts dump metadata from the provided path components.
//
// For example:
//
//	pathEntities := []string{
//		"test-namespace-1", "2024", "08", "01", "00", "00", "00",
//		"test-service-1-5cbcd847d-l2t7t_1719318147399",
//		"20240801T000000.td.txt",
//	}
//
// Returns:
//
//	&model.DumpInfo{
//	    Pod: model.Pod{
//	        Namespace:    "test-namespace-1",
//	        ServiceName:  "test-service-1",
//	        PodName:      "test-service-1-5cbcd847d-l2t7t_1719318147399",
//	        RestartTime:  2024-06-25 12:22:27.399 +0000,
//	    },
//	    CreationTime: 2024-08-01 00:01:42 +0000,
//	    DumpType:     heap,
//	}, nil
func parseDumpInfoFromDirs(pathEntities []string) (*model.DumpInfo, error) {
	// time information
	creationTime, err := parseTimeHourFromDirs(pathEntities[1:7])
	if err != nil {
		return nil, err
	}

	// pod information
	namespace := pathEntities[0]
	serviceName, podName, restartTime, err := ParseFromPodNameWithTs(pathEntities[7])
	if err != nil {
		return nil, err
	}

	// dump type
	var dumpType model.DumpType
	filename := pathEntities[8]
	if strings.HasSuffix(filename, model.TdDumpType.GetFileSuffix()) {
		dumpType = model.TdDumpType
	} else if strings.HasSuffix(filename, model.TopDumpType.GetFileSuffix()) {
		dumpType = model.TopDumpType
	} else if strings.HasSuffix(filename, model.HeapDumpType.GetFileSuffix()) {
		dumpType = model.HeapDumpType
	} else {
		return nil, fmt.Errorf("file name %s has incorrect type", filename)
	}

	return &model.DumpInfo{
		Pod: model.Pod{
			Namespace:   namespace,
			ServiceName: serviceName,
			PodName:     podName,
			RestartTime: restartTime,
		},
		CreationTime: *creationTime,
		DumpType:     dumpType,
	}, nil
}

func ParseFromPodNameWithTs(podNameWithTs string) (serviceName string, podName string, restartTime time.Time, err error) {
	re, err := regexp.Compile(`^(?<pod_name>(?<service_name>[a-z0-9]+(?:-[a-z0-9]+)*?)(?:-([a-f0-9]*))?-([a-z0-9]+)(?<ts>_([0-9]+))?)$`)
	if err != nil {
		return
	}
	if !re.MatchString(podNameWithTs) {
		err = fmt.Errorf("directory %s does not match pod name with ts format", podNameWithTs)
		return
	}
	match := re.FindStringSubmatch(podNameWithTs)
	for i, name := range re.SubexpNames() {
		switch name {
		case "pod_name":
			podName = match[i]
		case "service_name":
			serviceName = match[i]
		case "ts":
			if match[i] != "" {
				var ts int64
				ts, err = strconv.ParseInt(match[i][1:], 10, 0)
				if err != nil {
					err = fmt.Errorf("incorrect ts in pod name directory %s: %s", podNameWithTs, err)
					return
				}
				restartTime = time.UnixMilli(ts).UTC()
			} else {
				restartTime = time.UnixMilli(0).UTC()
			}
		}
	}

	return
}

// ParseDumpInfo extracts dump metadata from the provided path.
//
// For example:
//
//	pathEntities := "test-namespace-1/2024/08/01/00/00/00/test-service-1-5cbcd847d-l2t7t_1719318147399/20240801T000000.td.txt"
//
// Returns:
//
//	&model.DumpInfo{
//	    Pod: model.Pod{
//	        Namespace:    "test-namespace-1",
//	        ServiceName:  "test-service-1",
//	        PodName:      "test-service-1-5cbcd847d-l2t7t_1719318147399",
//	        RestartTime:  2024-06-25 12:22:27.399 +0000,
//	    },
//	    CreationTime: 2024-08-01 00:01:42 +0000,
//	    DumpType:     heap,
//	}, nil
func ParseDumpInfo(path string) (*model.DumpInfo, error) {
	pathEntities := splitDir(path)
	if len(pathEntities) != 9 || pathEntities[0] == ".." || pathEntities[0] == "." {
		return nil, fmt.Errorf("path %s does not match the dump file path format: %v, (len=%d) ", path, pathEntities, len(pathEntities))
	}

	return parseDumpInfoFromDirs(pathEntities)
}

func ParseTimeHour(path string) (*time.Time, error) {
	pathEntities := splitDir(path)
	if len(pathEntities) != 5 || pathEntities[0] == ".." || pathEntities[0] == "." {
		return nil, fmt.Errorf("path %s does not match the time hour path format", path)
	}

	pathEntities = append(pathEntities, "00", "00")
	return parseTimeHourFromDirs(pathEntities[1:7])
}

// FileDayDirInPV returns a relative path for the given time in the format "YYYY/MM/DD".
//
// For example, 2024-07-31 22:59:35 +0000 becomes "2024/07/31"
func FileDayDirInPV(t time.Time) string {
	return filepath.Join(fmt.Sprintf("%d", t.Year()), fmt.Sprintf("%02d", t.Month()), fmt.Sprintf("%02d", t.Day()))
}

// FileHourDirInPV returns a relative path for the given time in the format "YYYY/MM/DD/HH".
//
// For example, 2024-07-31 22:59:35 +0000 becomes "2024/07/31/22"
func FileHourDirInPV(t time.Time) string {
	return filepath.Join(FileDayDirInPV(t), fmt.Sprintf("%02d", t.Hour()))
}

// FileMinuteDirInPV returns a relative path for the given time in the format "YYYY/MM/DD/HH/mm".
//
// For example, 2024-07-31 22:59:35 +0000 becomes "2024/07/31/22/59"
func FileMinuteDirInPV(t time.Time) string {
	return filepath.Join(FileHourDirInPV(t), fmt.Sprintf("%02d", t.Minute()))
}

// FileSecondDirInPV returns a relative path for the given time in the format "YYYY/MM/DD/HH/mm/ss".
//
// For example, 2024-07-31 22:59:35 +0000 becomes "2024/07/31/22/59/35"
func FileSecondDirInPV(t time.Time) string {
	return filepath.Join(FileMinuteDirInPV(t), fmt.Sprintf("%02d", t.Second()))
}

// FileHourZipInPV returns a relative path to the ZIP archive for the given hour,
// in the format "YYYY/MM/DD/HH.zip".
//
// For example, 2024-07-31 22:59:35 +0000 becomes "2024/07/31/22.zip"
func FileHourZipInPV(t time.Time) string {
	return filepath.Join(FileDayDirInPV(t), fmt.Sprintf("%02d.zip", t.Hour()))
}

func FileNameInPV(t time.Time) string {
	return fmt.Sprintf("%d%02d%02dT%02d%02d%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

// HourArchiveName returns the archive name for top/thread dumps for the given hour.
//
// For example, 2024-07-31 23:00:00 +0000 becomes 23.zip
func HourArchiveName(t time.Time) string {
	return fmt.Sprintf("%02d.zip", t.Hour())
}
