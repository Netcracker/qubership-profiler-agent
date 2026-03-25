package helpers

import (
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage"
)

func GetTestCallS3FileName(namespace string, dr model.DurationRange) string {
	return fmt.Sprintf("%s-%s.parquet", namespace, dr.Title)
}

func GetTestDumpS3FileName(namespace string, dumpType model.DumpType) string {
	return fmt.Sprintf("%s-%s.parquet", namespace, dumpType)
}

func GetTestS3FileRemotePath(fileName string, ts time.Time) string {
	return fmt.Sprintf("%s/%s", common.DateHour(ts), fileName)
}
