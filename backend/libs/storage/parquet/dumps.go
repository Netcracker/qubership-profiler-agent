package parquet

import (
	"fmt"
)

// Data Structure for Parquet files

// -----------------------------------------------------------------------------

type DumpParquet struct {
	Time        int64             `parquet:"name=time, type=INT64"`
	Namespace   string            `parquet:"name=namespace, type=BYTE_ARRAY, convertedtype=UTF8"`
	ServiceName string            `parquet:"name=serviceName, type=BYTE_ARRAY, convertedtype=UTF8"`
	PodName     string            `parquet:"name=podName, type=BYTE_ARRAY, convertedtype=UTF8"`
	RestartTime int64             `parquet:"name=restartTime, type=INT64"`
	PodType     string            `parquet:"name=podType, type=BYTE_ARRAY, convertedtype=UTF8"`
	DumpType    string            `parquet:"name=dumpType, type=BYTE_ARRAY, convertedtype=UTF8"`
	BytesSize   int64             `parquet:"name=bytesSize, type=INT64"`
	Info        map[string]string `parquet:"name=params, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	BinaryData  string            `parquet:"name=binaryData, type=BYTE_ARRAY"`
}

func (dp *DumpParquet) String() string {
	return fmt.Sprintf("DumpParquet{time=%v, namespace=%v, service_name=%v, pod_namw=%v, restart_time=%v, pod_type=%v, dump_type=%v, bytes_size=%v}",
		dp.Time, dp.Namespace, dp.ServiceName, dp.PodName, dp.RestartTime, dp.PodType, dp.DumpType, dp.BytesSize)
}
