package parquet

import (
	"fmt"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

// Data Structure for Parquet files

// -----------------------------------------------------------------------------

// Parameters is the CallV2 params MAP column: per-param key → values. The Go
// map value is a plain []string; parquet-go/parquet-go schematizes it
// natively as MAP<UTF8, LIST<UTF8>> via the CallV2 field tags.
type Parameters map[string][]string

// AddVal appends values under key. The first insert copies the input slice so
// the map never aliases a caller-owned backing array.
func (p Parameters) AddVal(key string, values ...string) {
	if len(values) == 0 {
		return
	}
	if existing, ok := p[key]; ok {
		p[key] = append(existing, values...)
	} else {
		p[key] = append([]string(nil), values...)
	}
}

// Get returns the values of key, nil when the key is absent.
func (p Parameters) Get(key string) []string {
	return p[key]
}

// LegacyParameters is the params column of the legacy CallParquet shape: the
// xitongsys/parquet-go dialect could not derive a MAP value schema from a bare
// []string, so the values ride in the ParamsValueList wrapper. The writer that
// produced this shape (libs/parquet, via tools/data-generator) has been
// retired; the type is kept only so its own unit test can pin the shape.
type (
	LegacyParameters map[string]*ParamsValueList
	ParamsValueList  struct {
		ValueList []string `parquet:"name=valueList, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	}
)

func (p LegacyParameters) AddVal(key string, values ...string) {
	if len(values) == 0 {
		return
	}

	valList, ok := p[key]
	if !ok {
		p[key] = &ParamsValueList{ValueList: append([]string(nil), values...)}
		return
	}
	valList.ValueList = append(valList.ValueList, values...)
}

func (p LegacyParameters) Get(key string) []string {
	if list, has := p[key]; !has {
		return nil
	} else {
		return list.ValueList
	}
}

func (pvl *ParamsValueList) String() string {
	return fmt.Sprintf("%v", pvl.ValueList)
}

// -----------------------------------------------------------------------------

// CallParquet is the legacy dumps-collector row shape; the Stage 1 pipeline
// replaced it with CallV2. Its writer (libs/parquet, via tools/data-generator)
// has been retired, so nothing serializes it now; the struct tags still carry
// the old xitongsys/parquet-go dialect and the type survives only for its own
// unit test.
type CallParquet struct {
	Time              int64            `parquet:"name=time, type=INT64"`
	CpuTime           int64            `parquet:"name=cpuTime, type=INT64"`
	WaitTime          int64            `parquet:"name=waitTime, type=INT64"`
	MemoryUsed        int64            `parquet:"name=memoryUsed, type=INT64"`
	Duration          int32            `parquet:"name=duration, type=INT32"`
	NonBlocking       int64            `parquet:"name=nonBlocking, type=INT64"`
	QueueWaitDuration int32            `parquet:"name=queueWaitDuration, type=INT32"`
	SuspendDuration   int32            `parquet:"name=suspendDuration, type=INT32"`
	Calls             int32            `parquet:"name=calls, type=INT32"`
	Transactions      int32            `parquet:"name=transactions, type=INT32, convertedtype=UINT_32"`
	LogsGenerated     int64            `parquet:"name=logsGenerated, type=INT64"`
	LogsWritten       int64            `parquet:"name=logsWritten, type=INT64"`
	FileRead          int64            `parquet:"name=fileRead, type=INT64, convertedtype=UINT_64"`
	FileWritten       int64            `parquet:"name=fileWritten, type=INT64, convertedtype=UINT_64"`
	NetRead           int64            `parquet:"name=netRead, type=INT64, convertedtype=UINT_64"`
	NetWritten        int64            `parquet:"name=netWritten, type=INT64, convertedtype=UINT_64"`
	Namespace         string           `parquet:"name=namespace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ServiceName       string           `parquet:"name=serviceName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PodName           string           `parquet:"name=podName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RestartTime       int64            `parquet:"name=restartTime, type=INT64"`
	Method            string           `parquet:"name=method, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Params            LegacyParameters `parquet:"name=params, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8" `
	TraceId           string           `parquet:"name=index, type=BYTE_ARRAY, convertedtype=UTF8"` // seqId_bufOffset_recordIndex
	Trace             string           `parquet:"name=bytearray, type=BYTE_ARRAY"`
}

func (c *CallParquet) AppendParamsToIndex(fileUuid common.Uuid, idx *index.Map) {
	for paramName, values := range c.Params {
		idx.AddValues(fileUuid, paramName, values.ValueList)
	}
}

func (c *CallParquet) String() string {
	return fmt.Sprintf("CallParquet{time=%v, cpuTime=%v, waitTime=%v, memoryUsed=%v, "+
		"method=%v, duration=%v, queueWaitDuration=%v, suspendDuration=%v, "+
		"calls=%v, transactions=%v, traceId=%v, "+
		"params=%v}",
		c.Time, c.CpuTime, c.WaitTime, c.MemoryUsed, c.Method, c.Duration, c.QueueWaitDuration, c.SuspendDuration,
		c.Calls, c.Transactions, c.TraceId,
		c.Params)
}
