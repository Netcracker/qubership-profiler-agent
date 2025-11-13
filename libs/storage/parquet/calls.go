package parquet

import (
	"fmt"

	"github.com/Netcracker/qubership-profiler-backend/libs/storage/index"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
)

// Data Structure for Parquet files

// -----------------------------------------------------------------------------

type (
	Parameters      map[string]*ParamsValueList
	ParamsValueList struct {
		ValueList []string `parquet:"name=valueList, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	}
)

func (p Parameters) AddVal(key string, values ...string) {
	if len(values) == 0 {
		return
	}

	valList, ok := p[key]
	if !ok {
		p[key] = &ParamsValueList{ValueList: append([]string(nil), values...)}
		return
	}

	if cap(valList.ValueList)-len(valList.ValueList) < len(values) {
		newSlice := make([]string, len(valList.ValueList), len(valList.ValueList)+len(values))
		copy(newSlice, valList.ValueList)
		valList.ValueList = newSlice
	}
	valList.ValueList = append(valList.ValueList, values...)
}

func (p Parameters) Get(key string) []string {
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

type CallParquet struct {
	Time              int64      `parquet:"name=time, type=INT64"`
	CpuTime           int64      `parquet:"name=cpuTime, type=INT64"`
	WaitTime          int64      `parquet:"name=waitTime, type=INT64"`
	MemoryUsed        int64      `parquet:"name=memoryUsed, type=INT64"`
	Duration          int32      `parquet:"name=duration, type=INT32"`
	NonBlocking       int64      `parquet:"name=nonBlocking, type=INT64"`
	QueueWaitDuration int32      `parquet:"name=queueWaitDuration, type=INT32"`
	SuspendDuration   int32      `parquet:"name=suspendDuration, type=INT32"`
	Calls             int32      `parquet:"name=calls, type=INT32"`
	Transactions      int32      `parquet:"name=transactions, type=INT32, convertedtype=UINT_32"`
	LogsGenerated     int64      `parquet:"name=logsGenerated, type=INT64"`
	LogsWritten       int64      `parquet:"name=logsWritten, type=INT64"`
	FileRead          int64      `parquet:"name=fileRead, type=INT64, convertedtype=UINT_64"`
	FileWritten       int64      `parquet:"name=fileWritten, type=INT64, convertedtype=UINT_64"`
	NetRead           int64      `parquet:"name=netRead, type=INT64, convertedtype=UINT_64"`
	NetWritten        int64      `parquet:"name=netWritten, type=INT64, convertedtype=UINT_64"`
	Namespace         string     `parquet:"name=namespace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ServiceName       string     `parquet:"name=serviceName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PodName           string     `parquet:"name=podName, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RestartTime       int64      `parquet:"name=restartTime, type=INT64"`
	Method            string     `parquet:"name=method, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Params            Parameters `parquet:"name=params, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8" `
	TraceId           string     `parquet:"name=index, type=BYTE_ARRAY, convertedtype=UTF8"` // seqId_bufOffset_recordIndex
	Trace             string     `parquet:"name=bytearray, type=BYTE_ARRAY"`
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
