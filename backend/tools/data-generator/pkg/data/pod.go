package data

import (
	cryptoRand "crypto/rand"
	"fmt"
	"sort"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/common"
	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	commonmodel "github.com/Netcracker/qubership-profiler-backend/libs/storage"
	"github.com/Netcracker/qubership-profiler-backend/libs/storage/parquet"
)

type (
	PodInfoGen struct {
		Namespace   string
		ServiceName string
		PodName     string
		RestartTime time.Time
		Params      *data.Params
		Dictionary  *data.Dictionary
		Traces      map[string][]byte
	}
)

func (pi PodInfoGen) PodId() string {
	return fmt.Sprintf("%v-%v_%d", pi.Namespace, pi.PodName, pi.RestartTime.Unix())
}

func (pi PodInfoGen) EnrichedCalls(callsInfoList []*data.CallInfo) []*EnrichedCall {
	arr := make([]*EnrichedCall, 0, len(callsInfoList))
	for _, ci := range callsInfoList {
		cp := pi.EnrichedCall(ci.Call)
		arr = append(arr, &cp)
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i].Time > arr[j].Time
	})
	return arr
}

func (pi PodInfoGen) EnrichedCall(c data.Call) EnrichedCall {
	cp := pi.randomize(c)

	cp.Method = pi.transformMethod(c)
	cp.Params = pi.transformParams(c)
	cp.TraceId = c.TraceIndex()
	cp.Trace = string(pi.randomTrace(cp.TraceId))

	return cp
}

func (pi PodInfoGen) transformMethod(c data.Call) string {
	return pi.Dictionary.Get(c.Method)
}

func (pi PodInfoGen) transformParams(c data.Call) parquet.Parameters {
	params := make(parquet.Parameters)
	for k, v := range c.Params {
		key := pi.Dictionary.Get(k)
		params.AddVal(key, v...)
	}
	params.AddVal("thread", c.ThreadName) // TODO check for override
	return params
}

func (pi PodInfoGen) randomTrace(key string) []byte { // generate trace as binary
	if pi.Traces == nil {
		pi.Traces = map[string][]byte{}
	}
	if trace, has := pi.Traces[key]; has {
		return trace
	}

	b := make([]byte, 1024)
	_, err := cryptoRand.Read(b)
	if err != nil {
		panic(err)
	}
	pi.Traces[key] = b
	return b
}

func (pi PodInfoGen) randomize(c data.Call) EnrichedCall {
	t := common.RandomTime(time.Now())
	return EnrichedCall{
		CallParquet: parquet.CallParquet{
			Namespace:         pi.Namespace,
			ServiceName:       pi.ServiceName,
			PodName:           pi.PodName,
			RestartTime:       pi.RestartTime.UnixMilli(),
			Time:              t.UnixMilli(),
			Duration:          int32(c.Duration),
			CpuTime:           common.Random(20, int64(c.Duration)),
			WaitTime:          common.Random(15, int64(c.Duration)),
			MemoryUsed:        c.MemoryUsed, // utils.Random(0, 20*1024*1024) // TODO
			SuspendDuration:   int32(common.Random(20, int64(c.Duration))),
			Calls:             int32(common.Random(1, 700)),
			Transactions:      int32(common.Random(1, 400)),
			NetWritten:        common.Random(0, 20*1024*1024),
			NonBlocking:       c.NonBlocking,
			QueueWaitDuration: int32(c.QueueWaitDuration),
			LogsGenerated:     int64(c.LogsGenerated), // TODO random?
			LogsWritten:       int64(c.LogsWritten),
			FileRead:          int64(c.FileRead),
			FileWritten:       int64(c.FileWritten),
			NetRead:           int64(c.NetRead),
		},
		origin: &c,
	}
}

// -----------------------------------------------------------------------------

func (pi PodInfoGen) GetPodInfo() commonmodel.PodInfo {
	return commonmodel.PodInfo{
		PodId:       pi.PodId(),
		Namespace:   pi.Namespace,
		ServiceName: pi.ServiceName,
		PodName:     pi.PodName,
		ActiveSince: pi.RestartTime,
		LastRestart: pi.RestartTime,
		LastActive:  time.Now(),
		Tags:        map[string]string{},
	}
}

func (pi PodInfoGen) GetPodRestart() commonmodel.PodRestart {
	return commonmodel.PodRestart{
		PodId:       pi.PodId(),
		Namespace:   pi.Namespace,
		ServiceName: pi.ServiceName,
		PodName:     pi.PodName,
		ActiveSince: pi.RestartTime,
		RestartTime: pi.RestartTime,
		LastActive:  time.Now(),
	}
}

func (pi PodInfoGen) GetParams() []commonmodel.Param {
	params := make([]commonmodel.Param, len(pi.Params.List))
	for i, parserParam := range pi.Params.List {
		params[i] = commonmodel.Param{
			PodId:       pi.PodId(),
			PodName:     pi.PodName,
			RestartTime: pi.RestartTime,
			ParamName:   parserParam.Name,
			ParamIndex:  parserParam.IsIndex,
			ParamList:   parserParam.IsList,
			ParamOrder:  parserParam.Order,
			Signature:   parserParam.Signature,
		}
	}
	return params
}

func (pi PodInfoGen) GetDictionary() []commonmodel.Dictionary {
	dict := make([]commonmodel.Dictionary, len(pi.Dictionary.List))
	for i, v := range pi.Dictionary.List {
		dict[i] = commonmodel.Dictionary{
			PodId:       pi.PodId(),
			PodName:     pi.PodName,
			RestartTime: pi.RestartTime,
			Position:    i,
			Tag:         v.Word,
		}
	}
	return dict
}
