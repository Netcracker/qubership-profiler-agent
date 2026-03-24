package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/protocol/data"
	model "github.com/Netcracker/qubership-profiler-backend/libs/storage"

	data2 "github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/data"
	"github.com/Netcracker/qubership-profiler-backend/tools/data-generator/pkg/s3"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/parquet"
	"github.com/Netcracker/qubership-profiler-backend/libs/parser"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
)

type (
	// ToolWorker holds context of the data-generator tool to do its job (db connections, generated data,
	// list of generated files, etc.). Also, it has methods which actually do the heavy-lifting using that context
	ToolWorker struct {
		cfg      data2.Config
		cloud    s3.CloudStorage
		postgres pg.DbClient

		data    *GeneratedData   // data to be persisted (not 100% as it is, it could be adjusted during persistence)
		callsFm *parquet.FileMap // cache/registry of temporary parquet files
	}

	// GeneratedData data for all namespaces
	GeneratedData struct {
		cfg      data2.Config
		template *parser.ParsedPodDump
		pods     map[string][]data2.PodInfoGen // by namespaces
		calls    [][]*data2.EnrichedCall       // for duration ranges
	}
)

func New(ctx context.Context,
	cfg data2.Config, db pg.DbClient, cloud *s3.CloudStorageImpl,
	templatePodData *parser.ParsedPodDump) *ToolWorker {

	generatedData := &GeneratedData{
		cfg, templatePodData,
		nil, nil, // during init
	}
	callsFileMap := generatedData.init(ctx)
	cloud.SetFileCache(callsFileMap)

	return &ToolWorker{
		cfg, cloud, db,
		generatedData, callsFileMap,
	}
}

func (gd *GeneratedData) init(ctx context.Context) *parquet.FileMap {
	var allPods []data2.PodInfoGen
	gd.pods = map[string][]data2.PodInfoGen{}
	for i := 0; i < gd.cfg.Limit.NS; i++ {
		ns := gd.cfg.Namespace(i)
		gd.pods[ns] = gd.preparePods(ns)
		allPods = append(allPods, gd.pods[ns]...)
	}

	callsFm, calls := gd.prepareCalls(ctx, allPods)
	gd.calls = calls

	return callsFm
}

func (gd *GeneratedData) preparePods(ns string) (pods []data2.PodInfoGen) {
	t := gd.podStartTime()
	for j := 0; j < gd.cfg.Limit.Services; j++ {
		serviceName := gd.cfg.ServiceName(j)
		for k := 0; k < gd.cfg.Limit.Pods; k++ {
			pods = append(pods, data2.PodInfoGen{
				Namespace:   ns,
				ServiceName: serviceName,
				PodName:     gd.cfg.PodName(serviceName),
				RestartTime: t,
				Params:      gd.template.Params,
				Dictionary:  gd.template.Dictionary,
				Traces:      map[string][]byte{},
			})
		}
	}
	return pods
}

func (gd *GeneratedData) podStartTime() time.Time {
	t := time.Now()
	if gd.cfg.Limit.StartDate.Before(t) {
		t = gd.cfg.Limit.StartDate
	}
	if gd.cfg.Limit.HasHourTime() {
		if gd.cfg.Limit.HourDateTime.Before(t) {
			t = gd.cfg.Limit.HourDateTime
		}
	}
	return t
}

func (gd *GeneratedData) prepareCalls(ctx context.Context, pods []data2.PodInfoGen) (callsFm *parquet.FileMap, calls [][]*data2.EnrichedCall) {
	startTime := time.Now()

	callsFm = parquet.NewFileMap(fmt.Sprintf("%s/calls", gd.cfg.Out.OutputDir), gd.cfg.Parquet)
	defer func() {
		err := callsFm.CloseAllFiles()
		if err != nil {
			log.Error(ctx, err, "Can't close file descriptors")
		}
	}()

	for range model.Durations.List {
		calls = append(calls, []*data2.EnrichedCall{})
	}
	for _, pod := range pods {
		parquetCalls, _ := gd.generateCalls(pod)
		for _, call := range parquetCalls {
			dr := model.Durations.Get(call.Duration)
			calls[dr.Pos] = append(calls[dr.Pos], call)
		}
	}

	total := 0
	for i := range model.Durations.List {
		total += len(calls[i])
	}

	log.Info(ctx, "Generated %d partitioned files with total %d calls (for %d pods) and finished successfully in %v",
		callsFm.Count(), total, len(pods), time.Since(startTime))
	return callsFm, calls
}

func (gd *GeneratedData) generateCalls(pod data2.PodInfoGen) ([]*data2.EnrichedCall, int) {
	callsList := gd.template.Calls.List

	maximumCycleLimit := ((60 / 5) * gd.cfg.Limit.Calls) / len(callsList)

	var calls []*data.CallInfo
	for i := 0; i < maximumCycleLimit; i++ {
		calls = append(calls, callsList...)
	}

	parquetCalls := pod.EnrichedCalls(calls)
	return parquetCalls, len(parquetCalls)
}
