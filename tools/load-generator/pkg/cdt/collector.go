package cdt

import (
	"context"
	"fmt"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/tools/load-generator/pkg/utils"
	"github.com/Netcracker/qubership-profiler-backend/libs/generator"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
	"go.k6.io/k6/metrics"
)

type (
	Collector struct {
		VU      modules.VU
		Metrics Metrics
	}
)

func (module *Collector) Exports() modules.Exports {
	return modules.Exports{Default: module}
}

func (module *Collector) Validate(opts generator.Options) error {
	return opts.Validate()
}

func (module *Collector) Prepare(opts generator.Options) (*CdtAgentConnection, error) {
	module.check(opts.Validate())

	Tags := metrics.NewRegistry().RootTagSet().WithTagsFromMap(opts.Tags)

	cdtConn := &CdtAgentConnection{
		vu:      module.VU,
		metrics: module.Metrics,
		Opts:    opts,
		tags:    Tags,
	}
	return cdtConn, nil
}

func (module *Collector) Close(conn CdtAgentConnection) error {
	return conn.Close()
}

func (module *Collector) LoadData(opts generator.Options) (*generator.LoadedData, error) {
	ctx, _ := getContext(module.VU, opts.LogLevel)
	utils.LogInfo(ctx, "load data for suite")
	x, err := generator.LoadData(ctx, opts)
	module.check(err)
	return x, err
}

func (module *Collector) PrepareSuite(opts generator.Options) (*generator.Suite, error) {
	ctx, _ := getContext(module.VU, opts.LogLevel)
	utils.LogInfo(ctx, "load data for suite")
	data, err := generator.LoadData(ctx, opts)
	module.check(err)
	utils.LogInfo(ctx, "preparing suite")
	x, err := generator.PrepareSuite(ctx, opts, data)
	module.check(err)
	return x, err
}

func (module *Collector) ParseDuration(duration string) int64 {
	t, err := time.ParseDuration(duration)
	module.check(err)
	return t.Milliseconds()
}

func (module *Collector) check(err error) {
	if err != nil {
		fmt.Println(err)
		if module.VU != nil {
			common.Throw(module.VU.Runtime(), err)
		} else {
			panic(err)
		}
	}
}

func getContext(vu modules.VU, logLevel string) (ctx context.Context, fn context.CancelFunc) {
	if vu != nil {
		ctx, fn = context.WithCancel(vu.Context())
	} else {
		ctx, fn = context.Background(), func() {}
	}
	ctx = context.WithValue(ctx, utils.LogLevel, logLevel)
	return
}
