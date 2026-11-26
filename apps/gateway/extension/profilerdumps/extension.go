// Package profilerdumps is the profiler_dumps Collector extension: an HTTP
// listener for diagtools dump uploads that forwards them to the dumps
// collector or discards them, per source. The logic lives in
// internal/dumps; this package wires it into the Collector.
package profilerdumps

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/dumps"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/ruleswatch"
)

const scopeName = "github.com/Netcracker/qubership-profiler-agent/apps/gateway/extension/profilerdumps"

var componentType = component.MustNewType("profiler_dumps")

// NewFactory returns the profiler_dumps extension factory. The listener
// opens in Start, not when the factory or the extension is created.
func NewFactory() extension.Factory {
	return extension.NewFactory(
		componentType,
		func() component.Config { return createDefaultConfig() },
		create,
		component.StabilityLevelDevelopment,
	)
}

func create(_ context.Context, set extension.Settings, cfg component.Config) (extension.Extension, error) {
	return &dumpsExtension{cfg: cfg.(*Config), set: set}, nil
}

type dumpsExtension struct {
	cfg     *Config
	set     extension.Settings
	srv     *dumps.Server
	watcher *rules.Watcher
}

func (e *dumpsExtension) Start(_ context.Context, host component.Host) error {
	cfg := e.cfg
	logger := e.set.Logger
	policy := &rules.Policy{Default: cfg.Mode, Inline: cfg.Rules}

	sc := dumps.Config{
		Endpoint:              cfg.Endpoint,
		PathPrefix:            cfg.PathPrefix,
		AllowedMethods:        cfg.AllowedMethods,
		ConnectTimeout:        cfg.ConnectTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ReadHeaderTimeout:     cfg.ReadHeaderTimeout,
		IdleTimeout:           cfg.IdleTimeout,
		MaxBodySize:           int64(cfg.MaxBodySize),
		MaxConcurrentUploads:  cfg.MaxConcurrentUploads,
		DrainTimeout:          cfg.DrainTimeout,
	}
	if cfg.Upstream != "" {
		u, err := cfg.upstreamURL()
		if err != nil {
			return err
		}
		sc.Upstream = u
	}
	if len(cfg.MaxBodySizePerType) > 0 {
		sc.MaxBodySizePerType = make(map[string]int64, len(cfg.MaxBodySizePerType))
		for t, size := range cfg.MaxBodySizePerType {
			sc.MaxBodySizePerType[t] = int64(size)
		}
	}
	srv, err := dumps.New(sc, policy, logger, e.set.MeterProvider.Meter(scopeName))
	if err != nil {
		return err
	}
	e.srv = srv

	if cfg.RulesFile != "" {
		w, err := ruleswatch.Start(host, logger, rules.WatchOptions{
			Path:        cfg.RulesFile,
			Surface:     rules.Dumps,
			HasUpstream: cfg.Upstream != "",
			OnLoad:      func(set rules.Set, _ int) { policy.SetFileRules(set) },
		}, srv.RecordRulesReload)
		if err != nil {
			return err
		}
		e.watcher = w
	}

	if err := srv.Start(func(err error) {
		logger.Error("Dump upload listener failed", zap.Error(err))
		componentstatus.ReportStatus(host, componentstatus.NewFatalErrorEvent(err))
	}); err != nil {
		if e.watcher != nil {
			_ = e.watcher.Close()
		}
		return err
	}
	logger.Info("Serving dump uploads",
		zap.String("endpoint", srv.Addr().String()), zap.String("mode", string(cfg.Mode)), zap.String("upstream", cfg.Upstream))
	return nil
}

func (e *dumpsExtension) Shutdown(ctx context.Context) error {
	var errs []error
	if e.watcher != nil {
		errs = append(errs, e.watcher.Close())
	}
	if e.srv != nil {
		errs = append(errs, e.srv.Shutdown(ctx))
	}
	return errors.Join(errs...)
}
