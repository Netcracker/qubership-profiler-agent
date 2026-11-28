// Package profilertcp is the profiler_tcp Collector extension: a TCP
// listener for profiler agent connections that forwards them to the
// collector or discards them, per source. The logic lives in internal/tcp;
// this package wires it into the Collector.
package profilertcp

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/ruleswatch"
	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/tcp"
)

const scopeName = "github.com/Netcracker/qubership-profiler-agent/apps/gateway/extension/profilertcp"

var componentType = component.MustNewType("profiler_tcp")

// NewFactory returns the profiler_tcp extension factory. The listener
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
	return &tcpExtension{cfg: cfg.(*Config), set: set}, nil
}

type tcpExtension struct {
	cfg     *Config
	set     extension.Settings
	proxy   *tcp.Proxy
	watcher *rules.Watcher
}

func (e *tcpExtension) Start(_ context.Context, host component.Host) error {
	cfg := e.cfg
	logger := e.set.Logger
	policy := &rules.Policy{Default: cfg.Mode, Inline: cfg.Rules}
	proxy, err := tcp.New(tcp.Config{
		Endpoint:       cfg.Endpoint,
		Upstream:       cfg.Upstream,
		ConnectTimeout: cfg.ConnectTimeout,
		MaxConnections: cfg.MaxConnections,
		CopyBufferSize: int(cfg.CopyBufferSize),

		HandshakeTimeout:     cfg.HandshakeTimeout,
		RotationPeriod:       cfg.Discard.RotationPeriod,
		RequiredRotationSize: uint64(cfg.Discard.RequiredRotationSize),
	}, policy, logger, e.set.MeterProvider.Meter(scopeName))
	if err != nil {
		return err
	}
	e.proxy = proxy

	if cfg.RulesFile != "" {
		w, err := ruleswatch.Start(host, logger, rules.WatchOptions{
			Path:        cfg.RulesFile,
			Surface:     rules.TCP,
			HasUpstream: cfg.Upstream != "",
			OnLoad: func(set rules.Set, _ int) {
				policy.SetFileRules(set)
				// A profiler connection lives as long as its pod, so new
				// rules must reach the open ones too.
				proxy.Reevaluate()
			},
		}, proxy.RecordRulesReload)
		if err != nil {
			return err
		}
		e.watcher = w
	}

	if err := proxy.Start(); err != nil {
		if e.watcher != nil {
			_ = e.watcher.Close()
		}
		return err
	}
	logger.Info("Serving profiler agent connections",
		zap.String("endpoint", proxy.Addr().String()), zap.String("mode", string(cfg.Mode)),
		zap.String("upstream", cfg.Upstream))
	return nil
}

func (e *tcpExtension) Shutdown(ctx context.Context) error {
	var errs []error
	if e.watcher != nil {
		errs = append(errs, e.watcher.Close())
	}
	if e.proxy != nil {
		errs = append(errs, e.proxy.Shutdown(ctx))
	}
	return errors.Join(errs...)
}
