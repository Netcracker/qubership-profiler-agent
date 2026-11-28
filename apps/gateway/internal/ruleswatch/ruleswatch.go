// Package ruleswatch connects a rules file watcher to a Collector extension:
// it logs each load, counts it, and reports the component status, the same
// way for both extensions.
package ruleswatch

import (
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componentstatus"
	"go.uber.org/zap"

	"github.com/Netcracker/qubership-profiler-agent/apps/gateway/internal/rules"
)

// Start watches opts.Path. opts.OnLoad applies each loaded rule set and runs
// before the load is counted and logged; Start sets opts.OnError itself.
// record counts every load, successful or not. A failed load reports a
// recoverable error on host, and the next successful load reports OK.
func Start(host component.Host, logger *zap.Logger, opts rules.WatchOptions, record func(ok bool)) (*rules.Watcher, error) {
	apply := opts.OnLoad
	var failed atomic.Bool
	opts.OnLoad = func(set rules.Set, skipped int) {
		apply(set, skipped)
		record(true)
		logger.Info("Loaded the rules file",
			zap.String("path", opts.Path), zap.Int("rules", len(set)), zap.Int("skipped", skipped))
		if failed.Swap(false) {
			componentstatus.ReportStatus(host, componentstatus.NewEvent(componentstatus.StatusOK))
		}
	}
	opts.OnError = func(err error) {
		record(false)
		logger.Warn("Rejected the rules file; the rules from its last successful load stay active, or only the inline rules and mode before the first one", zap.Error(err))
		failed.Store(true)
		componentstatus.ReportStatus(host, componentstatus.NewRecoverableErrorEvent(err))
	}
	return rules.Watch(opts)
}
