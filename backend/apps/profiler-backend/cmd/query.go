package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	appenv "github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/health"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/metrics"
	ui "github.com/Netcracker/qubership-profiler-backend/apps/ui"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/query"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/oklog/run"
	pkgerrors "github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// inFlightGrace bounds in-flight requests at shutdown (03-lifecycle.md §7.3).
const inFlightGrace = 15 * time.Second

var queryCmd = &cobra.Command{
	Use:          "query",
	Short:        "Run the query read path: /api/v1 over the hot fan-out and cold S3 tiers",
	RunE:         runQuery,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(queryCmd)
}

func runQuery(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	cfg, err := appenv.ParseQuery()
	if err != nil {
		return err
	}
	ctx, err = log.SetLevelString(ctx, cfg.LogLevel)
	if err != nil {
		return err
	}

	// Bind the external port and gate before S3 (rather than after, as
	// before): a probe must see LOADING instead of connection-refused while
	// S3 is still coming up (PR 708 review #22), the same reasoning as the
	// collector's internal port (03-lifecycle.md §2).
	gate := health.NewGate("/api/v1")
	reg := metrics.NewRegistry()
	// Expose the cdt_minio_* series (registered on the default registry inside
	// s3.NewClient) on this subcommand's own registry too.
	s3.RegisterMetrics(reg)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ExternalAPIPort))
	if err != nil {
		return pkgerrors.Wrap(err, "bind external API")
	}
	// query has no internal port, so /metrics rides the external one (04 §12);
	// the ingress publishes /api/v1 only.
	external := &http.Server{Handler: metrics.Mux(reg, gate)}
	serveErr := make(chan error, 1)
	go func() { serveErr <- external.Serve(ln) }()

	fatal := func(stage string, err error) error {
		gate.Set(health.StateFatal, stage+": "+err.Error())
		_ = external.Close()
		return pkgerrors.Wrap(err, stage)
	}

	// §7.1 step 2: verify the S3 side before serving; unrecoverable → FATAL,
	// a temporarily unreachable endpoint retries with backoff instead.
	gate.Set(health.StateLoading, "connecting to S3")
	s3params, err := cfg.S3.Params()
	if err != nil {
		return fatal("resolve S3 credentials", err)
	}
	mc, err := s3.NewClientWithRetry(ctx, s3params, s3.RetryConfig{}, func(attempt int, delay time.Duration, retryErr error) {
		gate.Set(health.StateLoading, fmt.Sprintf("connecting to S3 (attempt %d failed, retrying in %v)", attempt, delay))
		s3.LogRetry(ctx, "connect to S3")(attempt, delay, retryErr)
	})
	if err != nil {
		return fatal("connect to S3", err)
	}

	// §7.1 step 1: resolve the collector service once to surface a
	// misconfiguration early. Collectors may legitimately come up later, so a
	// failure only warns.
	if cfg.CollectorService == "" {
		log.Warning(ctx, "COLLECTOR_HEADLESS_SVC is not set; serving the cold tier only")
	} else if _, err := net.LookupHost(cfg.CollectorService); err != nil {
		log.Warning(ctx, "collector service %q does not resolve yet: %s", cfg.CollectorService, err)
	}

	// The embedded UI (07 §6) is optional: a binary built without the npm
	// step still serves /api/v1 and only warns.
	uiAssets, uiErr := ui.Dist()
	if uiErr != nil {
		log.Warning(ctx, "/ui disabled: %s", uiErr)
	}

	svc := query.New(query.Options{
		Config: query.Config{
			CursorTTL:          cfg.CursorTTL,
			WideRangeLimit:     cfg.WideRangeLimit,
			MaxScanFiles:       cfg.MaxScanFiles,
			MaxScanBytes:       int64(cfg.MaxScanBytes),
			DurationThresholds: cfg.DurationThresholds,
			ListConcurrency:    cfg.ListConcurrency,
			CollectorService:   cfg.CollectorService,
			CollectorPort:      cfg.CollectorPort,
			FanoutTimeout:      cfg.FanoutTimeout,
			OverlapMargin:      cfg.OverlapMargin,
			DumpsCollectorURL:  cfg.DumpsCollectorURL,
		},
		ColdStore: query.NewS3ObjectReader(mc, cfg.S3.PathPrefix),
		Metrics:   query.NewMetrics(reg),
		UI:        uiAssets,
	})
	gate.Mount(svc.Handler())
	// Stateless service: READY as soon as the handler is mounted (§7.1 step 4).
	gate.Set(health.StateReady, "")
	// The thresholds must mirror the collector's, or the cold class pruning
	// silently drops rows (01 §6.4); logging the resolved value makes a drift
	// diagnosable from the two startup lines.
	thresholds := []time.Duration(cfg.DurationThresholds)
	if len(thresholds) == 0 {
		thresholds = model.DefaultDurationThresholds()
	}
	log.Info(ctx, "query ready: external API :%d, collector service %q, duration thresholds %v",
		cfg.ExternalAPIPort, cfg.CollectorService, thresholds)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(func() error {
		select {
		case err := <-serveErr:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-runCtx.Done():
			return nil
		}
	}, func(error) {
		gate.Set(health.StateTerminating, "shutting down")
		shCtx, shCancel := context.WithTimeout(context.Background(), inFlightGrace)
		defer shCancel()
		_ = external.Shutdown(shCtx)
		cancel()
	})
	gr.Add(signalActor(runCtx, gate, cfg.ShutdownDrainGrace))
	return gr.Run()
}
