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

	// §7.1 step 2: verify the S3 side before serving; unrecoverable → FATAL.
	s3params, err := cfg.S3.Params()
	if err != nil {
		return pkgerrors.Wrap(err, "resolve S3 credentials")
	}
	mc, err := s3.NewClient(ctx, s3params)
	if err != nil {
		return pkgerrors.Wrap(err, "connect to S3")
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

	reg := metrics.NewRegistry()
	svc := query.New(query.Options{
		Config: query.Config{
			CursorTTL:        cfg.CursorTTL,
			WideRangeLimit:   cfg.WideRangeLimit,
			MaxScanFiles:     cfg.MaxScanFiles,
			MaxScanBytes:     int64(cfg.MaxScanBytes),
			ListConcurrency:  cfg.ListConcurrency,
			CollectorService: cfg.CollectorService,
			CollectorPort:    cfg.CollectorPort,
			FanoutTimeout:    cfg.FanoutTimeout,
			OverlapMargin:    cfg.OverlapMargin,
		},
		ColdStore: query.NewS3ObjectReader(mc),
		Metrics:   query.NewMetrics(reg),
		UI:        uiAssets,
	})

	gate := health.NewGate("/api/v1")
	gate.Mount(svc.Handler())
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.ExternalAPIPort))
	if err != nil {
		return pkgerrors.Wrap(err, "bind external API")
	}
	// Stateless service: READY as soon as the port is bound (§7.1 step 4).
	gate.Set(health.StateReady, "")
	log.Info(ctx, "query ready: external API :%d, collector service %q",
		cfg.ExternalAPIPort, cfg.CollectorService)

	// query has no internal port, so /metrics rides the external one (04 §12);
	// the ingress publishes /api/v1 only.
	external := &http.Server{Handler: metrics.Mux(reg, gate)}
	serveErr := make(chan error, 1)
	go func() { serveErr <- external.Serve(ln) }()

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
