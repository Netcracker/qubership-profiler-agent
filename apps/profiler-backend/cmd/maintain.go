package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	appenv "github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/metrics"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/maintain"
	"github.com/Netcracker/qubership-profiler-backend/libs/s3"
	"github.com/oklog/run"
	pkgerrors "github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var maintainCmd = &cobra.Command{
	Use:          "maintain",
	Short:        "Run the S3 maintenance job: bucket compaction and per-class retention TTL",
	RunE:         runMaintain,
	SilenceUsage: true,
}

var maintainRunNow bool

func init() {
	maintainCmd.Flags().BoolVar(&maintainRunNow, "run-now", false,
		"run one maintenance pass and exit (for a k8s CronJob; 03-lifecycle.md §8.2)")
	rootCmd.AddCommand(maintainCmd)
}

func runMaintain(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	cfg, err := appenv.ParseMaintain()
	if err != nil {
		return err
	}
	ctx, err = log.SetLevelString(ctx, cfg.LogLevel)
	if err != nil {
		return err
	}

	// The job is stateless (03 §8): S3 access is its only dependency and a
	// failure is FATAL, mirroring the query startup. A temporarily
	// unreachable endpoint retries with backoff instead (PR 708 review #22).
	s3params, err := cfg.S3.Params()
	if err != nil {
		return pkgerrors.Wrap(err, "resolve S3 credentials")
	}

	if maintainRunNow {
		// One-shot CronJob mode (§8.2): no listener to keep alive, so retry
		// just rides out a slow-starting S3 within the job's own budget.
		mc, err := s3.NewClientWithRetry(ctx, s3params, s3.RetryConfig{}, s3.LogRetry(ctx, "connect to S3"))
		if err != nil {
			return pkgerrors.Wrap(err, "connect to S3")
		}
		job := newMaintainJob(mc, cfg)
		stats, err := job.Pass(ctx, time.Now())
		log.Info(ctx, "maintain pass: %+v", stats)
		return err
	}

	reg := metrics.NewRegistry()
	// Expose the cdt_minio_* series (registered on the default registry inside
	// s3.NewClient) on this subcommand's own registry too.
	s3.RegisterMetrics(reg)
	// Bind the metrics/liveness listener before S3 (rather than after, as
	// before): the liveness probe must see the process is alive while a
	// slow-starting S3 endpoint retries, not connection-refused.
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(reg))
	if cfg.PprofEnabled {
		metrics.RegisterPprof(mux)
	}
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	metricsSrv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.MetricsPort), Handler: mux}
	metricsServeErr := make(chan error, 1)
	go func() { metricsServeErr <- metricsSrv.ListenAndServe() }()
	shutdownMetrics := func() {
		shCtx, shCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shCancel()
		_ = metricsSrv.Shutdown(shCtx)
	}

	mc, err := s3.NewClientWithRetry(ctx, s3params, s3.RetryConfig{}, s3.LogRetry(ctx, "connect to S3"))
	if err != nil {
		shutdownMetrics()
		return pkgerrors.Wrap(err, "connect to S3")
	}

	job := newMaintainJob(mc, cfg)
	job.OnPass = metrics.RegisterMaintain(reg).Observe

	log.Info(ctx, "maintain ready: check interval %s, delete grace %s, min age %s, min files %d, metrics :%d",
		cfg.CheckInterval, cfg.CompactionDeleteGrace, cfg.CompactionMinAge, cfg.CompactionMinFiles, cfg.MetricsPort)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(func() error {
		err := job.RunLoop(runCtx, cfg.CheckInterval)
		if pkgerrors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}, func(error) {
		cancel()
	})
	// The metrics listener lives and dies with the loop; the job itself has no
	// readiness state (03 §8) — /health/live just says the process runs.
	gr.Add(func() error {
		select {
		case err := <-metricsServeErr:
			if pkgerrors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-runCtx.Done():
			return nil
		}
	}, func(error) {
		shutdownMetrics()
	})
	// No drain phase: the job serves no traffic, and a pass interrupted
	// between the compacted PUT and the input deletes is resumed by the next
	// run (the write → grace → delete protocol is restart-safe).
	sigDone := make(chan struct{})
	gr.Add(func() error {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sig)
		select {
		case s := <-sig:
			log.Info(ctx, "received %s, stopping the maintenance loop", s)
			return nil
		case <-sigDone:
			return nil
		}
	}, func(error) {
		close(sigDone)
	})
	return gr.Run()
}

func newMaintainJob(mc *s3.MinioClient, cfg appenv.Maintain) *maintain.Job {
	return maintain.NewJob(maintain.NewS3ObjectStore(mc, cfg.S3.PathPrefix), maintain.Config{
		TimeBucket:    cfg.TimeBucket,
		MinAge:        cfg.CompactionMinAge,
		MinFiles:      cfg.CompactionMinFiles,
		DeleteGrace:   cfg.CompactionDeleteGrace,
		MaxGroupBytes: int64(cfg.CompactionMaxBytes),
		// Explicit env TTLs win; unset classes keep the tier-table defaults
		// (№10), so the write classification, the read pruning, and the TTLs
		// share one source.
		ClassTTL:        cfg.ClassTTLs(),
		PodsManifestTTL: time.Duration(cfg.RetentionPodsTTL),
	})
}
