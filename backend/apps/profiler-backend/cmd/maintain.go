package cmd

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	appenv "github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/maintain"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
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
	// failure is FATAL, mirroring the query startup.
	mc, err := s3.NewClient(ctx, cfg.S3.Params())
	if err != nil {
		return pkgerrors.Wrap(err, "connect to S3")
	}

	job := maintain.NewJob(maintain.NewS3ObjectStore(mc), maintain.Config{
		TimeBucket:    cfg.TimeBucket,
		MinAge:        cfg.CompactionMinAge,
		MinFiles:      cfg.CompactionMinFiles,
		DeleteGrace:   cfg.CompactionDeleteGrace,
		MaxGroupBytes: int64(cfg.CompactionMaxBytes),
		ClassTTL: map[string]time.Duration{
			model.RetentionShortClean:  time.Duration(cfg.RetentionShortCleanTTL),
			model.RetentionNormalClean: time.Duration(cfg.RetentionNormalCleanTTL),
			model.RetentionLongClean:   time.Duration(cfg.RetentionLongCleanTTL),
			model.RetentionAnyError:    time.Duration(cfg.RetentionAnyErrorTTL),
			model.RetentionCorrupted:   time.Duration(cfg.RetentionCorruptedTTL),
		},
		SnapshotTTL: time.Duration(cfg.RetentionDictionaryTTL),
	})

	if maintainRunNow {
		stats, err := job.Pass(ctx, time.Now())
		log.Info(ctx, "maintain pass: %+v", stats)
		return err
	}

	log.Info(ctx, "maintain ready: check interval %s, delete grace %s, min age %s, min files %d",
		cfg.CheckInterval, cfg.CompactionDeleteGrace, cfg.CompactionMinAge, cfg.CompactionMinFiles)

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
