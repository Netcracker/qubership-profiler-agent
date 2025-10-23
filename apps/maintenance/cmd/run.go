package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"syscall"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/maintenance"
	"github.com/Netcracker/qubership-profiler-backend/libs/cron"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:    "run",
	Short:  "Run maintenance-job",
	PreRun: ParseRunFlags,
	Run:    Run,
}

func init() {
	rootCmd.AddCommand(runCmd)
	config.InitFlags(runCmd.Flags())
}

func ParseRunFlags(cmd *cobra.Command, args []string) {
	config.ParseFlags(cmd.Flags())
}

func Run(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	if err := config.PrepareConfig(ctx); err != nil {
		log.Error(ctx, err, "Error during reading configuration")
		os.Exit(1)
	}

	mJob, err := maintenance.NewMaintenanceJob(ctx, config.Cfg.JobConfig, config.Cfg.InvertedIndexConfig)
	if err != nil {
		log.Error(ctx, err, "Error during creating maintenance job")
		os.Exit(1)
	}

	if config.Cfg.CronRun {
		runService(ctx, mJob)
	} else {
		executeOnce(ctx, mJob)
	}

	if log.IsDebugEnabled(ctx) {
		mfs, err := prometheus.DefaultGatherer.Gather()
		if err != nil {
			log.Error(ctx, err, "can't get metric family")
			os.Exit(1)
		}

		var b bytes.Buffer
		w := io.Writer(&b)
		enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
		for _, mf := range mfs {
			if *mf.Name == "cdt_minio_operation_latency_seconds" || *mf.Name == "cdt_minio_operation_objects_count" {
				if err := enc.Encode(mf); err != nil {
					log.Error(ctx, err, "error getting metric %s", *mf.Name)
				}
			}
		}
		output := b.String()
		log.Debug(ctx, "Metrics:\n%s", output)
	}
}

func runService(ctx context.Context, mJob *maintenance.MaintenanceJob) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(run.SignalHandler(ctx, os.Interrupt, syscall.SIGINT, syscall.SIGTERM))
	gr.Add(
		func() error {
			return runCron(ctx, mJob)
		},
		func(_ error) {
			log.Warning(ctx, "run cron exiting")
			cancel()
		},
	)
	if err := gr.Run(); err != nil {
		log.Warning(ctx, "terminating... reason: %s", err)
		os.Exit(1)
	}
}

func runCron(ctx context.Context, mJob *maintenance.MaintenanceJob) error {
	c := cron.NewCron(ctx)
	_, err := c.AddFunc(config.Cfg.CronJobSchedule, execute(ctx, mJob))
	if err != nil {
		log.Error(ctx, err, "problem with cron")
		return err
	}

	c.Start()
	<-ctx.Done()
	log.Warning(ctx, "Stop cron, context is done")
	c.Stop()
	return nil
}

func execute(ctx context.Context, mJob *maintenance.MaintenanceJob) func() { // for cron
	return func() {
		executeOnce(ctx, mJob)
	}
}

func executeOnce(ctx context.Context, mJob *maintenance.MaintenanceJob) {
	if err := mJob.Execute(ctx); err != nil {
		log.Error(ctx, err, "Error during execution")
	}
}
