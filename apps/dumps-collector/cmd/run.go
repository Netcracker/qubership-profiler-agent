package cmd

import (
	"context"
	"os"
	"syscall"
	"time"

	db "github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/client"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/server"
	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/task"
	"github.com/Netcracker/qubership-profiler-backend/libs/cron"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/oklog/run"
	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run cloud-profiler-dumps-collector-go",
	Run:   Run,
}

const (
	InsertTaskCron = "* * * * *"  // Every minute
	PackTaskCron   = "6 * * * *"  // Every hour (6 is needed to avoid conflicts with insert task)
	RemoveTaskCron = "30 0 * * *" // Every day at 00:30

	InsertTaskPeriod = time.Minute * 5 // 5 mins

	PackTaskRange   = time.Hour      // 1 hour (is multiplied with DIAG_PV_HOURS_ARCHIVE_AFTER)
	RemoveTaskRange = time.Hour * 24 // 1 day (is multiplied with DIAG_PV_DAYS_DELETE_AFTER)
)

func init() {
	rootCmd.AddCommand(runCmd)
}

func Run(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	params := db.DBParams{
		DBHost:        envconfig.EnvConfig.DBHost,
		DBPort:        envconfig.EnvConfig.DBPort,
		DBUser:        envconfig.EnvConfig.DBUser,
		DBPassword:    envconfig.EnvConfig.DBPassword,
		DBName:        envconfig.EnvConfig.DBName,
		EnableMetrics: envconfig.EnvConfig.DBMetricsEnabled,
	}

	// Create db client
	dbClient, err := db.NewDumpDbClient(ctx, params)
	if err != nil {
		log.Error(ctx, err, "Error creating sqlite db client")
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var gr run.Group
	gr.Add(run.SignalHandler(ctx, os.Interrupt, syscall.SIGINT, syscall.SIGTERM))

	// Add Rescan task (if it's needed)
	gr.Add(func() error {
		return runRescanTask(ctx, dbClient, envconfig.EnvConfig.GetBasePVDir())
	}, func(err error) {
		if err != nil {
			// TODO: rescan should be rescheduled
			log.Warning(ctx, "Rescan task interupted...")
			cancel()
		}
	})
	// Add Insert task
	gr.Add(func() error {
		return runInsertTask(ctx, dbClient, envconfig.EnvConfig.GetBasePVDir())
	}, func(err error) {
		if err != nil {
			log.Warning(ctx, "Insert task interupted...")
			cancel()
		}
	})
	// Add Pack task
	gr.Add(func() error {
		return runPackTask(ctx, dbClient, envconfig.EnvConfig.GetBasePVDir())
	}, func(err error) {
		if err != nil {
			log.Warning(ctx, "Pack task interupted...")
			cancel()
		}
	})
	// Add Remove task
	gr.Add(func() error {
		return runRemoveTask(ctx, dbClient, envconfig.EnvConfig.GetBasePVDir())
	}, func(err error) {
		if err != nil {
			log.Warning(ctx, "Remove task interupted...")
			cancel()
		}
	})
	// Add http server
	gr.Add(func() error {
		return runServer(ctx, dbClient, envconfig.EnvConfig.GetBasePVDir(), envconfig.EnvConfig.BindAddress)
	}, func(err error) {
		if err != nil {
			log.Warning(ctx, "Http server interupted...")
			cancel()
		}
	})

	if err := gr.Run(); err != nil {
		log.Warning(ctx, "terminating... reason: %s", err)
		os.Exit(1)
	}
}

func runRescanTask(ctx context.Context, dbClient db.DumpDbClient, pvPath string) error {
	// Run and execute rescan task
	rescanTask, err := task.NewRescanTask(pvPath, dbClient)
	if err != nil {
		log.Error(ctx, err, "Error creating rescan task")
		return err
	}
	err = rescanTask.Execute(ctx)
	if err != nil {
		return err
	}
	log.Info(ctx, "Rescan task finished successfully")
	return nil
}

func runInsertTask(ctx context.Context, dbClient db.DumpDbClient, pvPath string) error {
	insertTask, err := task.NewInsertTask(pvPath, dbClient)
	if err != nil {
		log.Error(ctx, err, "Error creating insert task")
		return err
	}

	c := cron.NewCron(ctx)
	if _, err := c.AddFunc(InsertTaskCron, func() {
		dateTo := time.Now().UTC()
		dateFrom := dateTo.Add(-InsertTaskPeriod)
		if err := insertTask.Execute(ctx, dateFrom, dateTo); err != nil {
			log.Error(ctx, err, "Error executing insert task")
		}
	}); err != nil {
		log.Error(ctx, err, "Error starting insert cron task")
		return nil
	}
	c.Start()
	<-ctx.Done()
	log.Warning(ctx, "Stop insert cron, context is done")
	c.Stop()
	return nil
}

func runPackTask(ctx context.Context, dbClient db.DumpDbClient, pvPath string) error {
	packTask, err := task.NewPackTask(pvPath, dbClient)
	if err != nil {
		log.Error(ctx, err, "Error creating pack task")
		return err
	}

	c := cron.NewCron(ctx)
	if _, err := c.AddFunc(PackTaskCron, func() {
		tHour := time.Now().UTC().Add(-time.Duration(envconfig.EnvConfig.ArchiveHours) * PackTaskRange)
		if err := packTask.Execute(ctx, tHour); err != nil {
			log.Error(ctx, err, "Error executing pack task task for hour %v", tHour)
		}
	}); err != nil {
		log.Error(ctx, err, "Error starting pack cron task")
		return nil
	}
	c.Start()
	<-ctx.Done()
	log.Warning(ctx, "Stop pack cron, context is done")
	c.Stop()
	return nil
}

func runRemoveTask(ctx context.Context, dbClient db.DumpDbClient, pvPath string) error {
	removeTask, err := task.NewRemoveTask(pvPath, dbClient)
	if err != nil {
		log.Error(ctx, err, "Error creating remove task")
		return err
	}

	c := cron.NewCron(ctx)
	if _, err := c.AddFunc(RemoveTaskCron, func() {
		tHour := time.Now().UTC().Add(-time.Duration(envconfig.EnvConfig.DeleteDays) * RemoveTaskRange)
		if err := removeTask.Execute(ctx, tHour); err != nil {
			log.Error(ctx, err, "Error executing remove task task for hour %v", tHour)
		}
	}); err != nil {
		log.Error(ctx, err, "Error starting remove cron task")
		return nil
	}
	c.Start()
	<-ctx.Done()
	log.Warning(ctx, "Stop remove cron, context is done")
	c.Stop()
	return nil
}

func runServer(ctx context.Context, dbClient db.DumpDbClient, pvPath string, bindAddress string) error {
	requestProcessor, err := task.NewRequestProcessor(pvPath, dbClient, false)
	if err != nil {
		log.Error(ctx, err, "Error creating request processor")
		return err
	}

	return server.StartHttpServer(ctx, requestProcessor, bindAddress)
}
