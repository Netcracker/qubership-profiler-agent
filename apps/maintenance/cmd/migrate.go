package cmd

import (
	"os"

	"github.com/Netcracker/qubership-profiler-backend/apps/maintenance/pkg/config"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/Netcracker/qubership-profiler-backend/libs/pg"
	"github.com/spf13/cobra"
)

var migrateCmd = &cobra.Command{
	Use:    "migrate",
	Short:  "Migrate DB schema to the latest state",
	Run:    Migrate,
	PreRun: ParseMigrateFlags,
}

func init() {
	rootCmd.AddCommand(migrateCmd)
	config.InitPGFlags(migrateCmd.Flags())
}

func ParseMigrateFlags(cmd *cobra.Command, args []string) {
	config.ParsePGFlags(cmd.Flags())
}

func Migrate(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	pgParams, err := config.PreparePGConfig(ctx)
	if err != nil {
		log.Error(ctx, err, "Error during reading configuration")
		os.Exit(1)
	}

	postgres, err := pg.NewClient(ctx, *pgParams)
	if err != nil {
		log.Error(ctx, err, "cannot create new PostgresDB client")
		os.Exit(1)
	}

	log.Info(ctx, "Start DB migration")
	if err := postgres.MigrateSchema(ctx); err != nil {
		log.Error(ctx, err, "Migration failed")
		os.Exit(1)
	}
	log.Info(ctx, "Migration finished")
}
