package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/apps/dumps-collector/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "dumps-collector-go",
	Short: "cloud-profiler-dumps-collector-go",
	Long:  "Cloud Profiler Dumps Collector go",
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

func Execute() {
	ctx := context.Background()

	if err := envconfig.InitConfig(); err != nil {
		log.Error(ctx, err, "Error parsing env variables")
		os.Exit(1)
	}

	ctx, err := log.SetLevelString(ctx, envconfig.EnvConfig.LogLevel)
	if err != nil {
		log.Error(ctx, err, "Error during initializing logger")
		os.Exit(1)
	}

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "Error executing root command")
		os.Exit(1)
	}
}
