package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "maintenance-job",
	Short: "cloud-profiler-maintenance-job",
	Long:  "Cloud Maintenance Job is used to create and delete temp data acouring their lifetime",
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

func Execute() {
	ctx := context.Background()
	ctx, err := log.SetLevelString(ctx, os.Getenv("LOG_LEVEL"))
	if err != nil {
		fmt.Printf("Error during initializing logger: %s", err)
		os.Exit(1)
	}

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
