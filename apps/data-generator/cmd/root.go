package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "data-generator",
	Short: "cloud-profiler-data-generator",
	Long:  "CLI tool to generate calls in specified time range as Parquet files and upload them to S3-compatible storage",
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

func Execute() {
	ctx := context.Background()
	ctx = log.SetLevel(ctx, log.INFO)
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
