// Package cmd wires the profiler-backend subcommands: one binary, one
// subcommand per workload (04-storage-layout.md §2). `collect` runs the
// write path of libs/collector, `query` the read path of libs/query; the
// manifests pick the mode via the first positional argument.
package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "profiler-backend",
	Short: "Qubership profiler backend",
	Long:  "Qubership profiler backend: one image, one binary, one subcommand per workload.",
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true
}

// Execute runs the selected subcommand and exits non-zero on failure, so a
// FATAL startup condition surfaces as a container restart (03-lifecycle.md §2).
func Execute() {
	if err := rootCmd.ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "profiler-backend:", err)
		os.Exit(1)
	}
}
