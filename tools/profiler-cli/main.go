// Command profiler reads the Qubership profiler backend for coding agents and
// people at a terminal: list services, find calls, and explain a call. It is
// a thin front end over libs/insight; the profiler MCP server runs the same
// operations.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/tools/profiler-cli/cmd"
)

func main() {
	if err := cmd.NewRoot(os.Stdout).ExecuteContext(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "profiler:", err)
		os.Exit(1)
	}
}
