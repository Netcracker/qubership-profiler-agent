package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	appenv "github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/envconfig"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/health"
	"github.com/Netcracker/qubership-profiler-backend/apps/profiler-backend/pkg/mcpserver"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/log"
	"github.com/oklog/run"
	pkgerrors "github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// mcpPath is the MCP endpoint; the health probes answer beside it.
const mcpPath = "/mcp"

var mcpCmd = &cobra.Command{
	Use:          "mcp",
	Short:        "Run the MCP server: the profiler's agent tools over Streamable HTTP, reading from query",
	RunE:         runMCP,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(mcpCmd)
}

func runMCP(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()
	cfg, err := appenv.ParseMCP()
	if err != nil {
		return err
	}
	ctx, err = log.SetLevelString(ctx, cfg.LogLevel)
	if err != nil {
		return err
	}
	client, err := insight.New(cfg.QueryURL)
	if err != nil {
		return err
	}
	handler, err := mcpserver.NewHandler(mcpserver.Options{Client: client, Authenticator: mcpserver.AcceptAll{}})
	if err != nil {
		return err
	}

	// The server keeps no state and does not reach query at startup: query
	// may come up later, and a tool reports its errors to the agent. So the
	// gate goes READY at once, and only the drain moves it.
	gate := health.NewGate("")
	mux := http.NewServeMux()
	mux.Handle(mcpPath, handler)
	gate.Mount(mux)
	gate.Set(health.StateReady, "")
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		return pkgerrors.Wrap(err, "bind the MCP listener")
	}
	srv := &http.Server{Handler: gate, ReadHeaderTimeout: 10 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Info(ctx, "mcp ready: %s on :%d, query at %s", mcpPath, cfg.Port, cfg.QueryURL)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var gr run.Group
	gr.Add(func() error {
		select {
		case err := <-serveErr:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case <-runCtx.Done():
			return nil
		}
	}, func(error) {
		gate.Set(health.StateTerminating, "shutting down")
		shCtx, shCancel := context.WithTimeout(context.Background(), inFlightGrace)
		defer shCancel()
		_ = srv.Shutdown(shCtx)
		cancel()
	})
	gr.Add(signalActor(runCtx, gate, cfg.ShutdownDrainGrace))
	return gr.Run()
}
