// Package cmd holds the profiler CLI's commands. Each command parses flags,
// runs one libs/insight operation, and prints the result as compact text or,
// with --json, as the operation's typed result.
package cmd

import (
	"encoding/json"
	"io"
	"os"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// urlEnv names the query service base URL when --url is absent.
const urlEnv = "PROFILER_URL"

type rootFlags struct {
	url  string
	json bool
}

// NewRoot builds the command tree, writing results to out.
func NewRoot(out io.Writer) *cobra.Command {
	var flags rootFlags
	root := &cobra.Command{
		Use:   "profiler",
		Short: "Investigate slow calls with the Qubership profiler",
		Long: "profiler reads the Qubership profiler backend: list the services with data, find calls by\n" +
			"service, duration, error, or a request or trace ID, and explain where a call spent its time.\n\n" +
			"Set the query service URL with --url or " + urlEnv + ", for example after\n" +
			"  kubectl port-forward -n <profiler-namespace> svc/<release>-query 8080:8080\n" +
			"with " + urlEnv + "=http://localhost:8080.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetOut(out)
	root.PersistentFlags().StringVar(&flags.url, "url", "", "query service base URL (default $"+urlEnv+")")
	root.PersistentFlags().BoolVar(&flags.json, "json", false, "print the result as JSON")
	root.AddCommand(newServicesCmd(&flags), newCallsCmd(&flags), newExplainCmd(&flags))
	return root
}

func (f *rootFlags) client() (*insight.Client, error) {
	u := f.url
	if u == "" {
		u = os.Getenv(urlEnv)
	}
	if u == "" {
		return nil, errors.New("set --url or " + urlEnv + " to the query service, such as http://localhost:8080")
	}
	return insight.New(u)
}

// print writes the result as JSON with --json, or as the command's text.
func (f *rootFlags) print(cmd *cobra.Command, result any, text func(io.Writer) error) error {
	if f.json {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}
	return text(cmd.OutOrStdout())
}

// windowFlags are the time-range flags of services and calls.
type windowFlags struct{ since, from, to string }

func (w *windowFlags) add(cmd *cobra.Command) {
	cmd.Flags().StringVar(&w.since, "since", "", "window ending now (or at --to), such as 30m, 6h, or 2d (default 1h)")
	cmd.Flags().StringVar(&w.from, "from", "", "window start, RFC 3339 or Unix milliseconds")
	cmd.Flags().StringVar(&w.to, "to", "", "window end, RFC 3339 or Unix milliseconds (default now)")
}

func (w *windowFlags) window() (insight.Window, error) {
	return insight.ParseWindow(w.since, w.from, w.to)
}

// filterFlags are the condition flags of services and calls.
type filterFlags struct {
	conditions []string
	service    string
}

func (f *filterFlags) add(cmd *cobra.Command) {
	cmd.Flags().StringArrayVar(&f.conditions, "filter", nil,
		"condition such as 'duration>1s', 'request_id=abc123', or 'service in billing,orders' (repeatable; all must match)")
	cmd.Flags().StringVar(&f.service, "service", "", "<namespace>/<service>, shorthand for two conditions")
}

func (f *filterFlags) filters() insight.Filters {
	return insight.Filters{Conditions: f.conditions, Service: f.service}
}
