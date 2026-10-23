package cmd

import (
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight/render"
	"github.com/spf13/cobra"
)

func newServicesCmd(root *rootFlags) *cobra.Command {
	var window windowFlags
	var filters filterFlags
	cmd := &cobra.Command{
		Use:   "services",
		Short: "List the namespaces, services, and pods with profiling data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := root.client()
			if err != nil {
				return err
			}
			w, err := window.window()
			if err != nil {
				return err
			}
			res, err := client.Services(cmd.Context(), insight.ServicesInput{Window: w, Filters: filters.filters()})
			if err != nil {
				return err
			}
			return root.print(cmd, res, func(out io.Writer) error { return render.Services(out, res) })
		},
	}
	window.add(cmd)
	filters.add(cmd)
	return cmd
}

func newCallsCmd(root *rootFlags) *cobra.Command {
	var window windowFlags
	var filters filterFlags
	var limit int
	var cursor string
	cmd := &cobra.Command{
		Use:   "calls",
		Short: "Find calls by service, duration, error, or a request or trace ID, newest first",
		Example: "  profiler calls --since 30m --filter 'request_id=abc123'\n" +
			"  profiler calls --service prod/billing --filter 'duration>2s' --since 6h\n" +
			"  profiler calls --cursor <next cursor from the previous page>",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := root.client()
			if err != nil {
				return err
			}
			w, err := window.window()
			if err != nil {
				return err
			}
			res, err := client.Calls(cmd.Context(), insight.CallsInput{
				Window: w, Filters: filters.filters(), Limit: limit, Cursor: cursor,
			})
			if err != nil {
				return err
			}
			return root.print(cmd, res, func(out io.Writer) error { return render.Calls(out, res, "More calls: --cursor %s") })
		},
	}
	window.add(cmd)
	filters.add(cmd)
	cmd.Flags().IntVar(&limit, "limit", insight.DefaultCallsLimit, "calls per page")
	cmd.Flags().StringVar(&cursor, "cursor", "", "continue from the cursor a previous page printed; the page keeps that page's filters, so repeat them or leave them out")
	return cmd
}

func newExplainCmd(root *rootFlags) *cobra.Command {
	var focus string
	var node int
	var threshold float64
	var maxNodes int
	cmd := &cobra.Command{
		Use:   "explain <call-ref>",
		Short: "Show where a call spent its time: hot path, top methods by self time, and top SQL",
		Example: "  profiler explain 'prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error'\n" +
			"  profiler explain <call-ref> --node 12 --threshold 0.01\n" +
			"  profiler explain <call-ref> --focus Repository.load",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			client, err := root.client()
			if err != nil {
				return err
			}
			in := insight.ExplainInput{Ref: args[0], Focus: focus, Threshold: threshold, MaxNodes: maxNodes}
			if cmd.Flags().Changed("node") {
				in.FocusNode = &node
			}
			res, err := client.Explain(cmd.Context(), in)
			if err != nil {
				return err
			}
			return root.print(cmd, res, func(out io.Writer) error { return render.Explain(out, res) })
		},
	}
	cmd.Flags().IntVar(&node, "node", 0, "re-root at this node: a #<id> from an earlier explain of the same call")
	cmd.Flags().StringVar(&focus, "focus", "", "re-root at the slowest node whose method contains this text; the notes list other matches for --node")
	cmd.MarkFlagsMutuallyExclusive("node", "focus")
	cmd.Flags().Float64Var(&threshold, "threshold", insight.DefaultThreshold, "hide nodes under this share of the root, 0 to 1")
	cmd.Flags().IntVar(&maxNodes, "max-nodes", insight.DefaultMaxNodes, "most nodes in the hot path")
	return cmd
}
