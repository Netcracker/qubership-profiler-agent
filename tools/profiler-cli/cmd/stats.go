package cmd

import (
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight/render"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/spf13/cobra"
)

func newHotspotsCmd(root *rootFlags) *cobra.Command {
	var window windowFlags
	var filters filterFlags
	var groupBy, order string
	var limit, sample int
	cmd := &cobra.Command{
		Use:   "hotspots",
		Short: "Rank where the time goes: exact per-group statistics, plus inner methods and SQL from a sample of the slowest calls",
		Example: "  profiler hotspots --service prod/billing --since 1h\n" +
			"  profiler hotspots --service prod/billing --group-by pod --order count_desc\n" +
			"  profiler hotspots --service prod/billing --filter 'error=true' --sample 50",
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
			res, err := client.Hotspots(cmd.Context(), insight.HotspotsInput{
				Window: w, Filters: filters.filters(), GroupBy: groupBy, Limit: limit, Order: stats.Order(order), Sample: sample,
			})
			if err != nil {
				return err
			}
			return root.print(cmd, res, func(out io.Writer) error { return render.Hotspots(out, res) })
		},
	}
	window.add(cmd)
	filters.add(cmd)
	cmd.Flags().StringVar(&groupBy, "group-by", insight.DefaultGroupBy, "group by method (the root method), service, pod, or namespace")
	cmd.Flags().IntVar(&limit, "limit", insight.DefaultHotspotLimit, "groups to list; the rest are summed as other")
	cmd.Flags().StringVar(&order, "order", string(insight.OrderDurationSumDesc), "duration_sum_desc ranks by total time, count_desc by call count")
	cmd.Flags().IntVar(&sample, "sample", insight.DefaultSample, "slowest calls whose trees the sampled layer reads")
	return cmd
}

func newTimelineCmd(root *rootFlags) *cobra.Command {
	var window windowFlags
	var filters filterFlags
	var buckets int
	cmd := &cobra.Command{
		Use:   "timeline",
		Short: "Count calls and errors and show duration percentiles per time bucket, to find when a change started",
		Example: "  profiler timeline --service prod/billing --since 6h\n" +
			"  profiler timeline --service prod/billing --filter 'method=~OrderController' --since 3h --buckets 60",
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
			res, err := client.Timeline(cmd.Context(), insight.TimelineInput{Window: w, Filters: filters.filters(), Buckets: buckets})
			if err != nil {
				return err
			}
			return root.print(cmd, res, func(out io.Writer) error { return render.Timeline(out, res) })
		},
	}
	window.add(cmd)
	filters.add(cmd)
	cmd.Flags().IntVar(&buckets, "buckets", insight.DefaultTimelineBuckets, "number of equal time buckets")
	return cmd
}
