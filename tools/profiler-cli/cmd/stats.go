package cmd

import (
	"io"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight/render"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/stats"
	"github.com/pkg/errors"
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
	cmd.Flags().StringVar(&groupBy, "group-by", insight.DefaultGroupBy, "group by method (the root method), class, endpoint, service, pod, namespace, error, retention_class, "+
		"trace_id, request_id, param_name, or a call parameter name; a call with several values of a parameter counts in each value's group")
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

func newCompareCmd(root *rootFlags) *cobra.Command {
	var window windowFlags
	var filters filterFlags
	var baseFrom, baseTo, baseShift, groupBy, order string
	var limit, sample int
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Compare two windows: what got slower, new, or vanished, with inner methods and SQL from a sample of the changed groups",
		Long: "compare joins the top groups of a target window and a baseline window. The target is set with\n" +
			"--since, --from, and --to. The baseline is the window of the same length just before the target,\n" +
			"or the target moved back by --baseline-shift, or --baseline-from to --baseline-to.",
		Example: "  profiler compare --service prod/billing --from 2026-10-14T14:00:00Z --to 2026-10-14T15:00:00Z\n" +
			"  profiler compare --service prod/billing --since 1h --baseline-shift 1d\n" +
			"  profiler compare --service prod/billing --since 30m --baseline-from 2026-10-14T09:00:00Z --baseline-to 2026-10-14T09:30:00Z",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			client, err := root.client()
			if err != nil {
				return err
			}
			target, err := window.window()
			if err != nil {
				return err
			}
			in := insight.CompareInput{
				Target: target, Filters: filters.filters(), GroupBy: groupBy, Limit: limit, Order: stats.Order(order), Sample: sample,
			}
			if (baseFrom == "") != (baseTo == "") {
				return errors.New("give both --baseline-from and --baseline-to, or neither")
			}
			if baseFrom != "" {
				if in.Baseline, err = insight.ParseWindow("", baseFrom, baseTo); err != nil {
					return errors.Wrap(err, "baseline")
				}
			}
			if baseShift != "" {
				if in.BaselineShift, err = insight.ParseDuration(baseShift); err != nil {
					return errors.Wrap(err, "baseline shift")
				}
			}
			res, err := client.Compare(cmd.Context(), in)
			if err != nil {
				return err
			}
			return root.print(cmd, res, func(out io.Writer) error { return render.Compare(out, res) })
		},
	}
	window.add(cmd)
	filters.add(cmd)
	cmd.Flags().StringVar(&baseFrom, "baseline-from", "", "baseline start, RFC 3339 or Unix milliseconds")
	cmd.Flags().StringVar(&baseTo, "baseline-to", "", "baseline end, RFC 3339 or Unix milliseconds")
	cmd.Flags().StringVar(&baseShift, "baseline-shift", "", "baseline is the target moved back by this much, such as 1d (default: the target's length)")
	cmd.MarkFlagsMutuallyExclusive("baseline-shift", "baseline-from")
	cmd.MarkFlagsMutuallyExclusive("baseline-shift", "baseline-to")
	cmd.Flags().StringVar(&groupBy, "group-by", insight.DefaultGroupBy, "group by method (the root method), class, endpoint, service, pod, namespace, error, retention_class, "+
		"trace_id, request_id, param_name, or a call parameter name; a call with several values of a parameter counts in each value's group")
	cmd.Flags().IntVar(&limit, "limit", insight.DefaultHotspotLimit, "top groups each window lists")
	cmd.Flags().StringVar(&order, "order", string(insight.OrderDurationSumDesc), "duration_sum_desc ranks by total time, count_desc by call count")
	cmd.Flags().IntVar(&sample, "sample", insight.DefaultCompareSample, "slowest calls of the changed groups whose trees each window's sample reads")
	return cmd
}
