package render

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
)

// compareSections are the group statuses in print order, with their
// headings.
var compareSections = []struct{ status, title string }{
	{insight.StatusCompared, "Compared, the largest change in time per minute first"},
	{insight.StatusNew, "New: no call in the baseline"},
	{insight.StatusVanished, "Vanished: no call in the target"},
	{insight.StatusOutsideTop, "Outside the top N of one window, which the server cannot look up, so no figures there"},
	{insight.StatusUnknown, "Unknown in one window: a source failed there, so the group may have calls it did not count"},
}

// Compare prints a compare result: the exact join by status, then the
// sampled layer under a label that says it is a sample and of how many calls.
func Compare(w io.Writer, res insight.CompareResult) error {
	out := &errWriter{w: w}
	out.printf("Baseline: %s to %s (%s), %d calls, %d errors\n", stamp(res.Baseline.From), stamp(res.Baseline.To),
		res.Baseline.To.Sub(res.Baseline.From), res.BaselineTotals.Count, res.BaselineTotals.Errors)
	out.printf("Target:   %s to %s (%s), %d calls, %d errors\n", stamp(res.Target.From), stamp(res.Target.To),
		res.Target.To.Sub(res.Target.From), res.TargetTotals.Count, res.TargetTotals.Errors)
	out.printf("Exact: the %s groups in the top N of either window by %s. Rates are per minute of each window.\n",
		res.GroupBy, res.Order)
	writePartial(out, res.Partial)
	writeApproximate(out, res.Approximate)
	if len(res.Groups) == 0 {
		out.println("No calls match in either window.")
	}
	for _, sec := range compareSections {
		var groups []insight.CompareGroup
		for _, g := range res.Groups {
			if g.Status == sec.status {
				groups = append(groups, g)
			}
		}
		if len(groups) == 0 {
			continue
		}
		out.printf("\n%s:\n", sec.title)
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
		rows := &errWriter{w: tw}
		if sec.status == insight.StatusCompared {
			rows.printf("CHANGE ms/min\tTIME ms/min\tCALLS/min\tMEAN ms\tP95 ms\tERRORS\t %s\n", strings.ToUpper(res.GroupBy))
			for _, g := range groups {
				b, t := g.Baseline, g.Target
				key := groupKey(res.GroupBy, g.Key)
				if g.Slower {
					key += "  [slower]"
				}
				rows.printf("%+.1f\t%s\t%s\t%s\t%s\t%s\t %s\n", g.TimeMsPerMinDelta,
					arrow(fmt.Sprintf("%.1f", b.TimeMsPerMin), fmt.Sprintf("%.1f", t.TimeMsPerMin)),
					arrow(fmt.Sprintf("%.2f", b.CallsPerMin), fmt.Sprintf("%.2f", t.CallsPerMin)),
					arrow(fmt.Sprintf("%.0f", b.MeanMs), fmt.Sprintf("%.0f", t.MeanMs)),
					arrow(ms(b.P95Ms), ms(t.P95Ms)),
					arrow(percent(b.ErrorRate), percent(t.ErrorRate)), key)
			}
		} else {
			rows.printf("FIGURES\tTIME ms/min\tCALLS/min\tMEAN ms\tP95 ms\tERRORS\t %s\n", strings.ToUpper(res.GroupBy))
			for _, g := range groups {
				name, s := insight.WindowTarget, g.Target
				if s == nil {
					name, s = insight.WindowBaseline, g.Baseline
				}
				rows.printf("%s\t%.1f\t%.2f\t%.0f\t%s\t%s\t %s\n", name, s.TimeMsPerMin, s.CallsPerMin, s.MeanMs,
					ms(s.P95Ms), percent(s.ErrorRate), groupKey(res.GroupBy, g.Key))
			}
		}
		_ = tw.Flush()
	}

	if s := res.Sample; s != nil {
		out.printf("\nSampled, not exact: inner methods and SQL from the trees of the slowest calls of %d changed groups, per sampled call.\n",
			len(s.Groups))
		if s.Baseline != nil {
			out.printf("  baseline: the %d slowest of %d calls, %d trees merged\n", s.Baseline.Listed, s.Baseline.Of, s.Baseline.Trees)
			writeSampleInfo(out, *s.Baseline)
		} else {
			out.println("  baseline: none of these groups has calls there")
		}
		out.printf("  target: the %d slowest of %d calls, %d trees merged\n", s.Target.Listed, s.Target.Of, s.Target.Trees)
		writeSampleInfo(out, s.Target)
		if len(s.Methods) > 0 {
			out.println("\nInner methods, self ms per sampled call (baseline -> target, change):")
			for _, m := range s.Methods {
				out.printf("%9.1f -> %-9.1f %+9.1f  %s\n", m.BaselineSelfMsPerCall, m.TargetSelfMsPerCall, m.DeltaMsPerCall, m.Method)
			}
		}
		if len(s.SQL) > 0 {
			out.println("\nSQL, ms per sampled call (baseline -> target, change):")
			for _, q := range s.SQL {
				out.printf("%9.1f -> %-9.1f %+9.1f  %s\n", q.BaselineMsPerCall, q.TargetMsPerCall, q.DeltaMsPerCall, q.Statement)
			}
		}
		writeSampledCalls(out, s.Target.Calls)
	}
	writeNotes(out, res.Notes)
	return out.err
}

func arrow(before, after string) string { return before + " -> " + after }

func percent(share float64) string { return fmt.Sprintf("%.1f%%", 100*share) }
