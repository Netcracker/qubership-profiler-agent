package render

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
)

// Hotspots prints a hotspots result: the exact table, then the sampled
// layer under a label that says it is a sample and of how many calls.
func Hotspots(w io.Writer, res insight.HotspotsResult) error {
	out := &errWriter{w: w}
	writeWindow(out, res.Window)
	out.printf("Exact, over all %d calls (%d errors): %d %s groups, ranked by %s.\n",
		res.Totals.Count, res.Totals.Errors, res.Distinct, res.GroupBy, res.Order)
	writePartial(out, res.Partial)
	writeApproximate(out, res.Approximate)
	if len(res.Groups) == 0 {
		out.println("No calls match.")
		writeNotes(out, res.Notes)
		return out.err
	}
	var allMs int64
	for _, g := range res.Groups {
		allMs += g.DurationMsSum
	}
	if res.Other != nil {
		allMs += res.Other.DurationMsSum
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
	rows := &errWriter{w: tw}
	rows.printf("TOTAL ms\tSHARE\tCALLS\tERR\tMEAN ms\tP50\tP95\tP99\tCPU ms\tWAIT ms\tSUSP ms\t %s\n", strings.ToUpper(res.GroupBy))
	for _, g := range res.Groups {
		writeStatsRow(rows, g.StatsValues, allMs, groupKey(res.GroupBy, g.Key))
	}
	if res.Other != nil {
		writeStatsRow(rows, *res.Other, allMs, fmt.Sprintf("(other: %d groups)", res.Distinct-len(res.Groups)))
	}
	_ = tw.Flush()

	if s := res.Sample; s != nil {
		scope := "under the filters"
		if len(s.Groups) > 0 {
			scope = fmt.Sprintf("in the top %d groups", len(s.Groups))
		}
		out.printf("\nSampled, not exact: methods and SQL from the trees of the %d slowest of %s calls %s; %d trees merged.\n",
			s.Listed, callsOf(s.Of), scope, s.Trees)
		writeSampleInfo(out, s.SampleInfo)
		writeSampledMethods(out, "Inner methods by self time, summed over the sample", s.Methods)
		writeSampledSQL(out, "SQL, summed over the sample", s.SQL)
		writeSampledCalls(out, s.Calls)
	}
	writeNotes(out, res.Notes)
	return out.err
}

// Timeline prints a timeline result, one line per bucket.
func Timeline(w io.Writer, res insight.TimelineResult) error {
	out := &errWriter{w: w}
	writeWindow(out, res.Window)
	out.printf("%d calls, %d errors, in %d buckets of %s.\n", res.Totals.Count, res.Totals.Errors, len(res.Buckets),
		time.Duration(res.BucketMs)*time.Millisecond)
	writePartial(out, res.Partial)
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', tabwriter.AlignRight)
	rows := &errWriter{w: tw}
	rows.println("START\tCALLS\tERRORS\tP50 ms\tP95 ms\tP99 ms\t")
	for _, b := range res.Buckets {
		rows.printf("%s\t%d\t%d\t%s\t%s\t%s\t\n", stamp(b.Start), b.Count, b.Errors, ms(b.P50Ms), ms(b.P95Ms), ms(b.P99Ms))
	}
	_ = tw.Flush()
	return out.err
}

func writeStatsRow(rows *errWriter, v insight.StatsValues, allMs int64, key string) {
	share, mean := "-", "-"
	if allMs > 0 {
		share = fmt.Sprintf("%.0f%%", 100*float64(v.DurationMsSum)/float64(allMs))
	}
	if v.Count > 0 {
		mean = fmt.Sprintf("%d", v.DurationMsSum/v.Count)
	}
	rows.printf("%d\t%s\t%d\t%d\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t %s\n",
		v.DurationMsSum, share, v.Count, v.Errors, mean, ms(v.P50Ms), ms(v.P95Ms), ms(v.P99Ms),
		v.CPUMsSum, v.WaitMsSum, v.SuspendMsSum, key)
}

// groupKey shortens a method key as explain prints methods; other keys are
// values to filter on and print as they are.
func groupKey(field, key string) string {
	if field == "method" {
		return insight.ShortMethod(key)
	}
	return key
}

// callsOf renders the number of calls a sample is drawn from.
func callsOf(of *int64) string {
	if of == nil {
		return "an unknown number of"
	}
	return fmt.Sprint(*of)
}

func writeSampleInfo(out *errWriter, s insight.SampleInfo) {
	for _, r := range s.Skipped {
		out.printf("Skipped: %s\n", r)
	}
	writePartial(out, s.Partial)
}

func writeSampledMethods(out *errWriter, title string, methods []insight.SampledMethod) {
	if len(methods) == 0 {
		return
	}
	out.printf("\n%s (self ms, executions, sampled calls that ran it):\n", title)
	for _, m := range methods {
		out.printf("%8d ms  x%-6d in %-4d %s\n", m.SelfMs, m.Executions, m.Calls, m.Method)
	}
}

func writeSampledSQL(out *errWriter, title string, sql []insight.SampledSQL) {
	if len(sql) == 0 {
		return
	}
	out.printf("\n%s (ms, executions, sampled calls that ran it):\n", title)
	for _, s := range sql {
		out.printf("%8d ms  x%-6d in %-4d %s\n", s.DurationMs, s.Executions, s.Calls, s.Statement)
	}
}

func writeSampledCalls(out *errWriter, calls []insight.Call) {
	if len(calls) == 0 {
		return
	}
	out.println("\nSlowest sampled calls, to explain:")
	for _, c := range calls {
		out.printf("%8d ms  %s  %s\n", c.DurationMs, c.Ref, insight.ShortMethod(c.Method))
	}
}

func writeApproximate(out *errWriter, approximate bool) {
	if approximate {
		out.println("Approximate: a source kept only its heaviest groups, so a group's ranking figure may be overcounted.")
	}
}

func writeNotes(out *errWriter, notes []string) {
	if len(notes) == 0 {
		return
	}
	out.println("\nNotes:")
	for _, n := range notes {
		out.printf("- %s\n", n)
	}
}

// ms prints an optional millisecond figure, "-" when absent.
func ms(v *int64) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *v)
}
