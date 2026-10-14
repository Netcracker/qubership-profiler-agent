package cmd

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
)

// The text output is sized for a model's context: aligned columns, times in
// milliseconds, and one line per fact. --json carries every field.

const timeLayout = "2006-01-02T15:04:05.000Z"

func stamp(t time.Time) string { return t.UTC().Format(timeLayout) }

func writeWindow(out io.Writer, w insight.ResolvedWindow) {
	fmt.Fprintf(out, "Window: %s to %s (%s)\n", stamp(w.From), stamp(w.To), w.To.Sub(w.From))
}

func writePartial(out io.Writer, reasons []string) {
	for _, r := range reasons {
		fmt.Fprintf(out, "Partial: %s\n", r)
	}
}

func writeServices(out io.Writer, res insight.ServicesResult) error {
	writeWindow(out, res.Window)
	writePartial(out, res.Partial)
	if len(res.Namespaces) == 0 {
		_, err := fmt.Fprintln(out, "No pods have profiling data in the window.")
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "NAMESPACE\tSERVICE\tPOD\tSTARTED\tFIRST CALL\tLAST CALL")
	for _, ns := range res.Namespaces {
		for _, svc := range ns.Services {
			for _, pod := range svc.Pods {
				for _, r := range pod.Restarts {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
						ns.Name, svc.Name, pod.Name, stamp(r.StartedAt), stamp(r.FirstCall), stamp(r.LastCall))
				}
			}
		}
	}
	return tw.Flush()
}

func writeCalls(out io.Writer, res insight.CallsResult) error {
	if res.Window != nil {
		writeWindow(out, *res.Window)
	}
	if res.Restarted {
		fmt.Fprintln(out, "The cursor was stale, so this is page 1 again.")
	}
	writePartial(out, res.Partial)
	if len(res.Calls) == 0 {
		fmt.Fprintln(out, "No calls match.")
	} else {
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "START\tDUR ms\tCPU ms\tWAIT ms\tQUEUE ms\tSUSP ms\tERR\tPOD\tMETHOD\tIDS\tREF")
		for _, c := range res.Calls {
			errMark := "-"
			if c.Error {
				errMark = "yes"
			}
			fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
				stamp(c.Start), c.DurationMs, c.CPUMs, c.WaitMs, c.QueueWaitMs, c.SuspendMs, errMark,
				c.Namespace+"/"+c.Service+"/"+c.Pod, insight.ShortMethod(c.Method), ids(c), c.Ref)
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	if res.NextCursor != "" {
		fmt.Fprintf(out, "More calls: --cursor %s\n", res.NextCursor)
	}
	return nil
}

// ids joins the identifiers an investigation searches by, and marks a call
// whose tree the agent dropped.
func ids(c insight.Call) string {
	var parts []string
	if c.RequestID != "" {
		parts = append(parts, "request_id="+c.RequestID)
	}
	if c.TraceID != "" {
		parts = append(parts, "trace_id="+c.TraceID)
	}
	if c.HTTP != "" {
		parts = append(parts, c.HTTP)
	}
	if c.Truncated != "" {
		parts = append(parts, "no tree ("+c.Truncated+")")
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func writeExplain(out io.Writer, res insight.ExplainResult) error {
	if c := res.Call; c != nil {
		fmt.Fprintf(out, "Call: %s\n", c.Ref)
		fmt.Fprintf(out, "  %s %s/%s/%s %s\n", stamp(c.Start), c.Namespace, c.Service, c.Pod, insight.ShortMethod(c.Method))
		if id := ids(*c); id != "-" {
			fmt.Fprintf(out, "  %s\n", id)
		}
		errText := ""
		if c.Error {
			errText = ", failed"
		}
		fmt.Fprintf(out, "  duration %d ms, cpu %d ms, wait %d ms, queue %d ms, gc suspension %d ms%s\n",
			c.DurationMs, c.CPUMs, c.WaitMs, c.QueueWaitMs, c.SuspendMs, errText)
	}
	fmt.Fprintf(out, "Focus: %s\n", res.Focus)

	fmt.Fprintln(out, "\nHot path (total ms, self ms, executions):")
	writePath(out, res.HotPath, 0)

	if len(res.TopSelf) > 0 {
		fmt.Fprintln(out, "\nTop methods by self time:")
		for _, m := range res.TopSelf {
			fmt.Fprintf(out, "%8d ms  x%-6d %s\n", m.SelfMs, m.Executions, m.Method)
		}
	}
	if len(res.TopSQL) > 0 {
		fmt.Fprintln(out, "\nTop SQL:")
		for _, s := range res.TopSQL {
			fmt.Fprintf(out, "%8d ms  x%-6d %s\n", s.DurationMs, s.Executions, s.Statement)
		}
	}
	if len(res.Notes) > 0 {
		fmt.Fprintln(out, "\nNotes:")
		for _, n := range res.Notes {
			fmt.Fprintf(out, "- %s\n", n)
		}
	}
	return nil
}

func writePath(out io.Writer, n *insight.PathNode, depth int) {
	if n == nil {
		return
	}
	indent := strings.Repeat("  ", depth)
	line := fmt.Sprintf("%s%s  %d, %d, x%d", indent, n.Method, n.TotalMs, n.SelfMs, n.Executions)
	if n.SuspendMs > 0 {
		line += fmt.Sprintf(", gc %d", n.SuspendMs)
	}
	if n.Skipped > 0 {
		line += fmt.Sprintf("  [%d pass-through frames above]", n.Skipped)
	}
	fmt.Fprintln(out, line)
	for _, ch := range n.Children {
		writePath(out, ch, depth+1)
	}
	if n.Hidden > 0 {
		fmt.Fprintf(out, "%s  (%d more, %d ms)\n", indent, n.Hidden, n.HiddenMs)
	}
}
