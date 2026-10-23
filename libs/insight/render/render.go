// Package render prints libs/insight results as compact text for a model's
// context: aligned columns, times in milliseconds, and one line per fact. The
// profiler CLI prints it by default, and the MCP server returns it beside the
// typed result, so both front ends show an agent the same text.
package render

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
)

const timeLayout = "2006-01-02T15:04:05.000Z"

func stamp(t time.Time) string { return t.UTC().Format(timeLayout) }

// errWriter keeps the first write error and fails every later write with it,
// so a text writer can print without checking each call and still report a
// closed pipe or a full disk once, at the end.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	n, err := e.w.Write(p)
	e.err = err
	return n, err
}

// printf and println drop the per-call error; e.err holds the first one.
func (e *errWriter) printf(format string, a ...any) { _, _ = fmt.Fprintf(e, format, a...) }
func (e *errWriter) println(a ...any)               { _, _ = fmt.Fprintln(e, a...) }

func writeWindow(out *errWriter, w insight.ResolvedWindow) {
	out.printf("Window: %s to %s (%s)\n", stamp(w.From), stamp(w.To), w.To.Sub(w.From))
}

func writePartial(out *errWriter, reasons []string) {
	for _, r := range reasons {
		out.printf("Partial: %s\n", r)
	}
}

// Services prints a services result.
func Services(w io.Writer, res insight.ServicesResult) error {
	out := &errWriter{w: w}
	writeWindow(out, res.Window)
	writePartial(out, res.Partial)
	if len(res.Namespaces) == 0 {
		out.println("No pods have profiling data in the window.")
		return out.err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	rows := &errWriter{w: tw}
	rows.println("NAMESPACE\tSERVICE\tPOD\tSTARTED\tFIRST CALL\tLAST CALL")
	for _, ns := range res.Namespaces {
		for _, svc := range ns.Services {
			for _, pod := range svc.Pods {
				for _, r := range pod.Restarts {
					rows.printf("%s\t%s\t%s\t%s\t%s\t%s\n",
						ns.Name, svc.Name, pod.Name, stamp(r.StartedAt), stamp(r.FirstCall), stamp(r.LastCall))
				}
			}
		}
	}
	_ = tw.Flush()
	return out.err
}

// Calls prints a calls result. more phrases the input that continues the
// listing for the front end, with one %s for the cursor, such as
// "More calls: --cursor %s"; it is printed when the result has a next cursor.
func Calls(w io.Writer, res insight.CallsResult, more string) error {
	out := &errWriter{w: w}
	if res.Window != nil {
		writeWindow(out, *res.Window)
	}
	if res.Restarted {
		out.println("The cursor was stale, so this is page 1 again.")
	}
	writePartial(out, res.Partial)
	if len(res.Calls) == 0 {
		out.println("No calls match.")
	} else {
		tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
		rows := &errWriter{w: tw}
		rows.println("START\tDUR ms\tCPU ms\tWAIT ms\tQUEUE ms\tSUSP ms\tERR\tPOD\tMETHOD\tIDS\tREF")
		for _, c := range res.Calls {
			errMark := "-"
			if c.Error {
				errMark = "yes"
			}
			rows.printf("%s\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
				stamp(c.Start), c.DurationMs, c.CPUMs, c.WaitMs, c.QueueWaitMs, c.SuspendMs, errMark,
				c.Namespace+"/"+c.Service+"/"+c.Pod, insight.ShortMethod(c.Method), ids(c), c.Ref)
		}
		_ = tw.Flush()
	}
	if res.NextCursor != "" {
		out.printf(more+"\n", res.NextCursor)
	}
	return out.err
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

// Explain prints an explain result.
func Explain(w io.Writer, res insight.ExplainResult) error {
	out := &errWriter{w: w}
	if c := res.Call; c != nil {
		out.printf("Call: %s\n", c.Ref)
		out.printf("  %s %s/%s/%s %s\n", stamp(c.Start), c.Namespace, c.Service, c.Pod, insight.ShortMethod(c.Method))
		if id := ids(*c); id != "-" {
			out.printf("  %s\n", id)
		}
		errText := ""
		if c.Error {
			errText = ", failed"
		}
		out.printf("  duration %d ms, cpu %d ms, wait %d ms, queue %d ms, gc suspension %d ms%s\n",
			c.DurationMs, c.CPUMs, c.WaitMs, c.QueueWaitMs, c.SuspendMs, errText)
	}
	out.printf("Focus: #%d %s\n", res.FocusNode, res.Focus)

	out.println("\nHot path (#node method  total ms, self ms, executions):")
	writePath(out, res.HotPath, 0)

	if len(res.TopSelf) > 0 {
		out.println("\nTop methods by self time:")
		for _, m := range res.TopSelf {
			out.printf("%8d ms  x%-6d %s\n", m.SelfMs, m.Executions, m.Method)
		}
	}
	if len(res.TopSQL) > 0 {
		out.println("\nTop SQL:")
		for _, s := range res.TopSQL {
			out.printf("%8d ms  x%-6d %s\n", s.DurationMs, s.Executions, s.Statement)
		}
	}
	writeNotes(out, res.Notes)
	return out.err
}

func writePath(out *errWriter, n *insight.PathNode, depth int) {
	if n == nil {
		return
	}
	indent := strings.Repeat("  ", depth)
	line := fmt.Sprintf("%s#%d %s  %d, %d, x%d", indent, n.Node, n.Method, n.TotalMs, n.SelfMs, n.Executions)
	if n.SuspendMs > 0 {
		line += fmt.Sprintf(", gc %d", n.SuspendMs)
	}
	if n.Skipped > 0 {
		line += fmt.Sprintf("  [%d pass-through frames above]", n.Skipped)
	}
	out.println(line)
	for _, ch := range n.Children {
		writePath(out, ch, depth+1)
	}
	if n.Hidden > 0 {
		out.printf("%s  (%d more, %d ms)\n", indent, n.Hidden, n.HiddenMs)
	}
}
