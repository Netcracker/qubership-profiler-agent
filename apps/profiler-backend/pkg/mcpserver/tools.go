package mcpserver

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/Netcracker/qubership-profiler-backend/libs/insight/render"
	"github.com/Netcracker/qubership-profiler-backend/libs/query/model"
	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/pkg/errors"
)

// operation is one libs/insight operation as an MCP tool. Adding an
// operation is one entry in operations plus its input type: the tool runs
// the library function, and the result is its text and its typed value.
type operation struct {
	// name is the CLI subcommand of the same operation.
	name string
	// add registers the tool on the server.
	add func(s *mcp.Server, c *insight.Client) error
}

// operations lists the tools in the order tools/list returns them. hotspots,
// timeline, and compare join when libs/insight has them.
var operations = []operation{
	{name: "services", add: tool(&mcp.Tool{
		Name:  "profiler_list_services",
		Title: "List services",
		Description: "List the namespaces, services, and pods that have profiling data in the window, one entry " +
			"per pod restart. Start here to learn the names that the other tools filter on.",
	}, func(ctx context.Context, c *insight.Client, in ServicesArgs) (insight.ServicesResult, error) {
		w, f, err := parseScope(in.WindowArgs, in.FilterArgs)
		if err != nil {
			return insight.ServicesResult{}, err
		}
		return c.Services(ctx, insight.ServicesInput{Window: w, Filters: f})
	}, render.Services)},

	{name: "calls", add: tool(&mcp.Tool{
		Name:  "profiler_find_calls",
		Title: "Find calls",
		Description: "Find profiled calls, newest first, by service, duration, error, or a request or trace ID. " +
			"Each call has a ref to pass to profiler_explain_call.",
	}, func(ctx context.Context, c *insight.Client, in CallsArgs) (insight.CallsResult, error) {
		w, f, err := parseScope(in.WindowArgs, in.FilterArgs)
		if err != nil {
			return insight.CallsResult{}, err
		}
		return c.Calls(ctx, insight.CallsInput{Window: w, Filters: f, Limit: in.Limit, Cursor: in.Cursor})
	}, func(w io.Writer, res insight.CallsResult) error {
		return render.Calls(w, res, `More calls: pass "cursor": "%s" with the same filters`)
	})},

	{name: "explain", add: tool(&mcp.Tool{
		Name:  "profiler_explain_call",
		Title: "Explain a call",
		Description: "Show where one call spent its time: the hot path of its call tree, the methods with the " +
			"most self time, and the slowest SQL. Pass node to re-root at a #node of an earlier result.",
	}, func(ctx context.Context, c *insight.Client, in ExplainArgs) (insight.ExplainResult, error) {
		return c.Explain(ctx, insight.ExplainInput{
			Ref: in.Ref, Focus: in.Focus, FocusNode: in.Node, Threshold: in.Threshold, MaxNodes: in.MaxNodes,
		})
	}, render.Explain)},
}

// WindowArgs is the time range of every tool except explain.
type WindowArgs struct {
	Since string `json:"since,omitempty" jsonschema:"window ending now (or at to), such as 30m, 6h, or 2d; the default is 1h"`
	From  string `json:"from,omitempty" jsonschema:"window start, RFC 3339 or Unix milliseconds; excludes since"`
	To    string `json:"to,omitempty" jsonschema:"window end, RFC 3339 or Unix milliseconds; the default is now"`
}

// FilterArgs are the conditions of every tool except explain.
type FilterArgs struct {
	Service string      `json:"service,omitempty" jsonschema:"<namespace>/<service>, shorthand for two conditions"`
	Filters []Condition `json:"filters,omitempty" jsonschema:"conditions that every call must meet"`
}

// Condition is the structured form of one filter condition (02 §2.3.3): the
// same condition the CLI takes as the text "<field><op><value>".
type Condition struct {
	Field string `json:"field" jsonschema:"duration, namespace, service, pod, error, request_id, trace_id, method, or a call parameter name"`
	Op    string `json:"op" jsonschema:"the operator"`
	Value string `json:"value" jsonschema:"such as 1s or 500ms for duration, true for error, a,b for in, or 100ms..2s for between"`
}

// ops are the operators the filter model defines. A build may implement a
// subset; the query service names an operator it does not implement.
var ops = []string{
	string(model.OpEq), string(model.OpNe), string(model.OpMatch), string(model.OpNotMatch),
	string(model.OpGt), string(model.OpGe), string(model.OpLt), string(model.OpLe),
	string(model.OpIn), string(model.OpNotIn), string(model.OpBetween),
}

// text renders the condition the way insight.Filters takes it. Spaces around
// the operator keep a value that starts with an operator character, such as
// "=x", from reading as a longer operator.
func (c Condition) text() (string, error) {
	if !slices.Contains(ops, c.Op) {
		return "", errors.Errorf("filter on %s: op %q is not one of %s", c.Field, c.Op, strings.Join(ops, ", "))
	}
	text := c.Field + " " + c.Op + " " + c.Value
	parsed, err := model.ParseCondition(text)
	if err != nil {
		return "", errors.Wrapf(err, "filter %s %s %s", c.Field, c.Op, c.Value)
	}
	if string(parsed.Op) != c.Op || parsed.Field != c.Field {
		return "", errors.Errorf("filter %s %s %s: the field name holds an operator character", c.Field, c.Op, c.Value)
	}
	return text, nil
}

// parseScope reads the window and the conditions the way the CLI reads its flags.
func parseScope(w WindowArgs, f FilterArgs) (insight.Window, insight.Filters, error) {
	window, err := insight.ParseWindow(w.Since, w.From, w.To)
	if err != nil {
		return insight.Window{}, insight.Filters{}, err
	}
	filters := insight.Filters{Service: f.Service}
	for _, c := range f.Filters {
		text, err := c.text()
		if err != nil {
			return insight.Window{}, insight.Filters{}, err
		}
		filters.Conditions = append(filters.Conditions, text)
	}
	return window, filters, nil
}

// ServicesArgs is the input of profiler_list_services.
type ServicesArgs struct {
	WindowArgs
	FilterArgs
}

// CallsArgs is the input of profiler_find_calls.
type CallsArgs struct {
	WindowArgs
	FilterArgs
	Limit  int    `json:"limit,omitempty" jsonschema:"calls per page; the default is 20"`
	Cursor string `json:"cursor,omitempty" jsonschema:"continue the listing from an earlier result's next_cursor, with the same filters or none"`
}

// ExplainArgs is the input of profiler_explain_call.
type ExplainArgs struct {
	Ref       string  `json:"ref" jsonschema:"the call's ref from profiler_find_calls"`
	Node      *int    `json:"node,omitempty" jsonschema:"re-root at this #node from an earlier explain of the same call"`
	Focus     string  `json:"focus,omitempty" jsonschema:"re-root at the slowest node whose method contains this text; excludes node"`
	Threshold float64 `json:"threshold,omitempty" jsonschema:"hide nodes under this share of the root, 0 to 1; the default is 0.05"`
	MaxNodes  int     `json:"max_nodes,omitempty" jsonschema:"most nodes in the hot path; the default is 40"`
}

// readOnly marks every tool: the profiler tools only read.
var readOnly = &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: new(false)}

// tool returns the registration of one operation. The tool's result carries
// the operation's text as content and its typed value, unchanged, as
// structured content. An operation error becomes a tool error with the
// library's message, which already tells the agent what to do next.
func tool[In, Out any](t *mcp.Tool, run func(context.Context, *insight.Client, In) (Out, error),
	text func(io.Writer, Out) error,
) func(*mcp.Server, *insight.Client) error {
	return func(s *mcp.Server, c *insight.Client) error {
		schema, err := inputSchema[In]()
		if err != nil {
			return errors.Wrapf(err, "input schema of %s", t.Name)
		}
		def := *t
		def.InputSchema = schema
		def.Annotations = readOnly
		mcp.AddTool(s, &def, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
			out, err := run(ctx, c, in)
			if err != nil {
				return nil, nil, err
			}
			var b strings.Builder
			if err := text(&b, out); err != nil {
				return nil, nil, errors.Wrap(err, "render the result")
			}
			return &mcp.CallToolResult{
				Content:           []mcp.Content{&mcp.TextContent{Text: b.String()}},
				StructuredContent: out,
			}, nil, nil
		})
		return nil
	}
}

// inputSchema derives the schema from the input type and adds what struct
// tags cannot say: the operator list of a condition.
func inputSchema[In any]() (*jsonschema.Schema, error) {
	schema, err := jsonschema.For[In](nil)
	if err != nil {
		return nil, err
	}
	if filters, ok := schema.Properties["filters"]; ok && filters.Items != nil {
		op := filters.Items.Properties["op"]
		if op == nil {
			return nil, fmt.Errorf("the condition schema has no op")
		}
		for _, o := range ops {
			op.Enum = append(op.Enum, o)
		}
	}
	return schema, nil
}
