// Package mcpserver is the profiler's MCP front end: one tool per
// libs/insight operation over Streamable HTTP, for agents that cannot run
// the profiler CLI. Like the CLI, it holds no behavior of its own. Every
// tool calls the query service over HTTP through libs/insight, never
// libs/query in-process, so both front ends run one code path (section 3.1
// of the agent-access plan).
//
// It uses the official Go SDK, github.com/modelcontextprotocol/go-sdk. The
// SDK carries the protocol version negotiation, the input schema validation,
// and the transport, which a hand-written JSON-RPC subset would have to
// follow through every revision of the specification.
package mcpserver

import (
	"net/http"

	"github.com/Netcracker/qubership-profiler-backend/libs/insight"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Authenticator decides whether a request may reach the tools. It runs on
// every HTTP request, before the MCP layer reads the body. It returns the
// request to serve, whose context may carry the caller's identity (such as
// insight.WithCredential, which the client forwards to query), or writes the
// rejection to w and returns nil.
//
// v1 has no authentication (02-read-contract §10), so AcceptAll is the only
// implementation. A later one validates a bearer token and answers 401 with
// the resource metadata pointer that the MCP authorization specification
// requires; no tool changes with it.
type Authenticator interface {
	Authenticate(w http.ResponseWriter, r *http.Request) *http.Request
}

// AcceptAll is the v1 Authenticator: every request reaches the tools.
type AcceptAll struct{}

// Authenticate returns r unchanged.
func (AcceptAll) Authenticate(_ http.ResponseWriter, r *http.Request) *http.Request { return r }

// Options configure the server.
type Options struct {
	// Client calls the query service.
	Client *insight.Client
	// Authenticator guards every request; nil means AcceptAll.
	Authenticator Authenticator
	// Version is the build version the server reports to clients.
	Version string
}

// NewServer returns the MCP server with every operation's tool.
func NewServer(opts Options) (*mcp.Server, error) {
	version := opts.Version
	if version == "" {
		version = "dev"
	}
	s := mcp.NewServer(&mcp.Implementation{Name: "qubership-profiler", Title: "Qubership profiler", Version: version},
		&mcp.ServerOptions{Instructions: Instructions, Capabilities: &mcp.ServerCapabilities{}})
	for _, op := range operations {
		if err := op.add(s, opts.Client); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// NewHandler returns the Streamable HTTP handler of the server behind the
// Authenticator. The server is stateless: no session outlives a request,
// so any replica answers any request and a restart loses nothing.
func NewHandler(opts Options) (http.Handler, error) {
	s, err := NewServer(opts)
	if err != nil {
		return nil, err
	}
	auth := opts.Authenticator
	if auth == nil {
		auth = AcceptAll{}
	}
	mcpHandler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r = auth.Authenticate(w, r); r != nil {
			mcpHandler.ServeHTTP(w, r)
		}
	}), nil
}
