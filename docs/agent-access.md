# Connecting an AI agent to the profiler

An AI agent can investigate slow requests and services with the profiler's data in two ways:

- **The `profiler` CLI with its skill**, for an agent that runs shell commands on a workstation, such as Claude Code,
  Codex, or Cursor.
- **The MCP server**, for an agent that cannot run commands but speaks the Model Context Protocol.

Both run the same operations against the profiler's query service and return the same results: list the services with
data, find calls by service, duration, error, or a request or trace ID, explain where one call spent its time, rank
where a service spends its time, find when a change started, and compare two time windows.

## CLI and skill

### Install the CLI

In a checkout of this repository, run:

```bash
make -C tools/profiler-cli install
```

It installs `profiler` into `$(go env GOBIN)`, or into `$(go env GOPATH)/bin` when `GOBIN` is unset. `go install` by
module path does not work: the Go module is named `qubership-profiler-backend`, not after the repository.

### Reach the query service

The CLI reads the query service named by `--url` or `PROFILER_URL`. The chart names the Service after the release with
a `-query` suffix, such as `profiler-profiler-backend-query`. Find it and forward its port:

```bash
kubectl get svc -A | grep -- '-query'
kubectl port-forward -n <profiler-namespace> svc/<query-service> 8080:8080
export PROFILER_URL=http://localhost:8080
profiler services --since 1h
```

`profiler services` lists the namespaces, services, and pods with data. An empty list with no error means the
connection works and no profiled pod reported in the last hour.

### Install the skill

The skill teaches an agent when to use the CLI, the investigation workflow, how to read the output, and the query
rules that keep a query from being refused as too wide. It is an APM package in this repository:
[`agent-packages/qubership-profiler-cli-usage`](../agent-packages/qubership-profiler-cli-usage). Install it into a
project with [APM](https://github.com/microsoft/apm):

```bash
apm install Netcracker/qubership-profiler-agent/agent-packages/qubership-profiler-cli-usage
```

APM compiles the skill for each agent the project targets. The agent then picks it up when asked why a request or a
service is slow.

## MCP server

`profiler-backend mcp` serves the same operations as MCP tools over Streamable HTTP at `/mcp`:

| Tool | CLI equivalent | What it does |
| --- | --- | --- |
| `profiler_list_services` | `profiler services` | Lists the namespaces, services, and pods with data in a window. |
| `profiler_find_calls` | `profiler calls` | Finds the newest or slowest calls that match conditions. |
| `profiler_explain_call` | `profiler explain` | Reduces one call's tree to its hot path, top methods, and top SQL. |
| `profiler_hotspots` | `profiler hotspots` | Ranks calls by total time per group; samples inner methods and SQL. |
| `profiler_timeline` | `profiler timeline` | Counts calls and errors per time bucket, with duration percentiles. |
| `profiler_compare_windows` | `profiler compare` | Shows what got slower, new, or vanished against a baseline. |

Every tool is read-only. A result carries the CLI's text as its content and the typed result as structured content.
The server's `instructions` carry a short form of the skill: the workflows, how to read a sampled result, and the query
rules.

`filters` takes the CLI's conditions in structured form. `--filter 'duration>1s'` on the CLI is
`{"field": "duration", "op": ">", "value": "1s"}` in a tool, and `service` is the same `<namespace>/<service>`
shorthand as the CLI's `--service`. The other inputs are the CLI's flags in snake case, such as `group_by` for
`--group-by` and `baseline_shift` for `--baseline-shift`, with the same defaults.

### Deploy it

The Helm chart deploys the server when `mcp.enabled` is `true`:

```bash
helm upgrade profiler deploy/charts/profiler-backend --reuse-values --set mcp.enabled=true
```

The server reads from this release's query Service unless `mcp.queryUrl` names another. It needs no S3 access and no
storage. To run it outside Kubernetes, set `PROFILER_URL` to the query service and start `profiler-backend mcp`; it
listens on `PROFILER_MCP_PORT`, 8090 by default.

### In-cluster only

v1 has no authentication on the MCP server or on the query service's `/api/v1`. Anyone who reaches the endpoint reads
the profiling data of every namespace, including request parameters and SQL. So the chart exposes the server through
a ClusterIP Service only and renders no Ingress or HTTPRoute for it. Do not publish it through an ingress of your own.
Authentication for both is planned, and the server already has the slot for it.

Reach the endpoint in one of two ways:

- From a pod in the cluster: `http://<release-fullname>-mcp.<profiler-namespace>.svc:8090/mcp`.
- From a workstation, through a port-forward:

  ```bash
  kubectl port-forward -n <profiler-namespace> svc/<release-fullname>-mcp 8090:8090
  ```

  The endpoint is then `http://localhost:8090/mcp`.

### Configure the client

Claude Code:

```bash
claude mcp add --transport http profiler http://localhost:8090/mcp
```

A client that reads an `mcpServers` JSON file, such as a project's `.mcp.json` for Claude Code or `.cursor/mcp.json`
for Cursor:

```json
{
  "mcpServers": {
    "profiler": {
      "type": "http",
      "url": "http://localhost:8090/mcp"
    }
  }
}
```

Check the connection by asking the agent to list the services with profiling data in the last hour. It calls
`profiler_list_services`.
