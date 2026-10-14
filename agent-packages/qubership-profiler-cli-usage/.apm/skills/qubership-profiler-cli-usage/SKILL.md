---
name: qubership-profiler-cli-usage
description: Investigate why a request or a service is slow in a Java service profiled by the Qubership profiler. Find the calls by request ID, trace ID, service, or duration, and explain where a call spent its time with the `profiler` CLI.
---

# Investigating performance with the profiler CLI

The Qubership profiler records every root call of a profiled JVM (an HTTP request, a message handler, a scheduled
job) with its call tree. The `profiler` CLI reads that data from the profiler's query service.

## Setup

1. Get the binary. In a checkout of `Netcracker/qubership-profiler-agent`, run `make -C tools/profiler-cli install`;
   it installs `profiler` into `$(go env GOPATH)/bin`. `go install` by module path does not work: the Go module is
   named `qubership-profiler-backend`, not after the repository.
2. Reach the query service. It is the `<release>-query` service of the profiler's Helm release, on port 8080. Find
   it with `kubectl get svc -A | grep -- '-query'`, or ask the user for the namespace and release, then run:

   ```bash
   kubectl port-forward -n <profiler-namespace> svc/<release>-query 8080:8080
   export PROFILER_URL=http://localhost:8080
   ```

3. Check the connection with `profiler services --since 1h`. It lists the namespaces, services, and pods that have
   data, one row per pod restart.

Add `--json` to any command for the full typed result.

## Filter conditions

`calls` and `services` take repeatable `--filter` conditions; a call must meet all of them. `--service <ns>/<svc>`
is shorthand for `namespace=<ns>` and `service=<svc>`. Quote each condition for the shell.

| Field | Means | Examples |
|---|---|---|
| `duration` | Call duration; `ms` or `s`, a bare number is seconds | `duration>2s`, `duration between 100ms..500ms` |
| `namespace`, `service`, `pod` | Where the call ran | `service=billing`, `pod in billing-1,billing-2` |
| `error` | The call failed | `error=true` |
| `request_id` | The `x-request-id` header of an HTTP request | `request_id=abc123` |
| `trace_id` | The distributed trace ID (OpenTelemetry, Brave, Jaeger) | `trace_id=4bf92f3577b34da6a3ce929d0e0e4736` |
| `method` | The root method, as a regular expression | `method=~OrderController` |
| any other name | A call parameter the agent recorded | `user.tenant=acme` |

A field or operator the server does not implement yet fails with an error that names it. The implemented set is in
`curl -s $PROFILER_URL/api/v1/config`, under `filter_capabilities`.

## Workflow: why was this request slow?

1. Find the call. With a request or trace ID:

   ```bash
   profiler calls --since 6h --filter 'request_id=abc123'
   ```

   With a time and a service instead, bracket the time and keep only slow calls:

   ```bash
   profiler calls --service prod/billing --from 2026-10-14T11:58:00Z --to 2026-10-14T12:02:00Z --filter 'duration>1s'
   ```

   `request_id` reads the `x-request-id` header only. When it finds nothing (`No calls match.`), the service may
   record its request ID under a call parameter of its own: run `profiler calls --json` on a few calls of the
   service, look at the names in `params`, and filter on that name, such as `--filter 'request.id=abc123'`. Do not
   conclude that the request is missing until you have checked.

   A request that crossed several services shows one call per service that the profiler sees, all with the same
   `trace_id`. The slowest one that is not mostly waiting on the next service is usually the one to explain.
2. Explain it, passing the `REF` column as it is printed:

   ```bash
   profiler explain 'prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error'
   ```

3. Go deeper only where the time is. `--focus <text>` re-roots the explanation at the slowest method containing the
   text, such as `--focus OrderRepository.load`. `--threshold 0.01` shows nodes down to 1% of the time, and
   `--max-nodes 100` widens the hot path. When the output cut anything, a `Notes` section says what and which of
   these flags shows it; no `Notes` section means nothing was cut.

## Reading the output

- **Duration, CPU, wait.** CPU close to the duration means the call was computing. A large wait means it was blocked:
  on I/O, a lock, or a connection pool. Queue wait is time spent queued before the call started, usually a full
  thread pool.
- **GC suspension** is the part of the call during stop-the-world pauses of the whole JVM. When it is a large share
  of the duration, the call was slowed by GC rather than by its own code. Every node of the hot path shows its own
  suspension as `gc`.
- **Self against total.** A node's total includes its children; its self time is spent in the method itself. Look for
  large self time: that is where the time went. A large total with a small self points down the tree.
- **Executions.** `x90` is how often the method ran at that place in the tree, for the whole call. On a fast node
  under a slow parent it is a loop or an N+1 query; the cost is the count, not one execution.
- **Collapsed and hidden nodes.** `[3 pass-through frames above]` means frames that only passed the time down
  (filters, proxies, interceptors) were skipped. `(4 more, 37 ms)` counts the children cut by the threshold or the
  node budget.
- **Top SQL** sums each statement over the call. Statements that differ only in literals are one entry.
- **The `calls` table.** `IDS` shows the request ID, trace ID, and HTTP method and URL when the call has them; other
  parameters are only in `--json`. `REF` ends with the call's retention class (`short_clean`, `long_clean`,
  `any_error`, and so on), which decides how long the call is kept.
- **No tree.** A call listed with `no tree (<reason>)` was too big for the agent's buffers; `explain` fails for it.
- **Partial.** A `Partial:` line means a source of data failed; the result may miss calls. Retry before concluding
  that a call does not exist.

## Rules for queries

- Name a service or namespace whenever you know it. A query over every service scans more data and is slower.
- Keep the window at 6 hours or less, unless a condition narrows the data: `duration>1s` or more, `error=true`, or a
  `service` or `pod`. A request or trace ID does not narrow the scan, so bracket the time when you know it.
- `the query is too wide to scan` lists the conditions that would make it fit. Add one, or shorten the window; the
  same query will fail again.
- `the profiler is busy` means the server is short of read memory. Narrow the query, or retry after a minute.

## Limits

- The profiler has no thread dumps, heap dumps, or GC logs here; GC shows up only as suspension time.
- Fast successful calls are kept for a short time (two days by default); slow and failed calls are kept longer. An
  old fast call may be gone.
- `request_id` and `trace_id` exist only when the service received the header or ran under a supported tracer.
- Filters apply to root calls. A method inside a call is not searchable with `--filter`; find the calls by their root
  method, duration, or IDs, then use `explain` and `--focus`.
