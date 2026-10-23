---
name: qubership-profiler-cli-usage
description: Investigate why a request or a service is slow, or what got slower after a change, in a Java service profiled by the Qubership profiler. Find calls by request ID, trace ID, service, or duration, explain where a call spent its time, rank where a service spends its time, find when a change started, and compare two time windows with the `profiler` CLI.
---

# Investigating performance with the profiler CLI

The Qubership profiler records every root call of a profiled JVM (an HTTP request, a message handler, a scheduled
job) with its call tree. The `profiler` CLI reads that data from the profiler's query service.

## Setup

1. Get the binary. In a checkout of `Netcracker/qubership-profiler-agent`, run `make -C tools/profiler-cli install`;
   it installs `profiler` into `$(go env GOBIN)`, or into `$(go env GOPATH)/bin` when `GOBIN` is unset. `go install`
   by module path does not work: the Go module is named `qubership-profiler-backend`, not after the repository.
2. Reach the query service on port 8080. The chart names it after the release's full name with a `-query` suffix,
   such as `profiler-profiler-backend-query`, so find the exact name rather than guessing it:
   `kubectl get svc -A | grep -- '-query'`. If several match, ask the user which one. Then run:

   ```bash
   kubectl port-forward -n <profiler-namespace> svc/<query-service> 8080:8080
   export PROFILER_URL=http://localhost:8080
   ```

3. Check the connection with `profiler services --since 1h`. It lists the namespaces, services, and pods that have
   data, one row per pod restart.

Add `--json` to any command for the full typed result.

## Filter conditions

`calls`, `hotspots`, `timeline`, and `compare` take repeatable `--filter` conditions on any field below; a call must
meet all of them. `services` takes only `namespace`, `service`, and `pod` conditions. `--service <ns>/<svc>` is
shorthand for `namespace=<ns>` and `service=<svc>`. Quote each condition for the shell.

| Field | Means | Examples |
| --- | --- | --- |
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

   Add `--order duration_desc` to list the slowest calls first instead of the newest.

   `request_id` reads the `x-request-id` header only. When it finds nothing (`No calls match.`), the service may
   record its request ID under a call parameter of its own. The CLI does not print other parameters, so read them
   from the query service for a few recent calls of the service:

   ```bash
   now=$(date +%s)
   curl -sG "$PROFILER_URL/api/v1/calls" -d from=$(( (now - 3600) * 1000 )) -d to=$(( now * 1000 )) -d limit=3 \
     --data-urlencode 'filter=namespace=prod' --data-urlencode 'filter=service=billing' | jq '.calls[].params'
   ```

   Pick the parameter that holds the ID and filter on its name, such as `--filter 'request.id=abc123'`. Do not
   conclude that the request is missing until you have checked.

   A request that crossed several services shows one call per service that the profiler sees, all with the same
   `trace_id`. The slowest one that is not mostly waiting on the next service is usually the one to explain.
2. Explain it, passing the `REF` column as it is printed:

   ```bash
   profiler explain 'prod:billing:billing-7f8c:1760420000000:5:12340:0@1760433000123@any_error'
   ```

3. Go deeper only where the time is. Every hot-path node starts with its node ID, such as `#12`; rerun `explain`
   on the same call with `--node 12` to re-root the explanation at exactly that node. `--focus <text>` is a shortcut
   when the method is unique: it takes the slowest node whose method contains the text, and when several match, a
   note lists the others' IDs for `--node`. `--threshold 0.01` shows nodes down to 1% of the time, and
   `--max-nodes 100` widens the hot path. When the threshold or the node budget dropped hot-path nodes, a `Notes`
   section says what and which of these flags shows it; no `Notes` section means the hot path is complete. The top
   self-time and top SQL lists show at most ten entries each, with no note when they stop there.

## Workflow: what is slow in service X?

1. Rank where the time goes:

   ```bash
   profiler hotspots --service prod/billing --since 1h
   ```

   The table groups the calls by root method (the entry point: an endpoint, a listener, a job) and ranks them by
   total time. `SHARE` is the group's part of all the time in the window. A group with a large total and a small mean
   is called often; one with a large mean or `P95` is slow per call. `--order count_desc` ranks by call count, and
   `--group-by pod` shows whether one pod is worse than the others.
2. Read the sampled part under the table: the inner methods with the most self time and the slowest SQL across the
   slowest calls of the listed groups. It names where to look inside the code; see "Reading a sampled result".
3. Explain one of the `Slowest sampled calls` it lists, as in the request workflow, to see the path to the hot
   method. To study one group, run `profiler calls --filter 'method=~<part of its name>' --order duration_desc`
   and explain a few of its calls.

## Workflow: what changed?

1. Find when it started. `timeline` counts the calls and the errors and shows `P50`, `P95`, and `P99` per time
   bucket:

   ```bash
   profiler timeline --service prod/billing --since 6h
   ```

   Look for the first bucket where `P95` or `ERRORS` jumps. `--buckets 60` makes the buckets narrower; a narrower
   window around the jump does the same.
2. Compare the window after the change with the one before. By default the baseline is the window of the same
   length just before the target, so a target that starts at the change compares before with after:

   ```bash
   profiler compare --service prod/billing --from 2026-10-14T14:00:00Z --to 2026-10-14T15:00:00Z
   ```

   `--baseline-shift 1d` compares with the same hours a day before instead, which removes a daily load pattern;
   `--baseline-from` and `--baseline-to` set the baseline outright.
3. Read the sections in order. `Compared` ranks the groups in the top 10 of either window by the change in their
   time per minute, so windows of different lengths compare; `[slower]` marks a group whose mean or `P95` rose by
   20% or more. A group that only got more calls has a large change and no `[slower]` mark: it is load, not a
   regression. `New` and `Vanished` groups have no call at all in one window. `Outside the top N` and `Unknown`
   groups lack figures for one window; neither means new or vanished.
4. The sampled part compares the slowest calls of the slower and new groups before and after, per sampled call: the
   inner method or statement with the largest positive change is the first suspect. Explain a listed call to see
   where it sits.

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
  parameters are not in the CLI output, even with `--json`. `REF` ends with the call's retention class
  (`short_clean`, `long_clean`, `any_error`, and so on), which decides how long the call is kept.
- **No tree.** A call listed with `no tree (<reason>)` was too big for the agent's buffers; `explain` fails for it.
- **Partial.** A `Partial:` line means a source of data failed; the result may miss calls. Retry before concluding
  that a call does not exist.
- **Approximate.** An `Approximate:` line means the server kept only the heaviest groups; a group's figures may be
  overcounted. Narrow the filters to get exact figures.

## Reading a sampled result

`hotspots` and `compare` print two layers. The tables headed `Exact` count every call under the filters. The part
headed `Sampled, not exact` reads the call trees of a few calls only, because a profile of every call in a window is
too much data to decode:

- The sample is the slowest calls, `--sample` of them (20 for `hotspots`, 10 per window for `compare`). The label says
  how many were read and how many calls they were drawn from: `the 20 slowest of 1220 calls` describes the slow
  tail, not the typical call. A method that dominates the sample may be rare in fast calls.
- `in 19` after a method or statement counts the sampled calls that ran it. A method in 19 of 20 calls is a pattern;
  one in 1 of 20 is an outlier in a single call.
- `Skipped:` lines count sampled calls left out: calls without a tree, or trees that failed to load.
- `Notes` say when a list was cut and which flag shows more, such as a larger `--sample` or `--limit`.

Confirm a sampled finding before you report it: explain two or three of the listed calls and check that the method
or statement is on their hot paths.

## Rules for queries

- Name a service or namespace whenever you know it. A query over every service scans more data and is slower.
- Keep the window at 6 hours or less, unless a condition narrows the data: `duration>1s` or more, `error=true`, or a
  `service` or `pod`. A request or trace ID does not narrow the scan, so bracket the time when you know it.
- `hotspots`, `timeline`, and `compare` count every call of their windows, so a service condition or a short window
  matters more for them than for `calls`.
- Keep both windows of `compare` within the last two days, or add `--filter 'duration>=100ms'`: fast successful
  calls age out after two days, so an older baseline holds only the slower calls and every per-call figure shifts.
- `the query is too wide to scan` lists the conditions that would make it fit. Add one, or shorten the window; the
  same query will fail again.
- `the profiler is busy` means the server is short of read memory. Narrow the query, or retry after a minute.

## Limits

- The profiler has no thread dumps, heap dumps, or GC logs here; GC shows up only as suspension time.
- Fast successful calls are kept for a short time (two days by default); slow and failed calls are kept longer. An
  old fast call may be gone.
- `request_id` and `trace_id` exist only when the service received the header or ran under a supported tracer.
- Filters and the exact statistics apply to root calls. A method inside a call is not searchable with `--filter`;
  `hotspots` ranks inner methods from a sample only. Find the calls by their root method, duration, or IDs, then use
  `explain` and `--node`.
