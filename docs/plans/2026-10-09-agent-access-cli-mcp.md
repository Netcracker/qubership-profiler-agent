# Agent access to the profiler backend: CLI, skill, and MCP

Status: **draft**, from the brainstorm of October 9, 2026. Implements item C3 "MCP for profiler data" of
[`profiler-plan.md`](../design/profiler-plan.md).

## 1. Goal

Let coding agents and agent harnesses connect to the profiler backend, analyze calls, and help a user investigate
performance problems in a component. Two kinds of consumer are in scope:

- **Local agents with a shell** (Claude Code, Cursor, Codex) use a `profiler` CLI and a skill.
- **Hosted agents without a shell** use an MCP server.

Both get the same operations with the same results.

### 1.1 Investigations v1 supports

| Scenario | Example question | Phase |
|---|---|---|
| Request | "Why was the request with `x-request-id` abc123 slow?" | 1 |
| Service | "What is slow in `billing` over the last hour?" | 2 |
| Change | "What got slower in `billing` after the 14:00 deployment?" | 2 |
| JVM, limited | "Did GC pauses cause this?" Answered from per-call and per-node suspension only. | 1 |

### 1.2 Out of scope

- **Authentication** for `/api/v1` and for MCP. It is a planned follow-up; section 6 lists what v1 prepares for it.
- **A `dumps-collector` client** (thread dumps, heap dumps, GC logs). See section 8.
- **Precomputed per-method aggregates** at seal time.
- **Raw access** to the `/trace` blob or the unreduced tree through the agent tools.

## 2. Findings: is the read API enough?

The `query` service registers five routes, all under `/api/v1` (`libs/query/api.go`): `/pods`, `/calls`,
`/calls/{pk}/tree`, `/calls/{pk}/trace`, and `/config`. They cover the drill-down from a service to a call to its
tree. They do not cover ranking, statistics, or lookup by a request identifier.

| Gap | Effect on an agent | Resolution in this plan |
|---|---|---|
| No param filter on `/calls` (R3) | Cannot go from a `trace_id` or `x-request-id` to a call | Task 1.2 |
| No `order=duration_desc` (R2) | "Slowest calls" is a threshold plus paging in time order | Task 2.2 |
| `/stats` is specified but not implemented | No per-method counts or percentiles | Task 2.3 |
| `/tree` is int-keyed MessagePack and can be large | A model cannot read it as-is | Reduction in `libs/insight`, task 1.4 |
| `/calls` filters by exact pod, not by service | The agent thinks in services | Resolution through `/pods` in `libs/insight`, task 1.3 |
| No authentication | A network-reachable MCP server exposes all data | In-cluster only; section 6 |

Facts checked in the code:

- `calltree.Decode` exists next to `calltree.Encode` (`libs/calltree/msgpack.go`), so the Go client needs no new
  decoder.
- Both tiers already store call params: the hot call index has `params_json` (`libs/collector/hotstore/metadata.go`)
  and `CallV2` has a `params` list column (`libs/storage/parquet/callv2.go`). The param filter needs no storage change.
- Problem responses carry a stable `code`, and `query_too_wide` carries `suggested_filters`
  (`02-read-contract.md` §8). An agent can act on both without parsing prose.

## 3. Architecture

```text
 local agent ── skill ──▶ profiler CLI (tools/profiler-cli) ──┐
                                                              ├──▶ libs/insight ── HTTP ──▶ query /api/v1
 hosted agent ── MCP ───▶ profiler-backend mcp ───────────────┘
```

- **`libs/insight`** (working name) holds all behavior: a typed HTTP client for `/api/v1` that reuses
  `libs/query/model` and `calltree.Decode`, and one function per operation. It knows nothing about flags, MCP, or
  output formats.
- **`profiler` CLI** in `tools/profiler-cli` is a thin Cobra binary with one subcommand per operation.
- **`profiler-backend mcp`** is a fourth subcommand beside `collect`, `maintain`, and `query`. It registers one MCP
  tool per operation over streamable HTTP.

### 3.1 Decisions

1. **The library talks HTTP to `query`, even from the MCP server.** In-process calls into `libs/query` would save a
   hop but create a second code path that the CLI cannot use. With HTTP there is one path, and the MCP server deploys
   separately from `query`.
2. **Aggregation is server-side.** The param filter, the duration order, and `/stats` go into `query`, so the library
   never pages through thousands of calls.
3. **Tree reduction is in the library.** `/tree` stays the canonical contract. Reducing a tree for a model is a
   presentation concern that can change without a contract revision.
4. **Operations are task-level.** There are five tools, not a mirror of the REST routes. Tool schemas stay small and
   results stay short.
5. **Every endpoint this plan adds goes under `/api/v1`.** The client in `libs/insight` has one base-path constant.

## 4. Operations

Each operation is one library function, one CLI subcommand, and one MCP tool. Every operation takes a window as a
relative `since` (for example `30m`) or as absolute `from` and `to`, and echoes the resolved absolute window.

| Operation | CLI | MCP tool | Input | Returns |
|---|---|---|---|---|
| List services | `profiler services` | `profiler_list_services` | window, optional namespace | Namespaces, services, and pods with data, with pod-restart times |
| Find calls | `profiler calls` | `profiler_find_calls` | window, service, optional pods, method, minimum duration, errors only, params, order, limit, cursor | Calls with a call reference, time, duration, root method, CPU, wait, suspension, and key params |
| Explain a call | `profiler explain` | `profiler_explain_call` | call reference, optional focus method, threshold, node budget | Header, reduced hot path, top methods by self time, top SQL groups |
| Hotspots | `profiler hotspots` | `profiler_hotspots` | window, service, optional root method, sample size | Exact per-root-method statistics, plus sampled inner methods and SQL |
| Compare windows | `profiler compare` | `profiler_compare_windows` | baseline window, candidate window, service, optional root method | Per-root-method changes, new and vanished entries, sampled detail for what changed |

### 4.1 Call reference

`calls` prints one opaque token per call that bundles the PK, `ts_ms`, and `retention_class`. `explain` takes that
token. The agent never assembles the cold-tier hints of `02-read-contract.md` §2.2 by hand. The token is built
client-side in v1.

### 4.2 Tree reduction in `explain`

- **Header:** duration, CPU, wait, queue wait, suspension, and the error flag.
- **Hot path:** the tree with pass-through chains collapsed, children under a share threshold dropped (default 5% of
  the root), and a node budget. It follows the skip-degenerate-chains rule of `08-ui-backend-requirements.md` §5.
- **Top methods by self time** across the whole tree.
- **Top SQL groups** with duration, executions, and a truncated statement.
- **Going deeper:** a focus method re-roots the reduction at a subtree; the threshold and the node budget widen it.

Every result has a hard cap and states when it was cut and which input widens it.

### 4.3 Exact and sampled layers in `hotspots` and `compare`

Call rows carry only the root method and per-call metrics. Statistics for inner methods across calls would need a
trace blob decode per call, which the read memory budget rules out. So the two operations have two layers:

- **Exact:** per-root-method statistics from `/stats`, over all calls in the window.
- **Sampled:** the library fetches the trees of the K slowest calls (default 20) for the top root methods, merges
  them, and reports inner methods and SQL signatures. The output labels this part as a sample of K calls.

`compare` joins the exact layer across both windows and samples trees only for root methods that changed.

## 5. Backend changes

Each change starts with an update to `02-read-contract.md`, in the same PR as the code (`WORKFLOW.md` §7).

### 5.1 Param filter on `/calls` (R3)

- A repeatable `param=<key>=<value>` with exact match on one of the call's values for that key.
- A row filter on both tiers: over `params_json` in the hot call index and the `params` column in parquet.
- It does not exempt a query from the wide-query guard, like `method` (`02-read-contract.md` §2.3.2). A lookup by
  `trace_id` therefore needs a service and a window of at most `PROFILER_WIDE_RANGE_LIMIT`.
- The cursor's frozen query includes the new filter.

### 5.2 `order=duration_desc` on `/calls` (R2)

- As `08-ui-backend-requirements.md` R2 specifies: prune to the long and error classes, merge, and sort by
  `duration_ms`.
- A second keyset, `(duration_ms DESC, pk ASC)`, with the order frozen in the cursor.
- Open point for the contract update: whether the order is allowed without `duration_min_ms`, given that the short
  classes cannot be pruned then.

### 5.3 `GET /api/v1/stats`

- Parameters: the `/calls` filters, plus `group_by=method` (the root method; the only grouping in v1) and `limit`.
- Per group: call count, error count, total duration, p50, p95, and p99 duration, and sums of CPU, wait, queue wait,
  and suspension.
- Same wide-query guard, read memory budget, and `partial` semantics as `/calls`.
- Percentiles come from a sketch built during the scan. The contract update names the sketch and its error bound.
- This replaces the sketch in `02-read-contract.md` §2.7 and removes `/stats` from §10 there.

## 6. Front ends

### 6.1 CLI

- `--url` or `PROFILER_URL` names the `query` base URL. No config file in v1.
- Compact aligned text by default, sized for a model's context; `--json` prints the typed result.
- Port-forwarding stays outside the CLI. The skill gives the `kubectl port-forward` command.

### 6.2 Skill for the CLI

The skill is a deliverable of its own, authored in phase 1 and extended in phase 2.

- **Location:** an APM skill in this repository, packaged through `apm.yml`. Confirm the source path with the
  `apm-authoring` skill before creating it; `.claude/skills` holds compiled output and is not the place to author.
- **Contents of `SKILL.md`:**
  - A description with trigger phrases: slow request, slow service, performance regression, profiler, call tree.
  - Setup: how to get the `profiler` binary, `PROFILER_URL`, and the port-forward command.
  - Workflow "why was this request slow?": `services`, then `calls` with a param or a time and a service, then
    `explain`, then `explain` with a focus method.
  - Workflows "what is slow in service X?" and "what changed?" (added in phase 2).
  - How to read the output: self time against total time, suspension as GC pauses, wait against CPU, and what a
    sampled result means.
  - Guard rules: always pass a service; keep the window at 6 hours or less unless filtering by duration or errors;
    what to do on `query_too_wide`.
  - Limits: no thread or heap dumps, and no data older than the retention of a call's class.
- **Verification:** an agent with only the skill and the CLI finds a planted slow method in data seeded by
  `tools/ui-seed`.

### 6.3 MCP server

- Streamable HTTP only, as `profiler-backend mcp`, deployed as its own pod with `PROFILER_URL` pointing at `query`.
- The five tools of section 4, all annotated read-only. Each result is the CLI text plus the JSON as structured
  content.
- The server's `instructions` field carries a short version of the skill's workflows and guard rules, derived from
  the same source so the two do not drift.
- Compare the official Go MCP SDK with a hand-written handler before adding the dependency.

### 6.4 Authentication: future work, prepared in v1

v1 has no authentication on `/api/v1` or on MCP. The MCP server listens inside the cluster only and relies on the
operator's ingress for access control. Authentication for both is a follow-up after this plan
(`02-read-contract.md` §10 names Keycloak and bearer tokens as the likely choice).

v1 builds these seams with no behavior behind them, so adding authentication changes no operation and no tool
signature:

1. **Credential seam in the client.** The `libs/insight` client takes an injectable credential source that can attach
   an `Authorization` header to each request. It is empty in v1.
2. **Caller identity in the context.** Every operation takes a `context.Context`, and the client reads the credential
   from it. The MCP server can then forward each caller's own token to `query`, so `query` authorizes the end user and
   the MCP server needs no shared service account.
3. **Authenticator slot.** The MCP HTTP handler is a chain with an `Authenticator` interface in front of the tool
   dispatch. The v1 implementation accepts every request. A later one validates a bearer token and answers `401` with
   the resource-metadata pointer that the MCP authorization specification requires.
4. **Authorization hook.** One function decides whether a caller may query a namespace. Every operation calls it
   after resolving the service. It always allows in v1.
5. **No credentials in tool parameters.** Tool schemas never carry tokens.

### 6.5 Error handling

The library maps the contract's `code` values to typed errors. Both front ends render them the same way.

| `code` | Library behavior | What the agent sees |
|---|---|---|
| `query_too_wide` | No retry | The `suggested_filters` and the per-class estimate, phrased as the inputs to add |
| `read_budget_exhausted` | One retry after `Retry-After` | On a second failure, a "busy: narrow the query or retry" message |
| `cursor_rejected` | Restart from page 1 | Nothing |
| `call_not_found`, `trace_unavailable` | No retry | Which one it was; a truncated call has no tree |
| `invalid_request` | No retry | The server's detail |
| `partial: true` (not an error) | Pass the data through | The result, with a line naming the failed sources |

## 7. Tasks

One task is one PR (`WORKFLOW.md` §2). Unit tests live next to the code; cross-module suites go under
`libs/tests/integration/`. Fixtures are synthetic only.

### Phase 1: request investigation

Usable alone; the smallest backend change.

- [ ] **1.1 Contract update.** Specify the param filter in `02-read-contract.md` §2.3 and §2.3.2, and add a note to
  §10 that authentication is planned for `/api/v1` and MCP. Acceptance: the user reviews the contract.
- [ ] **1.2 Param filter in `query` and the collector.** `ParseCallsQuery` and `CallsQuery.Values` in
  `libs/query/model`, the hot filter over `params_json`, the cold row filter, and the cursor fingerprint. Tests:
  parsing, hot and cold matches, a guard case showing no exemption, and a frozen-query mismatch. Acceptance: a seeded
  call is found by its `x-request-id` on both tiers.
- [ ] **1.3 `libs/insight` client and the `services` and `calls` operations.** Typed client with the base-path
  constant, the credential seam and the authorization hook of section 6.4, service-to-pod resolution, the call
  reference, window parsing, and the error mapping of section 6.5. Tests: an `httptest` server replaying recorded
  responses, one case per error code, and a check that the credential header is attached when a credential is present.
- [ ] **1.4 `explain` operation.** Tree reduction: chain collapse, share threshold, node budget, top self-time
  methods, top SQL groups, and the focus method. Tests: table tests over trees built with `calltree.Encode`,
  including a deep pass-through chain, a wide fan-out, and a tree past the node budget.
- [ ] **1.5 `profiler` CLI.** `tools/profiler-cli` with `services`, `calls`, and `explain`; text and `--json` output;
  Makefile and build wiring like the other tools. Tests: golden files for the text output.
- [ ] **1.6 Skill for the CLI.** Section 6.2, with the request workflow. Acceptance: the verification run of
  section 6.2 passes.
- [ ] **1.7 End-to-end scenario.** Seed with `tools/ui-seed`, then run `calls` with a param filter and `explain`
  through the CLI against a running `query`.

### Phase 2: ranking and statistics

- [ ] **2.1 Contract update.** Specify `order=duration_desc` and the full `/stats` schema in `02-read-contract.md`,
  resolving the open point of section 5.2 and naming the percentile sketch.
- [ ] **2.2 `order=duration_desc`.** The second keyset in the cursor, class pruning, and the merge comparator on both
  tiers. Tests: cursor stability across the hot-to-cold migration for the new order, and guard cases.
- [ ] **2.3 `/stats` endpoint.** Aggregation over the hot and cold sources with dedup by PK before aggregation, the
  guard, the budget, and `partial`. Tests: exact counts against a known data set, percentile error within the stated
  bound, and a duplicate from the overlap window counted once.
- [ ] **2.4 `hotspots` operation and subcommand.** The exact layer from `/stats`, and the sampled layer: fetch K
  trees, merge, rank inner methods and SQL signatures. Tests: tree merging on built trees, and the sample label in the
  output.
- [ ] **2.5 `compare` operation and subcommand.** The join of two `/stats` results, the change ranking, new and
  vanished entries, and sampling for changed methods only. Tests: table tests on two synthetic windows.
- [ ] **2.6 Extend the skill.** Add the service and change workflows and how to read a sampled result.

### Phase 3: MCP front end

- [ ] **3.1 `profiler-backend mcp` subcommand.** Streamable HTTP, the five tools, the `Authenticator` slot with the
  accept-all implementation, and the `instructions` text. Tests: the tool list covers the same operations as the CLI,
  each tool returns the library result unchanged, and the authenticator is invoked on every request.
- [ ] **3.2 Deployment.** A Helm values block for the MCP pod, off by default, with an in-cluster service and no
  ingress. Document the in-cluster-only limit and the reason.
- [ ] **3.3 Documentation.** A page under `docs/` for connecting an agent: CLI and skill setup, and the MCP endpoint.

## 8. Later work

Record each item in `deferred.md` with its trigger when phase 3 closes.

- **Authentication for `/api/v1` and MCP.** Fills the seams of section 6.4. It gates any use of the MCP server from
  outside the cluster.
- **JVM diagnostics from `dumps-collector`.** Thread dumps, GC logs, and heap dumps as further operations.
  `dumps-collector` serves `/cdt/v2` and `/esc`, not `/api/v1`; decide first whether to move its routes under
  `/api/v1` or to add a client for the old prefix.
- **Precomputed inner-method aggregates** at seal time, if the sampled layer of section 4.3 proves too weak. This is a
  write-contract change.
- **A server-issued `call_ref`**, if a second client needs the token of section 4.1.

## 9. Risks

- **Scan cost of `/stats` over the short classes.** A service-wide window can hit the wide-query guard. The skill and
  the operations default to a narrow window, and `query_too_wide` tells the agent how to narrow.
- **Sample bias.** The K slowest calls over-represent outliers. The output states the sample rule, and the sample
  size is an input.
- **Output size.** A wrong default in the reduction floods a model's context. Golden-file tests pin the size of the
  default output for a large tree.
- **Drift between the CLI and MCP.** Both call the same library functions, and task 3.1 tests that the two surfaces
  cover the same operations.
