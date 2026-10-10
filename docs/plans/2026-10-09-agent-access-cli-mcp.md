# Agent access to the profiler backend: CLI, skill, and MCP

Status: **draft**, from the brainstorm of October 9, 2026. Implements item C3 "MCP for profiler data" of
[`profiler-plan.md`](../design/profiler-plan.md).

## 1. Goal

Let coding agents and agent harnesses connect to the profiler backend, analyze calls, and help a user investigate
performance problems in a component. Two kinds of consumer are in scope:

- **Local agents with a shell** (Claude Code, Cursor, Codex) use a `profiler` CLI and a skill.
- **Hosted agents without a shell** use an MCP server.

Both get the same operations with the same results.

The read API changes this needs are the same ones the UI redesign needs (section 2.1). The plan therefore specifies one
filter model, one aggregate endpoint, and one ordering parameter for both consumers, and builds them in phases.

### 1.1 Investigations v1 supports

| Scenario | Example question | Phase |
|---|---|---|
| Request | "Why was the request with `x-request-id` abc123 slow?" | 1 |
| Service | "What is slow in `billing` over the last hour?" | 2 |
| Change | "What got slower in `billing` after the 14:00 deployment, and when did it start?" | 2 |
| JVM, limited | "Did GC pauses cause this?" Answered from per-call and per-node suspension only. | 1 |

### 1.2 Out of scope

- **Authentication** for `/api/v1` and for MCP. It is a planned follow-up; section 6.4 lists what v1 prepares for it.
- **A `dumps-collector` client** (thread dumps, heap dumps, GC logs). See section 8.
- **Precomputed per-method aggregates** at seal time.
- **Raw access** to the `/trace` blob or the unreduced tree through the agent tools.
- **The UI redesign itself** and the UI's built-in assistant. This plan only keeps the API and the operations usable
  by both.

## 2. Findings: is the read API enough?

The `query` service registers five routes, all under `/api/v1` (`libs/query/api.go`): `/pods`, `/calls`,
`/calls/{pk}/tree`, `/calls/{pk}/trace`, and `/config`. They cover the drill-down from a service to a call to its
tree. They do not cover ranking, statistics, or lookup by a request identifier.

| Gap | Effect on an agent | Resolution in this plan |
|---|---|---|
| No filter by call param on `/calls` (R3) | Cannot go from a `trace_id` or `x-request-id` to a call | Filter model, section 5.1 |
| `/calls` filters by exact pod, not by service | The agent thinks in services | Filter model, section 5.1 |
| No ordering other than time (R2) | "Slowest calls" is a threshold plus paging in time order | `order`, section 5.2 |
| `/stats` is specified but not implemented | No per-method counts or percentiles | `/stats`, section 5.3 |
| `/tree` is int-keyed MessagePack and can be large | A model cannot read it as-is | Reduction in `libs/insight`, task 1.4 |
| No authentication | A network-reachable MCP server exposes all data | In-cluster only; section 6.4 |

Facts checked in the code:

- `calltree.Decode` exists next to `calltree.Encode` (`libs/calltree/msgpack.go`), so the Go client needs no new
  decoder.
- Both tiers already store call params: the hot call index has `params_json` (`libs/collector/hotstore/metadata.go`)
  and `CallV2` has a `params` list column (`libs/storage/parquet/callv2.go`). Filtering by a param needs no storage
  change.
- The agent records `web.method` and `web.url` as call params, so an `endpoint` field can be derived from them.
- Problem responses carry a stable `code`, and `query_too_wide` carries `suggested_filters`
  (`02-read-contract.md` §8). An agent can act on both without parsing prose.

### 2.1 Alignment with the UI redesign mockups

The UI redesign mockups of October 6–9, 2026 (calls page with a filter bar, and the call tree page) need the same
kind of API additions. The mockups are not in the repository yet; `07-ui-design.md` describes the earlier design.

| Mockup feature | What it needs from the API | Covered by |
|---|---|---|
| "Calls over time" histogram: completed and errors per bucket, following the filters | Counts per time bucket | `/stats` grouped by time |
| "N calls · M errors" readout | Totals under the filters | `/stats` totals |
| Value suggestions with counts, narrowed by the other filters | Top values of a field with counts | `/stats` grouped by a field |
| Filter tokens: `namespace`, `service`, `pod`, `endpoint`, `class`, `method`, `error`, `trace_id`, `request_id`, tags | Each field as a filter | Filter model |
| Operators `=`, `!=`, `=~`, `!~`, `in`, `not in`, `between`, and the values `None` and `Any` | An operator per condition | Filter model |
| "Sort by": start, duration, CPU, queue wait, child calls, net IO | Server-side ordering | `order` |
| "Hide system/proxy" switch | A server-side definition of a system call | Open point, section 5.4 |
| "Ask AI" on the calls page, reading the current filters and time range | The filter state as input to an assistant | `find_calls`, `hotspots`, `timeline` |
| "Ask AI" and "Explain" on the tree page, for the tree or the selected node | A tree explanation with a focus node | `explain_call` |
| Tree tabs: call tree, hotspots, flame graph, params, categories | `/tree` only; client-side transforms | No API change |

Three consequences for this plan:

1. A filter by one param with exact match would be a special case of the UI's filter tokens. The contract specifies
   the full filter model once.
2. The histogram, the value suggestions, and the per-method statistics are one aggregation with a different grouping
   key. `/stats` is one endpoint with a `group_by` parameter.
3. The UI's assistant and the agent tools do the same job. The assistant calls the same `libs/insight` operations
   when it is built.

## 3. Architecture

```text
 local agent ── skill ──▶ profiler CLI (tools/profiler-cli) ──┐
                                                              ├──▶ libs/insight ── HTTP ──▶ query /api/v1
 hosted agent ── MCP ───▶ profiler-backend mcp ───────────────┘                                ▲
                                                                                               │
 browser ──────────────▶ UI (apps/ui) ─────────────────────────────────────────────────────────┘
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
2. **Aggregation is server-side.** Filtering, ordering, and `/stats` go into `query`, so the library never pages
   through thousands of calls.
3. **Tree reduction is in the library.** `/tree` stays the canonical contract. Reducing a tree for a model is a
   presentation concern that can change without a contract revision.
4. **Operations are task-level.** There are six tools, not a mirror of the REST routes. Tool schemas stay small and
   results stay short.
5. **Every endpoint this plan adds goes under `/api/v1`.** The client in `libs/insight` has one base-path constant.
6. **One filter model for the UI and the agents.** A condition is a field, an operator, and a value. The UI's filter
   tokens, the CLI's `--filter` flag, and the MCP tools' `filters` input are the same conditions.
7. **The library passes conditions through.** It validates their syntax and nothing else. A field or operator that
   the backend gains later works in the CLI and MCP without a library change.

## 4. Operations

Each operation is one library function, one CLI subcommand, and one MCP tool. Every operation takes a window as a
relative `since` (for example `30m`) or as absolute `from` and `to`, and echoes the resolved absolute window. Every
operation except `explain` takes a list of filter conditions (section 5.1).

| Operation | CLI | MCP tool | Extra input | Returns |
|---|---|---|---|---|
| List services | `profiler services` | `profiler_list_services` | None | Namespaces, services, and pods with data, with pod-restart times |
| Find calls | `profiler calls` | `profiler_find_calls` | Order, limit, cursor | Calls with a call reference, time, duration, root method, CPU, wait, suspension, and key params |
| Explain a call | `profiler explain` | `profiler_explain_call` | Call reference, focus method, threshold, node budget | Header, reduced hot path, top methods by self time, top SQL groups |
| Hotspots | `profiler hotspots` | `profiler_hotspots` | Grouping field (default: root method), sample size | Exact per-group statistics, plus sampled inner methods and SQL |
| Timeline | `profiler timeline` | `profiler_timeline` | Bucket count | Call count, error count, and p95 duration per time bucket |
| Compare windows | `profiler compare` | `profiler_compare_windows` | A second window | Per-group changes, new and vanished entries, sampled detail for what changed |

`--service <namespace>/<service>` on the CLI and `service` in the MCP tools are shorthand for two conditions.

### 4.1 Call reference

`calls` prints one opaque token per call that bundles the PK, `ts_ms`, and `retention_class`. `explain` takes that
token. The agent never assembles the cold-tier hints of `02-read-contract.md` §2.2 by hand. The token is built
client-side in v1.

### 4.2 Tree reduction in `explain`

- **Header:** duration, CPU, wait, queue wait, suspension, and the error flag.
- **Hot path:** the tree with pass-through chains collapsed, children under a share threshold dropped (default 5% of
  the root), and a node budget. It follows the skip-degenerate-chains rule of `08-ui-backend-requirements.md` §5.
- **Top methods by self time** across the whole tree. This is the same view as the UI's Hotspots tab.
- **Top SQL groups** with duration, executions, and a truncated statement.
- **Going deeper:** a focus method re-roots the reduction at a subtree; the threshold and the node budget widen it.
  The UI's "Explain" on a selected node maps to the focus method.

Every result has a hard cap and states when it was cut and which input widens it.

### 4.3 Exact and sampled layers in `hotspots` and `compare`

Call rows carry only the root method and per-call metrics. Statistics for inner methods across calls would need a
trace blob decode per call, which the read memory budget rules out. So the two operations have two layers:

- **Exact:** per-group statistics from `/stats`, over all calls in the window.
- **Sampled:** the library fetches the trees of the K slowest calls (default 20) for the top groups, merges them, and
  reports inner methods and SQL signatures. The output labels this part as a sample of K calls.

`compare` joins the exact layer across both windows and samples trees only for groups that changed.

### 4.4 Deep links to the UI

`query` serves the UI from the same base URL as `/api/v1`. When the redesigned filter bar defines its permalink
format, `calls`, `hotspots`, `timeline`, and `explain` add a UI link with the same window and conditions to their
results. An agent's finding then opens in the UI. The link format belongs to the UI's URL module (`apps/ui/src/url`);
the library only fills it in.

## 5. Backend changes

Each change starts with an update to `02-read-contract.md`, in the same PR as the code (`WORKFLOW.md` §7). The
contract specifies each of the three models in full once. The implementation follows in stages; a field, operator,
ordering, or grouping that is specified but not yet implemented answers `400` with `code: invalid_request` and a
detail that names it.

### 5.1 Filter model for `/calls` and `/stats`

A repeatable `filter` parameter. Each value is one condition: a field, an operator, and a value. All conditions must
match. The text form of a condition is the form the UI's filter bar shows, so a filter reads the same in the UI, in a
URL, and in a CLI flag. Task 1.1 fixes the exact grammar and escaping.

Fields:

| Field | Source | Notes |
|---|---|---|
| `duration` | `duration_ms` column | Values such as `100ms`, `2s`, `100ms..2s` |
| `namespace`, `service`, `pod` | Identity columns and pod manifests | Resolved server-side to pod-restarts |
| `method` | `method` column | The root method |
| `error` | `error_flag` column | Boolean |
| `retention_class` | The object key | As today |
| `class` | Derived from `method` | Derivation specified in task 1.1 |
| `endpoint` | Derived from the `web.method` and `web.url` params | Raw URLs have high cardinality; see section 5.4 |
| `trace_id`, `request_id` | Call params | Aliases for the param names the agent records; list fixed in task 1.1 |
| Any other name | Call param of that name | For example `user.tenant` |

Operators and build stages:

| Stage | Operators | Fields | Driven by |
|---|---|---|---|
| A (phase 1) | `=`; `>`, `>=`, `<`, `<=`, and a range for `duration` | `duration`, `namespace`, `service`, `pod`, `error`, `retention_class`, params | Request investigation |
| B (phase 4) | `!=`, `in`, `not in`, `=~`, `!~`, and the values `None` and `Any` | All of the above, plus `method`, `class`, `endpoint` | The UI filter bar |

Rules:

- **Existing parameters stay.** `pod`, `method`, `duration_min_ms`, `duration_max_ms`, `error_only`, and
  `retention_class` keep working and are defined as shorthand for conditions.
- **Wide-query guard.** A condition exempts a query only if it prunes the discovered file set
  (`02-read-contract.md` §2.3.2): `namespace`, `service`, or `pod` with `=` or `in`; a lower bound on `duration` at or
  above a class bound; `error = true`; `retention_class`. Conditions on params, regular expressions, and negations
  filter rows inside listed files and do not exempt.
- **Cursor.** The frozen query includes every condition.

### 5.2 `order` on `/calls`

- One parameter, `order=<field>_<asc|desc>`, with `start_desc` as the default and today's behavior.
- Fields in the contract: `start`, `duration`, `cpu`, `queue`, `calls`, `net`, matching the UI's "Sort by" menu.
- Stage A (phase 2): `duration_desc`, as `08-ui-backend-requirements.md` R2 specifies: prune to the long and error
  classes, merge, and sort. A second keyset, `(duration_ms DESC, pk ASC)`, with the order frozen in the cursor.
- Stage B (phase 4): the remaining fields and `asc`. None of them prunes files, so they scan the window and work only
  inside the wide-query guard.
- Open point for the contract update: whether `duration_desc` is allowed without a lower bound on `duration`, given
  that the short classes cannot be pruned then.

### 5.3 `GET /api/v1/stats`

One aggregate endpoint for the UI's histogram, readout, and value suggestions, and for the agents' `hotspots`,
`timeline`, and `compare`.

- **Parameters:** the window and the `filter` conditions of `/calls`; `group_by`; `buckets` for the time grouping;
  `limit` and `order` (by count or by total duration) for the field grouping.
- **`group_by=time`:** one row per time bucket over the window.
- **`group_by=<field>`:** one row per value of a field of section 5.1, the top `limit` rows, and an `other` row that
  sums the rest.
- **Per row:** the key, call count, error count, total duration, and p50, p95, and p99 duration. The field grouping
  also returns sums of CPU, wait, queue wait, and suspension.
- **Totals:** every response carries the call count and the error count under the filters.
- **Semantics:** the same wide-query guard, read memory budget, and `partial` reporting as `/calls`. Duplicates from
  the hot and cold overlap are removed by PK before aggregation.
- **Percentiles** come from a sketch built during the scan. The contract update names the sketch and its error bound.
- **Memory:** grouping by a high-cardinality field (a request identifier) keeps a bounded top-N structure, and the
  contract states that the counts are then approximate.

Build stages:

| Stage | Groupings | Driven by |
|---|---|---|
| A (phase 2) | `time`, `method`, `namespace`, `service`, `pod` | `hotspots`, `timeline`, `compare`, and the UI histogram |
| B (phase 4) | Params and the derived fields `class` and `endpoint` | The UI's value suggestions |

This replaces the sketch in `02-read-contract.md` §2.7 and removes `/stats` from §10 there.

### 5.4 Open points from the mockups

- **`endpoint` cardinality.** `web.url` holds the raw path, so `/orders/123` and `/orders/124` are different values.
  A useful `endpoint` field needs a route template from the agent or a normalization rule on the server.
- **"Hide system/proxy" switch.** The backend has no definition of a system call. Options: a configured list of
  method patterns served by `/config`, or a client-side `method !~` condition.
- **Container.** The mockup's discovery tree has no container level because `/pods` returns namespace, service, and
  pod only.
- **Mockups in the repository.** The mockups exist as artifacts only. Update `07-ui-design.md` and `09-ui-screens.md`
  or add the mockups under `docs/design/` when the redesign is accepted.

## 6. Front ends

### 6.1 CLI

- `--url` or `PROFILER_URL` names the `query` base URL. No config file in v1.
- `--filter '<condition>'`, repeatable, on every subcommand except `explain`. `--service` is shorthand.
- Compact aligned text by default, sized for a model's context; `--json` prints the typed result.
- Port-forwarding stays outside the CLI. The skill gives the `kubectl port-forward` command.

### 6.2 Skill for the CLI

The skill is a deliverable of its own, authored in phase 1 and extended in phase 2.

- **Location:** an APM skill in this repository, packaged through `apm.yml`. Confirm the source path with the
  `apm-authoring` skill before creating it; `.claude/skills` holds compiled output and is not the place to author.
- **Contents of `SKILL.md`:**
  - A description with trigger phrases: slow request, slow service, performance regression, profiler, call tree.
  - Setup: how to get the `profiler` binary, `PROFILER_URL`, and the port-forward command.
  - The filter conditions: fields, operators, and examples.
  - Workflow "why was this request slow?": `services`, then `calls` with a condition on a request identifier or with
    a time and a service, then `explain`, then `explain` with a focus method.
  - Workflows "what is slow in service X?" and "what changed?" (added in phase 2), with `timeline` to find when a
    change started.
  - How to read the output: self time against total time, suspension as GC pauses, wait against CPU, and what a
    sampled result means.
  - Guard rules: always pass a service; keep the window at 6 hours or less unless filtering by duration or errors;
    what to do on `query_too_wide`.
  - Limits: no thread or heap dumps, and no data older than the retention of a call's class.
- **Verification:** an agent with only the skill and the CLI finds a planted slow method in data seeded by
  `tools/ui-seed`.

### 6.3 MCP server

- Streamable HTTP only, as `profiler-backend mcp`, deployed as its own pod with `PROFILER_URL` pointing at `query`.
- The six tools of section 4, all annotated read-only. Each result is the CLI text plus the JSON as structured
  content.
- `filters` is an array of `{field, op, value}` objects, the structured form of the same conditions.
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
4. **Authorization hook.** One function decides whether a caller may query a namespace. Every operation calls it with
   the namespaces its conditions name. It always allows in v1.
5. **No credentials in tool parameters.** Tool schemas never carry tokens.

### 6.5 The UI's assistant

The mockups have an "Ask AI" panel on the calls page and on the tree page. Building it is not part of this plan. When
it is built, it calls the `libs/insight` operations, through the MCP server or through an endpoint in `query`, with
the page's window, conditions, and selected node as input. It needs no operations of its own.

### 6.6 Error handling

The library maps the contract's `code` values to typed errors. Both front ends render them the same way.

| `code` | Library behavior | What the agent sees |
|---|---|---|
| `query_too_wide` | No retry | The `suggested_filters` and the per-class estimate, phrased as the conditions to add |
| `read_budget_exhausted` | One retry after `Retry-After` | On a second failure, a "busy: narrow the query or retry" message |
| `cursor_rejected` | Restart from page 1 | Nothing |
| `call_not_found`, `trace_unavailable` | No retry | Which one it was; a truncated call has no tree |
| `invalid_request` | No retry | The server's detail, including a field or operator that is not implemented yet |
| `partial: true` (not an error) | Pass the data through | The result, with a line naming the failed sources |

## 7. Tasks

One task is one PR (`WORKFLOW.md` §2). Unit tests live next to the code; cross-module suites go under
`libs/tests/integration/`. Fixtures are synthetic only.

### Phase 1: request investigation

Usable alone; the smallest backend change.

- [ ] **1.1 Contract update: the filter model.** Specify section 5.1 in full in `02-read-contract.md` §2.3 and
  §2.3.2: the grammar and escaping, every field with its derivation and aliases, every operator, the guard rule, and
  the stage marking. Add a note to §10 that authentication is planned for `/api/v1` and MCP. Review it against the UI
  mockups' filter bar. Acceptance: the user reviews the contract.
- [ ] **1.2 Filter model, stage A.** The condition parser in `libs/query/model`, server-side resolution of
  `namespace` and `service` to pod-restarts, the hot filter over `params_json`, the cold row filter, the mapping of
  the existing parameters, and the cursor fingerprint. Tests: parsing and escaping, hot and cold matches, guard cases
  for exempting and non-exempting conditions, a stage B operator answering `invalid_request`, and a frozen-query
  mismatch. Acceptance: a seeded call is found by its `x-request-id` on both tiers.
- [ ] **1.3 `libs/insight` client and the `services` and `calls` operations.** Typed client with the base-path
  constant, the credential seam and the authorization hook of section 6.4, condition syntax checks, the call
  reference, window parsing, and the error mapping of section 6.6. Tests: an `httptest` server replaying recorded
  responses, one case per error code, and a check that the credential header is attached when a credential is present.
- [ ] **1.4 `explain` operation.** Tree reduction: chain collapse, share threshold, node budget, top self-time
  methods, top SQL groups, and the focus method. Tests: table tests over trees built with `calltree.Encode`,
  including a deep pass-through chain, a wide fan-out, and a tree past the node budget.
- [ ] **1.5 `profiler` CLI.** `tools/profiler-cli` with `services`, `calls`, and `explain`; `--filter` and
  `--service`; text and `--json` output; Makefile and build wiring like the other tools. Tests: golden files for the
  text output.
- [ ] **1.6 Skill for the CLI.** Section 6.2, with the request workflow. Acceptance: the verification run of
  section 6.2 passes.
- [ ] **1.7 End-to-end scenario.** Seed with `tools/ui-seed`, then run `calls` with a condition on a request
  identifier and `explain` through the CLI against a running `query`.

### Phase 2: ranking and statistics

- [ ] **2.1 Contract update: `order` and `/stats`.** Specify sections 5.2 and 5.3 in full in `02-read-contract.md`,
  resolving the open point of section 5.2 and naming the percentile sketch. Review the `/stats` response against the
  UI mockups' histogram, readout, and value suggestions.
- [ ] **2.2 `order`, stage A.** `duration_desc`: the second keyset in the cursor, class pruning, and the merge
  comparator on both tiers. Tests: cursor stability across the hot-to-cold migration for the new order, and guard
  cases.
- [ ] **2.3 `/stats`, stage A.** Aggregation over the hot and cold sources for the `time`, `method`, `namespace`,
  `service`, and `pod` groupings, with dedup by PK before aggregation, totals, the guard, the budget, and `partial`.
  Tests: exact counts against a known data set, bucket boundaries, percentile error within the stated bound, and a
  duplicate from the overlap window counted once.
- [ ] **2.4 `hotspots` and `timeline` operations and subcommands.** The exact layer from `/stats`, and the sampled
  layer: fetch K trees, merge, rank inner methods and SQL signatures. Tests: tree merging on built trees, and the
  sample label in the output.
- [ ] **2.5 `compare` operation and subcommand.** The join of two `/stats` results, the change ranking, new and
  vanished entries, and sampling for changed groups only. Tests: table tests on two synthetic windows.
- [ ] **2.6 Extend the skill.** Add the service and change workflows, `timeline`, and how to read a sampled result.

### Phase 3: MCP front end

- [ ] **3.1 `profiler-backend mcp` subcommand.** Streamable HTTP, the six tools, the `Authenticator` slot with the
  accept-all implementation, and the `instructions` text. Tests: the tool list covers the same operations as the CLI,
  each tool returns the library result unchanged, and the authenticator is invoked on every request.
- [ ] **3.2 Deployment.** A Helm values block for the MCP pod, off by default, with an in-cluster service and no
  ingress. Document the in-cluster-only limit and the reason.
- [ ] **3.3 Documentation.** A page under `docs/` for connecting an agent: CLI and skill setup, and the MCP endpoint.

### Phase 4: the rest of the shared model

Driven by the UI redesign and scheduled with it. The contract already specifies everything here, and the CLI and MCP
gain each item without a change, because `libs/insight` passes conditions through.

- [ ] **4.1 Filter model, stage B.** `!=`, `in`, `not in`, `=~`, `!~`, `None` and `Any`, and the `method`, `class`,
  and `endpoint` fields.
- [ ] **4.2 `order`, stage B.** The `cpu`, `queue`, `calls`, and `net` fields, and `asc`.
- [ ] **4.3 `/stats`, stage B.** Grouping by params and by the derived fields, with the bounded top-N structure.
- [ ] **4.4 Deep links.** The UI permalink in operation results (section 4.4), once the filter bar defines it.
- [ ] **4.5 Skill and `instructions` update** for the new operators and groupings.

## 8. Later work

Record each item in `deferred.md` with its trigger when phase 3 closes.

- **Authentication for `/api/v1` and MCP.** Fills the seams of section 6.4. It gates any use of the MCP server from
  outside the cluster.
- **JVM diagnostics from `dumps-collector`.** Thread dumps, GC logs, and heap dumps as further operations.
  `dumps-collector` serves `/cdt/v2` and `/esc`, not `/api/v1`; decide first whether to move its routes under
  `/api/v1` or to add a client for the old prefix.
- **The UI's assistant** (section 6.5).
- **Precomputed inner-method aggregates** at seal time, if the sampled layer of section 4.3 proves too weak. This is a
  write-contract change.
- **A server-issued `call_ref`**, if a second client needs the token of section 4.1.

## 9. Risks

- **Scan cost of `/stats` over the short classes.** A service-wide window can hit the wide-query guard. The skill and
  the operations default to a narrow window, and `query_too_wide` tells the agent how to narrow.
- **Scan cost of stage B.** Regular expressions, negations, and non-duration orderings prune no files. The UI's
  histogram and value suggestions run one `/stats` scan each per query. Measure before stage B ships, and consider
  computing the histogram and the totals in one scan.
- **Contract ahead of code.** The contract specifies fields and operators before they exist. The stage marking and
  the `invalid_request` answer keep that visible to clients.
- **Sample bias.** The K slowest calls over-represent outliers. The output states the sample rule, and the sample
  size is an input.
- **Output size.** A wrong default in the reduction floods a model's context. Golden-file tests pin the size of the
  default output for a large tree.
- **Drift between the CLI and MCP.** Both call the same library functions, and task 3.1 tests that the two surfaces
  cover the same operations.
