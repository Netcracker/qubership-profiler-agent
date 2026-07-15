# 08 — UI-driven backend requirements (Stage 5)

The Stage 5 UI ([`07-ui-design.md`](07-ui-design.md)) needs richer data than the current `query` contract
returns. This document lists those requirements, grounded in a code inventory of what the agent sends and
what storage keeps, and classifies each by effort. The API source of truth stays
[`02-read-contract.md`](02-read-contract.md); the items here are additive deltas against it and against the
write path ([`01-write-contract.md`](01-write-contract.md)).

The headline finding: most of the "missing" data is not missing. The agent sends it and parquet stores it —
the read path simply does not project it. Those are cheap wins. The genuinely new work is on the tree.

## 1. Evidence base

- **Per-call.** The agent sends, and `CallV2` parquet stores, every column the old calls table showed. The
  list-path projection `CallV2Projected` (`backend/libs/storage/parquet/callv2.go:75`) already reads
  `queue_wait_ms`, `suspend_ms`, `transactions`, `file_read`, `file_written`, `net_read`, `net_written`,
  `logs_generated`, and `logs_written` — but `CallJSON` (`backend/libs/query/model/wire.go:19`) exposes only
  `duration_ms`, `cpu_time_ms`, `wait_time_ms`, `memory_used`, `child_calls`, `error_flag`.
- **Tree.** The `/tree` `Node` is a raw call tree: `{methodIdx, enterMsRel, durationMs, params, children}`. The
  old UI renders a *merged* node with self and total duration, self and total suspension, execution and
  child-execution counts, first and last invocation offsets, and per-method source location. The old node
  model is `profiler-ui/src/profiler.mjs` `M_*` (M_DURATION, M_SELF_DURATION, M_SUSPENSION,
  M_SELF_SUSPENSION, M_EXECUTIONS, M_CHILD_EXECUTIONS, M_START_TIME, M_END_TIME, M_TAGS).
- **Storage.** Parquet is bucketed by start time (5 min) and by retention class, and the retention class is a
  duration band (`short_clean < 100 ms`, `normal_clean 100–1000 ms`, `long_clean ≥ 1000 ms`, `any_error`).
  Within a file, rows are sorted `(ts_ms DESC, pk ASC)` — not by duration. Namespace and service are parquet
  columns (`namespace`, `service_name`, `pod_name`, `pod_id`) and also ride in the pod-restart manifests.

## 2. Effort classes

- **Class A — projection.** The data is already read on the list path; expose it. Additive JSON fields,
  backward-compatible.
- **Class B — compute-on-read.** The data is available upstream; the backend must compute or aggregate it at
  tree build.
- **Class C — new capability or storage.** Needs new backend work, a storage-layout change, or agent changes.

## 3. `/calls` requirements

| ID | Class | Requirement | Evidence / mechanism |
|----|-------|-------------|----------------------|
| R1 | A | Expose the dropped call columns in `CallJSON`: `suspend_ms`, `queue_wait_ms`, `transactions`, `file_read`, `file_written`, `net_read`, `net_written`, `logs_generated`, `logs_written`. | `CallV2Projected` already reads them; add them to `CallRow` / `CallJSON` and to the hot SQLite index projection so both tiers agree. Additive JSON fields. |
| R2 | C | `order=duration_desc` on `/calls` for "slowest calls" (goal #3). | Prune to `long_clean` + error classes (retention class is a duration band), k-way merge, sort by `duration_ms`, bound by the wide-query guard. No duration index today, so this scans and sorts within the pruned classes. |
| R3 | C (deferred) | Richer query — `$param=value` and `+`/`-` boolean terms, as the old search box had. | `/calls` `method` is a substring match only today. Needs a parameter index. |
| R4 | C (deferred) | `sql_count` / `rpc_count` columns for "most SQL/RPC" (goal #3). | Not captured by the agent today (`transactions` is the nearest: DB-access count). Derive from tree params, or add agent fields. |

`trace_id` / `span_id` were columns in the old calls model (`C_TRACE_ID`, `C_SPAN_ID`). Confirm whether the
agent still sends them and whether `CallV2` keeps them; expose in `CallJSON` if so.

## 4. `/tree` requirements

| ID | Class | Requirement | Evidence / mechanism |
|----|-------|-------------|----------------------|
| R5 | B | Return a **merged** tree, not a raw one, server-side in `calltree.Build`. Merge sibling invocations of the same method under a parent into one node carrying `selfExecutions` and total `executions` (self + children). | A raw tree is unbounded — a 1M-iteration loop is 1M nodes (the user-guide notes 1–10M calls per request). The old UI always merged. This gates both payload size and render viability. |
| R6 | B | Per-node self and total duration (`selfDurationMs`, `durationMs`). | Self-time is `durationMs − Σ children.durationMs`; the backend computes it during the merge. |
| R7 | B | Per-node self and total suspension (`selfSuspensionMs`, `suspensionMs`). | The agent records suspension as a global pod-restart stop-the-world timeline, not per method. The tree builder attributes it by intersecting each node's `[enter, exit]` work interval with the timeline, at build time (`calltree.Build`). **Data-path gap:** `calltree.Build` today takes only `(blob, recordIndex, Options)` — designing the suspend-timeline input and its hot/cold retrieval is part of R7 and precedes 5.2. |
| R11 | B | Aggregate node params into a mini-tree at merge. Group high-cardinality values (SQL, binds) by a normalised signature, keep the top-N groups by time with per-group `durationMs` / `executions`, bucket the rest into `::other`, and nest binds under their SQL. | A single node can hold thousands of SQL texts; shipping them raw defeats the merge. The old UI capped at top-256 + `::other` and grouped similar SQL by stripping string literals and digits (`profiler-ui/src/profiler.mjs:3469`). The exact contract — group key, attribution, the 256-group cap and eviction, bind nesting, and the deviations from the Java `parsers/` `Hotspot` aggregation — is formalised in `02-read-contract.md` §2.5.3. |

First/last invocation offsets are not needed — dropped from the node model.

Target node model (additive, int-keyed on the existing `Node` map, so old readers ignore new keys), uniform
`self* / total` across every metric: `selfDurationMs` / `durationMs`, `selfSuspensionMs` / `suspensionMs`,
`selfExecutions` / `executions`. `Param` is richer than today's `{paramIdx, values[]}`: each param carries
aggregated groups (`{ value, durationMs, executions }`), an `::other` bucket, and nested children (binds under
a SQL) — the mini-tree of R11. Node category stays client-side — it is a user-assigned label (Setup
categories), not a backend field. Method source `file:line` and `jar` need no wire change: they are already
encoded in the `methods[]` word (see §7).

## 5. Collapse / skip-degenerate-chains (client, node-model dependent)

Expanding a node must **skip pass-through chains** and land on the next node that spends time or branches. A
deep stack where each level passes ~100% of its time to a single child must expand in one click, not one
click per level — otherwise a typical deep stack is unusable.

- **Source of truth:** `profiler-ui/src/profiler.mjs` `sortNode` (line 6186), stored in `M_COLLAPSE_LEVELS`.
- **Heuristic (top-down, by duration):** collapse a node into its dominant first child when the part of the
  node's duration *not* explained by that child is ≤ 10% of the node's duration, the execution counts are
  consistent (not a fan-out), and the node carries no params/tags. Levels accumulate down the chain, so a
  long pass-through chain collapses to one expansion. This is a simplification: `sortNode` also uses several
  negative collapse states, a self-duration mode for bottom-up, tag presence, and execution fan-out — port the
  exact logic and its behaviour tests, and document any deviations.
- **Initial cutoff:** hide children below 10% of the parent's duration or 10% of the parent's calls.

This is a pure client-side transform, but it reads per-node self-duration and execution counts, so it depends
on R5 and R6. It is called out here because the node-model requirement is the reason it belongs to Stage 5.

## 6. Storage requirements

| ID | Class | Requirement | Note |
|----|-------|-------------|------|
| R9 | C (optional) | Duration-efficient top-N: a duration-sorted secondary layout, or a per-bucket top-N-by-duration manifest for `long_clean`. | Only if R2's scan-and-sort over `long_clean` proves too slow on a real cluster. Measure first. |
| R10 | C (optional at scale) | A daily namespace/service index manifest (`pods/v1/<yyyy>/<mm>/<dd>/index.json`) to avoid per-pod enumeration for the discovery tree. | Not needed for MVP correctness; only for clusters beyond a few hundred pods. |

## 7. Not needed

- **Namespace/service store change.** The data exists (parquet columns and pod manifests); `/pods` already
  serves the identity tuples, and the UI groups them into the tree client-side.
- **Per-node CPU.** The old tree never showed per-node CPU — only per-call `cpu_time_ms`. No requirement.
- **Method source `file:line` / `jar` fields.** The `methods[]` word is already the full agent string —
  `<returnType> <package.Class.method>(<args>) (<File>.java:<line>) [<jarPath>/<jarName>]` — and
  `calltree.go` interns it unmodified. The client parses source and jar from it; port the logic from
  `backend/libs/parser/dictionary/line_parser.go`. No wire change.
- **First/last invocation offsets.** Dropped from the node model.
- **Trace/span dedicated columns.** The tracing context is captured as params (`brave.trace_id`,
  `brave.span_id`, `x-request-id`; see `backend/libs/parser/dictionary/line_parser.go`) and already flows
  through `params` on both `/calls` and `/tree`. The UI reads it from there; no dedicated column is required.
  The correlation provision is §9.

## 8. Sequencing against Stage 5

- **Class A (R1)** lands with step 5.1 (calls). Cheap, unblocks the columns. The hover panel's source/jar
  need no backend change (§7).
- **Class B (R5–R7)** land with steps 5.2–5.3. R5 (merge) gates a usable tree and comes first.
- **Class C** is deferred or as-needed. R2 (`order=duration_desc`) is the first Class-C item, since it turns
  goal #3 "slowest calls" from a threshold approximation into a real ranking.

## 9. Decisions and the correlation provision

- **Merge location (R5): server-side**, in `calltree.Build`. Keeps `/tree` compact and matches the old
  semantics. The client computations (hotspots, local hotspots, find usages, adjust duration, categories,
  outgoing/incoming) run over this merged tree, client-side — the server sends the model once, the client
  transforms it.
- **Per-node suspension (R7): backend-computed** by intersecting each node's work interval with the global
  suspend timeline at tree build — not per-method agent deltas.
- **Correlation and external links (provisioned, deferred).** The profiler captures tracing context as
  params, so `trace_id` / `span_id` are already on calls and tree nodes (`brave.trace_id`, `brave.span_id`,
  `x-request-id`). Provision a UI link-template seam — a configurable map from a param to an external URL,
  interpolating the id plus pod, namespace, and time window — so a call or node deep-links out to a trace in
  Tempo or VictoriaTraces and to logs in VictoriaLogs / Grafana. Implementing the wiring is deferred;
  surfacing the params and the link seam now avoids a later rework. Inbound navigation (trace/log → profile)
  needs the param-filter query (R3) to find calls by `trace_id`.

## 10. Deferred: dump analysis and the `parsers/` module

The old UI's "Analyze dump" turned several offline artifacts into the same tree — thread dumps, stackcollapse,
Oracle DBMS_HPROF, and JFR (`parsers/src/main/java/com/netcracker/profiler/fetch/`). Stage 5 defers all of it;
the disposition is recorded here so it is not lost.

- **`parsers/` is not superseded wholesale.** The Go rewrite covers only the live path (agent protocol →
  parquet → `calltree`). The offline analyzers and the Excel export live only in `parsers/` (Java). Remove the
  live-path parts once Go proves out; keep the rest until ported.
- **Simple analyzers → Go, later.** Thread dump and stackcollapse are text; port them into the single binary
  and the CLI, reusing the `calltree` merge (thread-dump aggregation is the same merge as R5). DBMS_HPROF is a
  text format close to the agent trace (method enter/exit), so it reuses the trace → `calltree` path rather
  than a new parser.
- **JFR stays Java.** JFR is a complex JVM-native binary format; a Go port is a large, low-value
  reimplementation. Keep it as an optional Java tool that emits the canonical tree.
- **Input modes — no arbitrary path or URL in the deployed server.** CLI / local run reads a local path
  (trusted context); the deployed UI uploads a file or references a dump already in our storage by id
  (`dumps-collector` writes thread/heap/GC/top dumps to S3). An arbitrary URL or server-local path is the
  arbitrary-read risk to avoid.
- All modes feed the canonical `/tree` contract, so one tree UI renders live calls and every analysed dump.
