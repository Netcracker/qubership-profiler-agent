# Deferred design ideas

Design-level ideas that surfaced during Stage 0 (contracts) but are intentionally out of scope for the initial implementation. Each entry should explain WHAT, WHY deferred, and the trigger that would bring it back into scope.

## `cutoff=strict` query parameter

**What.** A `?cutoff=strict` flag on `/api/v1/calls` that disables the hot/cold overlap window: hot would be queried for `(now - flush_interval, now]` and cold for `[from, now - flush_interval]`, with no overlap and no deduplication. Reduces query CPU at the cost of a brief (~seconds) window where a Call flushed exactly at the boundary may be temporarily missing.

**Why deferred.** No identified MVP consumer that runs queries frequently enough for the dedup CPU to matter, while also being tolerant of momentary gaps. The two profiles that would benefit — dashboards refreshing every 10 s, alerting rules — do not yet exist as integrations.

**Trigger to revisit.** First concrete consumer that profiles `query` and identifies the dedup pass as a bottleneck under sustained refresh load.

**Implementation note.** The hot/cold model in `02-read-contract.md` §4 already separates hot retention from flush interval, so adding the strict variant is a localized change in `query`'s range planner. No data-model impact.

## Tree endpoint in alternate formats (Protobuf / JSON)

**What.** Alongside the MVP MessagePack encoding (`02-read-contract.md` §2.5), expose the same data under additional `?format=…` variants — typically `?format=proto` for partners who want a formal schema and `?format=json` for human debugging via curl.

**Why deferred.** MVP picks MessagePack + int-keyed maps (single in-team API surface). Multiple encodings carry a multiplicative testing burden, and the int-keyed map shape already maps 1:1 to protobuf field tags — a future migration is mechanical.

**Trigger to revisit.** External / third-party integration that requires a formal `.proto` schema (e.g. partner SDK distribution), or operational need to debug wire format via curl in production.

## Wide-query fail-soft scan budget

**What.** A per-request cap on bytes scanned or wall-clock spent inside `/calls` execution. On breach, `query` stops and returns the rows gathered so far with `partial: true` and `partial_reasons: [budget_exceeded]` — a `200`, not the `400` of the wide-query guard (`02-read-contract.md` §2.3.2). Backstops the guard's pre-flight estimate, which is by file size and so overshoots a projection-only read and cannot see a pathological row distribution.

**Why deferred.** The two-layer guard (§2.3.2) already rejects the queries that threaten the SLO before they run. The backstop only catches the residue the size estimate misjudges, which needs profiling at scale to size. The `budget_exceeded` reason is reserved in the contract now so the `partial_reasons` vocabulary stays stable.

**Trigger to revisit.** Query profiling at target scale shows accepted queries whose actual scan overruns the estimate, or a projection-heavy workload where file-size estimates are systematically too conservative.

**Trigger fired (2026-07-16, load campaign).** Concurrent guard-passing wide queries OOM-killed a 3 GiB query pod in 34 seconds (`load-testing-report.md` §7) — the per-request backstop cannot bound concurrent decoded state, so the need is promoted past this entry: a global read-path memory budget with admission control is the P1 item in `load-testing-backlog.md`. The `budget_exceeded` reservation stands; the per-request backstop remains a useful complement, not the fix.

**Outcome (2026-07-21).** The promoted need is implemented: the process-wide read memory budget with admission
control (`02-read-contract.md` §7.5) bounds concurrent materialized state and sheds load with an atomic `503`.
This entry's per-request fail-soft backstop stays deferred with the original trigger wording — it would catch a
single accepted query whose actual scan overruns the estimate, which the global budget only bounds process-wide —
and the `partial_reasons: [budget_exceeded]` vocabulary remains reserved for it (§7.5 explains why the budget
itself must not emit a partial page).

## Versioned CallV2 reader for non-additive schema changes

**What.** A cold reader that branches on the `profiler.schema_version` key in the parquet footer metadata (`01-write-contract.md` §5.2) and reads each file with the shape its version names. Needed the first time a `CallV2` column is renamed, retyped, or semantically redefined after release, while old and new files coexist inside the 30-day retention window.

**Why deferred.** The parquet reader matches columns by NAME, so additive changes and column removals are already backward-readable with the single current struct — a missing column null-fills. Every sealed file carries the version stamp from day one, so the branching reader can be added exactly when the first non-additive change lands, with no data migration.

**Trigger to revisit.** The first post-release `CallV2` change that renames a column, changes a column's type, or reinterprets stored values.

## Wire-protocol ack windowing / pipelining

**What.** Window or pipeline the agent↔collector acknowledgement flow (`06-wire-protocol-server.md` §5) so throughput no longer degrades with 1/RTT: larger effective socket windows, asynchronous ack draining, or batched acks across streams.

**Why deferred.** Agents and collectors co-locate in one cluster, where the measured ceiling is irrelevant. The change touches both sides of the wire contract (agent and collector), forfeiting the campaign's "no agent changes" invariant. The load campaign measured the cost of leaving it alone: 2 s of path RTT collapses ingest ~40× without breaking sessions (`load-testing-report.md` §9, `runs/20260717T235336Z-t7-agent-net`); the co-location assumption is now documented in `06-wire-protocol-server.md` §5.

**Trigger to revisit.** WAN-separated agents (multi-region, edge, or cross-cluster profiling) become a target deployment.

## Per-pod-key reconnect rate limit and tracked-pod-restart cap

**What.** Two of the T5 protection candidates (`load-testing-plan.md` §7.5.4): rate-limiting reconnects per pod key at accept, and capping the number of tracked pod-restarts a collector keeps.

**Why deferred.** Decided from the storm numbers (`load-testing-report.md` §8): the collector absorbs ~42 restarts/min at negligible CPU/RAM (`runs/20260717T133845Z-t5-reconnect-storm`), so shedding agent data to protect a purge queue is the wrong trade — the damage is purge bookkeeping, which the near-empty purge fast-path (`load-testing-backlog.md`) bounds at the source. A cap without faster purge only drops observability.

**Trigger to revisit.** Cluster-scale T3 or storm numbers showing pressure on the collector itself — the accept path or RAM — rather than on purge bookkeeping.

## `confirm_wide` / async wide-query override

**What.** An explicit escape hatch — a `confirm_wide=true` parameter, or an async job that returns a handle to poll — that runs a query the wide-query guard (`02-read-contract.md` §2.3.2) would reject, for a caller that deliberately wants the expensive scan.

**Why deferred.** No MVP consumer needs a full-cluster wide scan; interactive UI and automation both narrow by pod, class, or duration. Adding a mode switch speculatively repeats the `cutoff=strict` mistake, retired for the same reason.

**Trigger to revisit.** A concrete consumer — a batch export, a cluster-wide audit — that must scan wide and can tolerate seconds-to-minutes latency.
