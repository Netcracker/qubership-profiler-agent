# Profiler: work plan

Sources: meeting transcript `Profiler-discussions.txt`, working notes `profiler-mom.md`, the actual state of the code in `backend/`.

This document expands the items from `profiler-mom.md` with concrete code references and records the decisions we have taken.

---

## 0. Current state (what surfaced from reading the code)

`backend/` hosts four applications, with **different languages and different readiness levels**:

| App | Language / stack | Build status | Storage |
|---|---|---|---|
| `backend/apps/collector` | Java 21 / Quarkus 3.25 (Maven) | **excluded from `make build-all`**: `APPS := dumps-collector maintenance` in `backend/Makefile` | Postgres + S3 (MinIO) |
| `backend/apps/maintenance` | Go 1.25 | Builds | Postgres + S3 |
| `backend/apps/query` | React 18 + AntD 4.24 + `@netcracker/ux-react` 4.5 + `@netcracker/cse-ui-components` 2.1 | **excluded from `make build-all`** | — (UI) |
| `backend/apps/dumps-collector` | Go 1.25 | Builds | SQLite (`glebarez/sqlite`) + PV |

Important consequence: the meeting claim "all three pieces are in Go" applies to `maintenance` + `dumps-collector` + (a future Go-rewritten) collector. Today the agent's TCP traffic is received by the Java app `collector`; there is no Go replacement yet.

Additional notes:
- `backend/charts/profiler-stack/values.yaml` is the umbrella chart. It expects `INFRA_POSTGRES_*` and `INFRA_S3_MINIO_*` as inputs, so the "as designed" deployment requires Postgres + S3.
- `backend/libs/pg/db.go` describes the schema: temp tables `calls_<ts>`, `traces_<ts>`, `suspend_<ts>` at 5-minute granularity; `dumps_*` at 1-hour granularity with 7-day TTL; inverted index at 1-hour granularity with 14-day TTL. Old partitions are dropped wholesale (see `backend/libs/pg/resources/schema/migration/*`).
- The collector code that writes to Postgres lives in `backend/apps/collector/src/main/java/com/netcracker/persistence/adapters/cloud/` (DAO per entity: calls, traces, dictionary, params, dumps, pod_statistics). The question "what is actually written to Postgres" is answered by: metadata + decoded call records + the raw trace bytes themselves (`traces_<ts>.trace bytea`).
- `dumps-collector` is already on SQLite + PV with no Postgres — a working "all-in-one" reference. Its pattern (`backend/apps/dumps-collector/pkg/client/sqlite`, plus rescan/insert/pack/remove tasks orchestrated via `oklog/run`) is a useful template when collapsing the profiler stack to a single binary.
- The agent (`agent/`, `dumper/`, `runtime/`) is a separate project. It instruments Java applications and pushes a stream over TCP to the collector service (port 1715 in the Helm values).

---

## Approach: contracts-first, sequential replacement, no reanimation of the Postgres path

**We do not reanimate the existing Postgres path as a baseline.** We already know that writing the raw stream from the agent into Postgres is a dead end (Section 0, regarding `traces_<ts>.trace bytea`). Fixing this code only to delete it later is wasted time.

**We do not produce golden output / a walking skeleton.** The new architecture legitimately changes aggregate format and rotation boundaries; bit-exact comparison is meaningless. Validation is via synthetic input fixtures (already present: `backend/tools/load-generator/` and `backend/tools/data-generator/`) plus semantic assertions (top method, p95, number of calls in a range). No binaries committed to the repo.

**Work proceeds sequentially from contracts to code.** First: four contract documents plus architecture diagrams. Then: services implemented one by one, each with an integration test on top of synthetic input.

We **reuse code that survives the architecture change**: Go parser of the agent TCP protocol (`backend/libs/parser/`), parquet writer/reader (`backend/libs/storage/parquet/`), S3 abstraction (`backend/libs/s3/`), UI components (excluding the archived deps). We discard Postgres DAOs, temp-table jobs, and the Postgres read path in query.

---

## Stage 0. Contracts and diagrams (~3 days)

A gating stage: no service code is written until these documents exist.

### 0.1 Write contract
What the collector writes to local RWO PV and to S3.
- Dictionary WAL format: which records (method/param/tag), how they're serialized, when fsync, rotation.
- Trace stream cache: layout of files on local PV, lookup by `(traceFileIndex, bufferOffset, recordIndex)`, refcount and eviction.
- Parquet schema: columns, types, what aggregates, how the call-tree blob is encoded (reuse the format from `backend/libs/storage/parquet/`).
- Directory layout on local PV (`wal/`, `trace/`, `parquet-pending/`) and in S3 (`parquet/<duration-bucket>/<yyyy>/<mm>/<dd>/<hh>/...` or similar).
- File naming: prefix, suffix, uniqueness rules during recovery.
- Flush semantics: what a replica does when a parquet time bucket closes (uploads to S3, deletes locally, updates the metaindex).

### 0.2 Read contract
How query gets data.
- Collector read API endpoints: `/query/calls`, `/query/calltree`, `/query/pods`, `/query/stats` — full description of parameters and response format. JSON or protobuf — to be decided in Stage 0.
- S3 read path: how query knows which parquet files cover a time range; whether a metaindex (manifest file) in S3 is needed or LIST by prefix is sufficient.
- Hot/cold cutoff rules: query reads from collector for `[now - flushInterval, now]`, from S3 for older data; a flag for "absolute freshness" (query both collector and S3 for the overlap, to guarantee completeness).
- PK for deduplication: `(pod_id, restart_time, trace_file_index, buffer_offset, record_index)`. Where it comes from in the new schema, whether it is preserved from the agent protocol.

### 0.3 Lifecycle
- Collector readiness probe: returns Ready only after WAL has been read and dictionaries restored.
- What happens at restart: sequence "start → mount PV → read WAL → restore dicts → reload trace cache → open for agent connections → expose read API → Ready".
- Flush triggers: time (period), size (bytes), memory pressure (% of budget) — explicit thresholds and how they are configured.
- Shutdown: how the collector cleanly closes agent connections, finishes pending uploads, marks Not Ready to be excluded from fan-out.

### 0.4 Storage layout & discovery
- What lives on local PV vs in S3 — closed in 0.1; here it is consolidated into one diagram.
- k8s manifests: StatefulSet for collector with `volumeClaimTemplates` (RWO), Headless Service (`clusterIP: None`), Deployment for query (no PV), Deployment/CronJob for maintenance. Explicit resource limits.
- Helm chart: how `backend/charts/profiler-stack/values.yaml` changes. Target configuration is "S3 only (or filesystem emulator)", no Postgres. Postgres fields are removed from values.
- Env vars: `COLLECTOR_HEADLESS_SVC`, `S3_ENDPOINT`/`S3_BUCKET`, `POD_NAMESPACE`.

### 0.5 Diagrams
- Data flow: agent → collector (stream demultiplexing + WAL + trace cache) → parquet local → S3; query → collector fan-out + S3 → merge.
- Deployment: pods, PVs, Services, network boundaries.
- State diagram: lifecycle of a root call (received as a `Call` record → trace bytes resolved from cache → written to current parquet → flushed to S3).
- Committed as `.md` with Mermaid blocks, alongside the contracts (in `backend/docs/design/`).

### 0.6 What stays open after Stage 0
If something cannot be closed in design — list it explicitly and schedule it as a Stage 1 early spike, not as guesswork during coding.

---

## Stage 1. New collector: write path (~2–3 weeks)

Goal: collector receives the stream from the agent, demultiplexes it, persists it via WAL + trace cache, writes parquet to local PV. Nothing about read, nothing about S3.

### 1.1 Reused without changes
- Agent protocol parser: `backend/libs/parser/parser.go` (Go). The `Listener` interface (`backend/libs/parser/listener.go`) already exposes the right surface (`RegisterPod`, `RegisterStream`, `AppendData`); it currently consumes from a file (TCP dump replay), but the same primitives work over a live TCP connection. We add the response path (the parser already accepts `wr io.Writer` for it) and wrap in a TCP server.
- Parquet writer in `backend/libs/storage/parquet/`.
- S3 abstraction in `backend/libs/s3/` (used in Stage 2).

### 1.2 New code
- **TCP listener:** accepts agent connections, hands the per-connection reader to the protocol parser. Sticky-session-friendly (one connection ↔ one agent).
- **Stream router:** dispatches incoming chunks to per-stream handlers (dictionary, params, suspend, calls, trace, sql, xml).
- **Dictionary WAL:** every new dictionary entry is appended to a WAL file on PV with periodic fsync. Length-prefixed binary format, no parquet.
- **Trace stream cache:** raw trace bytes are written to per-pod-restart files on local PV; an in-memory index tracks `(traceFileIndex, bufferOffset) → (file, offset)` so a Call lookup is O(1). Disk-budget driven eviction; idle-timeout drop for never-claimed bytes. Refcount per file decremented when a referencing Call's parquet is flushed.
- **Parquet writer per (time bucket × duration bucket):** when a Call record arrives, the writer pulls trace bytes from the cache, resolves dictionary words, emits one row.
- **Restart recovery:** on startup the WAL is replayed, dictionaries are restored, and the trace cache index is rebuilt from existing files; pending parquet files trigger the upload retry path.

### 1.3 Integration test
- `load-generator` → in-process collector (no network, direct call) → check parquet files on PV.
- Semantic assertions: "for profile X with N methods, the top method is M with duration T±ε".
- Restart test: kill the process mid-processing → restart → check that WAL + trace cache are restored and the final parquet is correct.

### 1.4 Out of scope here
- S3 upload — Stage 2.
- Read API — Stage 3.
- Helm/StatefulSet — Stage 6.
- Retention / maintenance — Stage 2.

---

## Stage 2. Maintenance: S3 flush and retention (~1–2 weeks)

Goal: parquet files leave the local PV for S3; retention policies apply per duration bucket.

### 2.1 S3 upload
- Uploader (in collector or as a sidecar/CronJob): on parquet close, uploads the file to S3 using the layout from Stage 0; deletes the local copy on success.
- Reuses `backend/libs/s3/` (filesystem emulator in dev, MinIO in prod).

### 2.2 Retention
- In maintenance (new code, not the legacy `temp_db_*`): S3 walk that deletes parquet files older than the per-bucket TTL. Example policy: `duration < 100ms` — 1 day; `100ms–1s` — 7 days; `> 1s` — 30 days.
- Reuses logic from `backend/apps/maintenance/pkg/maintenance/s3_file_remove_job.go` but with the new per-bucket policy.

### 2.3 Test
- Upload a known set of parquet files into a test S3 bucket with various ages → run maintenance → verify the surviving set matches the policy.

---

## Stage 3. Collector read API (~1–2 weeks)

Goal: collector exposes an HTTP endpoint that returns aggregates for the hot window (in-memory + trace cache).

### 3.1 Endpoints
- From Stage 0.2 (Read contract): `/query/calls`, `/query/calltree`, `/query/pods`, `/query/stats`.
- Reads **only** local state. Nothing from S3.
- Reference implementation: `backend/apps/dumps-collector/pkg/server/http_server.go` (HTTP endpoint over local SQLite + PV).

### 3.2 Test
- `load-generator` → collector → wait `flush period / 2` → request the read API → verify the recent calls are visible.
- Integration with Stages 1/2: when data has been flushed to parquet (local) / S3, it disappears from the read API (this is expected — that is the hot cutoff).

---

## Stage 4. Query: fan-out + merge (~2–3 weeks)

Goal: a stateless service that serves user HTTP requests. Fan-out to collector replicas + read S3 + dedupe.

### 4.1 Discovery
- Reads env `COLLECTOR_HEADLESS_SVC`; uses `net.LookupHost` to get IPs of all Ready replicas. In dev — one replica; in prod — N.
- Re-resolves DNS on every request (Go stdlib does not cache at process level; OS cache ≤ CoreDNS TTL).

### 4.2 Fan-out
- Parallel HTTP requests to all collector replicas for the hot window. Per-replica timeout. Partial result with an explicit marker if a replica fails.

### 4.3 S3 read
- Read parquet from S3 for the cold window. Use readers from `backend/libs/storage/parquet/`. A manifest file may be needed — to be decided in Stage 0.2.

### 4.4 Merge & dedup
- Merge hot + cold; dedupe by the PK from Stage 0.2. Sort by time.

### 4.5 Test
- `load-generator` → N collector replicas → wait for partial S3 flush → request query for a range that spans hot+cold → verify no gaps and no duplicates.

---

## Stage 5. UI (~2–3 weeks)

Goal: query UI runs on top of the new API and builds without the archived npm registry.

### 5.1 Dependency migration
- Only now we replace `@netcracker/ux-react`, `@netcracker/cse-ui-components`, `@netcracker/ux-assets` with plain AntD 4.24 (already in dependencies).
- By feature in `backend/apps/query/src/features/cdt/`: start with `calls` + `sidebar` (the main scenario), then `pods-info`, `controls`, `heap-dumps`.
- Brand palette via AntD theme customization, not via component forks.

### 5.2 Wiring to the new query API
- Update the API client in query to the endpoints from Stage 0.2.
- Remove all requests that previously went to the old Postgres-backed query backend.

### 5.3 New designs from Figma
- Export the Figma designs we already have (calls table with tighter padding, mockups of other views).
- Hook the Figma MCP integration into Claude to speed up layout work.

### 5.4 CallTree — backlogged
- `backend/apps/query/src/features/cdt/calls-tree/` is jQuery-style. A separate large epic for later.

---

## Stage 6. Deployment: StatefulSet + Headless Service + Helm (~1–2 weeks)

Goal: a chart that deploys everything in k8s.

### 6.1 Manifests
- StatefulSet for collector with `volumeClaimTemplates` (RWO), readiness probe, env `COLLECTOR_HEADLESS_SVC`.
- Headless Service (`clusterIP: None`) co-located with the StatefulSet.
- Deployment for query, no PV.
- Deployment/CronJob for maintenance.
- MinIO — either an external dependency wired via values, or installed alongside.

### 6.2 Helm
- Update `backend/charts/profiler-stack/values.yaml`: drop `INFRA_POSTGRES_*`, reconfigure for the new schema.
- Keep S3 only (and S3 is optional if dev uses the filesystem emulator).

### 6.3 Readiness & HPA
- Collector readiness returns Ready only after WAL replay + trace cache index rebuild.
- HPA on collector by CPU/memory (caveat: sticky TCP means new replicas do not take load immediately; the first iteration can ship without HPA).

---

## Cross-cutting concerns

Work that runs in parallel with the main stages and does not fit any single one.

### C1. Agent-side filtering
- The agent already drops single calls below a threshold.
- New capability: don't write a whole call-tree if the root took less than a threshold. Vladimir noted that with per-duration retention policies (Stage 2.2), agent-side filtering may not be needed — short traces evict themselves quickly. Do this only if agent → collector network traffic becomes a bottleneck.

### C2. Runtime configuration of the agent
- Change filters/thresholds/enable-disable on the fly via a central endpoint, pushing into the agent over the existing TCP channel.
- Extend the protocol contract (currently in `backend/apps/collector/src/main/java/com/netcracker/common/ProtocolConst.java`) and the corresponding Go-side handling.
- Independent of Stages 1–6.

### C3. MCP / skills
- **Skill for downloading diagnostic dumps** via `dumps-collector` (endpoints already exist in `backend/apps/dumps-collector/pkg/server/http_server.go`). Quick win, can run in parallel with Stages 0–1.
- **MCP for profiler data** — on top of the query API from Stage 0.2. After Stage 4.
- Pankratov: "a good machine-readable API".

### C4. Metrics / Grafana
- Maintenance computes aggregates (top-N slowest methods, p95 latency, counters by tag) and exports Prometheus metrics.
- Infrastructure ready: `backend/libs/metrics/`.
- A Grafana dashboard on top of these metrics is an alternative to a separate analytics screen in the UI. Less code, slots into the existing stack.

### C5. Dumps-collector + heap dumps via S3
- Today dumps-collector reads from RWX PV. Switching to "agent → collector over TCP → S3 → dumps-collector reads from S3" removes the RWX requirement. Tackle this only when we get to it; for now `dumps-collector` runs independently and works.

---

## Decisions taken

1. **Contracts-first, no reanimation of the Postgres path.** No walking skeleton, no golden output. First contracts and diagrams (Stage 0), then services one by one with integration tests.
2. **The new collector is written in Go; the Java collector is legacy and removed after Stage 4.** Initially we considered keeping Java for reuse of `ProfilerAgentReader.java`, but the agent protocol parser already exists in Go (`backend/libs/parser/parser.go`); parquet writer and S3 lib are also in Go. Going Go avoids a long-term bilingual codebase and aligns with the team's "single Go binary" preference.
3. **Raw stream into Postgres is never written in production.** Architecture: stream demultiplexing on the collector + dictionary WAL + trace stream cache on local PV + parquet to S3. No RDBMS in the hot path.
4. **Two UIs (profiler and dumps-collector) is acceptable.** Unification is not a priority.
5. **RWX PV is not required.** State separation: shared multi-reader → S3 (object storage, not RWX); per-replica hot state → RWO PV via StatefulSet `volumeClaimTemplates`; query → stateless without PV. Discovery via Headless Service + DNS.
6. **Query reads from two tiers (hot collector + cold S3).** Fan-out + merge + dedup by PK. Hot cutoff = flush interval.
7. **Migrating `@netcracker/*` → AntD only in Stage 5.** Until then we don't touch the UI.

---

## Appendix A. "Idea from the discussion → code" map

| Idea from the meeting | Files / directories |
|---|---|
| Collector receives an agent stream over TCP | `backend/apps/collector/src/main/java/com/netcracker/cdt/collector/tcp/ProfilerAgentReader.java`, `.../CollectorOrchestratorThread.java` (Java, legacy); `backend/libs/parser/parser.go` (Go, future) |
| Collector writes "raw" Call-3 + metadata into Postgres | `backend/apps/collector/src/main/java/com/netcracker/persistence/adapters/cloud/` |
| 5-minute temp tables dropped wholesale | `backend/libs/pg/resources/schema/{calls,traces,suspend}_tables_template.gosql`, `backend/libs/pg/db.go` (`Granularity`, `TempTableLifetime`) |
| Maintenance job: aggregation → parquet | `backend/apps/maintenance/pkg/maintenance/maintenance_job.go` and neighbors |
| Different duration ranges in parquet | `backend/libs/storage/parquet/file_map_calls.go` |
| Dumps-collector as the all-in-one-on-SQLite reference | `backend/apps/dumps-collector/cmd/run.go`, `pkg/client/sqlite/`, `pkg/server/http_server.go` |
| UI on AntD + `@netcracker/ux-react` | `backend/apps/query/package.json`, `backend/apps/query/src/features/cdt/` |
| Helm stack requiring Postgres + S3 | `backend/charts/profiler-stack/values.yaml` |
| `collector`/`query` excluded from the build | `backend/Makefile` — `APPS := dumps-collector maintenance` |

