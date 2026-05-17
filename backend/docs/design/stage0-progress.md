# Stage 0 progress

Stage 0 is the contract-and-diagrams phase. No service code is written until these documents are reviewed and merged.

## Status

- [x] **01 — Write contract** (`01-write-contract.md`) — first draft, awaiting review
  - [x] Dictionary WAL format
  - [x] Trace stream cache (replaced "spill SQLite" — see review notes; SQLite not needed at this layer)
  - [x] Parquet schema (call rows) — drafted, see Section 5.3 Q-A
  - [x] Local PV directory layout
  - [x] S3 object key layout
  - [x] Flush semantics (time / size / memory pressure)
  - [ ] **Review pending:** verifications V1–V6 about agent behavior need confirmation from someone familiar with the agent code (`agent/`, `dumper/`, `runtime/`)
  - [ ] **Decision pending:** `trace_id` shape — single string or three integers (Section 5.3 Q-A)
- [ ] **02 — Read contract** (`02-read-contract.md`) — not started
  - [ ] External query API endpoints (preserve `/cdt/v2/calls/...` shape from Java collector)
  - [ ] Collector hot-read API endpoints
  - [ ] Hot/cold cutoff and deduplication rules
  - [ ] S3 LIST-based discovery semantics and time-range mapping
- [ ] **03 — Lifecycle** (`03-lifecycle.md`) — not started
  - [ ] Readiness probe state machine
  - [ ] Recovery sequence on restart (mount PV → read WAL → restore dicts → load spill)
  - [ ] Flush trigger configuration
  - [ ] Shutdown sequence (drain agents, flush spill, mark unready)
- [ ] **04 — Storage layout** (`04-storage-layout.md`) — not started
  - [ ] StatefulSet manifest sketch with `volumeClaimTemplates`
  - [ ] Headless Service manifest
  - [ ] Query Deployment manifest
  - [ ] Helm values diff against current `profiler-stack`
  - [ ] Env vars
- [ ] **05 — Diagrams** (`05-diagrams.md`) — not started
  - [ ] Data flow (agent → collector → S3 / query)
  - [ ] Deployment topology (pods, PVs, services, networking)
  - [ ] Call lifecycle state diagram

## Decisions log

Append-only log of decisions taken during Stage 0. Each entry has a date, the question, the choice, and the reason.

### 2026-04-23 — language for the new collector

**Question:** Java/Kotlin or Go for the new write-path code?

**Choice:** Go.

**Reason:** The agent protocol parser (`libs/parser/`), parquet writer (`libs/storage/parquet/`), and S3 abstraction (`libs/s3/`) are already implemented in Go. `dumps-collector` (Go) is a working template for the runtime shape (`oklog/run` + cobra + SQLite + HTTP). The team prefers a single Go binary in the VictoriaMetrics style. Going with Java would mean a long-term bilingual codebase for marginal short-term savings.

**Consequence:** The existing Java collector (`backend/apps/collector/`) becomes legacy. It is not modified during Stage 1; once the new collector is in place (after Stage 4), it is deprecated and removed.

### 2026-04-23 — parquet schema is up for redesign

**Question:** Reuse `CallParquet` from `backend/libs/storage/parquet/calls.go` verbatim?

**Choice:** No. Use it as a reference, but redesign with explicit justification per column.

**Reason:** No design documentation exists for the current schema. There is no migration requirement (no production data to preserve) and no external client locked to it.

**Consequence:** Stage 0 produces a new schema in `01-write-contract.md` with a per-column rationale. To be reviewed before Stage 1 begins.

### 2026-04-23 — external query API shape preserved

**Question:** Keep the existing `/cdt/v2/calls/...` endpoints from the Java collector or design fresh?

**Choice:** Preserve. Minor cleanup only if obvious mistakes surface during Stage 4.

**Reason:** The frontend (`backend/apps/query/`) is already wired to these endpoints. Rewriting the API forces a parallel UI rewrite, which contradicts the decision to defer UI migration to Stage 5.

**Consequence:** `02-read-contract.md` documents the existing endpoints as the contract. The new Go query service implements the same shape on top of the new backend.

### 2026-04-23 — JSON for inter-service traffic

**Question:** JSON or protobuf between query and collector replicas?

**Choice:** JSON.

**Reason:** Easy to debug with curl, no schema-compiler in the build, expected payload sizes (calls list responses) are well within JSON's comfortable range. Switch later if profiling shows it as a bottleneck.

### 2026-04-23 — S3 discovery via LIST, no manifest yet

**Question:** Maintain a manifest file in S3 for time-range → parquet-file lookups?

**Choice:** Start with LIST by prefix. Add manifest later if LIST becomes slow.

**Reason:** Avoid premature complexity. The S3 layout is designed to make LIST efficient via time-bucketed prefixes (see `01-write-contract.md`).

### 2026-04-23 — MinIO in docker-compose for dev

**Question:** Run a real S3 (MinIO) in dev or build a filesystem-backed emulator behind `libs/s3/`?

**Choice:** MinIO in docker-compose.

**Reason:** Closer to production behavior. Avoids divergence between dev and prod. The filesystem emulator stays only for unit tests where a docker dependency is unwanted.

### 2026-04-23 — spill triggers: memory pressure primary, idle secondary

**Question:** When does an unfinished call get evicted from memory to spill SQLite?

**Choice:** Primary — memory pressure (total in-flight aggregate size > budget); secondary — per-call idle timeout (no events for N seconds).

**Reason:** A long-running call doesn't need to be evicted while we have memory headroom. Eviction is forced only when memory budget is at risk. Idle timeout is a safety net for stuck/abandoned calls.

### 2026-04-23 — no in-memory call assembly, no spill SQLite

**Question:** Where does call assembly (composing a complete call from method-enter/exit events) happen?

**Choice:** It happens in the agent. The collector demultiplexes streams and writes parquet rows on `Call` arrival.

**Reason:** Reading `backend/libs/protocol/streams.go`, `data/calls.go`, `parser/parser.go` revealed that the agent already sends a `Call` record on the `calls` stream when a root call closes, with metric summaries and a back-reference into the `trace` stream. The collector never holds a partial call — at most it holds recent trace bytes that haven't yet been claimed by an arriving Call record.

**Consequence:**
- The "spill to SQLite" mechanism from earlier discussions is replaced by a simpler **trace stream cache on local PV** (Section 4 of `01-write-contract.md`). When a Call arrives, the collector seeks into the cached trace file at `(file_index, buffer_offset)` and embeds the bytes in the parquet row.
- "Memory pressure" eviction operates on the disk-cache budget, not on in-memory aggregates.
- The earlier mental model of "in-memory aggregator with spill" was wrong. Documenting here so future readers don't get pulled into it again.

### 2026-04-23 — agent-side behavior verifications block Stage 1

**Question:** Do we know the agent's wire behavior in detail?

**Choice:** Six items (V1–V6 in `01-write-contract.md` Section 1) are explicitly marked as needing verification before Stage 1 starts.

**Reason:** The author has not yet read the agent code. Plausible defaults are documented; if any of V1–V6 turns out to be wrong, parts of the write contract need rework. Better to flag now than discover during Stage 1.
