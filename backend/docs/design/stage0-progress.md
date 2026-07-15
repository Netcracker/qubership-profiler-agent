# Stage 0 progress

Stage 0 is the contract-and-diagrams phase. No service code is written until these documents are reviewed and merged.

## Status

**Stage 0 — drafted; awaiting review for merge.**

- [x] **01 — Write contract** (`01-write-contract.md`)
  - [x] Wire-protocol invariants verified against agent code (V1–V6)
  - [x] Dictionary WAL format on local PV + S3 dictionary snapshot lifecycle
  - [x] Per-call trace blob assembly at chunk granularity (the (A) decision)
  - [x] Raw chunks staging files on PV with refcount tracking
  - [x] Parquet schema (Call rows) with retention class + error flag columns
  - [x] Local PV directory layout
  - [x] S3 object key layout (per retention class, time-bucketed)
  - [x] Flush semantics (time / size / memory pressure)
  - [x] Hot retention of local parquet past S3 upload
  - [x] Retention class mapping (5 classes by `(duration, error_flag)`)
- [x] **02 — Read contract** (`02-read-contract.md`)
  - [x] External `/api/v1/*` endpoints (fresh design, Java collector API not preserved)
  - [x] Internal `/internal/v1/*` collector hot-read endpoints
  - [x] `/calls/{pk}/tree` endpoint — MessagePack with int-keyed maps + `v` version envelope
  - [x] `/pods/{pod-restart}/dictionary` endpoint
  - [x] Hot/cold model with `hot_retention + overlap_margin` window
  - [x] S3 LIST-based discovery, no manifest yet
  - [x] PK-based deduplication, always-on
  - [x] Fan-out with partial-result protocol
- [x] **03 — Lifecycle** (`03-lifecycle.md`)
  - [x] Collector startup state machine (`INIT/LOADING/RECOVERY/READY/DRAINING/TERMINATING/FATAL`)
  - [x] Recovery sequence (mount PV → SQLite → WAL replay → chunks index → finalize closed pod-restarts → re-attempt uploads)
  - [x] Readiness / liveness probe split
  - [x] Shutdown drain (~95 s budget)
  - [x] `query` / `maintain` / `all` lifecycle
- [x] **04 — Storage layout** (`04-storage-layout.md`)
  - [x] StatefulSet manifest with `volumeClaimTemplates` (RWO)
  - [x] Headless Service (`clusterIP: None`)
  - [x] `query` Deployment + ClusterIP Service
  - [x] `maintain` with two modes (long-running Deployment OR k8s CronJob, operator-toggled)
  - [x] Helm chart structure and values diff against current `profiler-stack`
  - [x] Resource defaults
  - [x] Single Docker image with subcommand args
- [x] **05 — Diagrams** (`05-diagrams.md`)
  - [x] Data flow
  - [x] Deployment topology
  - [x] Collector state machine
  - [x] Per-call write-side lifecycle (sequence)
  - [x] Hot/cold read flow (sequence)
  - [x] Artifact lifetime table (initially a Mermaid `gantt`, replaced with a table because mixed time scales rendered poorly)
- [x] **06 — Wire protocol, server side** (`06-wire-protocol-server.md`)
  - [x] Command table: what the collector reads and writes back per command, with flush timing
  - [x] Handshake reply fixed at `PROTOCOL_VERSION_V2` (never `V3`, which switches the agent to the `posDictionary` stream the collector cannot demux)
  - [x] Ack policy: one byte per `RCV_DATA` / `REQUEST_ACK_FLUSH`, value `0` in MVP; no diagnostic-command dispatch
  - [x] `INIT_STREAM_V2` response fields (non-nil stable handle, `requiredRotationSize` as the segment-size lever)
  - [x] Error / teardown: `ACK_ERROR_MAGIC` and null-UUID + close
  - [x] `libs/server` skeleton divergences cataloged as Stage 1.1 fix-ups
  - [x] Synthetic test spec extending `libs/tests/integration/`

## Other Stage 0 artifacts

- `deferred.md` — design ideas intentionally out of MVP scope, with re-visit triggers documented.
- `profiler-plan.md` — high-level roadmap, decisions, and Idea→code map.
- `profiler-mom.md` — meeting notes that seeded the plan.
- `profiler-plan-summary.md` — early code-vs-plan delta.

## Stage 0 → Stage 1 readiness

Items below are explicitly OPEN at the end of Stage 0 and tracked into Stage 1+:

- **`error_flag` from `isCallRed`** — wire-format presence to be verified during Stage 1b; until then `error_flag` is derived from `callInfo.isCorrupted` only (`01-write-contract.md` §5.6).
- **Filesystem-backed S3 emulator** (`backend/libs/s3/`) — implementation deferred; dev currently uses MinIO in docker-compose.
- **`/internal/v1/pods` targeting** — endpoint shipped in Stage 1b but kept dormant in `query` until cluster size justifies it.
- **`stats` endpoint full schema** — sketch only in MVP; full design at Stage 4.
- **Parser consolidation** `streams/ → pipe/` (`profiler-plan.md` decision 8) — execute opportunistically while touching protocol code.

These are not blockers for Stage 1 to begin.

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

**Consequence:** Stage 0 produces a new schema in `01-write-contract.md` §5.2.

### 2026-04-23 — JSON for inter-service traffic

**Question:** JSON or protobuf between query and collector replicas?

**Choice:** JSON for `/internal/v1/*` and most `/api/v1/*`. MessagePack with int-keyed maps for `/api/v1/calls/{pk}/tree` only (size-driven; see 2026-06-18 entry below).

**Reason:** Easy to debug with curl, no schema-compiler in the build, expected payload sizes (calls list responses) are well within JSON's comfortable range.

### 2026-04-23 — S3 discovery via LIST, no manifest yet

**Question:** Maintain a manifest file in S3 for time-range → parquet-file lookups?

**Choice:** Start with LIST by prefix. Add manifest later if LIST becomes slow.

### 2026-04-23 — MinIO in docker-compose for dev

**Question:** Run a real S3 (MinIO) in dev or build a filesystem-backed emulator?

**Choice:** MinIO in docker-compose primary; the filesystem emulator stays as a deferred option for unit tests.

### 2026-06-18 — single Go binary, VictoriaMetrics-style subcommands

**Question:** Three separate Go binaries (collect / query / maintain), or one binary with subcommands?

**Choice:** One binary, four subcommands: `collect`, `query`, `maintain`, `all` (dev-only).

**Reason:** Shared internal libraries (config, S3 client, metrics, lifecycle) live in one tree; integration tests can run `all` in-process; one Docker image to ship.

**Consequence:** k8s manifests use the same image with different `args` for each workload (`04-storage-layout.md` §2-§5).

### 2026-06-18 — V5 corrected: trace stream is chunk-interleaved

**Question:** Are trace bytes for one root call emitted contiguously?

**Choice:** No — corrected. The trace stream is a sequence of `LocalBuffer` chunks (~4096 events each) with `[threadId, startTime]` headers; chunks from different threads interleave.

**Reason:** Verified by reading `dumper/.../Dumper.java:820-1010` and `boot/.../LocalBuffer.java`. The agent does not emit one contiguous byte range per call.

**Consequence:** Major write-contract rework. The collector reassembles per-call blobs at chunk granularity at write time (decision below).

### 2026-06-18 — per-call reassembly at chunk granularity ((A) chunk-level)

**Question:** Given the V5 correction, how does the collector serve per-call trace bytes? Options were: (A) physically reassemble per-call blob at write time, (B) read-side reassembly from raw chunks, (C) write-side index, read-side fetch.

**Choice:** (A) — write-side reassembly at chunk granularity. Per-call blob stored directly in the parquet row's `trace_blob` column.

**Reason:** Per-call retention sharding is trivial (each blob lives in one retention-class bucket); reassembly cost is bounded chunk-level memcpy (no event-level parsing); read API is dumb (single blob fetch).

**Trade-off accepted:** Carry-over chunk on root-call boundary is duplicated into both blobs (~16 KB per pair of consecutive same-thread root calls). Negligible.

**Consequence:** `01-write-contract.md` §4 fully rewritten; in-memory accumulator state holds `(staging_file, offset, length)` per thread.

### 2026-06-18 — hot/cold model with overlap window

**Question:** How does query reconcile data that's in flight on collector with data that's already flushed to S3?

**Choice:** Hot tier (collector PV) retains parquet for `PROFILER_HOT_RETENTION` (default 15 min) past S3 upload. Query reads both tiers for an `overlap_margin` (default 5 min) window, dedupes by PK. Standard Prometheus / VictoriaMetrics / Loki pattern.

**Consequence:** `01-write-contract.md` §6.3, `02-read-contract.md` §4.

### 2026-06-18 — retention classes (5 classes by duration × error)

**Question:** How is per-call retention sharded?

**Choice:** 5 retention classes: `short_clean` (<100ms, no error, 1d), `normal_clean` (<1s, 7d), `long_clean` (≥1s, 30d), `any_error` (30d), `corrupted` (forensic, 7d). Each class is its own S3 path segment so retention is enforced by S3 LIST + DELETE without opening parquet.

**Consequence:** `01-write-contract.md` §6.4; per-class TTL env vars in §9.

### 2026-06-18 — dictionary snapshot uploaded to S3 on pod-restart close

**Question:** How is the dictionary kept available for blob decoding once a pod-restart's WAL is purged from the collector?

**Choice:** On pod-restart close (TCP termination + finalization), the collector serializes the final dictionary as JSON and uploads it to S3 at `dictionaries/v1/<yyyy>/<mm>/<dd>/<podRestartHash>.json`. Default TTL 35 days (exceeds longest parquet retention).

**Reason:** Without this, `trace_blob` columns in long-retention parquet become unreadable after the WAL is purged. Surfaced during read-contract design (`02-read-contract.md` §2.6 discussion).

**Consequence:** `01-write-contract.md` §3.5 + §3.6; `02-read-contract.md` §2.6.

### 2026-06-18 — `trace_id` as three INT32 columns

**Question:** Carry the trace pointer as a single string `"a_b_c"` or split into three integers?

**Choice:** Split: `trace_file_index`, `buffer_offset`, `record_index` as separate INT32 columns.

**Reason:** Compact storage, dictionary encoding works per-column, dedup on query side is cheaper.

### 2026-06-18 — `/tree` endpoint format: MessagePack with int-keyed maps + version envelope

**Question:** What format does the server-decoded tree endpoint return — JSON, MessagePack, Protobuf, FlatBuffers?

**Choice:** MessagePack with int-keyed maps (semantically equivalent to protobuf field tags) and a `v: 1` envelope.

**Reason:** Compact binary (~2-3× smaller than JSON), zero codegen / schema-file overhead, hand-written ~50-80 LOC decoders per language. Scales to MCP/CLI within the same team. Future migration to protobuf is mechanical because int-keyed maps map 1:1 to field tags.

**Trade-off accepted:** Manual code review enforces "append-only / reserved-numbers" conventions instead of compiler-checked schema evolution.

**Consequence:** `02-read-contract.md` §2.5 (full field-tag tables and versioning rules).

### 2026-06-18 — always dedup, do not trust sticky TCP

**Question:** Does query rely on sticky TCP to avoid duplicate Call records, or always dedup by PK?

**Choice:** Always dedup. Sticky TCP routing breaks down during replica failover, scale events, and the hot/cold overlap window.

**Consequence:** `02-read-contract.md` §6.

### 2026-06-18 — Java collector API not preserved

**Question:** Does the new `query` service preserve the legacy Java collector's `/cdt/v2/calls/...` endpoint shape?

**Choice:** No. Fresh design at `/api/v1/*`. The legacy UI is rewired in Stage 5 anyway, and there are no external production consumers.

**Reason:** This supersedes the 2026-04-23 "preserve external query API shape" entry, which was a conservative initial assumption.

**Consequence:** `02-read-contract.md` §1 explicitly notes the override.

### 2026-06-18 — `cutoff=strict` query parameter deferred

**Question:** Ship a `?cutoff=strict` mode that disables the hot/cold overlap and dedup?

**Choice:** Defer. No identified MVP consumer that warrants the API complexity.

**Consequence:** Recorded in `deferred.md`. Trivial to add later when a concrete consumer profiles dedup as a bottleneck.

### 2026-06-18 — server-decoded `/tree` is the canonical path, raw blob is secondary

**Question:** Should UI fetch the raw blob + dictionary and decode client-side, or fetch a server-decoded tree?

**Choice:** Server-decoded `/tree` endpoint is canonical (`02-read-contract.md` §2.5). Raw `/trace` + `/dictionary` remain as a secondary advanced path for tooling that wants the wire format.

**Reason:** Naive nested-JSON inflates blob 5-10×; the savings of raw bytes do not justify forcing every client to embed a wire-format decoder. MessagePack + int-keyed maps reaches the size sweet spot.

**Consequence:** `02-read-contract.md` §2.5.

### 2026-06-18 — internal `/internal/v1/pods` targeting endpoint shipped but dormant

**Question:** Implement query-side replica targeting via `/internal/v1/pods` in MVP?

**Choice:** Ship the collector endpoint in Stage 1b (~30 LOC). Keep the query-side optimization off by default; turn on when cluster size warrants.

**Consequence:** `02-read-contract.md` §7.3.

### 2026-06-18 — agent wire protocol unchanged

**Question:** Does this contract require any change to the agent's TCP wire protocol?

**Choice:** No. Every decision above is collector-side. Existing agents ship to the new backend with no modifications.

**Reason:** The `restartTime` is stamped by the collector at TCP accept; the V5 correction is handled by collector-side reassembly; the dictionary snapshot is built by the collector from its WAL.

**Consequence:** Stage 1 work plan does not include the `agent/` subtree.

**Scope note (2026-07-02):** "no protocol change" means the agent→collector direction. The collector still has a strict server-side obligation on the reply direction — most importantly it must answer the handshake with `PROTOCOL_VERSION_V2`, not `V3`, or the agent switches its dictionary to the `posDictionary` wire format the collector cannot demux. That obligation was previously unstated; it is now the server-side wire contract `06-wire-protocol-server.md` (see the 2026-07-02 log entry).

### 2026-07-01 — code re-review corrections (batch)

A re-review against the agent code and the existing Go parser surfaced several errors in the Stage 0 contracts. Each contract carries a dated amendment block at its top; the decisions are logged here.

**Framing levels.** `COMMAND_RCV_DATA` payload (1 KB, `ProtocolConst.DATA_BUFFER_SIZE`) ≠ logical `LocalBuffer` trace chunk (tens of KB) ≠ Go `Chunk` rolling-stream handle. One logical chunk spans many payloads; chunk boundaries have no length prefix and are found only by parsing events to `EVENT_FINISH_RECORD`. Recorded in `backend/CLAUDE.md`.

**Blob assembly.** Event-level and Call-driven, keyed by the Call pointer `(trace_file_index, buffer_offset, record_index)`, with depth tracking to the depth-0 exit. Not a chunk-memcpy and not accumulator-by-arrival. The trace-vs-calls stream ordering worry is dissolved by this model.

**Canonical stored form: raw trace.** In scope for decodability: the dictionary, the external value streams the trace references (`bigParams` / `bigParamsDedup`, and any `sql` / `xml`), and the trace stream's `timerStartTime` epoch.

**Hot store: gzip segments + SQLite index.** Raw trace stream → rotating multi-MB gzip segment files on the PV (one member per segment, the agent's `<seq>.gz` model); 1× write, `unlink` eviction. SQLite holds the call index, segment catalog, refcounts, upload checkpoints — no bulk bytes. Parquet is the derived immutable cold form; the hot store is a cache over parquet + S3. Supersedes "read from open parquet writers" and the SQLite-blob variant (rejected because WAL doubles bulk writes).

**Bucketing by `floor(ts_ms)`.** Not by processing time. Late calls append files to older buckets; discovery is by range overlap; the `maintain` job compacts small files. Compaction moves from "deferred" to a required MVP task; the S3 catalog/manifest stays deferred as long as compaction keeps the file count low.

**Sealed parquet is immutable.** Sealed once, never rewritten → the deterministic S3 key is genuinely idempotent; no content-hash key needed.

**Recovery drops unclosed calls.** No truncated-blob emission; no `(0,0,0)` placeholder PK; `thread_id` is not in the PK.

**Retention classes.** `corrupted` and `any_error` are not mutually exclusive; a row may be both. The storage bucket per row stays single (route to `corrupted`).

**Overload policy.** Evict oldest segments first and drop fast calls first (class-aware); degrade by dropping the trace body while keeping Call metadata; never reach PV `ENOSPC`.

**No secondary index / no auth in the MVP.** `method` / `params` / `/stats` accept a full scan over the range. `Cache-Control: public` on blob and dictionary is fine until auth lands.

**Target scale.** 100 pods, 100 K calls/s aggregate, `/tree` cold-read SLA 10 s.

**Open spikes carried into Stage 1.**

- Capacity: segment size, seal interval, and staging budget for the target scale; the raw-`trace`-bytes-per-call figure needed to size them.

(The cursor / stable-pagination spike is resolved — see the 2026-07-01 cursor entry below. The "which external value streams does the trace reference" spike is resolved — see the metadata-schema entry below.)

### 2026-07-01 — dictionary continuity on cross-replica reconnect (resolved)

**Question:** On reconnect to a different collector replica, does the agent re-send the full dictionary, or must replicas share dictionary state?

**Finding:** The agent re-sends the full dictionary; replicas need not share state. A dropped connection cannot heal in place: `DefaultCollectorClient` sets `needsReconnect` and throws on any failed write, flush, or ack, and never reconnects itself (`DefaultCollectorClient.java:305,319,452`). The exception unwinds `dumpLoop()` into the `DumperThread` incarnation loop (`DumperThread.java:59-108`), which closes the dumper and calls `initialize()` again. `initialize()` resets `lastWrittenDictionaryTag = 0` and builds a fresh client and socket (`Dumper.java:454,348`), so the next `dumpDictionary()` walks the dictionary from index 0 (`Dumper.java:654,1258`), and the first dictionary chunk carries `resetRequired = 1` because `resetExistingContents()` is true while the tag is 0 (`Dumper.java:271-285`).

**Why the new replica is never dictionary-less:** `restartTime` is stamped by the collector at TCP accept, not sent by the agent (§1 V4). Every reconnect is a new TCP accept, so the new replica sees a fresh `(namespace, service, pod, restartTime)` pod-restart it has not indexed, and the agent opens that connection by sending the whole dictionary with the reset flag. The old replica finalises its own pod-restart independently (`03-lifecycle.md` §3.7).

**Consequence:** No cross-replica dictionary sharing in the MVP. Continuity keys off the dictionary stream's rolling-sequence id and reset flag, as already stated in the contract. This rests on a static code trace; an optional `mock-collector` check — break the socket, then assert `resetRequired = 1` and a from-zero dictionary on the second connection — can confirm it empirically.

### 2026-07-01 — agent-side improvements deferred (out of git)

Two protocol improvements would simplify the collector but are not required for the MVP. Captured in an out-of-git working file (`backend/docs/design/future-improvements.local.md`), not the tracked contracts:

- Agent emits both start and end trace offsets in the Call record, so the collector need not parse events to find a call's end.
- Remote protocol compresses the trace payload while leaving framing uncompressed, so the collector can demux without a channel-wide gunzip and store trace compressed as received.

### 2026-07-01 — parquet materialized by a seal pass, not on the write path

**Question:** With the hot store now gzip segments + SQLite index (2026-07-01 batch above), should the collector keep writing parquet incrementally per Call record, or materialize it from the hot store by a batch seal pass?

**Choice:** Seal pass. The write path only appends trace segments, builds the per-thread `chunk_index`, appends Call records to `calls.wal`, and inserts a lightweight SQLite index row. A per-`(pod-restart, bucket)` seal pass assembles the per-call blobs in one segment-ordered walk (each segment decompressed once) and materializes the parquet.

**Reason:** Hot reads already bypass open parquet writers (`02-read-contract.md` §3), so incremental parquet bought nothing on the hot path while costing a large open-writer fan-out (pod-restart × bucket × retention class) and a second copy of the trace bytes (once in the gzip segments, once in the pending-parquet blob). The seal pass cuts open-writer memory, removes the double store, folds late-data patch files and compaction into one "re-materialize the bucket" operation, and simplifies recovery — no half-written parquet, just re-run the idempotent seal.

**Trade-off accepted:** Call metadata for the un-sealed window lives in `calls.wal` + SQLite rather than in compact pending parquet, and blob assembly buffers the long-call tail across the segment cursor. Both are bounded (overload policy) and sized in Stage 1.

### 2026-07-01 — metadata.sqlite schema and hot-store layout

**Question:** What tables does `metadata.sqlite` hold, how is the decoded call index stored and rotated, and which streams form the hot store?

**Choices.**

- **Central `metadata.sqlite` (one per replica):** `pod_restarts`, `segments` (catalog + refcount), `parquet_local` (sealed files + upload checkpoints), `seal_state` (watermarks), `call_partitions` (partition catalog). No bulk stream bytes. Rewritten into `03-lifecycle.md` §3.2 (supersedes the old `staging_files` / `parquet_local` / `pod_restarts` set).
- **Call index partitioned by time bucket.** The decoded `calls` stream lands in `call_index` rows held in one SQLite file per bucket (`calls-<bucket>.sqlite`), ATTACHed for reads and dropped wholesale past `hot_retention`. A partition-drop avoids the `DELETE`-churn + `VACUUM` of a single ~9M-row table at target scale. The calls stream is consumed sequentially (not offset-addressed), so decoding to rows fits; `trace` / `sql` / `xml` stay raw because they are offset-addressed.
- **Hot-store segment catalog covers `trace`, `sql`, `xml` only.** `dictionary` / `params` / `suspend` stay as append-only WAL files (§3), because the dictionary needs per-entry `fsync` durability — one lost entry makes every trace byte that references it undecodable.
- **Segment = agent stream file, 1:1.** A `segments` row is keyed by `(pod_restart, stream, rolling_seq)`, where `rolling_seq` is the agent's file index. A Call pointer `(trace_file_index, buffer_offset)` and a trace tag's `(rolling_seq, offset)` then resolve by opening `<stream>/<rolling_seq>.gz` and seeking — no offset-translation table. The collector governs segment size via `requiredRotationSize` in the `INIT_STREAM_V2` response, so it does not split agent files. Supersedes the earlier "collector rotates at its own size, catalog stores a logical byte range" wording in `01-write-contract.md` §4.4, which conflicted with §5.2 ("offset within `trace_file_index`").

**Spike resolved — external value streams.** Exactly two: `xml` = `PARAM_BIG` (1), `sql` = `PARAM_BIG_DEDUP` (3). A trace tag of either type carries `(rolling_seq, offset)` into the matching stream (`backend/libs/parser/pipe/traces.go`; `ParamTypes`). The `bigParams` / `bigParamsDedup` names in earlier drafts are these two streams, not additional ones. The `sql` dedup cache clears on rotation (`Dumper.java`), so `sql` offsets are valid only within their `rolling_seq` — consistent with the 1:1 segment model.

**Consequence:** `03-lifecycle.md` §3.2, §3.4, §3.5 rewritten; `01-write-contract.md` §4.4, §8, §9, §10 aligned.

**Consequence:** propagated into the contract bodies — `01-write-contract.md` §2, §4.3, §5.1, §6 (esp. §6.5 seal pass, §6.6 late data), §8, §9; `03-lifecycle.md` §2, §3.2, §3.5–§3.8, §5.3, §6; `05-diagrams.md` §5, §7; `04-storage-layout.md` §3.5 (sizing). The top-of-file 2026-07-01 amendment blocks were collapsed to a dated one-line pointer once the bodies carried them.

**Demux mechanism (recorded to prevent re-litigation).** The raw trace stream stays interleaved on disk (one sequential append per pod-restart); demultiplexing is virtual, via `chunk_index[threadId]` — pointers `(segment_file, logical_offset, length)`, not buffered bytes. Rejected alternatives: (a) demux on receipt into per-thread output streams — turns one sequential append into thousands of tiny per-thread appends (M ≈ live threads on the replica); (b) per-call temp files — one file per in-flight call, rejected as too many handles and writes. The per-thread index is what makes "read only this call's fragment" cheap without either.

### 2026-07-01 — cursor / stable pagination across hot→cold (resolved)

**Question:** How does `/calls` paginate consistently while calls migrate from the hot tier (collector SQLite index) to the cold tier (S3 parquet) between page fetches?

**Choice:** Keyset (seek) pagination on the total order `(ts_ms DESC, pk ASC)`, binary collation. The cursor is an opaque base64 token carrying a format version, the frozen query (`from`, `to`, filters, ordering), the last position `(ts_ms, pk)`, and an issue timestamp; TTL `PROFILER_CURSOR_TTL` (default 15m). One global position, no per-source scroll state: each page re-fans-out, every source seeks past the position, `query` k-way merges, then dedups by PK before truncating to `limit` and before computing `next_cursor`.

**Reason:** `ts_ms` and the PK are immutable and identical in both tiers, so a migrated call keeps its position — migration stops being a special case. The design rests on the existing zero-gap guarantee (`02-read-contract.md` §4.3) and PK dedup (§6). Termination: `next_cursor = null` only when the position passes `from`; an empty mid-range page (rows aged out of hot and TTL-deleted from cold) is valid, not end-of-stream.

**Consistency envelope (explicit):** a pagination session sees a snapshot as of each reported position, not a whole-window snapshot; late data re-sealed below an already-passed position (`01-write-contract.md` §6.6) is not surfaced in that session. Same envelope as §4.3.

**Deferred within this decision:** alternative sort orders (e.g. by duration — `/stats` territory); a stateful scroll cursor (until deep-pagination profiling warrants it); HMAC-signing the cursor.

**Consequence:** `02-read-contract.md` §2.3.1 (new), §4.3 (pointer updated), §9 (config row), §11 (checklist); this spike removed from "Open spikes carried into Stage 1".

### 2026-07-02 — calls-stream time is a running delta, not an absolute offset

**Finding:** A design re-review flagged that the calls stream encodes each record's start time as a zig-zag varint delta from the *previous* record, seeded by the file header, not as an absolute offset from that header. The agent writes the running delta and advances its timer per record (`Dumper.java:1062-1063`), resetting only on file rotation, which also writes a fresh 8-byte `base_ms` header (`Dumper.java:1394-1401`; `CompressedLocalAndRemoteOutputStream.java:156`). The reused Go decoder read each delta as an absolute offset (`base_ms + delta_i`) without accumulating (`backend/libs/parser/pipe/calls.go:52,146`; `streams/calls.go:100,158`). The two formulas coincide for the first record of a file, so short fixtures stayed green; every later record's `ts_ms` was wrong.

**Why it matters:** `ts_ms` is the primary time axis. Bucketing (§5.4), retention (§6.4), the PK, and the read cursor all key off it, so a silent per-record drift corrupts all four.

**Resolution:** `01-write-contract.md` §5.1 specifies the reconstruction (`ts_ms_i = ts_ms_{i-1} + delta_i`, reseeded at each file header) and §5.2 annotates the `ts_ms` column. Both Go decoders now accumulate the deltas (`backend/libs/parser/pipe/calls.go`, `backend/libs/parser/streams/calls.go`), preserving the raw `Call.Time` field so existing CSV fixtures stay valid. `TestCallsTimeAccumulation` in each package guards the reconstruction with a synthetic three-record stream (5 ms, then one and two minutes apart) from the versioned generator `backend/libs/tests/helpers/wire`; the pre-fix formula matches only the first record, so the test fails against it.

### 2026-07-02 — server-side wire protocol specified (new contract 06)

**Finding:** A design re-review flagged that the contracts specified only what the agent *sends* (`01-write-contract.md` §1), leaving the collector's *reply* direction unspecified — yet the agent is strict about the replies. Three gaps, each verified against the agent code and the existing Go server:

- **Handshake reply.** The agent sends `PROTOCOL_VERSION_V3` and accepts `V2` or `V3` back (`DefaultCollectorClient.java:134-142`). Replying `V3` switches the agent to the `posDictionary` stream (`Dumper.java:350-354`), which the Go stream set and parser do not know (`backend/libs/protocol/streams.go`). The reply must be `V2`. The existing skeleton replies `10` (`libs/server/common.go:9`), which a real agent rejects outright — the Go emulator masks this by not checking the reply (`libs/emulator/connection.go:100-103`).
- **Ack policy.** The agent expects one ack byte per `RCV_DATA` (`pendingAcks`, `DefaultCollectorClient.java:326-352`); the byte is a diagnostic-command count (`0` in the MVP), and `ACK_ERROR_MAGIC` = `-1` forces a reconnect. The skeleton's `RCV_DATA` never writes the byte (`libs/server/server_connection.go:244-251`), so a real agent's 5 s flush would stall on the 30 s ack timeout and reconnect.
- **`INIT_STREAM_V2` reply.** Four fields (handle, `rotationPeriod`, `requiredRotationSize`, `serverRollingSeq`); the skeleton returns all zeros (`libs/server/server_connection.go:186-218`), and `01` mentioned only `requiredRotationSize`.

**Resolution:** New contract `06-wire-protocol-server.md` — full command table, the `PROTOCOL_VERSION_V2` invariant, ack policy, `INIT_STREAM_V2` reply semantics, unknown-stream / unknown-command teardown (`ACK_ERROR_MAGIC`), and a catalog of the `libs/server` skeleton's divergences as Stage 1.1 fix-ups. `01-write-contract.md` §1 now points to it; `profiler-plan.md` §1.1 reworded from "reused without changes" to "reused with adaptation" (the offline `parser.go` reads server replies from its input and cannot serve the live reply path unchanged). Guarded by a synthetic test extending `libs/tests/integration/`: drive the emulator (or the real `Dumper`) through handshake → `INIT_STREAM_V2` → many `RCV_DATA` → `REQUEST_ACK_FLUSH` and assert the version is `V2`, every ack drains without `ACK_ERROR_MAGIC` or timeout, and the dictionary arrives on the `dictionary` stream.

**Implementation (2026-07-02):** the five `libs/server` divergences were fixed ahead of the general Stage 1 gate, at the developer's request, because they are self-contained and each is a concrete bug: named version/ack constants in `libs/protocol/versions.go` and an `IsKnownStream` validator in `streams.go`; `ProtocolVersion = PROTOCOL_VERSION_V2` and rotation defaults in `libs/server/common.go`; per-`RCV_DATA` ack, `REQUEST_ACK_FLUSH` reply, non-nil handle plus real rotation fields, unknown-stream null-UUID teardown, `ACK_ERROR_MAGIC` on unknown command, and connection close on handler exit in `server_connection.go`. The emulator gained `ServerVersion()` and `Flush()`; `emulator_test.go` asserts the `V2` reply, an ack cycle that drains without reconnect, and unknown-stream refusal. `06-wire-protocol-server.md` §8–§9 rewritten from "fix-ups" to the conformance record. This is code ahead of the Stage 0 merge gate — a deliberate exception, not the general rule.

### 2026-07-02 — trace-blob epoch: framing fix, not a data-loss blocker

**Finding:** A design re-review rated the trace `timerStartTime` epoch a cold-read blocker — a blob with no persisted epoch was said to be undecodable, with cross-chunk call times unrecoverable. The claim does not hold. The canonical `/tree` reader reconstructs each event as `eventRealTime = timerStartTime + Σ(event deltas)` (`TracePodReader.java:152-179`) and advances the tree by differences of that value, so the epoch is a constant offset that cancels in every duration and relative offset. Each logical chunk re-seeds its deltas from zero (`prevMillis = 0`, `Dumper.java:875`), so a call spanning several chunks needs no inter-chunk continuity: both ends anchor to the shared epoch and their difference drops it. Absolute wall-clock timestamps are the only quantity that needs the epoch, and they are recoverable regardless — every chunk header carries an absolute `startTime` (`Dumper.java:882`), and the agent also writes an explicit `PARAM_COMMON_STARTED` tag per persisted call (`Dumper.java:957`). The proposed test (compare `enterMsRel` / `durationMs`) passes for any epoch, so it does not exercise the claimed defect.

**Kernel of truth:** the existing trace readers unconditionally read a leading 8-byte epoch (`TracePodReader.java:105`, `streams/traces.go:44`), but the blob (§4.5) starts with a chunk header. Feeding a headerless blob to an unmodified reader desyncs. That is a framing gap, not data loss.

**Resolution:** `01-write-contract.md` §4.2 reworded (the epoch is a constant offset, recoverable from chunk headers, not a decodability gate); §4.5 pins the blob framing (the seal pass prepends the 8-byte `timerStartTime`, so the readers run unmodified and absolute times decode exactly); §4.3 captures the epoch once at stream start and holds it per pod-restart (re-read from the first segment on recovery); §6.5 step 3 prepends it during assembly. No parquet column and no dictionary-snapshot field added — the epoch rides in the blob. B2 retired as a blocker; kept as the framing note above.

### 2026-07-02 — S3 discovery keys on the bucket start, not object metadata

**Finding:** A design re-review flagged that `02-read-contract.md` §5.1 selected cold-tier files by an `[time_min, time_max]` overlap carried "in the object metadata (or the future catalog)" and widened the hour walk by the maximum call duration. `ListObjectsV2` returns only the key, size, and ETag, not user metadata, so that model needs a per-object HEAD or a footer read on every listed file. It also contradicts the write contract: a bucket is keyed by `floor(ts_ms)` of the call's *start* (`01-write-contract.md` §5.4) and `/calls` filters on that same `ts_ms` (§2.3), so every row of a file lies in `[timeBucketStart, timeBucketStart + PROFILER_TIME_BUCKET)`. The file's span follows from the `<timeBucketStart>` already in the object key, and a late call re-seals under that same key (§6.6), so the "widen by max call duration" step guards a case that cannot arise: a call is filed by its start, never its end.

**Resolution:** `02-read-contract.md` §5.1 step 4 now selects candidate files from `<timeBucketStart>` in the object key (`timeBucketStart < t2`, low side bounded by the hour walk) and applies a row-level `ts_ms ∈ [t1, t2)` filter as the exact bound. No footer read, no per-object HEAD, and the "widen the hour walk by the maximum call duration" instruction is gone. `01-write-contract.md` §6.6 point 3 is reworded — patches share the one `<timeBucketStart>` and are found by that key, not by `time_min` / `time_max` — and §5.4 points discovery at the bucket key. The local `time_min` / `time_max` columns in `metadata.sqlite` (§6.2, `03-lifecycle.md`) stay; they serve compaction, not discovery. Stage 2 synthetic test: seed three buckets plus a late-arrival patch file, then assert the query plan lists exactly the overlapping bucket keys and returns the late row.

### 2026-07-02 — S3 key carries per-file time_min/time_max; LIST scaling documented

**Refines the entry above.** Keying discovery on `<timeBucketStart>` alone is correct but coarse: it opens every bucket the hour walk enumerates, wasting up to one hour of five-minute files at the low edge of a range. A design discussion resolved three follow-ups.

**time_min/time_max in the key (adopted).** The M4 objection was specific to *user metadata* — `ListObjectsV2` does not return it, so an overlap test on it needs a per-object HEAD. The object *key* rides in every LIST result, so it does not. The seal pass already computes each file's `time_min` / `time_max` for `metadata.sqlite` (`01-write-contract.md` §6.2), so `01-write-contract.md` §7 now bakes them into the key after `<timeBucketStart>`, and `02-read-contract.md` §5.1 step 4 tests `[timeMin, timeMax]` overlap straight from the LIST: exact at file granularity, no footer read, no HEAD. `<timeBucketStart>` stays for bucket identity (patch grouping, chronological sort). The key stays deterministic — the late-data watermark (`01-write-contract.md` §6.6) fixes each `<seq>`'s row set, so a re-seal regenerates the same range. This is not a reversal of M4: the range now travels in the key, not in metadata a LIST cannot read.

**PROFILER_TIME_BUCKET in query config (rejected).** The width-aware low-edge prune would couple `query` to the collector's write-side bucket width and silently drop data if that width ever changed. The key range supersedes it; dropped.

**LIST scaling (documented).** New `02-read-contract.md` §5.5 records the two cost axes (folder breadth vs open count), the `12 × P × f` per-hour-class object estimate, the sequential-pagination-within-a-prefix latency metric, and the deferred levers in cost order: a 5-minute path segment, cross-pod-restart compaction, then the manifest (`02-read-contract.md` §5.3). None is built now; each is triggered by LIST profiling, not pre-optimized.

**Stage 2 synthetic test (updated):** seed three buckets plus a late-arrival patch file whose rows fall near a bucket edge, then assert discovery opens exactly the files whose `[timeMin, timeMax]` overlaps the query and returns the late row, while skipping an in-range bucket whose rows sit outside the query window.

### 2026-07-03 — cross-pod-restart compaction; compaction reader-safety needs a delete-grace, not just ordering

**Cross-pod-restart compaction (adopted).** `maintain` compacted only a `(bucket, retention_class, pod-restart)`'s patches, cutting the patch factor `f` but not the pod-restart factor `P` (`02-read-contract.md` §5.5). `01-write-contract.md` §6.6 point 4 now also merges the small per-pod-restart files of one `(bucket, retention_class)` into a single object once they accumulate. Rows keep their per-row PK (`pod_*`, `restart_time_ms`), so a mixed-pod-restart file needs no read-path coordination — dedup, dictionary resolution, and PK lookup key off the row, not the file. Both forms stay within one `retention_class` so the per-class TTL still applies by key. §7 generalizes the two leading key fields: a compacted object uses producer `maintain` and a hash of its inputs in place of `<replica>-<podRestartHash>`. `02-read-contract.md` §5.5 moves this lever from "deferred" to "already in §6.6".

**Compaction reader-safety: the two-line fix was insufficient.** The flagged fix was "write the compacted object before deleting inputs" plus "discovery treats a listed-then-`404` object as empty". That pair guarantees safety (no crash, no wrong row) but not completeness: a query whose LIST saw the inputs before the compacted object was written, and reads them after they are deleted, gets `404` on every input and never had the compacted object in its candidate set — it silently drops those rows. Write-then-delete ordering alone does not close this. **Resolution:** compaction delays the input delete by `PROFILER_COMPACTION_DELETE_GRACE` (default `5m`, `01-write-contract.md` §9), longer than one discovery-plus-read round (each page re-LISTs, `02-read-contract.md` §2.3.1). A query that listed the inputs reads them well within the grace; a query that lists after the compacted object exists sees it by S3 read-after-write consistency; within the grace both are visible and PK-dedup collapses the overlap. The `404`-as-empty rule stays as a backstop for a read that outlives the grace. Updated: `01-write-contract.md` §6.6, §7, §9; `02-read-contract.md` §5.1, §5.5. Stage 2 test to add: run discovery concurrently with a compaction of the queried bucket and assert no row is dropped across the write→grace→delete transition.

### 2026-07-03 — wide-query guard on the cold read path

**Question:** How does `/calls` stop a wide-window query with no file-pruning filter from blowing the read SLO — reject it, or silently narrow it?

**Choice:** Reject at validation with `400` (fail-closed, frame A), never narrow silently. Two layers, both before any parquet file is opened:

- **Span.** `to - from > PROFILER_WIDE_RANGE_LIMIT` (default `6h`) with no narrowing filter → `400`. No I/O; stops the pathological query before the multi-day LIST.
- **Estimated scan.** The discovery LIST already returns each object's size and key-encoded `retention_class`, so `query` sums `(file_count, total_bytes)` per class for free and rejects over `PROFILER_MAX_SCAN_FILES` / `PROFILER_MAX_SCAN_BYTES`. The two limits map to the two cost axes of `02-read-contract.md` §5.5.

Narrowing filters that exempt a query: `pod`, `retention_class`, `duration_min_ms`, `error_only` — each prunes the discovered file set. `method` / `params` do not: they filter rows inside listed files, not the scan.

**Reason:** A silent narrow changes the result set under the reader — `short_clean` (the biggest class, 1-day TTL) would drop out unannounced, hiding exactly the fast calls a "what ran" query wants. Fail-closed keeps the contract honest and lets the caller pick the axis; the `400` body carries `suggested_filters` and a per-class byte breakdown so Stage 5 UI renders a guided prompt, not a bare error. The estimate is free because discovery already lists sizes — no manifest or extra index for the first cut.

**Deferred:** a fail-soft per-request scan budget (`partial: true` + `partial_reasons: [budget_exceeded]`, a `200`) as a Stage 2 backstop for what the file-size estimate overshoots or misses; the stats manifest that would make the estimate precise (row counts, per-column sizes) and replace the LIST, already tracked under `02-read-contract.md` §5.3; a `confirm_wide` / async override, added only when a concrete consumer needs the expensive scan (same posture as the retired `cutoff=strict`).

**Consequence:** `02-read-contract.md` §2.3.2 (new), §5.1, §5.5, §7.4, §8, §9, §11; `deferred.md` (backstop and override entries).

### 2026-07-03 — contract fixes from the design re-review (M2, M6, M7, M3, M5) and MVP scope answers

A design re-review confirmed the model but flagged must-fix contract items that had not landed. Each is now in the contracts, verified against the agent code and the Go decoders before writing.

**M2 — `error_flag` from the `call.red` param, not `isCorrupted` (`01` §5.6, §6.4).** The old §5.6 derived `error_flag` from `callInfo.isCorrupted`, but a corrupted call never becomes a Call record (the `Dumper.java:945-947` persistence gate excludes it), so `error_flag` was always false and `any_error` / `corrupted` were always empty — retention degenerated to duration-only, and `retention_class` is an S3 key component. The error signal is already on the read path: `ExceptionLogger.callRed()` records the indexed param `call.red`, which the Go decoder reads into `Call.Params` (`pipe/calls.go`). Confirmed 2026-07-03 that `call.red` is indexed in every targeted deployment. New rule: `error_flag := dictId("call.red") ∈ keys(Call.Params)`. No agent change and no new struct field; the earlier "surface `isCallRed`" follow-up is retired. `corrupted` stays reserved but empty.

**M7 — segment file name = `serverRollingSequenceId + 1` (`01` §4.4).** The agent addresses its stream files as `serverRollingSequenceId + 1` (the "+1 for compatibility" in `CompressedLocalAndRemoteOutputStream.java:171-179`) and puts that value in every Call's `trace_file_index` (`Dumper.java:921`). §4.4 said only "names it by the stream's `rolling_seq`", which, read as the echoed id, shifts every trace pointer to the neighbouring segment — silent, total corruption of trace addressing. Now pinned, with a two-rotation synthetic test.

**M6 — parquet codec and row order pinned (`01` §5.2).** ZSTD compression (the blob is stored uncompressed after assembly, so the codec is the only thing shrinking ~6 MB/s of raw trace before the 30-day TTL), and rows sorted `(ts_ms DESC, pk ASC)` to match the read-path total order (`02` §2.3.1), for row-group pruning and a sorted-run k-way merge.

**M3 — pod-restart manifest for cold `/pods` (`01` §3.6, `02` §2.7).** `<podRestartHash>` in the parquet key is one-way, so closed pod-restarts had no cold identity source and `/pods` over a week was unserviceable. The collector now writes a small `pods/v1/<yyyy>/<mm>/<dd>/<podRestartHash>.json` for each day a pod-restart seals into, so cold `/pods` is a day-prefix LIST unioned with the hot tier — no parquet scan.

**M5 / `non_blocking_ms` — suspend storage decided; `non_blocking_ms` cut (`01` §3.6, §5.1, §5.2).** `non_blocking_ms` has no wire source, removed from `CallV2` (re-adding is additive). `suspend` is a small, global, per-pod-restart stream, so it does not go into `CallV2` rows. Two grains: the per-call `suspend_ms` scalar is derived at seal by intersecting the call interval with `suspend.wal` pauses (MVP-critical, fills the UI column); the raw pause timeline is persisted per pod-restart next to the dictionary snapshot as `suspend/v1/.../<podRestartHash>.json` for a future pod-level view, droppable if that view is not built.

**Scope answers (2026-07-03).** Agent-fleet questions from the re-review are resolved: only the `GET_PROTOCOL_VERSION_V2` handshake is supported (V1 reserved, `06` §2); no channel gzip in the fleet, so gunzip is out of MVP (`06` §7, `01` §1 corrected from "supports both modes"); `call.red` is guaranteed indexed (unblocks M2); the collector starts plain `1715` only, SSL `1717` deferred; the `heap/td/top` command channel is kept reserved (ack byte `> 0`, `06` §5). The wide-query guard (Q4) landed separately, above.
