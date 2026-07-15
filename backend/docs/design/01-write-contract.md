# 01 — Write contract

> Status: **draft**, awaiting review. Wire-protocol invariants (Section 1) verified against agent code. No agent or protocol changes are required by this contract for the MVP.

> **2026-07-01 redesign — now in the body.** The hot-store (gzip segments + SQLite index) and seal-pass model replaced the earlier write-path-parquet design and is folded into §2–§6 and §8–§9 below. Decision history and rationale: `stage0-progress.md` decisions log (2026-07-01 entries).

This document defines what the new Go collector writes to local PV and to S3, and on which events. It is the source of truth for Stages 1, 2, 6.

## 1. Background: what the agent actually sends

The agent opens a long-lived TCP connection to the collector and multiplexes seven named streams over it (`backend/libs/protocol/streams.go`). Each stream is a sequence of binary chunks delivered via `COMMAND_RCV_DATA` (`backend/libs/parser/parser.go`). This section covers what the agent **sends**; what the collector **reads from each command and writes back** (handshake reply, ack policy, `INIT_STREAM_V2` response, error teardown) is the server-side wire contract in `06-wire-protocol-server.md`.

| Stream | Contents | Cardinality |
|---|---|---|
| `dictionary` | Append-only `(int → utf-8 string)` map. Method names, class names, tag names. | One per pod-restart |
| `params` | Parameter metadata: `(name, isIndex, isList, order, signature)`. | One per pod-restart |
| `suspend` | GC / JIT pause events `(time, delta, amount)`. | One per pod-restart |
| `calls` | One record per **closed root call** (e.g. one HTTP request handler invocation). Carries per-call summary metrics + a back-reference into the `trace` stream (`TraceFileIndex`, `BufferOffset`, `RecordIndex`). | Many per pod-restart |
| `trace` | Binary log of `methodEnter` / `methodExit` events. Children are written before their parent's close. | Many per pod-restart |
| `sql`, `xml` | Captured payload bodies referenced from calls. | Many per pod-restart |

Important consequence: **the collector does not assemble calls.** A `Call` record arrives only when the root call has closed on the agent side. The collector's job is to demultiplex streams, persist them, and emit a parquet row per `Call`.

**Optional channel gzip.** `ProtocolConst.ZIPPING_ENABLED` (default `false`, `proto-definition/.../ProtocolConst.java:46`) gzips the whole multiplexed channel; when it is on, the collector must gunzip before it can demux `RCV_DATA`. The MVP targets the default (off); a gunzip wrapper around the socket is the only change if a deployment turns it on (`06-wire-protocol-server.md` §7).

### Verified against agent code

Sources: `dumper/src/main/java/com/netcracker/profiler/Dumper.java`, `boot/src/main/java/com/netcracker/profiler/agent/{LocalBuffer.java,DumperConstants.java}`.

**V1.** Verified. A `Call` record is emitted only after the root call has closed; its trace bytes are written to the trace stream during the same `writeBufferToFS` pass (`Dumper.java:940-983`).

**V2.** Partially verified. `(TraceFileIndex, BufferOffset, RecordIndex)` points to the START of the root call's first ENTER record (`Dumper.java:921-923`). The end is not transmitted on the wire. For the collector this is moot — we reassemble blobs at chunk granularity, not by byte range (§4).

**V3.** Verified. Dictionary entries are append-only within one pod-restart.

**V4.** Verified. A pod-restart boundary is a new TCP connection with a new `COMMAND_GET_PROTOCOL_VERSION_V2` handshake. **`restartTime` is stamped by the collector at TCP accept** — the agent does not transmit it. No protocol change required.

**V5.** Corrected — bytes for one root call are NOT contiguous in the stream. The trace stream is a sequence of `LocalBuffer` chunks: each chunk is prefixed with a 16-byte header `[threadId:long, startTime:long]`, holds events of ONE thread, and ends with `EVENT_FINISH_RECORD` (`Dumper.java:881-1004`, `LocalBuffer.SIZE=4096`). Chunks from different threads interleave at chunk boundaries. One business call spans N chunks of the same thread, separated by chunks of other threads. **Implication: the collector reassembles per-call blobs at chunk granularity at write time** (§4). No protocol change required.

**V6.** Verified. One TCP connection corresponds to one `(namespace, service, podName)` triple.

## 2. What the collector persists

The collector keeps five artifacts. They live in different storage tiers because their access patterns, durability requirements, and lifetimes differ.

| Artifact | Storage | Why |
|---|---|---|
| Dictionary / params / suspend WAL | Local RWO PV, append-only file | Required for restart recovery and for decoding trace bytes already received. The agent does not retransmit these streams (§3). |
| `calls.wal` | Local RWO PV, append-only file | Decoded Call records, one file per pod-restart, retaining raw parameter dictionary ids for seal re-derivation (§5.6). Source for the seal pass (§6) and for hot single-row `/calls/{pk}` fetches, located by the SQLite offset. |
| Trace segments | Local RWO PV, gzip segment files | The hot store: the raw interleaved trace stream, written once (§4.4). Source for per-call blob assembly at seal time. Refcounted; unlinked once every call whose chunks it holds is sealed and uploaded. |
| `metadata.sqlite` | Local RWO PV, SQLite | The call index (PK + filter columns + `calls.wal` offset), the segment catalog (logical byte range per segment), refcounts, seal watermarks, and upload checkpoints. No bulk bytes. |
| Parquet | Local RWO PV, then S3 | Materialized by the seal pass (§6), never on the write path. Sealed once, kept locally for `hot_retention` past upload (§6.3), authoritative in S3. |

In-memory state is small: the dictionary (a full copy for fast lookups) and the per-thread `chunk_index[threadId]` — ordered lists of `(segment_file, offset, length)` for that thread's logical chunks still within the hot window (§4.3). Parquet writers exist only transiently, inside a running seal pass. The collector parses trace events to find chunk and call boundaries (§4.1), but it does not build an in-memory call tree; the blob is a byte range, assembled at seal and decoded into a tree only on the read path.

## 3. Dictionary WAL

### 3.1 Why a WAL

The trace stream is encoded against the dictionary. A method-enter event references method ID 42, and the dictionary tells us "42 = `com.example.Service.handle`". If the collector restarts and the dictionary is lost, all subsequent trace bytes for that pod-restart are unreadable: the agent does not retransmit dictionary entries.

A WAL writes each new dictionary word to disk as it arrives, so on restart we can rebuild the dictionary by replay.

### 3.2 Format

One file per `(namespace, service, podName, restartTime)` tuple. Append-only. Records are length-prefixed:

```
record := varint(record_len) record_body
record_body := varint(word_id) varint(word_len) word_bytes
```

`word_id` is the dictionary position from the agent (sequential int starting at 0). `word_bytes` is UTF-8.

No header, no checksums per record. A single CRC32 at file footer is computed on close and verified on replay; partially written tail past the last valid record is truncated on recovery.

### 3.3 Durability

`fsync` is called every N entries or every T milliseconds, whichever first. Defaults: `N = 256`, `T = 100 ms`. Configurable via env.

Rationale: a power-loss between fsync windows costs at most 100 ms of dictionary entries. Subsequent trace bytes referencing those entries become unreadable for that window — those calls are dropped from the parquet output and logged. This is acceptable for a profiler.

### 3.4 Path

```
/data/pods/<namespace>/<service>/<podName>/<restartTime>/dictionary.wal
```

`<restartTime>` is a Unix-milliseconds string stamped by the collector at the moment of TCP accept (= the boundary marked by `COMMAND_GET_PROTOCOL_VERSION_V2`). The agent does not transmit it. No protocol change required.

### 3.5 Lifetime

The local WAL is deleted when the corresponding pod-restart is fully flushed: all `Call` records for it are written to parquet, parquet uploaded to S3, the dictionary snapshot is uploaded to S3 (§3.6), and a hold-back grace period (default 1 hour) elapses to allow late-arriving Calls.

`params` and `suspend` streams use the same WAL pattern with separate files (`params.wal`, `suspend.wal`).

### 3.6 Dictionary snapshot in S3

The per-call trace blobs (§4) embed `method_id` and `param_id` references that resolve against the per-pod-restart dictionary. Without the dictionary the blobs are not decodable.

Parquet rows have a per-bucket retention of up to 30 days (`long_clean`, `any_error`); the local dictionary WAL lives only until the pod-restart fully flushes (§3.5). Therefore the dictionary MUST be persisted alongside parquet.

**On pod-restart close** (TCP connection terminates AND all in-flight Calls flush), the collector:

1. Reads the local WAL and builds the final dictionary snapshot in memory.
2. Serializes it as a single JSON object: `{ version, methods: [...], params: [...] }`. **TODO (dictionary shape):** methods and params share one wire id space, so both arrays carry the full word list and a reader is correct against either; a future revision should collapse them to a single `words` array indexed by id (see `02-read-contract.md` §2.6, decision logged in `stage1-progress.md`).
3. Uploads it to S3 with a deterministic key:

   ```
   s3://<bucket>/dictionaries/v1/<yyyy>/<mm>/<dd>/<podRestartHash>.json
   ```

   Date hierarchy mirrors the parquet layout (§7) so maintenance can apply a single retention rule per day.

4. Deletes the local WAL after the hold-back grace period.

**Retention:** the S3 dictionary lives at least as long as the longest retention class touched by any parquet row of this pod-restart. The simplest policy — `PROFILER_RETENTION_DICTIONARY_TTL` (default `35d`) — covers the longest retention bucket (`30d`) plus a safety margin.

**Live pod-restart:** while the TCP connection is open, the dictionary lives only on local WAL + RAM. Reads go through the collector replica's `/internal/v1/pods/{pod-restart}/dictionary` (see `02-read-contract.md` §2.6). Query never reaches into the WAL directly.

**Growth during live pod-restart:** the dictionary is append-only (§1 V3). If a collector crashes and recovers mid-flight, the WAL is replayed; the on-disk monotonic `version` counter is rebuilt from the highest entry's index. Clients revalidating an old ETag get the fresh snapshot.

**Pod-restart manifest (S3).** Cold `/pods` (`02-read-contract.md` §2.7) lists the `(namespace, service, pod, restart_time)` tuples with data in a range, but a parquet key carries only `<podRestartHash>`, a one-way hash, so the readable identity cannot be recovered from a LIST. The collector closes the gap with a small manifest. For each UTC day a pod-restart seals a bucket into, it writes or refreshes `s3://<bucket>/pods/v1/<yyyy>/<mm>/<dd>/<podRestartHash>.json` = `{ namespace, service, pod, restart_time_ms, timer_start_ms, replica, time_min_ms, time_max_ms }`. Emission is idempotent per `(day, pod-restart)`: the first seal for the day writes it, later seals refresh `time_max_ms`. A pod-restart that spans several days gets one manifest per day it holds data in, so a range query finds it in the day prefixes it already walks (`02-read-contract.md` §5.1), with no widened walk. Live pod-restarts surface from the hot tier (`/internal/v1/pods`), so cold `/pods` over a range is `LIST(pods/v1/<days>)` unioned with the hot replicas. `PROFILER_RETENTION_DICTIONARY_TTL` (§9) covers its retention.

**Suspend timeline (S3).** The `suspend` stream is a small, global, per-pod-restart series of stop-the-world pauses (GC / JIT), not a per-call signal, so it does not belong in the `CallV2` rows. It has two consumers at two grains. Per call, `suspend_ms` (§5.2) measures how much of a call's wall-clock time was a global pause; the seal pass derives it (§5.1), and it is the MVP-critical part that fills the column the UI shows. Per pod-restart, the raw pause timeline drives a "when did this pod stall" view; on pod-restart close the collector persists it next to the dictionary snapshot as `s3://<bucket>/suspend/v1/<yyyy>/<mm>/<dd>/<podRestartHash>.json` = `{ restart_time_ms, timer_start_ms, events: [ { start_ms, duration_ms }, ... ] }`. It is fetched by pod-restart, like the dictionary, so one object per pod-restart is enough. The stream is sparse and tiny; a deployment that does not need the pod-level view can drop this object and keep only the per-call scalar.

### 3.7 Reconnect continuity

A dropped agent connection never heals in place: the agent tears its dumper down to `initialize()` (`lastWrittenDictionaryTag = 0`) and reconnects, re-sending the whole dictionary from index 0 with the reset flag set (`resetRequired = 1`). Each reconnect is a new TCP accept, so the collector stamps a fresh `restartTime` and treats it as a new, independent pod-restart. Because the agent re-sends the full dictionary, that pod-restart is self-contained — the replica that receives it is never dictionary-less, even when it differs from the one that held the previous connection, and replicas need not share dictionary state. The `resetRequired = 1` flag is what tells the collector the incoming dictionary starts from index 0. This is why the collector-stamped `restartTime` at TCP accept (§1 V4) is safe: reconnect does not need cross-connection continuity. Full code trace: `stage0-progress.md` decisions log (2026-07-01).

## 4. Per-call trace blob assembly

### 4.1 Why per-call reassembly

The trace stream is chunk-interleaved (§1 V5): bytes of one root call are scattered across N chunks of the same thread, with chunks of other threads in between. We cannot serve "the bytes for call X" as a single byte range from the raw stream.

The collector extracts each root call's bytes into a contiguous per-call blob and embeds it in the parquet row as the `trace_blob` column. Extraction is **event-level and driven by the Call record**: the collector parses trace events to find chunk and call boundaries. It is not a blind chunk-memcpy — a chunk carries no length prefix, so its end can only be found by parsing events to `EVENT_FINISH_RECORD`, and a root call's end can only be found by tracking call depth to the depth-0 exit (verified against `libs/parser/pipe/traces.go:65-150`).

### 4.2 Chunk model and the three framing levels

Do not conflate three different "chunk" notions:

- **`COMMAND_RCV_DATA` payload** — up to `DATA_BUFFER_SIZE` = 1 KB of one stream's bytes (`proto-definition/.../ProtocolConst.java:4`; the agent chops at `DefaultCollectorClient.java:314`). The collector concatenates these per stream before anything else.
- **Logical trace chunk** — `[threadId:long, startTime:long]` (16 bytes) + events + `EVENT_FINISH_RECORD`, `LocalBuffer`-sized (≈ tens of KB, `LocalBuffer.SIZE = 4096` events). One chunk's body belongs to one thread, and one chunk spans many `RCV_DATA` payloads. The trace stream also opens with a one-time `timerStartTime` (8 bytes) before the first chunk. Event times reconstruct as `timerStartTime + Σ(event deltas)` (`TracePodReader.java:152-179`), so the epoch is only a constant offset on absolute timestamps. It cancels in every time difference: call durations and the relative call tree decode without it. Only absolute wall-clock timestamps need the epoch, and each chunk header's `startTime` is itself an absolute anchor (`Dumper.java:882`), so it is recoverable even when lost. The per-call blob carries it as a prefix (§4.5), so the trace readers decode absolute times exactly and run unmodified.
- **Go `Chunk` type** — a rolling-stream handle in the existing parser, unrelated to either of the above.

Within one logical chunk, multiple short root calls of a thread may open and close (§4.5).

### 4.3 Indexing on the write path, assembly at seal

Blob assembly does not happen on the write path. The write path only captures bytes and builds the index; the seal pass (§6.5) assembles the blobs. This keeps ingest a single sequential append and lets one seal pass decompress each segment exactly once.

Per-thread state held in RAM (mirrored in the SQLite segment catalog for recovery):

- `chunk_index[threadId]` — ordered list of `(segment_file, offset, length)` for that thread's logical chunks still within the hot window.

On the demultiplexed trace stream, the collector first reads the one-time 8-byte `timerStartTime` (§4.2) and holds it per pod-restart for blob framing (§4.5); on recovery it re-reads the value from the first trace segment. Then, as bytes arrive:

1. Append the raw stream to the current gzip segment file on the PV (§4.4); track the running logical offset.
2. Parse forward: read the 16-byte chunk header, then events, to `EVENT_FINISH_RECORD` — this delimits the chunk. Record `(segment_file, offset, length, threadId)` in `chunk_index[threadId]` and in the SQLite segment catalog.

On each `Call` record for thread T (calls stream), keyed by its pointer `(trace_file_index, buffer_offset, record_index)`:

1. Append the full record to `calls.wal`.
2. Insert one row into the SQLite call index: PK, filter columns (§5.2), `bucket = floor(ts_ms)`, the start pointer, and the `calls.wal` offset. Mark `bucket` dirty for the seal loop (§6.1).

No blob is assembled and no parquet is written here. The blob's byte range — walk T's chunk chain from the pointer, tracking depth (`enter +1`, `exit −1`, skip tags) to the depth-0 exit — is resolved later by the seal pass, together with the external value-stream references the blob carries (`bigParams` / `bigParamsDedup`, and any `sql` / `xml`).

Indexing is order-independent: it keys off the Call pointer, not the arrival order of the two streams. A Call record whose trace bytes are not yet fully parsed is still indexed; the seal pass runs only after the bucket's grace, by which point the trace has caught up. Calls with no Call record (filtered by the agent's persistence gate, `Dumper.java`) are never sealed, matching the agent's intent.

### 4.4 Segment files on PV (hot store)

```
/data/pods/<ns>/<svc>/<pod>/<restartTime>/{trace,sql,xml}/<rolling_seq>.gz
```

The hot store is the three offset-addressable bulk streams: `trace`, plus the external value streams `sql` and `xml` that a blob points into. All three are written the same way.

- **One segment file per agent stream file.** The collector opens a segment on each `COMMAND_INIT_STREAM_V2` and names it by the agent's reported stream-file index (see the segment-naming note below); the demultiplexed bytes for that handle are appended and gzip-compressed once, with no WAL double-write (the agent's `<seq>.gz` model, `CompressedLocalAndRemoteOutputStream.java:210-216`). Keeping segments 1:1 with the agent's files lets a Call pointer `(trace_file_index, buffer_offset)` and a trace tag's `(rolling_seq, offset)` resolve by opening `<stream>/<rolling_seq>.gz` and seeking — no offset-translation table. The collector governs segment size through `requiredRotationSize` in the `INIT_STREAM_V2` response; a smaller segment favours partial reads, a larger one favours compression.
- **Addressing.** `trace` chunks are located by the Call pointer (§4.3); `sql` / `xml` values by the `(rolling_seq, offset)` that a `PARAM_BIG_DEDUP` / `PARAM_BIG` trace tag carries (`backend/libs/parser/pipe/traces.go`). The catalog stores each segment's `(stream, rolling_seq)` and decompressed length; `trace` segments also carry a chunk time range.
- **Refcount and eviction.** Refcounted in SQLite (§8): a segment is deletable once every sealed row whose blob sources from it has been uploaded (refcount 0), or once it is evicted under the overload policy. Refcounts span buckets — one segment can carry chunks or values for several buckets' calls.
- **Value segments never reach S3.** The seal pass resolves each `PARAM_BIG` / `PARAM_BIG_DEDUP` reference a blob carries against the `sql` / `xml` segments — guaranteed present at seal by the refcount pinning above — and inlines the values into the row's `big_params_json` column (§5.2, §6.5 step 3). The blob itself keeps the raw references (`02-read-contract.md` §2.4 serves it verbatim); the column is the cold tier's only source for the values. A reference whose segment was already evicted seals without its value, and the read path marks it unresolved (`02-read-contract.md` §2.5) — degraded explicitly, like a truncated blob, never silently.
- These segment files ARE the hot store: `/internal/v1/calls/{pk}/trace` reads them directly, and the internal values endpoint reads `sql` / `xml` for the hot `/tree` rendering (`02-read-contract.md` §3).

**Segment name = the agent's file index, not the echoed id.** The agent addresses its stream files as `serverRollingSequenceId + 1` — the `+ 1` marked "for compatibility" in `CompressedLocalAndRemoteOutputStream.java:171-179` — and writes that value into every Call's `trace_file_index` (`Dumper.java:921`). The collector MUST name each `<rolling_seq>.gz` segment by that same `serverRollingSequenceId + 1`, not by the id it echoed in the `INIT_STREAM_V2` reply (`06-wire-protocol-server.md` §4). Off by one shifts every `(trace_file_index, buffer_offset)` pointer to the neighbouring segment, a silent and total corruption of trace addressing. A synthetic test guards it: after two stream rotations, each Call's `trace_file_index` must resolve to the segment holding its root ENTER.

### 4.5 Reader semantics

The per-call blob is a self-contained chunk stream: the 8-byte `timerStartTime` epoch (§4.2), then a concatenation of full chunks that all carry the SAME `threadId` in their headers. The reader walks events to build the call tree:

- **Tail noise:** the first chunk may begin with events of the PREVIOUS root call of this thread, ending at its depth-0 EXIT. The reader skips events until it reaches the depth-0 ENTER matching the row's `record_index`.
- **Head noise:** the last chunk may end with the start of the NEXT root call of this thread, beginning after the depth-0 EXIT of the call we want. The reader stops at depth-0 EXIT.

Noise is bounded by `LocalBuffer.SIZE` (4096 events ≈ tens of KB per side).

**Framing and the epoch.** The blob opens with the 8-byte `timerStartTime`, mirroring the raw trace stream (§4.2), so the trace readers (`TracePodReader.java:105`, `streams/traces.go:44`) consume it unmodified. The seal pass prepends it during assembly (§6.5). The epoch shifts only absolute timestamps; call durations and the relative tree are independent of it (§4.2), and each chunk header's `startTime` anchors absolute time on its own. A blob whose prefix is lost therefore still decodes to a correct tree, with absolute times recoverable from the headers.

### 4.6 Recovery and budget

**Disk budget:** `PROFILER_CHUNKS_STAGING_MAX_BYTES` (default 10 GB) bounds total trace-segment disk usage per replica. Eviction is class-aware, dropping fast, short-duration calls first:

1. First, drop segments whose refcount is zero.
2. If still over budget, drop the oldest segments even with pending refs. A call that then reaches the seal pass (§6.5) with its source segments gone is sealed with `trace_blob = NULL` and `truncated_reason = disk_budget` (§5.2); metrics counter incremented.

**Memory budget:** `PROFILER_MEM_BUDGET` caps the RAM held by `chunk_index[*]` and the per-call buffers of any running seal pass. Under pressure the collector seals the oldest bucket first to drain its buffers, then applies the class-aware drop above. It never drives the PV to `ENOSPC`; it degrades by dropping the trace body while keeping Call metadata.

**Idle timeout:** `PROFILER_IDLE_ACCUMULATOR_TIMEOUT` (default 10 min) bounds how long a thread's chunk index is held with no new chunks and no Call record. On expiry the index entries are released; the call is treated as never closed and is not sealed (dropped, §4.3) — its Call record was lost or its thread died.

**Crash recovery:** on collector restart, scan the gzip trace segments in `trace/`, re-parse chunk headers and events, and rebuild `chunk_index[*]` and the SQLite segment catalog. `metadata.sqlite` carries refcounts, seal watermarks, and upload checkpoints. Unclosed calls (no Call record received) are dropped, not reconstructed (`03-lifecycle.md` §3.7); the durable copy of any already-sealed call is in parquet + S3.

## 5. Calls stream → parquet

### 5.1 Pipeline

The calls stream feeds two stages separated in time: the write path indexes each record as it arrives; the seal pass (§6.5) materializes the parquet rows once the bucket is complete.

**Time reconstruction.** `ts_ms` is not a wire field. The calls stream encodes each record's start as a zig-zag varint delta from the *previous* record, so the decoder must accumulate. Each calls file opens with a 16-byte header: an 8-byte `[0xFFFEFDFC, version]` marker (`CompressedLocalAndRemoteOutputStream.rotate`) followed by `base_ms`, an absolute Unix-ms epoch written by `CallsCompressedLocalAndRemoteOutputStream.fileRotated` (`Dumper.java:1400`). Reconstruct within the file:

```
ts_ms[0] = base_ms + delta[0]
ts_ms[i] = ts_ms[i-1] + delta[i]      (i >= 1)
```

The running total reseeds at every file boundary: a rotation writes a fresh header and resets the agent-side timer (`Dumper.java:1062-1063`, `1394-1401`). This axis carries the whole pipeline: the `bucket` below, retention (§6.4), the `ts_ms` PK component (§5.2), and the read cursor (`02-read-contract.md`) all key off it. A decoder that reads each delta as an offset from `base_ms` is correct only for the first record and corrupts every record after it. Both Go decoders (`backend/libs/parser/pipe/calls.go`, `backend/libs/parser/streams/calls.go`) accumulate the deltas; `TestCallsTimeAccumulation` in each package guards the reconstruction with a synthetic three-record stream from `backend/libs/tests/helpers/wire`.

**Write path, per `Call` record:**

1. Resolve dictionary words for `Method` and any tag IDs in `Params`; derive `retention_class` from `(duration_ms, error_flag)` (§6.4). Both go into the SQLite filter columns, so hot queries do not need the blob.
2. Append the full record to `calls.wal`; insert the SQLite index row (PK, filter columns, `bucket`, start pointer, `calls.wal` offset); mark `bucket` dirty.

**Seal pass, per `(pod-restart, bucket)`** (§6.5):

3. For each call in the bucket, assemble the blob by its pointer `(trace_file_index, buffer_offset, record_index)` (§4.3) during the segment-ordered walk.
4. Derive `suspend_ms` by intersecting the call's `[ts_ms, ts_ms + duration_ms]` with the pod-restart's stop-the-world pause intervals decoded from `suspend.wal` (§3.6): a linear scan over the bucket window.
5. Write the row — filter columns from the SQLite index, the remaining columns from `calls.wal`, `trace_blob` from the assembly, `suspend_ms` from step 4 — to the retention-class writer for the bucket.

A call whose dictionary entry is missing is sealed with `trace_blob` NULL and `truncated_reason = dict_miss`; a call whose source segments were evicted under the §4.6 budgets is sealed with `trace_blob` NULL and the matching reason. Counters are exposed as Prometheus metrics.

### 5.2 Parquet schema

Starting from the existing `CallParquet` (`backend/libs/storage/parquet/calls.go`) and refining. Each column carries a one-line rationale.

```
schema CallV2 {
  -- identity
  ts_ms             INT64                      -- call start, Unix ms UTC; primary time axis. Reconstructed by accumulating per-record deltas from the file header (§5.1), not a raw wire value
  pod_id            BYTE_ARRAY (UTF8) DICT     -- "<ns>/<service>/<pod>"; dictionary-encoded for compact storage
  restart_time_ms   INT64                      -- pod-restart boundary; dedupe key component
  trace_file_index  INT32                      -- PK component; agent trace-stream file index at the start of the call's bytes
  buffer_offset     INT32                      -- PK component; offset within trace_file_index where the call's first chunk begins
  record_index      INT32                      -- PK component; event index of the root ENTER within that chunk
  thread_name       BYTE_ARRAY (UTF8) DICT     -- thread name; high cardinality bounded by app threadpool size

  -- dimensions
  namespace         BYTE_ARRAY (UTF8) DICT
  service_name      BYTE_ARRAY (UTF8) DICT
  pod_name          BYTE_ARRAY (UTF8) DICT
  method            BYTE_ARRAY (UTF8) DICT     -- root method, resolved from dictionary at write time

  -- metrics (raw, not aggregated)
  duration_ms       INT32                      -- main filter axis
  cpu_time_ms       INT64
  wait_time_ms      INT64
  memory_used       INT64
  queue_wait_ms     INT32
  suspend_ms        INT32                      -- GC/JIT pause time within this call
  child_calls       INT32                      -- count of child method invocations in the trace tree
  transactions      INT32
  logs_generated    INT64
  logs_written      INT64
  file_read         INT64
  file_written      INT64
  net_read          INT64
  net_written       INT64

  -- classification (collector-derived at write time)
  error_flag        BOOLEAN                    -- agent-indicated error; see §5.6
  retention_class   BYTE_ARRAY (UTF8) DICT     -- one of short_clean / normal_clean / long_clean / any_error / corrupted; see §6.4

  -- semi-structured
  params            MAP<UTF8, LIST<UTF8>>      -- per-param key→values, e.g. "request.id" → ["abc123"]
  trace_blob        BYTE_ARRAY                 -- per-call blob assembled at chunk granularity (§4); reader parses against the dictionary; NULL when truncated
  truncated_reason  BYTE_ARRAY (UTF8) DICT     -- NULL on success; one of mem_pressure / disk_budget / idle_timeout / dict_miss (§5.1, §4.6)
  big_params_json   BYTE_ARRAY (UTF8)          -- JSON {"<stream>:<seq>:<offset>": value}: the call's big-param values resolved at seal (§4.4); NULL when none. A scalar column (not a MAP) so the list-path projection drops it like trace_blob
}
```

**Compression and row order.** Every file is ZSTD-compressed. This matters most for `trace_blob` and `params`: the blob is stored uncompressed after seal assembly (§6.5), so the parquet codec is what keeps the cluster's ~6 MB/s of raw trace from landing in S3 unshrunk for the whole TTL. Rows are written sorted by `(ts_ms DESC, pk ASC)`, the total order the read path paginates on (`02-read-contract.md` §2.3.1). Sorting inside the file gives each row group a tight `ts_ms` min/max for pruning and lets the cold-tier k-way merge treat the file as an already-sorted run, with no in-memory re-sort.

**Schema evolution.** The reader (`parquet-go/parquet-go`) matches a file's footer schema to `CallV2` by column NAME: adding a column and removing a column are backward-readable changes — a column missing from an older file reads back as zero/NULL, and a column dropped from the struct is skipped without fetching its chunks. Column names are therefore the compatibility contract: never reuse a name with a different meaning. Non-additive changes — renaming a column, changing its type, or reinterpreting its values — are NOT transparently readable: they need a versioned reader that branches on the `profiler.schema_version` key the seal pass stamps into every file's key-value footer metadata (currently `2`; bumped only by a non-additive change — additive ones do not touch it). That reader is deferred until the first such change (`deferred.md`); it must land before old and new files coexist inside the 30-day retention window.

### 5.3 Differences from old `CallParquet`

| Change | Reason |
|---|---|
| `time` → `ts_ms`, with `_ms` suffix | The unit is the most common confusion source. Always carry it in the name. |
| `restartTime` → `restart_time_ms` | Same. |
| Added `pod_id` derived column | Trivially computable but speeds up filtering and dictionary-encodes well. |
| `Method` stays as resolved string, not `int TagId` | Otherwise every reader needs the dictionary on hand. The cost is dictionary-encoding overhead in parquet — minor. |
| Renamed `Calls` → `child_calls` | "calls" is overloaded with "list of calls"; this is the per-tree counter. |
| Removed `convertedtype=UINT_*` annotations | Parquet's UINT_64 is poorly supported in some readers. INT64 with documented "always non-negative" suffices. |
| `TraceId string "seqId_bufOffset_recordIndex"` → three `INT32` columns (`trace_file_index`, `buffer_offset`, `record_index`) | Better column compression, cheaper integer comparison at dedup time, no string parsing on the read path. Decision recorded; no open question remains. |
| Removed `non_blocking_ms` | No wire source: `writeCall` never emits it and the Go decoder has no field for it (`Dumper.java:1059-1108`, `backend/libs/parser/pipe/calls.go`). Re-adding a column later is additive — backward-readable by name, older rows read as zero; see Schema evolution in §5.2. |

### 5.4 Sharding: time bucket × retention class

Each parquet file covers one (time bucket × retention class × pod-restart):

- **Time bucket:** 5 minutes by default, keyed by `floor(ts_ms)` of the call's start (00:00, 00:05, …), NOT by processing time. Configurable. A call that closes late lands in the bucket of its start; discovery finds it by range overlap on the file's `time_min` / `time_max` (`02-read-contract.md` §5.1), and the `maintain` job compacts the small late files.
- **Retention class:** computed from `(duration_ms, error_flag)` at write time and stored in the SQLite index. Five classes by default — see §6.4.

A seal pass materializes one bucket at a time, opening up to five retention-class writers for the duration of that pass and closing them when it ends. No parquet writers stay open between seals. Late arrivals re-seal the bucket into an additional `<seq>` file (§6.6). `duration_ms` remains a row-level column for query-time filtering.

### 5.5 Path

A seal pass writes to scratch, then the finished file moves to its sealed name:
```
/data/pods/<namespace>/<service>/<podName>/<restartTime>/parquet-sealing/<timeBucket>/<retentionClass>-<seq>.parquet
```

On a clean seal the file is uploaded to S3 (Section 7) and kept locally for `hot_retention` (§6.3). A seal that crashes leaves an unreadable scratch file (no parquet footer); recovery discards it and re-seals the bucket. `<seq>` distinguishes the original seal from later late-arrival patches (§6.6).

### 5.6 error_flag derivation

The agent marks a call as errored through `ExceptionLogger.callRed()` (typically from a caught exception, `boot/src/main/java/com/netcracker/profiler/agent/ExceptionLogger.java:29-35`), which records the indexed parameter `call.red` on the call. `call.red` is an indexed parameter in every targeted deployment (`installer/.../config/_config.xml`), so it is serialized into the Call record's params and the Go decoder already reads it into `Call.Params` (`backend/libs/parser/pipe/calls.go`). No agent change and no new struct field are needed.

At seal, the collector resolves the dictionary id of the literal `call.red` and sets:

```
error_flag := dictId("call.red") ∈ keys(Call.Params)
```

`callInfo.isPersist` is not an error flag — it is the persistence gate the agent uses to decide whether to emit a Call record at all (`Dumper.java:945-947`), so it is not folded in.

`callInfo.isCorrupted` is not available on the read path: a corrupted call never becomes a Call record (the same `Dumper.java:945-947` gate excludes it), so the collector never sees one. The `corrupted` retention class (§6.4) therefore stays reserved but empty in the MVP; keeping its key space reserved means that surfacing corruption later, if the agent ever emits it, would not re-partition history.

**Seal is authoritative; the write-time value is provisional.** `error_flag` and the `retention_class` it feeds are also computed on the write path (§5.1) into the SQLite index, but only for hot-tier filtering. That value is provisional: `dictId("call.red")` is unknown until the dictionary entry arrives, and the `dictionary` and `calls` streams decode on independent pipelines, so the first errored call of a pod-restart can be indexed before `call.red` resolves. The seal pass re-derives both from the call's raw parameter ids against the complete dictionary — guaranteed present by the bucket-close grace — and is the sole source of truth for the S3 key (§7) and the parquet `retention_class` column. For this to hold, `calls.wal` must retain raw parameter dictionary ids, not resolved names: a dictionary miss at write time must stay recoverable at seal.

## 6. Seal semantics

Parquet is produced by a seal pass, never on the write path (§4.3, §5.1). A seal loop watches the SQLite index and runs one pass per `(pod-restart, bucket)`.

### 6.1 Seal triggers

A bucket is sealed when **any** of:

1. Its time bucket has ended and `time_bucket_grace` has elapsed (wall-clock ≥ bucket end + grace). Default `time_bucket_grace = 30 s`, enough to absorb late Call arrivals from the agent's flush window.
2. A late Call re-marks an already-sealed bucket dirty (§6.6); the loop re-seals it into a patch file.
3. Memory pressure: the collector seals the oldest dirty bucket early to drain its `chunk_index` and seal-pass buffers (§4.6).

Within one pass, output larger than `parquet_max_size` (default 64 MB) is split into successive `<seq>` files. Concurrency across buckets is bounded by `PROFILER_SEAL_CONCURRENCY`.

### 6.2 Atomic upload

Upload sequence, per file a seal pass finishes:

1. Close the writer; move the scratch file from `parquet-sealing/` to its sealed name. The file now has a valid footer and is fully on disk.
2. Record it in `metadata.sqlite` (`parquet_local`: path, retention_class, time_bucket_end, `time_min` / `time_max`, row_count, `uploaded_at NULL`) and advance the bucket's seal watermark.
3. PUT to S3 with `Content-MD5`. On 200 OK, set `uploaded_at` and decrement segment refcounts (§4.4). **The local file is not deleted here** — it serves the hot tier for `hot_retention` past upload (§6.3).
4. On any S3 error, retry with exponential backoff. The local file remains until upload succeeds.

If the collector crashes mid-seal, the scratch file has no footer: recovery discards it and re-seals the bucket. If it crashes after a clean seal but before upload, the sealed file is valid and is re-uploaded from `parquet_local`. Both are idempotent at the S3 layer because the object key is deterministic (Section 7).

### 6.3 Hot retention of local parquet

After a successful upload, local parquet files are retained for `PROFILER_HOT_RETENTION` (default `15m`) to back the collector's hot-read API (`02-read-contract.md` §4.2). A janitor goroutine deletes files where `now > uploaded_at + hot_retention` and removes the corresponding `parquet_local` row.

`hot_retention ≥ seal_interval + overlap_margin` must hold, or a query is not guaranteed to see every Call from at least one tier during the overlap window.

### 6.4 Retention class

Each parquet file holds rows of exactly one retention class. Maintenance applies per-class TTL.

Default mapping (configurable per-deployment):

| Class | Condition | Default TTL |
|---|---|---|
| `short_clean` | `duration_ms < 100` AND `!error_flag` | 1 day |
| `normal_clean` | `100 ≤ duration_ms < 1000` AND `!error_flag` | 7 days |
| `long_clean` | `duration_ms ≥ 1000` AND `!error_flag` | 30 days |
| `any_error` | `error_flag = true` (any duration) | 30 days |
| `corrupted` | reserved; not populated in the MVP — the agent does not emit corrupted calls (§5.6) | 7 days |

The classifier runs per Call record at write time; `retention_class` is stored in the SQLite index, and the seal pass routes each row to the matching one of up to five retention-class writers for the bucket. Maintenance reads `<retentionClass>` from the S3 object key (§7); it does not open parquet files to apply TTL.

`any_error` is populated from `error_flag` (§5.6, the `call.red` param). `corrupted` stays a distinct but currently-empty bucket; keeping its key space reserved means emitting corrupted calls later would not re-partition history.

### 6.5 The seal pass

A seal pass for one `(pod-restart, bucket)`:

1. Reads the bucket's calls from the SQLite index (rows with this `bucket`).
2. Collects the segments those calls reference from the segment catalog and walks them **in segment order**. Each segment is decompressed exactly once; its chunks are routed into the per-call blob under assembly, following each call's chunk chain to the depth-0 exit (§4.3).
3. Finalizes a blob when its last chunk is read: prefixes it with the pod-restart's 8-byte `timerStartTime` (§4.5), reads the call's remaining columns from `calls.wal` by offset, resolves the external value-stream references into `big_params_json` (§4.4 — each referenced `sql` / `xml` segment is read once per pass, references in offset order), re-derives `error_flag` and `retention_class` from the call's raw parameter ids against the complete dictionary (§5.6, not the provisional write-time value), and appends the row to the writer for that class (§6.4). The per-call buffer is then freed.
4. Closes the writers, uploads (§6.2), advances the seal watermark, and releases segment refcounts.

Because the walk is segment-ordered rather than call-ordered, no segment is decompressed twice within a pass, however many or however long the calls that reference it. Peak buffer memory is the trace volume of calls still open across the segment cursor — dominated by long calls, capped by `PROFILER_MEM_BUDGET`, with overflow spilling to a temp file under `parquet-sealing/`.

### 6.6 Late data and compaction

A sealed bucket is immutable in S3 (§6.2), so late data never rewrites an existing file:

1. A late Call — one whose `floor(ts_ms)` falls in an already-sealed bucket — appends to `calls.wal`, inserts its SQLite index row, and re-marks the bucket dirty.
2. The seal loop re-seals the bucket, emitting a **patch file** with a fresh `<seq>` for the same `(bucket, retention_class, pod-restart)`. The watermark records which calls each seal covered, so a re-seal writes only the new rows.
3. S3 now holds the original file plus its patches. They share one `<timeBucketStart>`, but each carries its own `<timeMin>` / `<timeMax>` over the rows it holds, so range-overlap discovery finds them all (`02-read-contract.md` §5.1); PK-dedup makes concurrent visibility safe (`02-read-contract.md` §6).
4. The `maintain` job compacts a bucket S3-side, in two forms:
   - **Patch compaction** merges a `(bucket, retention_class, pod-restart)`'s original file and its patches into fewer objects, cutting the per-bucket file count (the `f` factor in `02-read-contract.md` §5.5).
   - **Cross-pod-restart compaction** merges the small per-pod-restart files of one `(bucket, retention_class)` into a single object once they accumulate, regardless of pod-restart. Each row keeps its own PK (`pod_*`, `restart_time_ms`), so a mixed-pod-restart file needs no read-path coordination: dedup, dictionary resolution (`02-read-contract.md` §2.6), and PK lookup all key off the row, not the file. This is the only lever that cuts the pod-restart factor `P` in the object count (`02-read-contract.md` §5.5). Both forms stay within one `retention_class`, so the per-class TTL (§6.4) still applies by object key.

   **Reader safety without a lock.** Compaction writes the compacted object first, then deletes the inputs, and delays that delete by `PROFILER_COMPACTION_DELETE_GRACE` (default `5m`, §9). The grace, not the write-then-delete order alone, is what guarantees completeness: a query whose LIST saw the inputs before the compacted object existed still finds those inputs in S3 when it reads them, because one discovery-plus-read round — each page re-LISTs (`02-read-contract.md` §2.3.1) — is far shorter than the grace. A query that lists after the compacted object exists sees it by S3 read-after-write consistency. Within the grace both copies are visible and PK-dedup collapses the overlap. As a backstop for a read that still outlives the grace, discovery treats a `404` on a listed object as empty, not an error (`02-read-contract.md` §5.1).

Blob completeness for a late Call is bounded by segment survival (§4.6): the row is always sealed, but `trace_blob` is `NULL` with a `truncated_reason` once the source segments have been evicted.

## 7. S3 object layout

Path pattern:
```
s3://<bucket>/parquet/v1/<retentionClass>/<yyyy>/<mm>/<dd>/<hh>/<replica>-<podRestartHash>-<timeBucketStart>-<timeMin>-<timeMax>-<seq>.parquet
```

- `v1` — schema version. Bump on incompatible changes.
- `<retentionClass>` — one of `short_clean` / `normal_clean` / `long_clean` / `any_error` / `corrupted`. Maintenance applies per-class TTL by listing this segment (§6.4). Filenames sort chronologically within a class.
- Date hierarchy `<yyyy>/<mm>/<dd>/<hh>` — primary access pattern is "give me parquet for time range [t1, t2]"; this hierarchy makes that a small LIST.
- `<replica>` — the producer. For a write-path seal it is the StatefulSet ordinal (`collector-0`, `collector-1`, …), so distinct replicas don't collide on object keys; a `maintain` compaction (§6.6) uses the reserved token `maintain`.
- `<podRestartHash>` — short hash of `(namespace, service, podName, restartTime)`, identifying the pod-restart that produced the file. A cross-pod-restart compaction (§6.6) covers several pod-restarts, so it substitutes a short hash of its inputs; the per-row `pod_*` / `restart_time_ms` columns stay the authoritative pod-restart identity either way.
- `<timeBucketStart>` — the bucket's start as `yyyymmddTHHMMSSZ`. Fixes the file's bucket identity: the patch files (§6.6) and size-split `<seq>` files of one bucket share it, and it keeps the key chronologically sortable within a class.
- `<timeMin>` / `<timeMax>` — the file's actual `min(ts_ms)` / `max(ts_ms)`, as `yyyymmddTHHMMSSZ`. The seal pass computes both for `metadata.sqlite` (§6.2), so carrying them in the key is free and lets range discovery test overlap straight from the `ListObjectsV2` result, with no footer read and no per-object HEAD (`02-read-contract.md` §5.1). Both lie inside `[timeBucketStart, timeBucketStart + PROFILER_TIME_BUCKET)`. The key stays deterministic: the late-data watermark (§6.6) fixes each `<seq>`'s row set, so a re-seal regenerates the same `<timeMin>` / `<timeMax>`. Because the stamp is second-granularity while `ts_ms` is milliseconds, `<timeMin>` is floored to its second and `<timeMax>` is ceiled to the end of its second (`+999 ms`); the overlap test (`02-read-contract.md` §5.1) reads the key range as inclusive at both ends. Without the `<timeMax>` ceiling, a query whose lower bound falls inside the file's last second skips rows the file actually holds.
- `<seq>` — sequence number when one bucket spawned multiple files via size trigger.

Example:
```
s3://profiler-data/parquet/v1/normal_clean/2026/04/23/14/collector-2-a7f3-20260423T140000Z-20260423T140003Z-20260423T140457Z-0.parquet
```

Why date in the path even though `ts_ms` is in the file: query needs to LIST efficiently. Filtering by reading every parquet file's footer to check time range is too expensive at scale.

## 8. Local PV layout (full)

```
/data/
  pods/
    <namespace>/<service>/<podName>/<restartTime>/
      dictionary.wal
      params.wal
      suspend.wal
      calls.wal              # decoded Call records (raw param dict-ids retained for seal re-derivation, §5.6); source for the seal pass and hot single-row fetches
      trace/
        000000.gz            # raw interleaved trace stream, gzip segments (hot store); one file per agent rolling_seq
        000001.gz
        ...
      sql/                   # external value stream (PARAM_BIG_DEDUP), one gzip segment per agent rolling_seq
        000000.gz
      xml/                   # external value stream (PARAM_BIG), one gzip segment per agent rolling_seq
        000000.gz
      parquet-sealing/       # scratch for the running seal pass; a crashed seal's files are discarded and re-sealed
        20260423T140000Z/
          short_clean-0.parquet
          normal_clean-0.parquet
          long_clean-0.parquet
          any_error-0.parquet
          corrupted-0.parquet
  upload-failed/             # parquet that S3 rejected and needs human attention
    ...
  collector.lock             # exclusive PV ownership (one collector replica)
  metadata.sqlite            # segment catalog, refcounts, seal watermarks, upload checkpoints, call-partition catalog
  calls-20260423T140000Z.sqlite  # per-bucket call index (call_index table); ATTACHed for reads, dropped past hot_retention
  calls-20260423T140500Z.sqlite
  ...
```

`collector.lock` is a flock'd file written at startup. Prevents two collector processes from sharing a PV — critical when `volumeClaimTemplates` is misconfigured.

## 9. Configuration

| Env | Default | Description |
|---|---|---|
| `PROFILER_DATA_DIR` | `/data` | Root of the local PV. |
| `PROFILER_TIME_BUCKET` | `5m` | Parquet time bucket length. |
| `PROFILER_TIME_BUCKET_GRACE` | `30s` | Wait after bucket end before the seal pass runs (§6.1). |
| `PROFILER_PARQUET_MAX_SIZE` | `64MB` | Size at which a seal pass splits its output into a new `<seq>` file. |
| `PROFILER_SEAL_CONCURRENCY` | `4` | Maximum seal passes running in parallel (§6.1). |
| `PROFILER_MEM_BUDGET` | `2GB` | Soft memory budget for `chunk_index` plus running seal-pass buffers (§4.6). |
| `PROFILER_CHUNKS_STAGING_MAX_BYTES` | `10GB` | Total disk budget for trace segment files (§4.6). |
| `PROFILER_SEGMENT_ROTATION_SIZE` | `4MB` | Segment size the collector requests from the agent via `requiredRotationSize` in the `INIT_STREAM_V2` response (§4.4). Segments stay 1:1 with agent files, so the collector does not split them. |
| `PROFILER_IDLE_ACCUMULATOR_TIMEOUT` | `10m` | Release a thread's chunk index when no chunk arrives for that thread within the window; its unclosed call is not sealed (§4.6). |
| `PROFILER_DICT_FSYNC_RECORDS` | `256` | Dictionary WAL fsync trigger by record count. |
| `PROFILER_DICT_FSYNC_INTERVAL` | `100ms` | Dictionary WAL fsync trigger by time. |
| `PROFILER_DURATION_THRESHOLDS` | `100ms,1s` | Boundaries for retention class derivation (§6.4). |
| `PROFILER_RETENTION_SHORT_CLEAN_TTL` | `1d` | TTL for `short_clean` class. |
| `PROFILER_RETENTION_NORMAL_CLEAN_TTL` | `7d` | TTL for `normal_clean` class. |
| `PROFILER_RETENTION_LONG_CLEAN_TTL` | `30d` | TTL for `long_clean` class. |
| `PROFILER_RETENTION_ANY_ERROR_TTL` | `30d` | TTL for `any_error` class. |
| `PROFILER_RETENTION_CORRUPTED_TTL` | `7d` | TTL for `corrupted` class. |
| `PROFILER_RETENTION_DICTIONARY_TTL` | `35d` | TTL for S3 dictionary snapshots (§3.6). Must exceed the longest parquet retention class. |
| `PROFILER_HOT_RETENTION` | `15m` | Local parquet retention past flush (§6.3 and `02-read-contract.md` §4.2). |
| `PROFILER_COMPACTION_DELETE_GRACE` | `5m` | Delay before a `maintain` compaction deletes its input objects, after the compacted object is written (§6.6). Must exceed one discovery-plus-read round so a concurrent query never loses rows mid-compaction. |
| `S3_ENDPOINT` | — | MinIO/S3 endpoint URL. |
| `S3_BUCKET` | — | Target bucket. |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | — | Credentials. |
| `S3_PATH_PREFIX` | `parquet/v1` | Object key prefix below the bucket. |
| `STATEFULSET_ORDINAL` | (from `HOSTNAME`) | Used in S3 object key. |

## 10. What this contract does not cover

These are intentional gaps to be addressed by other documents:

- Recovery sequence on startup (read WAL, re-attempt pending uploads, etc.) → `03-lifecycle.md`.
- Read API for hot data → `02-read-contract.md`.
- Heap and thread dumps → out of scope; still served by `dumps-collector` until Stage C5. (The `sql` and `xml` value streams are *in* scope — a blob references them, so they are hot-store segments alongside `trace`; §4.4.)
- Maintenance retention rules (per-bucket TTL, cleanup of S3) → covered briefly in main plan, detailed in maintenance design when Stage 2 begins.

## 11. Review checklist

Before this document is merged and Stage 1 starts, please confirm or correct:

- [x] V1–V6 verified against agent code (§1).
- [x] `restartTime` source — collector-stamped on TCP accept (§3.4).
- [x] Trace-bytes extraction strategy — per-call chunk-level reassembly (§4).
- [x] `trace_id` column shape — three `INT32` columns (§5.2, §5.3).
- [x] `error_flag` source — the `call.red` indexed param resolved from `Call.Params` (§5.6); no agent change.
- [x] Default retention class TTLs and duration thresholds (§6.4, §9) — defaults accepted.
- [ ] S3 path structure (§7) — operational fit.
- [x] Dictionary cold-path lifecycle — final snapshot uploaded to S3 on pod-restart close (§3.6); local WAL purged after upload + grace.
- [x] Calls-stream `ts_ms` reconstruction — delta accumulation specified (§5.1, §5.2) and implemented in both Go decoders, guarded by `TestCallsTimeAccumulation`.
- [ ] Configuration defaults (§9).

Follow-ups out of scope for this contract:

- Consolidate `backend/libs/parser/streams/` into `backend/libs/parser/pipe/` (decision 8 in `profiler-plan.md`).
