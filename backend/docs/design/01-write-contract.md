# 01 — Write contract

> Status: **draft**, awaiting review. Items marked **[verify]** depend on agent-side behavior the document author has not yet confirmed. They block Stage 1.

This document defines what the new Go collector writes to local PV and to S3, and on which events. It is the source of truth for Stages 1, 2, 6.

## 1. Background: what the agent actually sends

The agent opens a long-lived TCP connection to the collector and multiplexes seven named streams over it (`backend/libs/protocol/streams.go`). Each stream is a sequence of binary chunks delivered via `COMMAND_RCV_DATA` (`backend/libs/parser/parser.go`).

| Stream | Contents | Cardinality |
|---|---|---|
| `dictionary` | Append-only `(int → utf-8 string)` map. Method names, class names, tag names. | One per pod-restart |
| `params` | Parameter metadata: `(name, isIndex, isList, order, signature)`. | One per pod-restart |
| `suspend` | GC / JIT pause events `(time, delta, amount)`. | One per pod-restart |
| `calls` | One record per **closed root call** (e.g. one HTTP request handler invocation). Carries per-call summary metrics + a back-reference into the `trace` stream (`TraceFileIndex`, `BufferOffset`, `RecordIndex`). | Many per pod-restart |
| `trace` | Binary log of `methodEnter` / `methodExit` events. Children are written before their parent's close. | Many per pod-restart |
| `sql`, `xml` | Captured payload bodies referenced from calls. | Many per pod-restart |

Important consequence: **the collector does not assemble calls.** A `Call` record arrives only when the root call has closed on the agent side. The collector's job is to demultiplex streams, persist them, and emit a parquet row per `Call`.

### Open verifications **[verify]**

V1. Confirm: a `Call` record is sent only after the root call has closed and all its child trace bytes have been transmitted on the `trace` stream. If not, we need a different sequencing model.

V2. Confirm: `(TraceFileIndex, BufferOffset, RecordIndex)` uniquely identifies the byte range in the `trace` stream that belongs to one root call. If the agent doesn't communicate the byte length, we infer it from "everything in this trace file from this offset until the next root-call boundary".

V3. Confirm: dictionary entries are append-only and never reused with a different value within one pod-restart.

V4. Confirm: a pod-restart boundary is signalled by a new TCP connection with a new `COMMAND_GET_PROTOCOL_VERSION_V2`, and the dictionary/params state resets at that boundary.

V5. Confirm: trace stream bytes for one root call are emitted contiguously in the stream (no interleaving from a sibling call on a different thread). If interleaving is allowed, we need the agent to tag trace events with a thread/call id, or accept that "extracting bytes for one call" requires more bookkeeping.

V6. Confirm: a single agent process never spans multiple `(namespace, service, podName)` tuples — i.e. one TCP connection ↔ one pod ↔ one namespace/service/pod triple.

These need to be checked in the agent code (`agent/`, `dumper/`, `runtime/`) before Stage 1. Asking for help: a one-shot answer from someone familiar with the agent would unblock the whole document.

## 2. What the collector persists

There are four distinct artifacts. They live in different storage tiers because they have different access patterns, durability requirements, and lifetimes.

| Artifact | Storage | Why |
|---|---|---|
| Dictionary WAL | Local RWO PV, append-only file | Required for restart recovery. Without it, after a collector restart the trace bytes already received but not yet decoded are unreadable. |
| Trace stream cache | Local RWO PV, append-only files | Holds raw trace bytes until a `Call` record arrives that references them. |
| Pending parquet | Local RWO PV, parquet writer state | Current "writing" parquet file for each duration bucket. Closed and uploaded on flush. |
| Closed parquet | S3 | Read by query (cold path) and by maintenance (retention). |

In-memory state is small: dictionary (full copy for fast lookups), open parquet writers, and a tiny index from `TraceIndex` → `(file, offset)` for trace lookups. There is no in-memory call-tree assembly.

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

`<restartTime>` is a Unix milliseconds string from `COMMAND_GET_PROTOCOL_VERSION_V2`'s payload **[verify: agent currently emits this — check]**. If not currently emitted, agent change is required.

### 3.5 Lifetime

The WAL is deleted when the corresponding pod-restart is fully flushed: all `Call` records for it are written to parquet, parquet uploaded to S3, and a hold-back grace period (default 1 hour) elapses to allow late-arriving Calls.

`params` and `suspend` streams use the same WAL pattern with separate files (`params.wal`, `suspend.wal`).

## 4. Trace stream cache

### 4.1 Why

`Call` records carry a `(TraceFileIndex, BufferOffset, RecordIndex)` back-reference. When a Call arrives, the collector needs to look up the corresponding trace bytes to embed them in the parquet row.

The cache is the on-disk store of recent trace bytes, keyed by `(pod-restart, traceFileIndex)`.

### 4.2 Format

Files mirror the agent's own rolling trace files. The agent rotates trace files; on rotation it sends a new chunk on the `trace` stream **[verify]**. The collector creates one file per `traceFileIndex`:

```
/data/pods/<namespace>/<service>/<podName>/<restartTime>/trace/<traceFileIndex>.bin
```

Append-only. No length-prefix per event — the agent's binary format is self-delimiting **[verify: confirm by reading the trace decoder, which lives in maintenance today — `backend/libs/storage/parquet/...` and call decode logic]**.

### 4.3 Lookup

When a `Call` record arrives:

1. Open `trace/<call.TraceFileIndex>.bin`.
2. Seek to `call.BufferOffset`.
3. Read until the next root-call boundary (determined by **[verify V2]** — either a length carried in the Call record, or the next `BufferOffset` from a subsequent Call, or a structural marker in the trace bytes).
4. Embed those bytes as the `Trace` column in the parquet row.

For performance, keep an in-memory index `(traceFileIndex, bufferOffset) → (file, offset, length)` so step 1–2 is constant time.

### 4.4 Lifetime

A trace file is deletable once **all** Call records that reference it have been flushed to parquet (and parquet uploaded to S3 with grace period). Tracking: a refcount per trace file, decremented when a referencing Call's parquet flushes successfully.

If the refcount stays at zero for longer than `idle_trace_file_timeout` (default 10 min after last byte received), the file is dropped — these are calls whose Call record never arrived (long-running, killed, or lost).

### 4.5 Disk budget

Configurable per-replica: `TRACE_CACHE_MAX_BYTES` (default 10 GB). Eviction policy when budget is hit:

1. First, drop trace files past their idle timeout (no harm — they have no pending references).
2. If still over budget, drop the oldest trace files even if they have pending Call references. Affected calls fail to render and are logged.

This is the spill scenario from the high-level plan, expressed at a lower level. The "spill SQLite" in the high-level plan is not needed: the trace files on disk **are** the spill.

## 5. Calls stream → parquet

### 5.1 Pipeline

For each `Call` record arriving on the calls stream:

1. Resolve dictionary words for `Method` and any tag IDs in `Params`.
2. Look up trace bytes from the trace cache (Section 4.3).
3. Pick the target parquet file based on the call's duration (Section 5.3) and current time bucket (Section 5.4).
4. Append a row to that parquet's open writer.

Calls that fail step 1 (missing dictionary entry) or step 2 (trace bytes not in cache) are logged with reason and dropped. Counter exposed as Prometheus metric.

### 5.2 Parquet schema

Starting from the existing `CallParquet` (`backend/libs/storage/parquet/calls.go`) and refining. Each column carries a one-line rationale.

```
schema CallV2 {
  -- identity
  ts_ms             INT64                      -- call start, Unix ms UTC; primary time axis
  pod_id            BYTE_ARRAY (UTF8) DICT     -- "<ns>/<service>/<pod>"; dictionary-encoded for compact storage
  restart_time_ms   INT64                      -- pod-restart boundary; dedupe key component
  trace_id          BYTE_ARRAY (UTF8)          -- "<traceFileIndex>_<bufferOffset>_<recordIndex>"; PK component
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
  non_blocking_ms   INT64                      -- agent-specific
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

  -- semi-structured
  params            MAP<UTF8, LIST<UTF8>>      -- per-param key→values, e.g. "request.id" → ["abc123"]
  trace_blob        BYTE_ARRAY                 -- raw trace bytes for this call's tree, decoded against dictionary at query time
}
```

### 5.3 Differences from old `CallParquet`

| Change | Reason |
|---|---|
| `time` → `ts_ms`, with `_ms` suffix | The unit is the most common confusion source. Always carry it in the name. |
| `restartTime` → `restart_time_ms` | Same. |
| Added `pod_id` derived column | Trivially computable but speeds up filtering and dictionary-encodes well. |
| `Method` stays as resolved string, not `int TagId` | Otherwise every reader needs the dictionary on hand. The cost is dictionary-encoding overhead in parquet — minor. |
| Renamed `Calls` → `child_calls` | "calls" is overloaded with "list of calls"; this is the per-tree counter. |
| Removed `convertedtype=UINT_*` annotations | Parquet's UINT_64 is poorly supported in some readers. INT64 with documented "always non-negative" suffices. |
| `TraceId string "seqId_bufOffset_recordIndex"` → three INT32 columns? | **Open question — see Q-A below.** |

**Q-A:** carry `trace_id` as a single string `"a_b_c"`, or split into three integers (`trace_file_index`, `buffer_offset`, `record_index`)? Strings are simpler to carry; three integers compress better and dedupe in query is cheaper. Recommendation: split. Need decision before Stage 1.

### 5.4 Time bucket and duration bucket

Each parquet file covers one (time bucket × duration bucket × pod-restart):

- **Time bucket:** 5 minutes by default, aligned to wall clock (00:00, 00:05, …). Configurable.
- **Duration bucket:** computed from `duration_ms` at write time. Default thresholds (in ms): `<10`, `10–100`, `100–1000`, `1000–10000`, `≥10000`. Five buckets. Configurable.

So for each pod-restart, at any given moment there are up to five open parquet writers (one per duration bucket) for the current time bucket.

### 5.5 Path

Local pending:
```
/data/pods/<namespace>/<service>/<podName>/<restartTime>/parquet-pending/<timeBucket>/<durationBucket>.parquet
```

After flush, uploaded to S3 (Section 7).

## 6. Flush semantics

### 6.1 Triggers

A pending parquet file is closed and uploaded when **any** of:

1. Its time bucket has ended (current wall-clock time ≥ bucket end + `time_bucket_grace`). Default `time_bucket_grace = 30 s` to absorb late Call arrivals.
2. Its file size exceeds `parquet_max_size` (default 64 MB). New writer for the same bucket is opened and continues with a sequence suffix.
3. Memory pressure: the collector exceeds `mem_budget` and selects largest parquet writers to evict early.

Trigger 3 is rare in practice — parquet writers buffer little (row groups are small).

### 6.2 Atomic upload

Upload sequence:

1. Close the parquet writer locally → file is fully on disk.
2. PUT to S3 with `Content-MD5`.
3. On 200 OK, delete the local file and decrement refcounts on referenced trace files.
4. On any S3 error, retry with exponential backoff. The local file remains until upload succeeds.

The refcount decrement happens **after** S3 confirms the write. If the collector crashes between local close and S3 upload, on restart we re-read pending parquet files and re-attempt upload. Idempotent at the S3 layer because the object key is deterministic (Section 7) — re-uploading the same file produces the same object.

## 7. S3 object layout

Path pattern:
```
s3://<bucket>/parquet/v1/<duration_bucket>/<yyyy>/<mm>/<dd>/<hh>/<replica>-<podRestartHash>-<timeBucketStart>-<seq>.parquet
```

- `v1` — schema version. Bump on incompatible changes.
- `<duration_bucket>` — `lt10ms`, `10ms-100ms`, `100ms-1s`, `1s-10s`, `gte10s`. Filenames must sort chronologically within a bucket.
- Date hierarchy `<yyyy>/<mm>/<dd>/<hh>` — primary access pattern is "give me parquet for time range [t1, t2]"; this hierarchy makes that a small LIST.
- `<replica>` — the StatefulSet ordinal (`collector-0`, `collector-1`, …). Ensures distinct replicas don't collide on object keys.
- `<podRestartHash>` — short hash of `(namespace, service, podName, restartTime)`. Distinguishes pod-restarts.
- `<timeBucketStart>` — the bucket's start as `yyyymmddTHHMMSSZ`.
- `<seq>` — sequence number when one bucket spawned multiple files via size trigger.

Example:
```
s3://profiler-data/parquet/v1/100ms-1s/2026/04/23/14/collector-2-a7f3-20260423T140000Z-0.parquet
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
      trace/
        0.bin
        1.bin
        ...
      parquet-pending/
        20260423T140000Z/
          lt10ms.parquet
          10ms-100ms.parquet
          ...
  upload-failed/             # parquet that S3 rejected and needs human attention
    ...
  collector.lock             # exclusive PV ownership (one collector replica)
```

`collector.lock` is a flock'd file written at startup. Prevents two collector processes from sharing a PV — critical when `volumeClaimTemplates` is misconfigured.

## 9. Configuration

| Env | Default | Description |
|---|---|---|
| `PROFILER_DATA_DIR` | `/data` | Root of the local PV. |
| `PROFILER_TIME_BUCKET` | `5m` | Parquet time bucket length. |
| `PROFILER_TIME_BUCKET_GRACE` | `30s` | Wait after bucket end before flush. |
| `PROFILER_PARQUET_MAX_SIZE` | `64MB` | Size trigger for early flush. |
| `PROFILER_MEM_BUDGET` | `2GB` | Soft memory budget for in-flight buffers. |
| `PROFILER_TRACE_CACHE_MAX_BYTES` | `10GB` | Disk budget for trace stream cache. |
| `PROFILER_IDLE_TRACE_TIMEOUT` | `10m` | Drop trace files with no recent activity and no pending Call references. |
| `PROFILER_DICT_FSYNC_RECORDS` | `256` | Dictionary WAL fsync trigger by record count. |
| `PROFILER_DICT_FSYNC_INTERVAL` | `100ms` | Dictionary WAL fsync trigger by time. |
| `PROFILER_DURATION_BUCKETS` | `10ms,100ms,1s,10s` | Comma-separated thresholds (in addition to `<min` and `≥max`). |
| `S3_ENDPOINT` | — | MinIO/S3 endpoint URL. |
| `S3_BUCKET` | — | Target bucket. |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | — | Credentials. |
| `S3_PATH_PREFIX` | `parquet/v1` | Object key prefix below the bucket. |
| `STATEFULSET_ORDINAL` | (from `HOSTNAME`) | Used in S3 object key. |

## 10. What this contract does not cover

These are intentional gaps to be addressed by other documents:

- Recovery sequence on startup (read WAL, re-attempt pending uploads, etc.) → `03-lifecycle.md`.
- Read API for hot data → `02-read-contract.md`.
- Heap/thread dump streams (`sql`, `xml`, dumps) → out of scope for now; remains served by `dumps-collector` until Stage C5.
- Maintenance retention rules (per-bucket TTL, cleanup of S3) → covered briefly in main plan, detailed in maintenance design when Stage 2 begins.

## 11. Review checklist

Before this document is merged and Stage 1 starts, please confirm or correct:

- [ ] Items V1–V6 in Section 1.
- [ ] `restartTime` source from agent (Section 3.4).
- [ ] Trace bytes extraction strategy V2 (Section 4.3).
- [ ] `trace_id` column shape — single string vs three integers (Section 5.3 Q-A).
- [ ] Default duration bucket thresholds (Section 5.4) — are these reasonable for typical workloads?
- [ ] S3 path structure (Section 7) — does this match operational expectations?
- [ ] Configuration defaults (Section 9) — any obviously wrong defaults?
