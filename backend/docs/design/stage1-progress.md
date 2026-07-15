# Stage 1 progress

Stage 1 is the collector write path: ingest the agent streams, persist them in
the hot store, seal parquet, and recover after a restart (`profiler-plan.md`
Stage 1, contracts `01-write-contract.md`, `03-lifecycle.md`,
`06-wire-protocol-server.md`). This document tracks status, decisions, and open
issues, per `WORKFLOW.md` §7. A full `stage1-plan.md` task breakdown is still
pending (see open issues).

## Status

- [x] **Ingest → hot store** (first slice; branch `feat/stage1-hot-store-ingest`)
  - [x] `libs/collector/hotstore` — WAL writer/replayer (`01` §3.2–§3.3: varint framing, fsync N/T, CRC32 footer, torn-tail truncate)
  - [x] `libs/collector/hotstore` — gzip segments for `trace`/`sql`/`xml`, named `serverRollingSequenceId + 1` (`01` §4.4)
  - [x] `libs/collector/hotstore` — `metadata.sqlite` + per-bucket `calls-<bucketStart>.sqlite` call index (`03` §3.2; `parquet_local`/`seal_state` created but unused until the seal pass)
  - [x] `libs/collector/hotstore` — recovery: close open pod-restarts, replay WALs, rescan segments into the catalog and `chunk_index[threadId]`, reconcile `calls.wal` against partitions (`03` §3.3–§3.5)
  - [x] `libs/collector/ingest` — `server.Listener` routing demuxed streams: trace tee (segment append + chunk parse), calls decode → `calls.wal` + index row (`ts_ms` delta accumulation, retention class, `call.red` error flag), dictionary/params/suspend → WALs
  - [x] `libs/collector` — `oklog/run` service composing the store and the TCP listener (dumps-collector pattern)
  - [x] `libs/server` — `RestartTimeMs` stamped at TCP accept (`01` §1 V4); `PodDisconnected` callback; listener errors propagate to `ACK_ERROR_MAGIC` / null-UUID teardown (`06` §6); `Stop()` waits for connection teardown
  - [x] Synthetic integration test `libs/tests/integration/hotstore_test.go`: segment naming + pointer resolution (M7), `ts_ms` accumulation across buckets (B1), chunk index / catalog / WALs, recovery from PV alone after wiping every SQLite file
- [x] **Seal pass → parquet** (second slice; branch `feat/stage1-seal-pass`)
  - [x] `libs/collector/hotstore` — `Store.Seal(key, bucket)`: segment-ordered walk (`01` §6.5, each segment decompressed once), per-call blob assembly to the depth-0 exit with spill to `parquet-sealing/`, `suspend_ms` from `suspend.wal` intersection (`01` §5.1 step 4)
  - [x] `libs/collector/hotstore` — `error_flag` / `retention_class` re-derived at seal from `calls.wal` raw param ids against the full dictionary (`01` §5.6); the provisional index value is never trusted
  - [x] `libs/storage/parquet` — `CallV2` schema (`01` §5.2): ZSTD, rows sorted `(ts_ms DESC, pk ASC)`, one file per retention class, `trace_blob` NULL + `truncated_reason` on `dict_miss` / `disk_budget` / `idle_timeout`
  - [x] `libs/collector/hotstore` — sealed name = S3 key with `timeMin`/`timeMax` (`01` §7); `parquet_local` rows with `uploaded_at NULL`; segment refcounts pinned via the new `parquet_segments` table
  - [x] `libs/collector/hotstore` — minimal seal trigger: `SealDue` / `RunSealLoop` (bucket end + grace, late-data patch files with the next `<seq>`); wired into `libs/collector` behind `SealCheckInterval > 0`
  - [x] Recovery additions (`03` §3.6): discard `parquet-sealing/` scratch, clear `parquet_local` rows whose file is missing and release their refcounts
  - [x] Synthetic integration test `libs/tests/integration/seal_test.go`: production-like ordering (calls indexed before the dictionary decodes `call.red`), blob byte-equality + tree decode with §4.5 noise trimming, ZSTD + sort + naming, `suspend_ms`, eviction → `disk_budget`, refcounts, idempotent re-seal
- [ ] S3 upload + dictionary/pods/suspend snapshots (`01` §3.6, §6.2)
- [ ] Hot-read API `/internal/v1/*` (`02-read-contract.md` §3)
- [ ] Budgets and janitors: segment refcounts/eviction, idle accumulator timeout, memory budget (`01` §4.6)
- [ ] Collector app wiring: `profiler-backend collect` subcommand, readiness states, Prometheus metrics (`03` §2)

## Decisions log

### 2026-07-03 — calls.wal record body is length-prefixed JSON, not raw wire bytes

**Question:** `01` §2 describes `calls.wal` as "full Call records as received".
Should the WAL store the raw wire bytes of each record?

**Choice:** No. Each record is stored in the shared WAL framing
(varint length + body + CRC footer) with a JSON body `{ts_ms, call}` carrying
the decoded record and its absolute start time.

**Reason:** A raw wire record is not self-contained: its start time is a
zig-zag delta from the *previous* record and its thread name is an index into a
per-file table (`01` §5.1). The contract requires reading one record by offset
(hot `/calls/{pk}` fetch, seal-pass column read, recovery reconciliation), so
the stored form must decode standalone. JSON was picked over a bespoke binary
codec for the first slice: one codec, debuggable, and the WAL lives only for
the hot window, so the format can change with a version bump before Stage 2
scale tests. `params.wal` and `suspend.wal` use the same framing with JSON
bodies; `dictionary.wal` keeps the exact binary body pinned by `01` §3.2.

### 2026-07-03 — listener errors surface through the wire error path

**Question:** `06` §6 specifies `ACK_ERROR_MAGIC` + close when the `RCV_DATA`
handler fails, and null-UUID + close when the `INIT_STREAM_V2` handler fails,
but the `server.Listener` interface returned no errors, so a failing hot store
could only log and silently drop data.

**Choice:** `RegisterPod`, `RegisterStream`, and `AppendData` now return
errors; the connection handler maps them to the `06` §6 teardown responses.
Decode errors *inside* a stream (a malformed record on an otherwise healthy
connection) still only log and skip — the decoder drains the rest of the file
so the connection never stalls.

### 2026-07-03 — chunk_index stays RAM-only; recovery always rescans segments

`01` §4.3 stores chunk refs "in `chunk_index[threadId]` and in the SQLite
segment catalog", while the `03` §3.2 `segments` schema has no per-chunk rows
and `03` §3.5 rebuilds the index by re-parsing the segments. Implemented per
`03`: the catalog holds one row per segment (path, logical size, time range);
per-chunk refs live in RAM and are rebuilt by the recovery rescan. The rescan
also repopulates the catalog itself, so deleting a corrupt `metadata.sqlite`
(the `03` §3.2 step-4 repair) loses nothing — the integration test wipes every
SQLite file before recovery to pin that property.

### 2026-07-03 — WAL footer is marked by a zero-length record

`01` §3.2 pins "a single CRC32 at file footer" but not its byte encoding. A
bare 4-byte trailer is ambiguous on replay: a torn tail could parse as a
footer. The footer is therefore encoded as a zero-length record (varint `0`)
followed by the 4-byte CRC32; a zero-length record cannot occur as data, so
replay distinguishes "cleanly closed", "crash without footer", and "torn tail"
deterministically.

### 2026-07-03 — error_flag race resolved at seal, not by sequencing the pipelines

**Question:** the first-slice open issue — a Call indexed before its `call.red`
dictionary word arrives stores `error_flag = false`. Re-resolve at seal, or
sequence the dictionary and calls pipelines?

**Choice:** re-resolve at seal. The pipelines stay independent; the index value
stays provisional, per `01` §5.6.

**Reason:** sequencing would couple two live decoders for a value only parquet
needs to get right, and `01` §5.6 already names the seal authoritative. The
seal-slice integration test pins the behaviour by indexing the errored call
before the dictionary decodes a single word and asserting the parquet row still
lands in `any_error`.

### 2026-07-03 — sealed files live under `<dataDir>/<s3Key>`

`01` §6.2 moves a sealed file "to its sealed name" and §6.3 keeps it locally
for `hot_retention`, but the §8 PV layout does not name the location. The
sealed local path is `<dataDir>/parquet/v1/<class>/<yyyy>/<mm>/<dd>/<hh>/<name>`
— exactly the S3 object key of `01` §7 rooted at the data dir — and
`parquet_local.s3_key` stores the key at seal time. The upload task PUTs the
file at its recorded key verbatim, which keeps the seal pass the single source
of truth for S3 placement. Implementation choice, not a contract change.

### 2026-07-03 — seal watermark is the first uncovered calls.wal offset

`03` §3.2 gives `seal_state` a `watermark` column without pinning its meaning.
It stores `max(calls_wal_offset) + 1` over the rows a seal covered: offsets are
per-pod-restart monotone, so `calls_wal_offset >= watermark` selects exactly
the rows later seals owe, including a first-record offset of zero. The same
comparison doubles as the late-data dirty check in `SealDue` — a late Call
raises the partition's max offset past the watermark.

### 2026-07-03 — refcount unit is un-uploaded sealed rows, tracked per file

`03` §3.2 defines `segments.refcount` as "the un-uploaded sealed rows whose
blobs source from the segment". The seal pass increments it by the per-segment
row count of each sealed file and records that count in a new
`parquet_segments (path, pod_restart, stream, rolling_seq, row_count)` table,
so the upload task (and the missing-file reconciliation, which already uses
it) can decrement exactly what the seal added without reopening the parquet.
Truncated rows pin nothing — a NULL blob sources no segment. `sql`/`xml`
segments join the refcount when a `PARAM_BIG` / `PARAM_BIG_DEDUP` tag appears
within the call's span, per `03` §3.2.

### 2026-07-03 — seal loop is opt-in until the collector app wiring

`RunSealLoop` implements the `01` §6.1 bucket-end + grace trigger, but
`collector.Service` starts it only when `SealCheckInterval > 0` and `Normalize`
leaves the interval zero. Synthetic tests replay history (their buckets are
due immediately), so an always-on loop would race every test's explicit
`Seal` call. The `collect` subcommand wiring sets the interval in production.

## Open issues

- **`stage1-plan.md` does not exist yet.** This slice was specified directly
  by the user; the remaining Stage 1 tasks (seal pass, S3, read API, budgets,
  app wiring) need a plan document with dependencies and acceptance criteria.
- **Seal-pass gaps deferred to later tasks.** No `parquet_max_size` splitting
  (one file per class per pass, `01` §6.1); no `PROFILER_SEAL_CONCURRENCY` and
  no guard against two concurrent seals of one bucket (`SealDue` runs them
  sequentially); blob spill is per-call (`SealSpillBytes`), whole-pass
  `PROFILER_MEM_BUDGET` accounting belongs to the budgets task, and
  `mem_pressure` is emitted only when a spill itself fails. The `sql`/`xml`
  refcount path (big-param tags) has no test coverage yet.
- **`parquet_local` is not rebuilt from footers.** Recovery clears rows whose
  file is missing (`03` §3.6 step 10), but a wiped `metadata.sqlite` with
  sealed files on disk leaves them orphaned — re-reading parquet footers
  (`03` §3.2 step 4) is not implemented. Orphans re-seal from the WAL, so the
  cost is duplicate rows collapsed by PK-dedup, plus leaked local files.
- **`server.Service.Stop()` waits for live agent connections** and is bounded
  only by the socket read timeout (~40 s). The `03` §5.2 drain (send
  `COMMAND_CLOSE`, 5 s per-connection timeout) is not implemented yet; it
  belongs to the collector app wiring task.
- **Ingest decode errors only log.** A malformed calls/dictionary record skips
  the record; there is no metric yet. Prometheus counters land with the app
  wiring task (`01` §5.1 expects counters for dropped/truncated calls).
- **`params.wal` phrase-length quirk.** The agent's params/suspend phrase
  length includes bytes (version byte, suspend base time) that the pipe
  decoders do not subtract; single-phrase streams parse fine, which is what
  the agent produces today. Revisit only if multi-phrase params streams appear
  (would belong in the `streams/ → pipe/` consolidation, `profiler-plan.md`
  decision 8).
- **Pre-existing test failures** in `libs/parser/...` (`TestIntegration`,
  `TestParsePodDump`, `streams` suites) come from binary fixtures that are
  deliberately not committed (`WORKFLOW.md` §6); they fail identically with
  and without this slice. Worth a `t.Skip` when the fixture is absent.
