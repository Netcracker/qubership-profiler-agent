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
- [x] **S3 upload + snapshots** (third slice; branch `feat/stage1-s3-upload`)
  - [x] `libs/collector/hotstore` — `Uploader.Pass`: PUT every pending `parquet_local` file at its seal-recorded `s3_key`, then commit `uploaded_at` and the segment refcount release in ONE SQLite transaction (`01` §6.2 step 3; the C1 guard deletes the file's `parquet_segments` rows in the same transaction, so a repeat releases nothing)
  - [x] `libs/collector/hotstore` — snapshots of closed pod-restarts gated by `dict_uploaded_at`: dictionary + suspend timeline, plus a pods manifest per UTC day the pod-restart sealed into (`01` §3.6, `03` §3.9)
  - [x] `libs/collector/hotstore` — error handling: retryable failures back off exponentially in-pass and stay pending across passes; a 4xx moves the file to `upload-failed/` with its refcounts kept (`01` §8, new `parquet_local.upload_failed_at` column)
  - [x] `libs/collector/hotstore` — sweep unlinks refcount-0 segments (file + catalog row) of closed, fully sealed pod-restarts (`03` §3.7 step 14)
  - [x] `libs/collector` — `S3ObjectStore` MinIO adapter (Content-MD5 on every PUT, 4xx → `PermanentUploadError`); upload loop opt-in via `UploadCheckInterval`, mirroring the seal loop
  - [x] Tests: `libs/tests/integration/upload_test.go` (fake object store: happy path, C1 crash window, restart recovery, 4xx quarantine, retry backoff), `upload_minio_test.go` (`integration` tag: real MinIO round-trip and live 4xx classification), `hotstore/upload_test.go` (double-release guard at the SQLite layer)
- [x] **Cold read path `/api/v1/calls` + `/api/v1/pods`** (fourth slice; branch `feat/stage1-cold-read`)
  - [x] `libs/query/model` — Call PK with byte-wise collation, the `(ts_ms DESC, pk ASC)` total order, the frozen `/calls` query, k-way merge with PK dedup before truncation (`02` §2.3.1, §6); multi-source by shape (`Tier` tiebreak, cold preferred) so the hot fan-out slice plugs in without reshaping
  - [x] `libs/query/cold` — LIST discovery (`02` §5.1: hour walk per pruned class, `timeMin`/`timeMax` parsed from the key with no footer/HEAD, overlap select, listed-then-`404` → empty), class pruning (`02` §5.5, §2.3.2), projected parquet scan (the `trace_blob` column is never read on the list path), cold `/pods` from `pods/v1` manifests without opening parquet (`02` §2.7)
  - [x] `libs/query` — `/api/v1/calls` + `/api/v1/pods` with RFC 7807 errors (`02` §8), opaque keyset cursor (frozen query + last position + TTL; `400` on expiry and on re-sent-filter mismatch, `02` §2.3.1), two-layer wide-query guard before any parquet open (span, then the LIST-derived estimate with `suggested_filters` / `estimated_*` / `by_class`; verdict rides in the cursor, `02` §2.3.2), `oklog/run` service; only the cold source is wired
  - [x] `libs/query/s3store.go` — MinIO read adapter (prefix LIST, ranged `ReadAt`, `NoSuchKey` → `cold.ErrNotFound`)
  - [x] Tests: unit (merge/collation, cursor TTL/version, guard layers, key parse + pruning) and synthetic integration `libs/tests/integration/coldread_test.go` — two pods over two UTC days sealed and uploaded by the slice-2/3 machinery, a late-arrival patch file, a planted duplicate-PK object, ordering/filters/pagination/guard/`/pods`/discovery acceptance, projection proven by read-offset recording with an unprojected positive control; `coldread_minio_test.go` (`integration` tag) round-trips against real MinIO
- [x] **Hot-read API `/internal/v1/*` + query fan-out** (fifth slice; branch `feat/stage1-hot-fanout`)
  - [x] `libs/collector/hotread` — `/internal/v1/calls` (same params as `/api/v1/calls` plus the `after_ts_ms`/`after_pk` keyset) from the SQLite call index in the tier-shared `(ts_ms DESC, pk ASC)` order; `/calls/{pk}`; `/calls/{pk}/trace` assembled by the seal machinery (`consumeChunk` + blob framing, `01` §4.3/§4.5) with the §2.4 caching headers; `/pods` with per-pod-restart data bounds; `/pods/{pod-restart}/dictionary` (§2.6 snapshot + ETag/304); `/health/hot-window`
  - [x] `libs/collector/hotstore` — read-side store surface (`hotquery.go`): window/point index reads, per-pod-restart bounds, `HotWindowOldestMs`, `AssembleTraceBlob` (shared with seal), `DictionaryWords` (shared with the S3 snapshot)
  - [x] `libs/query/hot` — replica discovery (`Discovery` seam + DNS over `COLLECTOR_HEADLESS_SVC`, re-resolved per request, `02` §7.1) and the per-replica HTTP client with `PROFILER_FANOUT_TIMEOUT`
  - [x] `libs/query` — full fan-out per page (`02` §2.3.1): parallel hot-window probes → dynamic cold cutoff `min(to, max(oldest) + PROFILER_OVERLAP_MARGIN)` (§4.3, degraded hot state falls back to the full cold window), per-replica `/internal/v1/calls` runs + the cutoff-clamped cold scan into `model.MergeRuns` with cold-preferred PK dedup (§6.3), `/pods` union on the §2.7 entry shape (`time_min_ms`/`time_max_ms`, bounds widened across tiers), 504 only when every attempted source failed (§8)
  - [x] `libs/query/model` — shared wire shapes (`CallJSON`, `PodEntry`, PK path codec, `ParseCallsQuery`/`Values`) so the external API, the internal API, and the fan-out client cannot drift
  - [x] Tests: `hotread` unit suite pins the pod_restart-string vs PK collation trap (a pod name that prefixes another, numeric restart ordering), keyset seek, filters, hot-window, dictionary revalidation; `query` unit test pins the cutoff rule; synthetic integration `libs/tests/integration/fanout_test.go` — a real collector with un-sealed hot data plus a scaled-down pod's S3-only data: merge across the cutoff without gap or duplicate, cold-preferred `error_flag` on an overlap row, LIST-skip for a hot-only window vs both tiers for a week, §2.7 union bounds, internal trace byte-equal to the sealed `trace_blob`, and stable pagination across a simulated hot→cold migration (§2.3.1)
- [x] **External `/api/v1/calls/{pk}/tree` + `/trace`** (sixth slice, MVP closer; branch `feat/stage1-tree-api`)
  - [x] `libs/calltree` — shared tree builder (§4.5 reader semantics: tail/head noise by `record_index`, event time = `timerStartTime` + per-chunk delta sum, depth-0 exit) and the hand-written MessagePack codec of `02` §2.5.1-§2.5.4 (int-keyed maps, `v` envelope, reference decoder that skips unknown keys); both tiers render through it
  - [x] `libs/collector/hotstore` — big-param resolution at seal: `consumeChunk` keeps each call's `(stream, seq, offset)` references, `resolveBigValues` reads every referenced `sql`/`xml` segment once per pass, and the row lands with the new additive `big_params_json` column (`01` §4.4, §5.2); `trace_blob` stays byte-identical. `BigValues`/`ParseValueRef` expose the same reader to the internal API
  - [x] `libs/collector/hotread` — `GET /internal/v1/pods/{pod-restart}/values?ref=<stream>:<seq>:<offset>` (batched; unresolvable refs absent, `02` §3)
  - [x] `libs/query/cold` — `FetchCall` point read (blob + `big_params_json`; candidates pre-filtered by the key's pod-restart hash), `Dictionary` from the `dictionaries/v1/<day of restart_time_ms>/<hash>.json` snapshot, list projection now drops both blob-sized columns
  - [x] `libs/query` — `GET /api/v1/calls/{pk}/trace` (§2.4: octet-stream, PK ETag, immutable, Range via `ServeContent`) and `GET /api/v1/calls/{pk}/tree` (§2.5: msgpack, per-tree dictionary, per-route gzip, `Accept-Version`); tiered point lookup (replicas first, then cold within the `?ts_ms=`/`?retention_class=` hint window, `02` §2.2), per-pod-restart dictionary caches (hot: ETag revalidation; cold: immutable), §8 verdict (504 only when every attempted source failed)
  - [x] `libs/query/model` — `PodRestartHash` + `DictionarySnapshotKey` shared by the seal/upload writers and the cold reader (day pinned cross-midnight by a unit test)
  - [x] Tests: `calltree` unit suite (nesting/times, delta continuation, noise trimming, multi-chunk, params, dict miss, msgpack roundtrip + unknown-key skip), `hotstore` value-reader suite, `hotread` values endpoint, synthetic integration `libs/tests/integration/tree_test.go` — hot and cold `/tree` (names, rel-times, durations, inlined sql/xml values, explicit `unresolved` marker, self-contained minimal dictionary, `v` envelope, gzip, `Accept-Version`), cold dictionary from the restart-day snapshot, `/trace` byte-equal on both tiers + Range, guided 404 without `ts_ms`
- [x] **Parquet library migration: xitongsys → parquet-go/parquet-go** (branch `feat/stage1-parquet-go-migration`)
  - [x] `libs/storage/parquet` — `CallV2` tags in the parquet-go dialect (column names unchanged — the name is now the compatibility contract), the `CallV2Projected` list-path read shape, `Parameters` simplified to `map[string][]string`, `profiler.schema_version` stamp constants; a unit test pins the projected twin to the full schema
  - [x] `libs/collector/hotstore` — seal writer on `GenericWriter`: ZSTD kept, row order kept, `profiler.schema_version = 2` stamped into the footer metadata, page bounds skipped for the blob-sized columns
  - [x] `libs/query/cold` — reader on `GenericReader` with native name-based projection (the `ColumnBuffers` deletion hack is gone), read errors checked and wrapped, the `source.ParquetFile` adapter replaced by the object's own `ReadAt` + `Size`
  - [x] Integration tests ported; the coldread read-offset assertion (no read starts inside a `trace_blob` chunk, with the unprojected positive control) survives against the new reader; new `schemaevolution_test.go` pins narrow-file → current-struct null-fill on both cold read paths
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

### 2026-07-03 — dictionary snapshot carries the full word list in both arrays

**Question:** `02` §2.6 gives the snapshot shape `{version, methods: [...],
params: [...]}` with `methods[i]` and `params[i]` resolving `method_id = i`
and `param_id = i` independently, but the wire dictionary is one id space: a
trace ENTER's method id and a tag's param id index the same word list.

**Choice:** both arrays carry the full dictionary; `version` is the word
count.

**Reason:** readers stay correct under either reading of the contract, and
splitting would need a method/param classification the write path does not
have. The duplication costs one extra copy of a small JSON object. The
contract may want a single `words` array instead; revisit when the read slice
consumes the snapshot.

### 2026-07-03 — snapshot keys derive their day from restart_time_ms

`01` §3.6 pins `dictionaries/v1/<yyyy>/<mm>/<dd>/<hash>.json` (and the same
hierarchy for `suspend/v1`) without naming the day. The close day is not
crash-stable — recovery re-closes open pod-restarts at recovery time, so a
crash across midnight would change the key and break the idempotent re-PUT of
§6.6. The UTC day of `restart_time_ms` is stable and derivable by any reader
that already holds the pod-restart tuple. The §3.6 TTL margin (35 d against
the 30 d longest class) absorbs pod-restarts spanning up to five days.

### 2026-07-03 — segment deletion needs closed + fully sealed, not bare refcount 0

`01` §4.4 calls a refcount-0 segment deletable, but refcounts are pinned only
at seal: a segment of a live (or not-yet-sealed) pod-restart sits at zero
while future seals still owe rows from it, and deleting it would lose their
blobs. The sweep therefore unlinks a refcount-0 segment only when its
pod-restart is closed and no bucket holds indexed calls past the seal
watermark — the `03` §3.7 step 14 "no remaining un-sealed call" condition.
Forced over-budget eviction of segments with live references stays with the
budgets task (`01` §4.6).

### 2026-07-03 — quarantined files keep their parquet_local row

`01` §8 names `upload-failed/` but not the metadata side. The row follows the
file — `path` is updated and the new `upload_failed_at` column takes it out
of the upload queue — rather than being deleted: `DropParquetLocal` releases
refcounts, and a rejected file must keep its segments pinned until a human
resolves it. Recovery leaves the row alone because the quarantined path
exists on disk.

### 2026-07-03 — the query service composes in libs/query; the binary is app wiring

`02` §1 places the read API in a separate `query` service but the repo has no
Go binary for it (`backend/apps/query` is the React UI). The service composes
in `libs/query` — `Options`/`Service.Run` over `oklog/run`, mirroring
`libs/collector` — and the executable (`profiler-backend query` or similar),
env parsing (`PROFILER_*`, `02` §9), readiness, and metrics land with the
same app-wiring task that owns the collector binary.

### 2026-07-03 — cold /calls returns trace_blob_size = null (contract gap)

**Question:** `02` §2.3 puts `trace_blob_size` in every `/calls` row, but the
CallV2 schema (`01` §5.2) has no blob-size column, and the same contract
forbids reading `trace_blob` on the list path (§2.3.2, §5.4 — the projection
is also this slice's acceptance invariant). The cold tier cannot know the
size without violating one of the two.

**Choice:** the projection wins. The cold list path emits
`trace_blob_size: null` ("unknown; fetch `/trace`"), and `0` for a truncated
row as §2.3 pins. Blob presence stays derivable: `truncated_reason == null`
implies the blob exists.

**Reason:** reading the blob column to fill a cosmetic field would defeat the
scan-cost model the guard is built on. Fixing it properly is an additive
`trace_blob_size INT32` column in CallV2 — logged as an open issue; old files
without the column would read as NULL, which degrades to today's behaviour.

### 2026-07-03 — /pods response shape

`02` §2.7 pins the tuple set but no JSON shape. Chosen:
`{"pods": [{namespace, service, pod, restart_time_ms}], "partial": ...,
"partial_reasons": [...]}` — member names follow the `pods/v1` manifest
fields (`01` §3.6), the partial envelope matches `/calls` (§7.4). Sorted by
`(namespace, service, pod, restart_time_ms)` for a stable order.

### 2026-07-03 — column projection = dropping the reader's column buffer

xitongsys/parquet-go cannot read a partial struct against a wider file
schema (its footer rename aligns schema elements by index), so the cold scan
opens the reader with the full CallV2 schema and deletes the `trace_blob`
entry from `ColumnBuffers` before the first `Read`. Buffer creation only
positions a reader at the chunk offset — data pages load lazily — so the
column's chunks are never fetched; the unmarshaller leaves the field nil.
The integration test pins this by recording read offsets: no read may start
inside a `trace_blob` chunk, with an unprojected control read proving the
assertion bites. Buffered transports read in ~4 KB granularity, so
neighbouring-column reads may sweep across a small blob chunk — byte-range
non-overlap would be a false invariant; read-start is the correct one.

### 2026-07-03 — key timeMax widens to the end of its second at parse

The `01` §7 key stamps are second-precision while `ts_ms` is milliseconds:
both bounds truncate downward, so a file whose true `max(ts_ms)` has a
sub-second tail would fail `timeMax >= from` against a `from` inside that
second and discovery would drop rows the file does hold. `ParseKey` widens
`timeMax` by 999 ms; the floor of `timeMin` already errs on the inclusive
side. Implementation choice, not a contract change — the key format is
untouched.

### 2026-07-03 — duration_min_ms exempts the span guard at any positive value

`02` §2.3.2 lists `duration_min_ms` as a narrowing filter without
qualifying the value, so any positive value exempts a wide window from the
span layer — even one below 100 ms, which prunes no class. The cost layer
still gates such a query by its actual LIST estimate, so nothing pathological
slips through; class pruning itself stays honest (`< 100 ms` keeps all five
classes listed).

### 2026-07-03 — per-file upload order: PUT object, upsert manifest, then commit

The pods-manifest PUT runs after the parquet PUT but before the `MarkUploaded`
commit. Any failure or crash before the commit leaves `uploaded_at` NULL, so
the next pass re-runs both PUTs — idempotent by deterministic key — and no
durable "manifest dirty" flag is needed. Manifest bounds cover every file the
pod-restart sealed into that day, so a later seal or a retry only widens
`time_max_ms`, matching the §3.6 upsert semantics.

### 2026-07-03 — internal keyset rides as explicit after_ts_ms/after_pk params

**Question:** `02` §3 says `/internal/v1/calls` takes "same params as
`/api/v1/calls`", whose table includes `cursor`. Should the collector accept
the opaque external cursor?

**Choice:** the internal endpoint takes the §2.3 filter params verbatim plus
an explicit position pair `after_ts_ms` / `after_pk` (the §2.2 colon
serialization); the opaque cursor stays a query-service artifact.

**Reason:** §2.3.1 requires of a source only that it "seeks past the cursor
position" — the position is the whole seek state. The external token also
carries the frozen query and the TTL, which are the query service's
pagination-session concerns; decoding it on the collector would couple every
replica to the token format and its rotation. Both sides of the internal API
are ours, so this is an implementation choice, not a contract change.

### 2026-07-03 — /pods rows carry time_min_ms/time_max_ms (supersedes the shape entry above)

The earlier "/pods response shape" entry chose 4-member rows; the hot fan-out
slice restores the full `02` §2.7 entry — `{namespace, service, pod,
restart_time_ms, time_min_ms, time_max_ms}` — because the union across tiers
is specified on exactly that shape. Cold bounds come from the pods/v1
manifests, hot bounds from the call index (both unclamped by the query
window); a pod-restart present in several sources merges into one entry with
widened bounds. The envelope (`partial` / `partial_reasons`) is unchanged.

### 2026-07-03 — hot /calls ordering is computed in Go, not by SQLite ORDER BY

The call partitions key rows by the scalar `pod_restart` string
(`ns/svc/pod/restartMs`), and its byte order diverges from the §2.3.1
component-wise PK collation in two ways: a pod name that prefixes another
compares through the `/` separator (`'/'` > `'-'` and `'.'`, so `a/...` sorts
AFTER `a-b/...` while the PK puts `a` first), and `restart_time_ms` as text
puts `1000` before `999`. `ORDER BY pod_restart` would therefore break the
byte-for-byte cross-tier order the merge and dedup rest on. The hot API
fetches each overlapping partition's window rows and sorts with the shared
`model` comparator; partitions are disjoint ts ranges, so their runs
concatenate newest-first and the walk stops at `limit`. A unit test pins both
divergence cases.

### 2026-07-03 — dynamic cutoff: max over healthy replicas, full window on any degradation

`02` §4.3 gives the static rule `now - hot_retention + overlap_margin`; the
dynamic form implemented is `coldTo = min(to, max over replicas of
hot_window_oldest_ms + PROFILER_OVERLAP_MARGIN)`. The max (the YOUNGEST hot
window) is what zero-gap needs: data below that replica's window start exists
only in cold. Any degraded hot state — no discovery configured, resolution
failure, zero replicas, or one failed health probe — widens cold to the full
query window, so the guarantee never depends on an unreachable replica; the
cost is a wider LIST exactly when the hot tier is already in trouble. An
empty replica reports `oldest = now`, which keeps cold covering everything.

### 2026-07-03 — hot /internal/v1/calls serves from the SQLite index alone

`02` §3 lists recently sealed local parquet as a fourth hot source, for calls
"already moved out of the hot index". Nothing moves out yet — call partitions
are never dropped (the hot-retention janitor is a later task) — so the index
covers every call the replica holds and reading local parquet would only
produce duplicates for the dedup to collapse. The parquet source must land
together with the janitor that starts dropping partitions; recorded as an
open issue so the two cannot ship apart.

### 2026-07-03 — big params resolve at seal into big_params_json; the blob keeps its references

**Question:** the blob's `PARAM_BIG` / `PARAM_BIG_DEDUP` tags reference the
`sql` / `xml` value segments, which live only on the PV and are deleted after
upload — so a cold `/tree` cannot resolve them. Resolve at seal and inline,
upload the value streams to S3, or mark them unresolved on cold?

**Choice:** resolve at seal. The pass already parses every blob event for the
refcounts, so it also keeps each call's `(stream, seq, offset)` references,
reads every referenced segment once, and writes the values into a new
`big_params_json` column (`{"<stream>:<seq>:<offset>": value}`) next to the
blob. The blob itself is untouched — `/trace` stays byte-identical across
tiers and the raw-path contract ("the blob keeps the references") holds.

**Reason:** uploading the value streams would add a second S3 object family
with its own retention and offsets addressing for data that only `/tree`
needs, and "unresolved on cold" would make the canonical endpoint lossy
exactly where it matters (SQL texts). Inlining costs one JSON column that
ZSTD compresses next to the blob it accompanies, and the list projection
drops it the same way it drops `trace_blob`. A reference that still fails to
resolve (segment evicted before the seal, file sealed before this column)
renders with an explicit `unresolved` marker carrying the reference text
(`02` §2.5.3 field 2) — degraded like a truncated blob, never silent.

### 2026-07-03 — CallV2 columns are NOT freely additive with the current parquet reader

Adding `big_params_json` surfaced a library constraint: xitongsys/parquet-go's
`RenameSchema` writes the struct schema over `Footer.Schema` by index, so
reading a file written WITHOUT a column through a struct WITH it panics —
"additive" holds on the write side only. No production files exist yet
(Stage 1 is pre-release), so the column lands without a migration; but any
post-release CallV2 change needs a footer-sniffing versioned reader (pick the
struct by the footer's element count) or a reader-library change. This also
qualifies the earlier `trace_blob_size` entry, which called such a column
"additive; old files would read as NULL" — with today's reader they would not
read at all.

### 2026-07-03 — point endpoints locate cold calls by explicit ts_ms/retention_class hints

`02` §2.2 says the client "passes the ts_ms and retention_class from the
/calls response"; implemented as plain query parameters of those names on
`/calls/{pk}/trace` and `/calls/{pk}/tree`. The hot replicas are probed
first (no S3 round-trip for live calls); the cold lookup discovers
`[ts_ms, ts_ms+1)` and pre-filters candidates by the key's pod-restart hash,
reading only the matching pod-restart's files. A PK that misses the hot tier
with no `ts_ms` answers a guided `404` naming the hint — never an unbounded
scan. The contract now pins the parameter names (§2.2).

### 2026-07-03 — unknown Accept-Version is refused, not answered with v1

`02` §2.5.4 defines the header's meaning only once a v2 exists. Until then
the server emits v1 for an absent header or `Accept-Version: 1`, and answers
`400` for anything else: silently serving v1 bytes to a client that asked for
a version this server has never heard of would defer the failure to the
client's decoder, where it is harder to diagnose.

### 2026-07-03 — hot /tree fetches big values over a new internal values endpoint

The tree renders in `query` for both tiers (one `libs/calltree`
implementation, one gzip/versioning surface), but hot big-param values live
only in the replica's `sql`/`xml` segments. Added
`GET /internal/v1/pods/{pod-restart}/values?ref=...` (batched, one
round-trip per tree; unresolvable refs absent from the reply). Both sides of
the internal API are ours — an implementation choice like the slice-5 keyset
params, now recorded in `02` §3. The external API still never exposes the
value streams (`02` §2.5).

### 2026-07-03 — a missing dictionary renders placeholders, not a failed tree

A cold pod-restart whose dictionary snapshot is absent (crashed before the
close upload, or TTL-expired) and a hot replica whose dictionary fetch fails
against an empty cache both render the tree with the "#<id>" placeholders the
list path already uses — the structure and timings are still worth serving.
The hot dictionary cache revalidates by ETag per pod-restart and falls back
to its cached copy on a fetch error (the dictionary is append-only, so a
stale copy only turns the newest ids into placeholders); cold snapshots are
immutable and cache forever (capacity-bounded).

### 2026-07-03 — 504 means every attempted source failed

With two tiers wired, the §8 rule is implemented as: count each hot replica
(health probe or calls fetch) and the cold LIST as attempted sources; return
504 only when at least one source was attempted, none succeeded, and at least
one failed. A tier legitimately skipped — cold under the cutoff, a replica
whose hot window misses the range — counts as neither, so a hot-only query
with a dead S3 still answers from the replicas, and vice versa.

### 2026-07-04 — parquet library switched to parquet-go/parquet-go: columns match by NAME

**Question:** the 2026-07-03 "CallV2 columns are NOT freely additive" decision
left schema evolution blocked: xitongsys/parquet-go's `RenameSchema` aligns a
file's footer schema to the target struct by column INDEX, so reading an
older, narrower file through a wider CallV2 panics instead of null-filling.
Build the footer-sniffing versioned reader, or change the library?

**Choice:** replace xitongsys/parquet-go with github.com/parquet-go/parquet-go
across writer and reader in one change. The library matches file columns to
the struct by NAME and applies add/remove conversion rules: a column missing
from the file null-fills, a column absent from the read struct is masked so
its pages are never fetched. No production files exist (Stage 1 is
pre-release), so no data migration.

Consequences:

- **Supersedes "CallV2 columns are NOT freely additive"** — additive changes
  and column removals are now backward-readable by name; renames and type
  changes still are not. The contract rule moved with it (`01` §5.2, `02`
  §2.3), per `WORKFLOW.md` §8.
- **Supersedes "column projection = dropping the reader's column buffer"** —
  the list path now reads through `CallV2Projected`, the CallV2 twin without
  the blob-sized columns; the name match IS the projection. The coldread
  read-offset test still pins that no read starts inside a `trace_blob`
  chunk, and a unit test pins the twin against the full schema.
- **Discharges two open issues:** "parquet-go swallows column read errors"
  (the new `readRows` checks and wraps every error and fails on a row-count
  shortfall — a corrupted column now fails the scan instead of yielding
  zeros) and the per-column transport buffer allocated at chunk size for
  dropped columns (masked chunks allocate nothing).
- **Every sealed file stamps `profiler.schema_version = 2`** into its
  key-value footer metadata — the escape hatch for a future NON-additive
  change; additive changes do not bump it. The versioned reader keyed on the
  stamp is recorded in `deferred.md`.
- `trace_blob` is `[]byte` in Go now (was `*string`); the parquet type is
  unchanged (optional BYTE_ARRAY, no UTF8 annotation). `params` is written as
  a standard `MAP<UTF8, LIST<UTF8>>` via the `parquet-value:",list"` tag, and
  `Parameters` simplified to `map[string][]string`. The `*ParamsValueList`
  wrapper survives as `LegacyParameters` on the legacy `CallParquet` only —
  the xitongsys dialect cannot derive a MAP value schema from a bare
  `[]string`, and the legacy file shape must not change under its consumers.
  Page bounds (min/max statistics) are skipped for `trace_blob` and
  `big_params_json` so footers do not carry blob prefixes as column
  statistics.
- The library reports an unconvertible file schema (rename / type change) by
  panicking inside the reader; `readRows` recovers it into that file's scan
  error so a foreign or future-versioned object degrades to a failed source,
  not a crashed query service.
- **xitongsys is gone from `go.mod`.** Its only remaining importer was the
  legacy `libs/parquet` writer, used solely by `tools/data-generator` (support
  tooling of the retired dumps pipeline). Porting the writer would have changed
  the legacy file shapes it emits for consumers this stage does not own, so the
  maintainer retired `libs/parquet` and `tools/data-generator` outright;
  `go mod tidy` then dropped `xitongsys/parquet-go` and
  `xitongsys/parquet-go-source`.

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
- **Snapshot and manifest PUTs have no quarantine.** A permanent 4xx on a
  dictionary, suspend, or pods-manifest object logs and retries on every
  pass; only parquet files move to `upload-failed/`. Harmless while the
  bucket policy matches the parquet PUTs, noisy if it ever diverges.
- **WAL purge after upload is not implemented.** `01` §3.6 step 4 and `03`
  §3.9 step 18 delete a closed pod-restart's WALs once its dictionary and
  parquet are uploaded and the hold-back grace has elapsed; closed
  pod-restarts currently keep their WALs on the PV.
- **No hot-retention janitor yet.** Uploaded parquet stays on the PV past
  `PROFILER_HOT_RETENTION` (`01` §6.3) and call partitions are never dropped;
  both belong to the budgets/janitors task.
- **Upload backoff state is per-pass.** Attempts restart on every pass, with
  no jitter and no per-file cross-pass schedule. `UploadStats` is the seam
  for the Prometheus counters that land with the app wiring task.
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
  and without this slice. Worth a `t.Skip` when the fixture is absent. The
  same applies to `libs/generator` and `TestGenerator_GenerateCalls` in
  `libs/tests/integration` (missing `ui5min.bin`).
- **CallV2 needs a `trace_blob_size` column.** The cold list path emits
  `trace_blob_size: null` because the schema carries no size and the
  projection forbids reading the blob (see the 2026-07-03 decision). Adding
  the column at seal is additive and backward-readable by name (2026-07-04
  decision): rows sealed before it read back as NULL, which degrades to
  today's behaviour. Do it before Stage 5 wires the UI if the calls table
  wants to show blob sizes.
- **Cold scan reads each candidate file whole, sequentially.** `ScanFile`
  materializes every projected row of a file before filtering, files scan one
  after another, and each page re-reads every file (`02` §2.3.1 accepts the
  re-scan). Row-group `ts_ms` pruning from the sorted layout (`01` §5.2),
  parallel per-file scans, and streaming reads are deferred until profiling
  shows the need.
- **Hot `/internal/v1/calls` does not read sealed local parquet** (`02` §3
  source 4). Safe today because call partitions are never dropped, so the
  index covers everything; the parquet source MUST land in the same task as
  the hot-retention janitor that starts dropping partitions, or aged calls
  become invisible to the hot tier before their S3 copy is preferred anyway.
- **Hot /calls materializes each overlapping partition's window rows** before
  sorting in Go (no SQL-level keyset; see the collation decision). Bounded by
  a partition's ~5 minutes of calls per page, but worth a pushed-down seek
  (component PK columns in the partition schema) if profiling shows it.
- **Replica "more rows" is inferred from a full page.** The internal API
  returns no continuation flag; the fan-out treats `len(rows) == limit` as
  "may have more", which can cost one extra empty page with a non-null
  cursor — explicitly allowed by §2.3.1's termination rule.
- **`/internal/v1/calls/{pk}` probes every partition.** A bare PK carries no
  time hint (`02` §2.2 suggests a `call_ref`); with few partitions per
  replica the point SELECTs are cheap, revisit when the janitor lands.
- **Fan-out health probes run on every page.** Two HTTP round-trips per
  replica per page (hot-window + calls); a short-TTL cache of the hot-window
  report is the obvious lever if page latency ever matters.
- **`/api/v1/calls/{pk}` and `/api/v1/pods/{pod-restart}/dictionary` are not
  implemented.** Both sit in the `02` §2.1 endpoint table; the slice-6 scope
  covered only `/trace` and `/tree` (which needs no external dictionary — the
  per-tree dictionary is inline). The single-row fetch composes from the
  existing `FindCall`/`FetchCall` seams; the external dictionary endpoint
  composes from the same sources the tree path already resolves.
- **Cold point fetch reads each candidate file whole,** including every
  row's `trace_blob`/`big_params_json`, to find one PK. Bounded by the one
  pod-restart's files of one 5-minute bucket (hash pre-filter), but a
  row-group `ts_ms`/PK prune is the lever if point-fetch latency ever shows
  up; same deferral as the list-path scan above.
- **Point-fetch hot probing is sequential** (replica by replica, first 200
  wins) and the fan-out's `/pods` targeting (`02` §7.3) is still dormant, so
  a large replica set pays worst-case one timeout per dead replica before
  falling cold. Parallel probes or targeting fix it when replica counts grow.
- **Dictionary caches evict arbitrarily** (map iteration) at a fixed 512
  entries per tier and the hot cache holds no negative entries; fine for the
  MVP's pod counts, revisit with real cardinality data alongside the fan-out
  health-probe cache noted above.
