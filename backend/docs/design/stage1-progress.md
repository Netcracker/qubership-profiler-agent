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
- [x] **App wiring + dev stack + end-to-end smoke** (seventh slice, Stage 1 closer; branch `feat/stage1-app-wiring`)
  - [x] `apps/profiler-backend` — the single Go binary of `04` §2 with `collect` and `query` subcommands (cobra + `oklog/run`, the dumps-collector pattern); env parsing per `01` §9 / `02` §9 / `03` §10 in `pkg/envconfig`, covering only the knobs the composed services honour (`2GB`-style sizes and the duration-threshold pair get decoders)
  - [x] `collect` — the internal port binds at process start behind a `pkg/health` gate serving `/internal/v1/health/{ready,live}` (`03` §2/§4: LOADING/RECOVERY answer 503, the hotread handler mounts at READY); recovery completes before the agent TCP listener starts; the seal and upload loops default ON (15 s / 30 s); SIGTERM flips DRAINING and holds the drain grace, a second signal skips it
  - [x] `query` — S3 access verified at boot (`03` §7.1, FATAL on failure), replica discovery from `COLLECTOR_HEADLESS_SVC`, the same gate on `/api/v1/health/*`, 15 s in-flight bound at shutdown (§7.3)
  - [x] `backend/docker-compose.yaml` (`04` §11.1) — MinIO (healthcheck) + collector + query from one image (`apps/profiler-backend/Dockerfile`: distroless static, `/data` owned by 65532 so the named volume inherits it); the services create the bucket themselves (idempotent `MakeBucket`)
  - [x] Smoke `libs/tests/smoke` (build tag `smoke`; `make smoke`): a `libs/emulator` agent sends dictionary + trace + calls + suspend over real TCP; the hot phase asserts `/api/v1/calls` and `/tree` answer while `parquet/v1/` is still empty in MinIO; the cold phase ages a bucket two hours back, waits for the parquet and dictionary-snapshot objects, runs `docker compose stop collector`, and asserts the wide range and `/tree?ts_ms&retention_class` answer from S3 alone; a final phase restarts the collector and re-reads the hot rows through recovery
- [x] **Lifecycle janitors: hot retention, WAL purge, disk budget, snapshot quarantine** (eighth slice; branch `feat/stage1-lifecycle-janitors`)
  - [x] `libs/collector/hotstore` — `JanitorPass`/`RunJanitorLoop` (`janitor.go`): aged local parquet deleted per `01` §6.3 (`uploaded_at + hot_retention`), call-index partitions dropped oldest-first behind a contiguity barrier (see the decisions log — zero-gap across the hot→cold drop), WALs purged per `03` §3.9 step 18 (closed + snapshots uploaded + nothing hot left + grace; the pod-restart dir and in-RAM state go with them), disk-budget eviction per `01` §4.6 (refcount-0 first, then oldest referenced, never an open segment; the row stays `evicted` so the seal records `disk_budget`)
  - [x] `libs/collector/hotstore` — snapshot/manifest quarantine mirroring the slice-3 parquet path: permanent 4xx on a dictionary/suspend object sets the new `dict_upload_failed_at` and parks the body under `upload-failed/<s3-key>`; a rejected pods manifest is parked the same way and the parquet still commits `uploaded_at`
  - [x] `libs/collector/hotstore` — dropped-bucket escape hatch: `InsertCall` into a dropped partition resurrects it (`dropped_at` cleared, one retry over a racing drop), so a very late Call re-enters the seal loop instead of landing invisible
  - [x] `apps/profiler-backend` — `PROFILER_HOT_RETENTION`, `PROFILER_CHUNKS_STAGING_MAX_BYTES`, `PROFILER_WAL_PURGE_GRACE`, `PROFILER_JANITOR_CHECK_INTERVAL` (default 30 s, ON) wired into `collect`; the janitor loop joins the seal/upload loops in `collector.Service`
  - [x] Tests: `hotstore/janitor_test.go` (lifecycle gates in order, quarantine contiguity barrier, deterministic eviction order, partition resurrect) and `libs/tests/integration/janitor_test.go` — zero-gap acceptance (`/api/v1/calls` returns the same rows before and after the hot drop, hot window provably empty, S3 LIST provably consulted; recovery over the purged PV comes up clean and cold still answers), disk-budget eviction sealing `truncated_reason=disk_budget` next to a surviving blob, snapshot/manifest quarantine stopping the retry and pinning the WALs
- [x] **Maintain job: S3-side compaction + per-class TTL** (ninth slice; branch `feat/stage1-maintain`)
  - [x] `libs/maintain` — stateless S3-only worker (`01` §6.6, §6.4): per class, LIST → TTL sweep → per-`(bucket, class)` compaction groups; patch and cross-pod-restart compaction are one code path (every row keeps its own PK, so the merge does not care whose file a row came from)
  - [x] `libs/maintain/compact.go` — write → grace → delete as three separate passes: a fresh compaction only PUTs the merged object (`maintain-<hashOfInputs>` producer key, `01` §7); a later pass recognises the output by recomputing the hash over the remaining group members and deletes the inputs once `now - LastModified(output) ≥ PROFILER_COMPACTION_DELETE_GRACE`; the merge restores `(ts_ms DESC, pk ASC)` with PK-dedup and rewrites through the full `CallV2` schema (all columns, ZSTD, schema stamp via the new shared `storageparquet.CallV2WriterOptions`)
  - [x] `libs/maintain/ttl.go` — parquet expiry by the key's `timeMax` stamp alone (widened +999 ms, so the comparison errs on the keep side); `dictionaries`/`pods`/`suspend` snapshots aged from the end of the key's UTC day against `PROFILER_RETENTION_DICTIONARY_TTL`
  - [x] `libs/query/cold` — mid-read 404 backstop (`02` §5.1): a listed object deleted between the LIST and the column reads now degrades to an empty result (existence re-check on the error path) instead of a spurious `partial`; before this, the §5.1 rule held only for a delete before the first byte
  - [x] `libs/query/cold` — point fetch sees compacted objects: `ParseKey` now parses the `<replica>` token (everything left of the hash, dashes and all) into `FileRef`, and `FetchCall` treats a file whose replica is the reserved `maintain` token (`01` §7) as a candidate for every PK — read whole and matched row-by-row — instead of pruning it by a hash that covers the compaction's inputs, not one pod-restart. Before this, `/calls/{pk}/tree` and `/trace` answered 404 for a call whose bucket had been compacted cold-side
  - [x] `apps/profiler-backend` — `maintain` subcommand: singleton loop with `PROFILER_MAINTAIN_CHECK_INTERVAL` (immediate first pass), `--run-now` one-shot for a k8s CronJob (`03` §8.2); env: `PROFILER_COMPACTION_{MIN_AGE,MIN_FILES,DELETE_GRACE,MAX_BYTES}`, per-class retention TTLs with a `d` suffix decoder (`35d`)
  - [x] Tests: `libs/maintain/maintain_test.go` (grace lifecycle on a fake store with a steerable clock, unsettled/small/oversized guards, residue convergence, TTL boundaries, key parsing incl. discovery round-trip) and `libs/tests/integration/maintain_minio_test.go` (`integration` tag) — seeded per-pod-restart + patch files compact to one object with the identical PK set, order, columns, and floor/ceil key stamps; `/api/v1/calls` answers the same rows through every write → grace → delete phase with a concurrent reader; a point fetch (the `/tree`, `/trace` path) finds a PK that, after the inputs are deleted, lives only in the `maintain-`keyed object; converged bucket re-pass is a no-op; TTL deletes only expired parquet and snapshots
- [ ] Budgets, remaining: idle accumulator timeout, memory budget (`01` §4.6 — `PROFILER_IDLE_ACCUMULATOR_TIMEOUT`, `PROFILER_MEM_BUDGET`)
- [ ] Prometheus metrics (`03` §2; `01` §5.1 expects dropped/truncated-call counters — `UploadStats`, `SealCounters`, the new `JanitorStats`, and the ingest decode-error paths are the seams)

## Decisions log

### 2026-07-04 — compacted objects are identified by the reserved `maintain` replica token, not a hash sentinel

**Question:** The cold point fetch (`FetchCall`) prunes a candidate whose
key-encoded pod-restart hash cannot match the PK. A cross-pod-restart
compaction keys its output by the hash of its *inputs*, so that hash matches
no live PK and the object was skipped. How should the point-fetch path
recognise a compacted object it must read whole?

**Choice:** By the key's `<replica>` slot. `ParseKey` now parses the replica
(everything left of the hash, dashes included) into `FileRef`, and `FetchCall`
treats a file whose replica is the reserved `maintain` token (`01` §7) as a
candidate for every PK — read whole, matched row-by-row. The `scan.go` comment
that anticipated "compaction may blank the hash later" was wrong: the contract
never blanks the hash; it substitutes the reserved replica token.

**Reason:** The replica token is already the contract's marker for a compacted
producer (`01` §6.6, §7); no key change or hash sentinel is needed. The list
path (`/calls`) is unaffected — it never filters by hash — so only the point
endpoints (`/tree`, `/trace`) needed the fix.

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

### 2026-07-04 — the Go binary lives at apps/profiler-backend; collect and query are subcommands

`04` §2 pins one image / one binary with per-workload subcommands, and the
slice-4 decision deferred the executable to the app-wiring task. The paths
`apps/collector` (legacy Java collector) and `apps/query` (React UI) are
taken until their Stage 4/5 retirement, so the binary and its wiring live in
`apps/profiler-backend` (`main.go`, `cmd/`, `pkg/envconfig`, `pkg/health`).
`maintain` and `all` (`03` §8-§9) are not implemented yet.

### 2026-07-04 — readiness is an app-level gate in front of the API handlers

`03` §2 binds the ports during LOADING with probes answering 503, but the
libs services bind their own listeners only inside `Run`, after recovery. The
app therefore owns both HTTP servers: a `health.Gate` answers
`<prefix>/health/{ready,live}` from the §2 state machine and hands everything
else to the hotread/query handler mounted at READY
(`collector.Options.InternalAPIAddr` stays empty; the libs surface is
unchanged). The "no agent traffic before recovery" invariant needs no gate —
`collector.New` completes recovery before `Run` binds the agent TCP listener.
The §5.2 per-connection drain (`COMMAND_CLOSE`, 5 s) remains open.

### 2026-07-04 — loop pacing envs: PROFILER_SEAL_CHECK_INTERVAL / PROFILER_UPLOAD_CHECK_INTERVAL

`01` §6.1 defines the seal trigger but no poll cadence, and `hotstore.Config`
deliberately leaves the intervals zero so tests can seal explicitly. The app
wiring names the knobs `PROFILER_SEAL_CHECK_INTERVAL` (default 15 s) and
`PROFILER_UPLOAD_CHECK_INTERVAL` (default 30 s) — ON by default, because a
collector that never seals is not a collector. Both subcommands verify S3
connectivity at boot and treat a failure as FATAL; the compose stack orders
startup on MinIO health instead of retrying in-process.

### 2026-07-04 — the smoke proves the cold tier by stopping the collector

With no hot-retention janitor, every call stays in the hot index, so a
wide-range read alone cannot prove rows came from S3. The smoke
(`libs/tests/smoke`, driven by `make smoke`) checks the tiers by
construction instead: the hot phase asserts `/calls` and `/tree` answer while
`parquet/v1/` is still empty in MinIO, and the cold phase stops the collector
container before reading the aged bucket back — the answer can only come from
the parquet and dictionary-snapshot objects. The final phase restarts the
collector and re-reads the hot rows, exercising recovery over the compose
volume.

### 2026-07-04 — hot drop is contiguous oldest-first; the hot index outlives the local parquet question

**Question:** `02` §3 lists recently sealed local parquet as a hot source for
calls "already moved out of the hot index", and the slice-5 open issue pinned
that the parquet source must land together with the janitor that starts
dropping partitions. Read local parquet from the hot API, or never drop a
partition before its rows stop needing a hot source?

**Choice:** the second. A call-index partition is dropped only when every
`parquet_local` row of its bucket is gone — which requires `uploaded_at` set
AND `uploaded_at + hot_retention` elapsed, the same §6.3 clock that deletes
the local file — and drops walk oldest-first with a contiguity barrier: the
first bucket that is unsealed, pending, quarantined, or merely young stops
the walk. The hot index therefore covers everything the local parquet covers
at every instant, `/internal/v1/calls` keeps its single SQLite source, and
the `02` §3 "recently sealed local parquet" source is dead code we never
build. The barrier is what keeps `hot_window_oldest` truthful for the §4.3
cutoff: a quarantined bucket pins itself AND every newer bucket in the hot
tier, so no row is ever hot-invisible while its cold copy is unconfirmed —
the zero-gap invariant. The cost: one stuck upload keeps later partitions on
the PV until a human resolves it (bounded by partition size, not segment
size). The local parquet file itself now serves only as re-seal insurance
between seal and upload; past upload it is dead weight until its §6.3 clock
deletes it.

### 2026-07-04 — WAL purge waits for the hot drop; env name PROFILER_WAL_PURGE_GRACE

`01` §3.5 / `03` §3.9 step 18 gate the WAL purge on "fully flushed + grace"
but name no env and no interaction with the hot tier. Implemented gates:
closed + `dict_uploaded_at` set + no `parquet_local` row with `uploaded_at`
NULL + **no indexed calls left in any live partition** + `max(closed_at,
dict_uploaded_at) + PROFILER_WAL_PURGE_GRACE` (default 1 h) elapsed. The
added no-indexed-calls gate means the purge strictly follows the partition
drop, so a restart never leaves hot rows whose dictionary WAL is gone (the
hot `/tree` would render placeholders); it also makes it safe to release the
pod-restart's in-RAM state and remove its directory in the same step —
after the purge the pod-restart exists only in S3 and recovery has nothing
to resurrect. The grace is measured from close/snapshot-upload, not from
`max(uploaded_at)`: the hot-retention janitor deletes aged parquet rows
before the grace expires, so their timestamps are not durable inputs.

### 2026-07-04 — snapshot quarantine = dict_upload_failed_at; manifest quarantine unblocks the parquet

The slice-3 open issue: a permanent 4xx on a dictionary, suspend, or pods
manifest object retried forever. Mirrored the parquet quarantine with one
asymmetry. Dictionary/suspend: the rejected body is written under
`upload-failed/<s3-key>`, and the new `pod_restarts.dict_upload_failed_at`
takes the pod-restart out of the snapshot queue while `dict_uploaded_at`
stays NULL — so the WAL purge never fires and the WALs (the only remaining
decodable source) wait on the PV with the quarantined body. Pods manifest:
the body is parked the same way, but the file's `uploaded_at` still commits —
the parquet object IS durable, and blocking it would re-PUT a confirmed
object forever for a listing-only artifact; the marker file's existence
stops further manifest PUTs for that (day, pod-restart). Degradation: cold
`/pods` misses that day's entry until a human uploads the parked body; the
calls themselves stay discoverable. Refcounts are untouched on every path.

### 2026-07-04 — disk budget counts on-disk bytes of all three streams; eviction marks, never deletes rows

`01` §4.6 words the budget as "trace-segment disk usage";
`03` §3.2 pins that `sql`/`xml` are "refcounted and evicted like trace". The
janitor accounts the compressed on-disk size (stat, not `logical_size` — the
budget guards the PV) of every non-evicted segment across all three streams
and evicts in the deterministic order refcount-0 first, then referenced,
each oldest-first by `created_at` with the catalog key as tie-break; open
segments are skipped (a live gzip writer owns the file). Eviction removes
the file but keeps the catalog row as `status='evicted'` with its refcount:
the seal pass already maps a missing/evicted segment to
`truncated_reason=disk_budget`, and a pinned refcount must survive for the
upload release to balance. In-RAM `chunk_index` entries of evicted segments
are not released — that is the memory-budget task's job.

### 2026-07-04 — a dropped partition resurrects on a late insert

A Call whose bucket was already dropped (theoretically possible only with a
pathological agent clock: the bucket aged a full `hot_retention` past upload)
would land in a partition file that `Buckets()` never lists — invisible to
the seal loop, permanently. `partition()` therefore clears `dropped_at` when
it re-opens a dropped bucket, and `InsertCall` retries once with a fresh
handle when a concurrent janitor drop closed the cached one. The
resurrected bucket is unsealed again, so the janitor leaves it to the seal
loop and the whole seal→upload→drop cycle repeats for the late row.

### 2026-07-04 — maintain's delete-grace is stateless: hash-of-inputs + output LastModified

**Question:** `01` §6.6 orders write → grace → delete, but the maintainer is
stateless (`03` §8) and may restart — or run twice — between the write and
the delete. Where does the grace timer live?

**Choice:** nowhere locally. A compaction pass only PUTs the merged object,
keyed `maintain-<hash(sorted input keys)>` (`01` §7 hash-of-inputs). A later
pass recognises the output by recomputing the hash over the *other* members
of its `(bucket, class)` group and deletes those inputs only when
`now - LastModified(output) ≥ PROFILER_COMPACTION_DELETE_GRACE` — S3 itself
carries the clock.

**Reason:** every intermediate state is one the read path already tolerates
(both copies visible → PK-dedup; inputs gone → the output answers), so a
crash between any two steps loses nothing. The key is deterministic over the
input set, so two racing maintainers PUT the same object — the singleton
deployment is an optimisation, not a correctness requirement. A group whose
maintain object matches no subset (stragglers arrived, or a delete was cut
short) recompacts wholesale, even below `PROFILER_COMPACTION_MIN_FILES`, so
a bucket converges to exactly one object; the merge's PK-dedup absorbs the
duplicated rows.

### 2026-07-04 — maintain envs: check interval 5m, min age 30m, min files 4, max group 256MB

`01` §9 pins only `PROFILER_COMPACTION_DELETE_GRACE` (5m); `03` §10 sketches
a 1h `PROFILER_MAINTAIN_INTERVAL` for the cron mode. Implemented as the loop
knob `PROFILER_MAINTAIN_CHECK_INTERVAL` (default 5m, mirroring the
collector's `*_CHECK_INTERVAL` family; the delete step lands on the first
tick past the grace, so the interval should not dwarf the grace) plus
`PROFILER_COMPACTION_MIN_AGE` (30m — hot retention 15m + margins, the
late-arrival window a bucket must clear before compaction),
`PROFILER_COMPACTION_MIN_FILES` (4), and `PROFILER_COMPACTION_MAX_BYTES`
(256MB), a safety valve: the merge materialises every input row in RAM, so
an oversized group is skipped with a warning rather than OOMing the
singleton. The maintain TTL envs accept the contract's `d` suffix (`35d`);
`PROFILER_TIME_BUCKET` is parsed by maintain too — the settled check needs
the bucket end and the key carries only the start. Implementation choices,
not contract changes.

### 2026-07-04 — a 404 racing the column reads is the §5.1 empty case, fixed in cold

The maintain acceptance test (a reader hammering `/calls` while the
compaction deletes its inputs) surfaced a gap in the `02` §5.1 backstop: the
cold scan mapped a listed-then-deleted key to an empty result only when the
delete landed before the first byte (`Open`/`Stat`). A delete racing the
column reads — any TTL sweep does this to a slow reader, no compaction
needed — surfaced as a raw S3 error and a spurious `partial: true`.
`readRows` now re-checks existence on the error path (`gone`, one extra
round-trip on failures only) and degrades to the empty result §5.1 pins. The
row set stays complete in the compaction case by construction: inputs are
deleted only after their compacted copy has been listable for the whole
grace. A `query`-side fix landed with the maintain slice because its
acceptance depends on it; the contract text needed no change.

## Open issues

- **Maintain LISTs each whole class prefix every pass.** O(objects alive in
  the class) keys per tick, paged serially within the prefix (`02` §5.5).
  Fine at MVP scale; bound the walk to the TTL window's hour prefixes, or a
  manifest, if it profiles.
- **A compaction group is merged in RAM and written as one object.** No
  `PROFILER_PARQUET_MAX_SIZE` split of the output; a group over
  `PROFILER_COMPACTION_MAX_BYTES` is skipped with a warning and stays
  fragmented. A streaming k-way merge (inputs are already sorted) with a
  size-split output is the upgrade path.
- **The seal writer does not use `CallV2WriterOptions` yet.** The shared
  helper landed with the maintain slice for the compactor; `hotstore/seal.go`
  still carries the identical option list inline. Converge on the next
  collector touch so the writer invariants cannot drift.
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
- **Upload backoff state is per-pass.** Attempts restart on every pass, with
  no jitter and no per-file cross-pass schedule. `UploadStats` is the seam
  for the Prometheus counters that land with the metrics task.
- **`server.Service.Stop()` waits for live agent connections** and is bounded
  only by the socket read timeout (~40 s). The `03` §5.2 drain (send
  `COMMAND_CLOSE`, 5 s per-connection timeout) is not implemented; the app
  wiring shipped without it, so on SIGTERM agents see a closed socket after
  the drain grace instead of a polite close.
- **Ingest decode errors only log.** A malformed calls/dictionary record skips
  the record; there is no metric yet. Prometheus counters land with the
  metrics task (`01` §5.1 expects counters for dropped/truncated calls).
- **`collect` env coverage is partial.** `PROFILER_PARQUET_MAX_SIZE`,
  `PROFILER_SEAL_CONCURRENCY`, `PROFILER_MEM_BUDGET`,
  `PROFILER_IDLE_ACCUMULATOR_TIMEOUT`, and `S3_PATH_PREFIX` are not parsed
  because nothing behind them is implemented yet (remaining budgets tasks);
  `PROFILER_STARTUP_LOCK_WAIT` is likewise absent — the flock fails fast
  instead of waiting the `03` §3.1 30 s. (`PROFILER_HOT_RETENTION` and
  `PROFILER_CHUNKS_STAGING_MAX_BYTES` landed with the janitors slice; the
  retention TTLs landed with the maintain slice, parsed by `maintain` only —
  `collect` has no consumer for them.)
- **The smoke runs only locally.** `make smoke` needs the Docker CLI and a
  fresh compose stack (it stops and restarts the collector container); no CI
  job runs it yet.
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
- **Hot /calls materializes each overlapping partition's window rows** before
  sorting in Go (no SQL-level keyset; see the collation decision). Bounded by
  a partition's ~5 minutes of calls per page, but worth a pushed-down seek
  (component PK columns in the partition schema) if profiling shows it.
- **Replica "more rows" is inferred from a full page.** The internal API
  returns no continuation flag; the fan-out treats `len(rows) == limit` as
  "may have more", which can cost one extra empty page with a non-null
  cursor — explicitly allowed by §2.3.1's termination rule.
- **`/internal/v1/calls/{pk}` probes every partition.** A bare PK carries no
  time hint (`02` §2.2 suggests a `call_ref`); the janitor now bounds the
  partition count to roughly `hot_retention / PROFILER_TIME_BUCKET`, so the
  point SELECTs stay cheap.
- **The disk-budget pass stats every segment file on every tick.** O(catalog)
  `stat` calls per `PROFILER_JANITOR_CHECK_INTERVAL`; cache the sizes in the
  catalog if it ever profiles.
- **A quarantined manifest's parked body carries first-failure bounds.** The
  marker file stops further PUT attempts, so a later seal of the same
  (day, pod-restart) never refreshes the parked `time_max_ms`; whoever
  uploads the body manually gets slightly narrow bounds. The rows themselves
  are unaffected.
- **Eviction leaves in-RAM chunk refs of evicted segments.** The seal and the
  trace endpoint tolerate them (they map to `disk_budget` / 404), but the
  memory they hold is only released by the future memory-budget task.
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
