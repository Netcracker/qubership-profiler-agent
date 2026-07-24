# 03 — Lifecycle

> Status: **draft**, awaiting review. Process lifecycle for each subcommand of the single Go binary: startup, recovery, readiness, flush, and shutdown. The collector path carries most of the complexity because of crash recovery and event-level blob assembly (`01-write-contract.md` §4).

> **2026-07-01 alignment — now in the body.** Recovery is aligned with the hot-store + seal-pass model (drop unclosed calls, rebuild the index from gzip segments, dictionary continuity on reconnect); it is folded into §2–§6 below.

## 1. Scope

The single Go binary `profiler-backend` runs in one of four modes (`profiler-plan.md`, single-binary VictoriaMetrics-style decision):

| Subcommand | k8s shape | Persistent state | Lifecycle complexity |
|---|---|---|---|
| `collect` | StatefulSet + RWO PV | dictionary/params/suspend WAL, `calls.wal`, trace segments, `metadata.sqlite`, sealed parquet | high (recovery, drain) |
| `query` | Deployment, stateless | — | low |
| `maintain` | Deployment or CronJob | — | low |
| `all` | dev only, single process | combination of the above on a local FS | varies |

This document defines the state machine for each. `04-storage-layout.md` covers the k8s manifests that wire these processes up.

## 2. `collect` startup state machine

```
INIT → LOADING → RECOVERY → READY → DRAINING → TERMINATING → (exit)
              ↘ FATAL (on unrecoverable corruption) → (exit)
```

| State | Readiness probe | What is happening |
|---|---|---|
| `INIT` | 503 | Process started; binary initialization. |
| `LOADING` | 503 | Mount PV; acquire `collector.lock`; open `metadata.sqlite`; bind ports but DO NOT accept agent TCP yet. |
| `RECOVERY` | 503 | Replay dictionary/params/suspend/calls WALs; rebuild the segment catalog and `chunk_index` from trace segments; reconcile local parquet; re-seal un-sealed buckets; re-trigger pending uploads. |
| `READY` | 200 | TCP listener accepts new agent connections; `/internal/v1/*` serves reads; seal loop runs. |
| `DRAINING` | 503 | Received SIGTERM. Marked Not-Ready so kubelet removes from headless service endpoints. Still serves in-flight requests and TCP connections. |
| `TERMINATING` | 503 | TCP listener closed; finalizing in-flight pod-restarts (seal all dirty buckets; upload parquet; upload dictionaries). |
| `FATAL` | 503 | An unrecoverable condition (corrupt SQLite that cannot be repaired, repeated S3 PUT failures during recovery, PV mount missing). Process exits non-zero so kubelet restarts it; alert fires. |

The readiness endpoint is `GET /internal/v1/health/ready` (separate from `/internal/v1/health/hot-window` from `02-read-contract.md` §3).

## 3. Recovery sequence (`LOADING` → `READY`)

This is the heaviest section because chunk-level reassembly (`01-write-contract.md` §4) and the dictionary cold-path lifecycle (`01-write-contract.md` §3.6) both depend on it.

### 3.1 Mount PV and acquire exclusive lock

1. Verify `/data` is mounted and writable. If not → `FATAL`.
2. Open `/data/collector.lock` with `flock(LOCK_EX | LOCK_NB)`. If another process holds it → wait up to `STARTUP_LOCK_WAIT` (default 30 s), then `FATAL`. (Misconfigured `volumeClaimTemplates` would lead to two pods racing; the lock guarantees exclusive ownership.)

### 3.2 Open metadata SQLite

3. Open `/data/metadata.sqlite` and run schema migrations. One central database per collector replica; it holds no bulk stream bytes (`01-write-contract.md` §2). Central tables:

   - `pod_restarts (pod_restart PRIMARY KEY, namespace, service, pod_name, restart_time_ms, opened_at, closed_at NULL, wals_purged_at NULL)` — one row per TCP connection; parent of every table below. `closed_at NULL` marks a live connection.
   - `segments (pod_restart, stream, rolling_seq, path, logical_size, time_min_ms NULL, time_max_ms NULL, refcount, status, created_at, evicted_at NULL, PRIMARY KEY (pod_restart, stream, rolling_seq))` — hot-store segment catalog for the offset-addressable bulk streams `trace`, `sql`, `xml`. One row per agent stream file; `rolling_seq` is the agent's file index (`01-write-contract.md` §4.4), so a pointer resolves by opening `<stream>/<rolling_seq>.gz` and seeking. `refcount` counts the un-uploaded sealed rows whose blobs source from the segment; it reaches 0 when the segment is unlinkable. `time_min_ms` / `time_max_ms` apply to `trace` only.
   - `parquet_local (path PRIMARY KEY, pod_restart, time_bucket_ms, retention_class, seq, row_count, time_min_ms, time_max_ms, file_size, sealed_at, uploaded_at NULL, upload_failed_at NULL, first_failed_at NULL, body_durable_at NULL, s3_key NULL)` — every locally held sealed parquet file. `uploaded_at NULL` means the upload is still pending. `body_durable_at` is set once the parquet body is confirmed in S3 while `uploaded_at` is still NULL — the "body durable, manifest pending" state a rejected pods manifest leaves the row in (`01-write-contract.md` §6.2), so the §8 slow re-test retries only the manifest and never re-PUTs the durable body.
   - `seal_state (pod_restart, bucket, retention_class, watermark, last_sealed_at, dirty, PRIMARY KEY (pod_restart, bucket, retention_class))` — which calls each seal covered, and whether the bucket needs re-sealing after late data (`01-write-contract.md` §6.6).
   - `call_partitions (bucket PRIMARY KEY, path, created_at, dropped_at NULL)` — catalog of the per-bucket call-index databases (below). `bucket = floor(ts_ms / PROFILER_TIME_BUCKET)`.

   The call index does not live in `metadata.sqlite`. It is partitioned by time bucket into one SQLite file per bucket (`calls-<bucket>.sqlite`), so eviction drops a whole partition instead of running a large `DELETE`. `query` ATTACHes the partitions overlapping the request range; the hot janitor DROPs a partition once it ages past `hot_retention`. Each partition holds one table:

   - `call_index (pod_restart, trace_file_index, buffer_offset, record_index, ts_ms, duration_ms, method_id, thread_name, retention_class, error_flag, cpu_time_ms, wait_time_ms, memory_used, child_calls, params_json, calls_wal_offset, blob_size, truncated_reason NULL, PRIMARY KEY (pod_restart, trace_file_index, buffer_offset, record_index))`, indexed by `ts_ms`, `duration_ms`, and `method_id`. It backs hot `/internal/v1/calls` and drives the seal loop (`01-write-contract.md` §5.1); the filter columns answer a query without touching the blob, and `calls_wal_offset` locates the full record in `calls.wal` for the seal pass.

   Notes on the model:

   - **The PK is the read contract's Call PK** (`02-read-contract.md` §2.2). `pod_restart` expands to `(namespace, service, pod_name, restart_time_ms)`; the three trace-pointer integers complete it. There is no `thread_id` component — recovery drops unclosed calls (§3.7), so the placeholder PK is gone.
   - **`trace_file_index` is the agent's rolling sequence id, not a collector ordinal.** Segment files stay 1:1 with the agent's stream files, so `(trace_file_index, buffer_offset)` resolves without an offset-translation table (`01-write-contract.md` §4.4).
   - **`sql` and `xml` sit in the same catalog as `trace`.** A trace tag of type `PARAM_BIG` points into `xml`, `PARAM_BIG_DEDUP` into `sql`, each by `(rolling_seq, offset)` (`backend/libs/parser/pipe/traces.go`). Both must survive in the hot store for a blob to decode, so both are refcounted and evicted like `trace`.
   - **`dictionary`, `params`, `suspend`, and the raw Call records are not in the segment catalog.** They use append-only WAL files (§3.4; `01-write-contract.md` §3): the dictionary needs per-entry `fsync` durability, because one lost entry makes every trace byte that references it undecodable.

4. Self-check: run `PRAGMA integrity_check` on `metadata.sqlite` and on each attached call partition. On corruption, repair by deleting the affected file and rebuilding from PV contents — rescan the gzip segments to rebuild `segments` (§3.5), re-read parquet footers to rebuild `parquet_local` (§3.6), and re-decode `calls.wal` to rebuild the partitions (§3.4). Rebuild is costly but recoverable. If repair fails → `FATAL`.

### 3.3 Determine which pod-restarts are closed

Because the collector crashed (or was killed), every TCP connection has been broken. Agents have reconnected — likely to a different replica — and started fresh pod-restarts there with new `restart_time_ms`. Therefore:

**All pod-restarts in `pod_restarts` table with `closed_at IS NULL` are now closed.** Update each: `closed_at = now()`.

5. The collector's job is to finalize these closed pod-restarts (steps 3.4–3.7) and the pending uploads (step 3.8), then begin serving.

### 3.4 Replay dictionary, params, suspend WALs

For each closed pod-restart with WAL files on PV:

6. Open `dictionary.wal`. Read length-prefixed records (`01-write-contract.md` §3.2). Reconstruct the in-memory dictionary. Stop at first structurally invalid record; truncate file at that point (this is the standard WAL tail-corruption recovery).
7. Same for `params.wal`, `suspend.wal`. Then reconcile `calls.wal` against the per-bucket call partitions: for any record past the last-recorded `calls_wal_offset` (e.g. a crash between the `calls.wal` append and the SQLite insert), re-insert its `call_index` row into the partition for its bucket, creating the partition if it does not exist, so the WAL and the index agree before the seal loop reads them.

If a WAL is missing (e.g. crash between TCP accept and first dictionary entry), the pod-restart is recorded with an empty dictionary — any blobs from its chunks will be uninterpretable, but the Call rows themselves still have resolved method names (resolution happens at write time; `01-write-contract.md` §5.1).

**Torn tail vs. mid-file corruption.** Every WAL record carries its own trailing CRC32, not just the length prefix, so replay can tell the two ways truncation happens apart. A torn tail (the process crashed mid-write) fails the length or bounds check on an incomplete record. Mid-file corruption (a bit flip on disk) fails the CRC check on a record whose length looks valid. Replay treats both the same way: it truncates the file at the first invalid record and keeps only the records before it (`libs/collector/hotstore/wal.go` `ReplayWal`). For `calls.wal` this truncation is enforced end to end: `PurgeCallsPastWalEnd` deletes every `call_index` row whose `calls_wal_offset` lands past the truncated end, even where the underlying WAL bytes happen to be intact (the "inverse skew" case in step 7 above). Recovery deliberately drops the entire valid tail after the corruption point, not just the corrupted record, because it does not scan past a broken record to find where corruption ends. This keeps the WAL and the SQLite index consistent with each other, but it is not maximally data-preserving — a single flipped bit can discard records that were otherwise intact.

### 3.5 Rebuild the segment catalog and chunk index

For each closed pod-restart:

8. Walk `trace/*.gz`. Each file is one agent stream segment named by its `rolling_seq`; record a `segments` row `(pod_restart, 'trace', rolling_seq, path, logical_size, time_min_ms, time_max_ms, …)`, taking `logical_size` from the decompressed length and the time range from the chunk headers. While decompressing, parse the logical chunks by their 16-byte header `[threadId, startTime]` and trailing `EVENT_FINISH_RECORD` to rebuild the per-thread chunk index (step 9).
9. Rebuild the per-thread `chunk_index[threadId]` from the parse — an ordered list of `(rolling_seq, offset, length)` per thread. This is the same index the write path maintains (`01-write-contract.md` §4.3); the seal pass reads it to assemble blobs. In the same walk, catalog `sql/*.gz` and `xml/*.gz`: these carry offset-addressed values, not chunked events, so record one `segments` row per file (`stream` = `sql` or `xml`, `logical_size` = decompressed length, no time range) without parsing the body. A blob's `(rolling_seq, offset)` references into them resolve by opening the matching segment.

### 3.6 Reconcile local parquet and seal scratch

10. Delete everything under `parquet-sealing/`: a seal pass in flight at crash time left scratch files with no footer, so they are unreadable and are re-produced by re-sealing (§3.7). If a `parquet_local` row points at a file missing on disk, reconcile it by upload state. An **uploaded** row (`uploaded_at` set) is durable in S3 — the hot-retention janitor dropped the local copy and crashed before deleting the row — so clear the row alone. A **pending** row (`uploaded_at IS NULL`) was lost in the seal-commit-to-upload window: clearing it alone would strand its calls below the seal watermark forever, invisible to upload and cold queries even though their segment data survives. Recovery first rewinds the bucket's seal watermark to `wal_offset_lo` (the start of the pass that produced the file), re-exposing those calls for a re-seal, and then clears the row — both in one transaction, so a crash cannot drop the row while the watermark stays ahead. The rewind is bounded to that pass and later; any surviving parquet it re-seals produces a cold duplicate that PK-dedup collapses (`02-read-contract.md` §6). A row written before `wal_offset_lo` existed carries `lo = 0`, so a lost legacy pending file rewinds the whole bucket — still bounded and dedup-safe, never the silent-loss bug.
11. For each sealed parquet with `uploaded_at IS NULL`, enqueue it for upload retry (step 3.8). These are valid files — seal finished (footer present) but upload did not.

> **Known gap.** The reverse repair — rebuilding `parquet_local` from the footers of orphaned sealed files after a lost `metadata.sqlite` (§3.2 step 4) — is not implemented. A sealed file with no catalog row is left on disk; its calls re-seal from the WAL (duplicates collapse by PK-dedup) and the orphaned file leaks until deleted by hand.

### 3.7 Finalize closed pod-restarts: drop unclosed calls

The trace bytes still in the hot store belong to root calls that the agent **may or may not have closed** before the TCP connection broke. Without a Call record the collector has neither the call's metrics nor its end pointer, and a call split across two connections (reconnect to another replica) is incomplete on any single replica anyway. Decision: **drop unclosed calls.**

For each closed pod-restart:

12. Run a seal pass (`01-write-contract.md` §6.5) for every bucket whose `seal_state` watermark trails its indexed calls. Only calls with a `call_index` row are sealed; the trace bytes of any unclosed call (no Call record) are discarded.
13. Do not emit placeholder rows. There is therefore no `(0,0,0)` PK and no cross-thread PK collision.
14. Release segment refcounts as each sealed bucket uploads; a segment referenced by no remaining un-sealed call is unlinked.

Cost trade-off: the trace bytes of an unclosed call are lost. This is acceptable — any call the agent had already closed and flushed is durable in parquet + S3, and the agent continues an in-flight call on its new connection (a fresh pod-restart) after reconnect. Dedup by PK (`02-read-contract.md` §6) still collapses duplicates from recovery vs. normal write for closed calls.

### 3.8 Re-trigger pending uploads

15. For each row in `parquet_local` with `uploaded_at IS NULL`, schedule an upload job in the upload worker pool. Idempotent because the S3 key is deterministic (`01-write-contract.md` §7).
16. After each upload completes: set `uploaded_at = now()`; decrement segment refcounts for the calls that file sealed.

This step runs asynchronously; `READY` does not wait for it.

### 3.9 Purge WALs of fully uploaded pod-restarts

17. Sealed rows carry their own dictionary subset and suspend pauses (`01-write-contract.md` §3.6, schema version 3), so no snapshot upload step exists any more.
18. Delete a closed pod-restart's local WAL files once (a) every sealed parquet row is uploaded (steps 3.7+3.8 complete), (b) no `calls.wal` offset it owns is still indexed in any live partition, and (c) `PROFILER_WAL_PURGE_GRACE` (default 1 h) has elapsed past its `closed_at`. Gate (b) is what a reconnect storm stresses: purges wait out the hot-index aging (hot retention + eviction cadence), not just the grace, and the load campaign measured the tracked-restart backlog growing unbounded as purge eligibility lagged (`load-testing-report.md` §8, `runs/20260717T133845Z-t5-reconnect-storm`; step 18a is the bound). Gate (b) makes the purge strictly follow the call-index partition drop: purging earlier could strand hot rows whose dictionary WAL is already gone, and hot `/tree` would then render `#<id>` placeholders — the exact hazards step 18a removes before it skips the gate.

18a. **Near-empty fast path.** A closed pod-restart whose on-PV directory (WAL files plus segments) is at or under `PROFILER_WAL_PURGE_FAST_MAX_BYTES` (default 16 MiB; `0` disables the fast path) purges under gates (a) and (c) alone, without waiting for gate (b). Two properties replace gate (b). First, every indexed call is sealed — an in-flight seal pass keeps its watermark uncommitted, so the check defers the purge to the next janitor tick; combined with gate (a) this also means the upload already released the segment refcounts, so no seal or upload pass reads the deleted segments. Second, before the dictionary WAL goes, the purge materializes each still-indexed row's method name into the partition's `method_text` column, committed before the first file deletion so a crashed purge re-runs as a no-op; the hot `/calls` path prefers that column, so the rows keep rendering names until the partition drops. Hot trace reads of those rows answer 404 and the query service falls through to the cold copy via the `ts_ms` hint (`02-read-contract.md` §2.2, §2.4); a request without the hint stays 404, exactly as for any call outside the hot tier. This is what bounds the tracked backlog under a reconnect storm: eligible near-empty pod-restarts accumulate at most `restart rate × WAL_PURGE_GRACE`, independent of hot-window drift, while restarts over the floor keep waiting for gate (b) (`01-write-contract.md` §3.5; `load-testing-report.md` §8). The floor is sized to cover a full dictionary resend plus seconds of trace — the churn-restart shape — while a pod-restart with real traffic history outgrows it within minutes.

Steps 3.8 and 3.9 are background tasks; `READY` is reached after step 3.7 completes.

### 3.10 Become READY

After 3.1–3.7 finish:

19. Bind the TCP listener for agent connections (`PROFILER_AGENT_PORT`, default `1715`).
20. Bind the internal HTTP listener for `/internal/v1/*` (`PROFILER_INTERNAL_API_PORT`, default `8081`).
21. Start the seal loop, the segment janitor, the parquet upload worker pool, and the hot-retention janitor.
22. Flip `/internal/v1/health/ready` to 200.

Expected duration of 3.1–3.7 on a healthy PV: seconds to tens of seconds. Dominated by step 3.5 (decompress-and-walk of the trace segments) when the PV holds gigabytes of segments.

## 4. Readiness probe semantics

`GET /internal/v1/health/ready`:

- `200 OK { "state": "READY" }` once step 3.10 completes.
- `503 Service Unavailable { "state": "INIT"|"LOADING"|"RECOVERY"|"DRAINING"|"TERMINATING"|"FATAL", "details": "..." }` otherwise.

The state name is for kubelet logs and human debugging; kubelet only cares about the HTTP code.

`/internal/v1/health/live` (liveness):

- `200 OK` while the process is healthy enough to keep running.
- `503` only if a deadlock, OOM-imminent, or repeated fatal errors are detected. Most failures should fail readiness, not liveness — liveness failure causes a kubelet kill, which is more disruptive.

Headless service relies on readiness — non-ready pods do not appear in DNS A-records (`02-read-contract.md` §7.1), so `query` won't fan out to them.

## 5. Shutdown sequence (`READY` → exit)

Triggered by SIGTERM (kubelet drain) or SIGINT (operator).

### 5.1 Drain phase (`DRAINING`)

1. Flip readiness to 503 with `state: "DRAINING"`.
2. **Do not close TCP listener yet.** Wait `PROFILER_SHUTDOWN_DRAIN_GRACE` (default 30 s) so kubelet removes the pod from the headless service's endpoints. `query`'s next DNS resolution skips this replica.
3. During this grace period, the collector continues to:
   - Accept new agent TCP connections (sticky-TCP routing isn't aware of readiness; new agents may still land here).
   - Serve `/internal/v1/*` reads for any in-flight `query` requests already routing to this replica.
   - Flush parquet on the normal schedule.

### 5.2 Stop new connections (`DRAINING` → `TERMINATING`)

4. After the drain grace, close the TCP listener (no new agent connections accepted).
5. For each active agent TCP connection, send `COMMAND_CLOSE` (`backend/libs/protocol/commands.go`); wait for the agent's acknowledgement up to `PROFILER_AGENT_CLOSE_TIMEOUT` (default 5 s). If timeout → close from collector side.
6. The affected agents will reconnect — to a different collector replica (this one is not in DNS anymore) — and start a fresh pod-restart there. The current pod-restart on this replica is now closed.

> **Known gap.** Step 5's `COMMAND_CLOSE` drain is not implemented. On shutdown `server.Service.Stop()` force-closes each live agent connection instead of sending `COMMAND_CLOSE`, so an agent sees a dropped socket rather than a graceful close (it reconnects either way, `06-wire-protocol-server.md` §6). The force-close is deliberate — a drain would otherwise hold `Stop()` until each idle connection hit its ~40 s read deadline — but the polite `COMMAND_CLOSE` handshake with the 5 s per-connection timeout is still owed.

### 5.3 Finalize closed pod-restarts

7. For each pod-restart closed in 5.2:
   - Force-seal every dirty bucket (`01-write-contract.md` §6.5), regardless of the bucket-end trigger. Calls with no Call record are dropped, same as recovery step 3.7.
   - Upload each sealed file to S3; the rows are self-contained (`01-write-contract.md` §3.6), so nothing else is owed.
8. Wait for all pending uploads to complete, bounded by `PROFILER_SHUTDOWN_UPLOAD_TIMEOUT` (default 60 s). Uploads that don't complete: leave parquet on PV (next collector start will retry; `metadata.sqlite` carries the state).

### 5.4 Final cleanup

9. Close `metadata.sqlite`.
10. Release `collector.lock`.
11. Exit 0.

Total shutdown budget = `SHUTDOWN_DRAIN_GRACE + AGENT_CLOSE_TIMEOUT + SHUTDOWN_UPLOAD_TIMEOUT` = `30 + 5 + 60` = ~95 s. k8s `terminationGracePeriodSeconds` should be at least this (see `04-storage-layout.md`).

## 6. Seal triggers (reference)

The collector's seal loop is defined in `01-write-contract.md` §6.1. Summary:

- **Bucket end + grace** (default 5 min + 30 s).
- **Late Call re-marks a sealed bucket dirty** (re-seal into a patch file).
- **Memory pressure** (collector exceeds `mem_budget`).

The seal loop runs only in `READY`. During `DRAINING`, sealing continues on the normal schedule; during `TERMINATING`, every dirty bucket is force-sealed regardless of triggers (§5.3).

## 7. `query` subcommand lifecycle

Stateless. Simple state machine:

```
INIT → LOADING → READY → TERMINATING → exit
```

### 7.1 Startup

1. Parse config; resolve `COLLECTOR_HEADLESS_SVC` once at boot to verify it resolves at all (warn but don't fail if it doesn't — collectors may come up later).
2. Bind both listeners and set the gate to `LOADING`, before S3: the external API listener (`PROFILER_EXTERNAL_API_PORT`, default `8080`) and the metrics listener (`PROFILER_METRICS_PORT`, default `8081`). A probe must see `LOADING` rather than connection-refused while S3 comes up (PR 708 review #22). Bind synchronously so a port collision fails startup at once instead of hiding behind S3 retries. The metrics listener carries `/metrics` and `/debug/pprof` off the external port, so the ingress never exposes them (`04-storage-layout.md` §12).
3. Connect to S3 endpoint; verify bucket access. If unrecoverable → `FATAL`.
4. Flip readiness to 200 (`READY`).

### 7.2 Per-request

DNS is re-resolved on every external request (`02-read-contract.md` §7.1). No caching at the process level.

### 7.3 Shutdown

1. SIGTERM: flip readiness to 503, wait `PROFILER_SHUTDOWN_DRAIN_GRACE` (same default 30 s) for kubelet to remove from service endpoints.
2. Stop accepting new HTTP requests.
3. Allow in-flight requests to complete, bounded by 15 s.
4. Exit 0.

## 8. `maintain` subcommand lifecycle

Stateless. Two run modes:

### 8.1 Cron mode (`profiler-backend maintain --cron`)

Process runs continuously; uses `backend/libs/cron/` to schedule jobs. Same simple startup as `query`. Shutdown closes the cron scheduler and exits.

### 8.2 One-shot mode (`profiler-backend maintain --run-now`)

Process runs the scheduled jobs once and exits. Used by k8s CronJob if we prefer one-pod-per-run over a long-running scheduler. Decision deferred to `04-storage-layout.md`.

## 9. `all` subcommand lifecycle (dev only)

In-process composition of `collect` + `query` + `maintain`. Used for dev (`profiler-plan.md` decision). Lifecycle is the union:

1. Filesystem-emulated S3 (`backend/libs/s3/` filesystem emulator, deferred — currently MinIO in docker-compose; both supported as dev variants).
2. Local `/data` directory, no PV semantics.
3. Combined startup runs all three subcommands in goroutines under one `oklog/run` group.
4. Shutdown sends one signal; each subcommand drains in its own grace period (collector's longest), then process exits.

The `all` mode is documented as **dev-only**; production uses three separate k8s workloads. Detection: refuse to start in `all` mode if `KUBERNETES_SERVICE_HOST` is set (`PROFILER_ALLOW_K8S_ALL_MODE=true` override exists for k8s-based dev clusters).

## 10. Configuration

### `collect`

| Env | Default | Description |
|---|---|---|
| `PROFILER_AGENT_PORT` | `1715` | TCP listener for agent connections. |
| `PROFILER_INTERNAL_API_PORT` | `8081` | HTTP listener for `/internal/v1/*`. |
| `PROFILER_STARTUP_LOCK_WAIT` | `30s` | How long to wait for `collector.lock` before `FATAL`. |
| `PROFILER_SHUTDOWN_DRAIN_GRACE` | `30s` | DRAINING phase: wait for kubelet endpoint removal (§5.1). |
| `PROFILER_AGENT_CLOSE_TIMEOUT` | `5s` | Per-connection drain timeout (§5.2). |
| `PROFILER_SHUTDOWN_UPLOAD_TIMEOUT` | `60s` | Bound on completing pending uploads at shutdown (§5.3). |

### `query`

| Env | Default | Description |
|---|---|---|
| `PROFILER_EXTERNAL_API_PORT` | `8080` | HTTP listener for `/api/v1/*` and the UI. |
| `PROFILER_METRICS_PORT` | `8081` | Separate listener for `/metrics` and `/debug/pprof`, kept off the external port so the ingress never exposes them (`04-storage-layout.md` §12). |
| `COLLECTOR_HEADLESS_SVC` | — | Headless service DNS for collector discovery (`02-read-contract.md` §7.1). |
| `PROFILER_SHUTDOWN_DRAIN_GRACE` | `30s` | Same semantics as `collect`. |

### `maintain`

| Env | Default | Description |
|---|---|---|
| `PROFILER_MAINTAIN_INTERVAL` | `1h` | Cron-mode scheduling interval. |
| (retention TTLs) | (see `01-write-contract.md` §9) | Per retention class. |

### `all`

| Env | Default | Description |
|---|---|---|
| `PROFILER_ALLOW_K8S_ALL_MODE` | `false` | Override to allow `all` mode under k8s (dev cluster); refuses by default. |
| (all of the above) | as above | Subcommands inherit their respective env vars. |

## 11. What this contract does NOT cover

- **k8s manifests** (StatefulSet, Headless Service, PVC templates, probe wiring) → `04-storage-layout.md`.
- **Agent reconnection behavior** (timing, jitter, backoff) — set by the agent, not the collector. Out of scope here.
- **maintenance job specifics** (compaction algorithms, retention enforcement loop) — covered briefly in `profiler-plan.md`, detailed when Stage 4 begins.
- **Backpressure to the agent** when the collector is overloaded — the protocol has no graceful backpressure signal. The collector's only lever is refusing `RCV_DATA` with `ACK_ERROR` under the pending-upload budget (`01-write-contract.md` §4.6), which the agent answers by dropping the refused window and reconnecting; the refused bytes are counted and alerted on. A protocol-level signal remains a future C-track item in `profiler-plan.md` (C2 runtime config).

## 12. Review checklist

- [x] State machine names (`INIT`/`LOADING`/`RECOVERY`/`READY`/`DRAINING`/`TERMINATING`/`FATAL`) — accepted.
- [x] Recovery decision to mark ALL on-disk pod-restarts as closed (§3.3) — accepted.
- [x] Truncated-blob emission strategy for recovery (§3.7) — accepted.
- [x] Shutdown budget total (~95 s) — accepted; `terminationGracePeriodSeconds` will be set to at least 100 s in `04-storage-layout.md`.
- [x] `liveness` vs `readiness` split (§4) — accepted.
- [x] Cron vs one-shot for `maintain` (§8) — accepted; ship BOTH modes (`--cron` for long-running deployment, `--run-now` for k8s CronJob). Operator picks per environment.
- [x] `all`-mode design (§9) — accepted; current refusal-under-k8s + env override stays as-is.
