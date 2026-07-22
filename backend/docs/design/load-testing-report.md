# Load-testing report: Go profiler backend

Status: **campaign closed 2026-07-20 — the local program is complete; every cluster number is frozen as a
placeholder with the spec and checklist step that fills it** (`tools/load-generator/doc/cluster-checklist.md`).
The harness is in maintenance mode: no new scenarios, fixes only. Owner: @vlsi.

Every number in this report must cite its run: the artifact directory (`runs/<ts>-<name>/`, kept outside the
repository), the image digests, and the frozen run spec inside it. A number without a run citation is a placeholder,
not a result. Numbers from the local stand (OrbStack) validate the harness and are marked *(local, not a ceiling)* —
they must never be quoted as collector limits. One class of local outcome IS a result: the accelerated-timer soak's
lifecycle verdict (§5) is functional, not numeric, and carries to the cluster with the plan-§10 caveat that slow
leaks need the real-timer run.

## 1. Scope and method

This report covers the ceiling campaign (plan §9.3): the throughput ceiling (T2, plan §7.2) and the connection-count
ceiling (T3, plan §7.3) of a single collector replica, and the contract + soak campaign (plan §9.4): the contract
run (T1, §7.1), the soaks (T4, §7.4), and the background query load (T6, §7.6). Ceilings are measured with:

- traffic: the virtual dumper (`virtual-dumper.md`), calibrated against the real Java agent (plan §12);
- generator: the k6 fleet module (`tools/load-generator/pkg/cdt`), externally-controlled executor;
- orchestration: `tools/load-generator/runner` (`doc/run-orchestration.md`) — stepwise VU ramp with holds until the
  key series flatten, saturation detectors, pprof capture at 70% / 100% of the ceiling, artifact archival;
- runbooks: `tools/load-generator/doc/ceiling-runs.md`.

A step counts toward a ceiling only when the generator guard held (k6 pod CPU under 70% of its limit) and the level
was confirmed (`k6_vus`, and for T3 the `profiler_ingest_active_connections` gauge) before the hold started.

### 1.1 Goals: where the campaign closed

Plan §1 goals, each with its honest status at close-out. "Blocked-cluster" means the harness, spec, and detectors
are validated and the run itself waits for the large cluster — the checklist step names the mechanical path.

| Plan §1 goal | Status |
| --- | --- |
| (1) Resource utilization under load | **Blocked-cluster.** Specs frozen; observability validated end to end (plan §11). Checklist steps 1–4. |
| (2) Contract ~6 MB/s confirmed | **Blocked-cluster.** `specs/t1-contract.yaml` frozen; spec mechanics validated (§6, `runs/20260716T155045Z-t1-contract`). Checklist step 3. |
| (3) Single-replica ceilings (T2/T3) | **Blocked-cluster.** Ramp, saturation detection, and the connection gauge validated by smoke runs (§3–§4). Checklist steps 1–2. |
| (4) Long-run stability (soak) | **Functional verdict closed locally**: the accelerated soak cycles the full lifecycle cleanly under the §8 checker (§5, `runs/20260717T091106Z-t4-soak-accelerated`). The mandatory real-timer 24–48 h run is blocked-cluster (checklist step 4) — the accelerated run cannot see slow leaks (plan §10). |
| (5) Crashloop characterization + protection decision | **Closed locally.** T5 characterized (§8), the §7.5.4 protection decision recorded; the accept-side cap alone defers to the cluster T3 numbers (checklist §3). |

### 1.2 Portable findings

What carries to any stand regardless of scale — mechanisms and orderings, not absolute numbers. Each line points at
the detailed section and its run.

- **The backpressure gate order depends on the backlog mix**: a WAL-dominant backlog trips `IngestPaused` with
  `SealPaused` never firing; the documented seal-first order presupposes a pending-dominant backlog. Folded into
  `01-write-contract.md` §4.6 (§9, `runs/20260717T165508Z-t7-s3-outage`).
- **The storm backlog is purge-gated and unbounded**: WAL purge waits out hot-index aging, purge eligibility
  degrades as the backlog ages, and churn keeps every restart near-empty — the basis of the §7.5.4 fast-path
  decision, folded into `01-write-contract.md` §3.5 (§8, `runs/20260717T133845Z-t5-reconnect-storm`).
- **Recovery is fast and flat**: READY ~10 s after a grace-0 kill through one designed `collector.lock` crash
  cycle, with no cycle-over-cycle degradation across ten kills — folded into `01-write-contract.md` §8 (§8,
  `runs/20260717T142638Z-t5-restart`, `runs/20260717T152844Z-t5-crashloop`).
- **Class-aware eviction holds a shrunken budget exactly**, at a counted truncation cost the read side degrades
  explicitly on (§9, `runs/20260717T215007Z-t7-small-pv`).
- **The wire protocol is latency-bound**: 2 s RTT costs ~40× throughput without breaking sessions — the
  co-location assumption now lives in `06-wire-protocol-server.md` §5 (§9, `runs/20260717T235336Z-t7-agent-net`).
- **Sustained late data floods compaction** with patch files for closed buckets — a designed degradation, folded
  into `01-write-contract.md` §6.6 (§9, same run).
- **The read path has no global memory bound**: the per-request scan guard counts compressed bytes and multiplies
  under concurrency — OOM in 34 s at 3 Gi; the P1 backlog item (§7, `load-testing-backlog.md`).
- **Deep pagination costs a full re-scan per page** (~23 s/page on the local probe shape), exactly as the read
  contract prices it, and the span guard rejects fail-closed in ~4 ms with no I/O (§7).

## 2. Stands

| Stand | Purpose | Disk / S3 | Status |
| --- | --- | --- | --- |
| OrbStack (local) | harness development, smoke runs | local SSD, in-cluster MinIO | used for everything below |
| Large k8s cluster | final numbers | network PVs, real node limits | **not yet available** |

MinIO understates real object-storage latency on both stands (plan §10); cold-read conclusions will carry that
caveat when T6 runs.

## 3. T2: single-replica throughput ceiling

> Placeholder — to be filled from large-cluster runs. Per sweep (bytes/s, calls/s small, calls/s large, dictionary
> churn): the ceiling in MB/s and calls/s, the firing detector, the limiting stage (ingest decode / SQLite index /
> seal / upload) read from the CPU profiles, and the CPU/RAM/disk-I/O curves over the ramp. Filled by checklist
> step 1 (`tools/load-generator/doc/cluster-checklist.md`, specs derived from `specs/t2-bytes.yaml`).

| Sweep | Ceiling | Firing detector | Limiting stage | Run |
| --- | --- | --- | --- | --- |
| bytes/s (pods) | — | — | — | — |
| calls/s, small calls | — | — | — | — |
| calls/s, large calls | — | — | — | — |
| dictionary churn | — | — | — | — |

### Harness validation *(local, not a ceiling)*

Two OrbStack runs on 2026-07-16, images `profiler-backend:dev@sha256:8a7ecf…` and
`cdt-load-generator:dev@sha256:1c580c…`:

- **Ramp mechanics** (`runs/20260716T111912Z-t2-bytes-smoke`): 2 → 4 → 8 pods (1 pod/VU, 8 threads, 5 calls/s).
  Every step confirmed through `k6_vus` (~40 s remote-write lag), reached its plateau, and scaled linearly —
  65 / 130 / 255 KB/s ingest. All six pprof profiles (CPU, heap, goroutine at 70% and 100%) landed, `steps.jsonl`,
  the series exports, and `result.json` are complete. Generator CPU stayed at 1–2% of its limit.
- **Saturation detection** (`runs/20260716T113242Z-t2-bytes-sat`): with `PROFILER_PENDING_UPLOAD_MAX_BYTES` shrunk
  to 8 MiB the first step fired three detectors at once — `ingest-paused`, `refused-bytes`, and the generator-side
  `ack-errors` — proving the whole backpressure chain end to end: the gate refuses `RCV_DATA`, the virtual dumpers
  see `ACK_ERROR_MAGIC` and enter the agent's reconnect path, and the runner stops the ramp with verdict
  `saturated`.

## 4. T3: connection-count ceiling

> Placeholder — to be filled from large-cluster runs: RAM and goroutines per idle connection, cost per tracked
> pod-restart, where accept latency / `PROFILER_MEM_BUDGET` / the fd limit bites first, and what the failure looks
> like to the agent (there is no accept-side connection cap today, `libs/server/services.go`). Filled by checklist
> step 2 (`specs/t3-connections.yaml`); the ramp-continuation rule and the accept-cap decision criteria are frozen
> in the checklist's §3.

| Metric | Value | Run |
| --- | --- | --- |
| RAM per idle connection | — | — |
| Goroutines per connection | — | — |
| RAM per tracked pod-restart | — | — |
| First limit hit | — | — |

The idle-pod profile is not empty (handshake + seven streams + initial dictionary/params, then keep-alive flush
cycles); the per-connection cost separates the one-time setup burst from the steady keep-alive cost
(`doc/ceiling-runs.md`).

### Harness validation *(local, not a ceiling)*

`runs/20260716T113525Z-t3-connections-smoke` (OrbStack, 2026-07-16): 3 → 6 fleets × 100 idle pods
(`THREADS_PER_POD=0`, `DICT_INITIAL=100`). The confirm phase waited on the new
`profiler_ingest_active_connections` gauge, which tracked the fleet size exactly (300, then 600). Step deltas
*(local, not a ceiling)*: +300 connections cost +3595 goroutines (~12 per connection), +2394 fds (~8 per
connection), and +78 MB RSS. Heap and goroutine profiles captured at the top level. One measurement nuance recorded
for real runs: k6 exports Time-typed trends in **seconds** over remote write, and connect-time trends only update
while connects happen, so accept-latency reads come from the confirm phase of each step, not from the plateau
window.

## 5. T4: soak

### Accelerated-timer lifecycle validation (local — a functional result)

Run `runs/20260716T144315Z-t4-soak-accelerated` (OrbStack, 2026-07-16; `local-soak` environment, images and frozen
spec in the run directory): 3 collector replicas, 20 pods × 8 threads × 3 calls/s ≈ 393 KB/s ingest, 1 m time
buckets, 15–45 m class TTLs, the full §8 checker (`checker.md`) plus the T6 UI profile in the background.

**Verdict: FAIL — and the failure is the deliverable.** After 52 healthy minutes, collector-2 crashed with
`panic: index out of range [-1]` in `libs/parser/pipe.CallsPipeReader` (`calls.go:70`: a negative thread index
from ignored mid-record read errors; stack in `collector-2-panic.log`, fix tracked separately). The checker and
the runner caught the crash and its whole cascade, each through its own §8 clause:

- §8.8 latched the collector-2 restart (restart budget 0);
- §8.5 latched compaction lateness while maintain drowned in the crash-recovery backlog (the `any_error` hour
  prefix grew to ~400 small objects);
- the runner's `pending-parquet-growth` detector ended the hold (`result.json`: `saturated`);
- the query API degraded during recovery (latched as a scrape gap).

During the healthy window every lifecycle stage cycled repeatedly and cleanly under the checker: 1 m buckets
sealed and uploaded (~100 small objects/hour/class), maintain merged 20–50-file groups per pass and TTL-deleted
expired objects (its pass logs show `CompactedGroups:5` at a ~1 m cadence), §8.7 markers stayed retrievable, and
settled hour prefixes drained to single-digit object counts. The functional conclusion carries to the cluster:
the lifecycle machinery works, the invariants catch real failures, and **the real-timer soak is blocked on the
`CallsPipeReader` panic** — under this traffic shape a collector dies in under two hours.

### Re-run at the declared rate (phase 5, step 0) *(local — a functional result)*

The phase-4 verification run above carried a wiring defect: the stand never received `CALLS_PER_SEC=3` — the
helmfile left the knob unset, `scenario.js` silently defaulted to 5, and the soak ran at ~761 KB/s while every
artifact claimed 393 KB/s (measured from `runs/20260716T205434Z-t4-soak-accelerated`, whose values snapshot shows
no override). The workload wiring now fails loudly instead (no scenario defaults, the `k6_workload_info`
fingerprint check, `confirm.ingest`; `doc/run-orchestration.md`, "Workload wiring"), and the per-VU rate was
re-measured at 19,798 B/s for the 8 × 3 calls/s shape (`runs/20260717T085030Z-t4-cal-3cps`).

`runs/20260717T091106Z-t4-soak-accelerated` (OrbStack, 2026-07-17) is the same 2.5 h accelerated soak at the
honestly delivered 3 calls/s (~460 KB/s ingest, ingest-confirm within tolerance): runner verdict **completed**
with every detector silent, and the full §8 checker latched **zero violations across the whole hold** — §8.5
included, under the same 1 m maintain cadence and 3 m compaction min-age that latched continuously in the
mis-wired run. That closes the §8.5 question: the compaction lateness of the verification run was pressure from
the undeclared 1.9× overload, not a throughput ceiling of the single maintain replica. The overload observation
itself remains useful context — at ~2× the declared rate, one maintain replica no longer meets the accelerated
3 m deadline — but it is an artifact of an artificial deadline, not a cluster-portable ceiling. The reworked
§8.6 trend rule stayed silent through legal goroutine oscillation (115–130 at constant connections) that the old
range rule had latched.

Two violations did latch **after** the hold, in the 13 minutes the checker outlived the runner: §8.7 freshness
("no calls in the last 7m" — the generator had scaled to 0) and a §8.5 small-file share rising as TTL deletion
drained large objects out of a closing hour prefix. Both are post-run artifacts of a checker judging a stand with
no feed; the runbook now says to stop the checker when the runner exits (`doc/soak-runs.md`).

### Real-timer soak (pending cluster)

> Placeholder — the mandatory 24–48 h run on real timers (`specs/t4-soak.yaml`): checker verdict over the full §8
> set, RSS/goroutine trends, S3 object-count trends, and the slow-leak check the accelerated run cannot see
> (plan §10). Runs with the T6 UI profile in the background (`k6-query`). The `CallsPipeReader` panic that
> blocked it is fixed and survived the phase-5 accelerated re-run cleanly. Filled by checklist step 4, with the
> frozen ui-companion block of the checklist's §4.

## 6. T1: contract-level run (pending cluster)

> Placeholder — `specs/t1-contract.yaml` on 3 replicas, 500 pods, ~6 MB/s total, fixed 2 h hold. Deliverable:
> the baseline utilization table (collector CPU/RSS/PV I/O, query CPU/RSS, S3 traffic) and the headroom estimate
> of plan goal (a). Filled by checklist step 3.

| Metric | Value | Run |
| --- | --- | --- |
| ingest bytes/s (cluster total) | — | — |
| collector CPU / replica | — | — |
| collector RSS / replica | — | — |
| PV write bytes/s | — | — |
| query CPU / RSS | — | — |
| ack-flush p95 | — | — |

### Spec-mechanics smoke *(local, not a contract number)*

`runs/20260716T155045Z-t1-contract` (OrbStack, 2026-07-16): the T1 spec scaled to `levels: [10]` and a fixed 12 m
hold, on the `local-soak` stand. Verdict `completed`: the confirm phase waited on
`profiler_ingest_active_connections` (connectionsPerVU: 1), the hold ran its full fixed length with every
§8-shaped detector silent (191 KB/s ingest, plateau reached, generator at 1% of its CPU limit), and the capture
step took CPU/heap/goroutine profiles at 100% of the level. The contract-run mechanics — fixed hold, detector
set, connection-gated confirm, `pprof.points: [1.0]` — are ready for the cluster.

## 7. T6: query load

> Cluster numbers are placeholders until T4 runs there; the local profiles below establish the qualitative
> behavior of the guards, the pagination cost model, and the read-vs-ingest interaction. Absolute latency and
> LIST/GET volumes are not portable from MinIO on a local SSD (plan §10). The UI rows are filled by checklist
> step 4 (ui companion) and the cold/pagination rows by step 5 (dedicated probe), both under the frozen
> safe-profile blocks of the checklist's §4. The incident and concurrent-wide rows are **no longer blocked-on-P1**:
> the global read-path memory budget landed and holds the report-§7 OOM shape (P1 acceptance below,
> `runs/20260722T104300Z-t6-p1-readbudget`). Their cluster numbers stay placeholders behind the single-MinIO
> S3-throughput ceiling that caps concurrent wide reads locally (plan §10), not behind a memory limit.

### Read-path memory: concurrent wide queries OOM the query pod *(local, mechanism portable)*

The sharpest T6 finding. `PROFILER_MAX_SCAN_BYTES` (default 2 GB) is a **per-request** budget: concurrent
guard-passing wide-range queries multiply it, and the read path has no global memory budget. Observed on the
2026-07-16 stand (pod events in the k8s log; §8.8 caught every restart):

- 2 Gi limit, mixed cold + incident load: OOMKilled after ~29 min;
- 3 Gi limit, 3 UI + 5 incident VUs on wide ranges: OOMKilled after **34 seconds**;
- 3 Gi limit with the scan budget cut to 256 MB *and* incident off: stable.

Sizing the pod around the guard is backwards — the guard must be sized to the pod, and even then concurrency
multiplies it. The finding is folded into `02-read-contract.md` §2.3.2 (what the guard actually bounds) and is the
P1 item in `load-testing-backlog.md`: a global read-path memory budget with admission control.

### Read-path memory budget: the concurrent-wide OOM is fixed *(local, mechanism portable)*

The P1 read-memory budget (`02-read-contract.md` §7.5) closes the OOM above. A repeat of the report-§7 shape against
the budget, on the same OrbStack stand, held for 27 minutes with bounded RSS
(`runs/20260722T104300Z-t6-p1-readbudget`, SHA `2d897510`): eight concurrent guard-passing wide `/calls` queries
plus three UI VUs plus concurrent 3 calls/s ingest, a pod-sized budget (`PROFILER_READ_MEMORY_BUDGET=1GB` on the
3 Gi query pod), against a bucket at the report-§7 density (~677 small pre-compaction objects per settled hour).

- Query RSS peaked at **120 MB** (3.9% of the 3 Gi limit); zero OOMKills and zero query-pod restarts (checker §8.8).
- `read_budget_used_bytes` peaked at **8.6 MB** of the 1 GB budget and `read_budget_denials_total` stayed **0**: the
  incremental page-lease batching caps in-flight decode, so the admission gate never had to shed. It is a backstop
  here, not the operating point.
- The load was real, not stalled: `cold_lists` ran to 22/s, query CPU to 1.5 cores, and ingest to ~460 KB/s — at or
  above the 2026-07-16 run. The same scan volume that OOM-killed the pre-budget pod in 34 s now runs bounded, so the
  budget holds RSS rather than the load easing off.

Two caveats carry, both local and non-portable (plan §10):

- **The single local MinIO is the S3-throughput ceiling.** Eight concurrent wide cold scans drive MinIO CPU-bound at
  ~3 cores over hundreds of small objects, and the query pod then sits I/O-bound at ~0.8 cores. Every wide query
  exceeds the k6 default 60 s client timeout, so a positive "narrow queries stay fast under concurrent wide load"
  measurement is not achievable here — the read cost is the documented pagination price (~23 s/page, below). A real
  S3 endpoint does not saturate the same way. The admission budget itself never starves narrow queries.
- **The HTTP 500 seen under contention is a benign client-cancel.** `libs/query/cold` records every S3 error as a
  partial; `handleCalls` returns a hard error (Echo-default 500) only on `ctx.Err()`, which without a server-side
  request deadline is `context.Canceled` from a client abandoning a slow query. A patient client gets 200 (observed
  at 114 s); no waiting client receives an undeclared status. Labeling a client-cancel 499 instead of 500 is a
  cosmetic follow-up.

### Guards and deep pagination *(local)*

- **Span guard** (probe `cold-probe-b`, 2 VUs, 400 m range > the 6 h limit): 36/36 requests rejected fail-closed
  with HTTP 400 in ~4 ms average — no I/O spent on rejected ranges.
- **Cost path** (probe `cold-probe-a`, 2 VUs, 350 m range, paging every `next_cursor`, against a bucket holding
  ~400 small pre-compaction objects in the hot hour): 21 pages in 4 min — **~23 s per page average, p95 59 s**,
  query CPU at 0.84 cores (84% of its 1-core limit), RSS ~1 GB. Every page re-resolves the fan-out and re-lists
  S3 by design (02 §2.3.1); the probe confirms the cost model directly.
- **Accelerated-density lesson**: guards scale with data density. At ~0.5 MB/s ingest the 256 MB scan budget
  rejected even a 7-minute freshness window; accelerated-timer stands must scale read windows and budgets
  together with the timers (`doc/soak-runs.md`).

### UI profile during the soak *(local)*

3 UI VUs (10 m windows at accelerated density, trace + tree per row) ran through the soak's healthy window with
no §8.7 freshness violations and no effect on ingest: 393 KB/s ingest stayed flat, `ingest_paused` never fired.
Hot-read pressure at UI levels does not push ingest toward backpressure on this stand; the incident/cold levels
that *do* hurt hit the query pod's memory first (above).

## 8. T5: crashloop and restarts (phase 5) *(local — functional results, mechanisms portable)*

All T5/T7 runs use the accelerated-timer stand; absolute numbers are *(local, not a ceiling)*, the mechanisms and
orderings carry. Fault runs are judged by the checker's scoped allowances (`doc/checker.md`, "Expected failures"):
declared consequences latch as expected, everything else fails the run.

### T5.1 agent reconnect storm

Four runs (`runs/20260717T{120457,122802,125426,133845}Z-t5-reconnect-storm`; the last is the definitive one):
40 churn-mode pods (45 s ± 20% cycles, `virtual-dumper.md` §1.1) sustained ~42 restarts/min for up to 39 minutes —
each cycle an abrupt disconnect, a 10 s restart, and a full dictionary resend under the same pod name.

- **The tracked pod-restart backlog is NOT bounded under a sustained storm.** It first plateaus at
  `restart rate × purge-eligibility lag` (~0.73/s × ~5 min ≈ 220 tracked restarts, sawtooth 196–263 as purge
  batches run), but purge eligibility itself degrades: the collector logs show WAL purges running 3.8 → 7.5 min
  "past full flush" as the run ages, and on the longest attempt the purge rate collapsed from 0.72/s to 0.35/s
  against a steady 0.7/s production at minute ~35 — tracked restarts jumped to 327 and WAL to 110 MB, both still
  climbing when the `wal-bytes-growth` detector ended the run.
- **The purge gate is hot-partition indexing, not `WAL_PURGE_GRACE`.** Purges wait for the hot index to age past
  each restart's rows (~5 min on this stand: 3 m retention + eviction cadence + drift), far past the 2 m grace.
  At the production 1 h grace the same storm floors at ~2 500 tracked restarts before any degradation — the
  backlog scales with `max(grace, hot-indexing lag)`.
- **RAM and CPU are not the pressure point at this scale**: 600 MB RSS and 0.2 CPU cores across 3 replicas,
  in-RAM state 14 MB, accept path unstressed (session-ready p95 flat). No backpressure, zero refused bytes, zero
  ack errors — and zero failure reconnects: deliberate churn cycles count separately (`k6_vdumper_churns_total`).
- **Compaction feels the storm.** Churn doubles per-bucket object counts (each incarnation seals its own files);
  during storm fronts maintain missed the accelerated deadlines with 49–80 sealed objects per (bucket, class)
  (persisting across listings in the first two attempts — real lateness, unlike the one-shot listing-staleness
  race the campaign later fixed in the checker).

The storm campaign also hardened the harness itself: the `monotonic-growth` detector was rebuilt twice on this
run's evidence (first-to-last delta → least-squares fit; then a three-plateau-window minimum span), and the final
thresholds are derived from the measured purge-cycle amplitude (±11%), not guessed
(`doc/run-orchestration.md`, "Detector kinds").

### T5.2 collector restart under load

`runs/20260717T142638Z-t5-restart`: the accelerated-soak shape, one grace-0 kill of collector-1 at hold+20 m,
runner verdict **completed** with every detector silent.

- **READY 10.1 s after the kill** — including one extra container restart: the replacement pod's first start
  died on `collector.lock held by another process` (the killed process's flock on the shared PV had not been
  released yet), kubelet restarted it, the second start recovered the WAL and went READY. The lock does its
  two-writers job at the cost of one crash cycle; the §8.8 allowance model now carries this as
  `restartBudget: 2` per grace-0 kill.
- **Zero refused bytes**: the loss window of a hard kill is confined to unacked data, exactly as the write
  contract documents; agents failed over with a reconnect spike and ingest returned to the declared rate.
- The checker matched the replacement to the kill's allowance and kept an unexpected latch for the
  lock-collision restart — which is how the mechanism was discovered and then modeled.
- **Gap (follow-up)**: there is no recovery-duration metric; time-to-READY comes from the fault log and probe
  transitions.

### T5.3 collector crashloop

`runs/20260717T152844Z-t5-crashloop`: ten ready-gated kills of collector-1 (the next kill only after observed
READY + 90 s), runner verdict **completed**.

- **Recovery does not degrade cycle over cycle**: the `readyAt − at` series is 10.1, 10.1, 25.1, 10.0, 15.1,
  10.1, 15.1, 30.1, 15.1, 10.0 s — flat, with the variance explained by kubelet's restart backoff on the
  lock-collision restart, not by store state.
- All 20 §8.8 units (10 replacements + 10 lock-collision restarts) matched the budget-2 allowances; no orphan
  parquet or WAL survived stabilization (janitor counters, post-run listings).
- **The startup gate holds**: during every recovery the agent port stayed unbound (k6 saw connect errors, never
  an accepted-then-stalled session), matching the design (`libs/collector/service.go` binds after READY).

### Protection decision (plan §7.5.4)

By the numbers above — designs are follow-ups, nothing is implemented in this campaign:

1. **Aggressive purge of near-empty pod-restarts: warranted.** The storm's unbounded backlog is gated by purge
   eligibility, and churn restarts are near-empty by construction (a dictionary and seconds of data). A
   fast-path that frees a pod-restart's WAL set once its rows are sealed and below a size floor — skipping the
   hot-index wait — bounds the backlog at `rate × grace` regardless of hot-window drift.
2. **Per-pod-key reconnect rate limit: not now.** The collector absorbs 42 restarts/min at negligible CPU/RAM;
   the damage is bookkeeping downstream. Shedding agent data to protect a purge queue is the wrong trade at
   this scale; revisit with cluster-scale T3 numbers.
3. **Cap on tracked pod-restarts: defer, coupled to (1).** A cap without faster purge only drops observability;
   with (1) the set is bounded anyway.
4. **Accept-side connection cap (plan §7.3): still open, cluster-pending** — the T3 ceiling run owns that
   decision; nothing local stressed the accept path.

## 9. T7: fault injection (phase 5) *(local — functional results, mechanisms portable)*

### S3 unavailable (`runs/20260717T165508Z-t7-s3-outage`)

MinIO scaled to 0 for 20 minutes under the soak shape, with `PENDING_UPLOAD_MAX_BYTES` pinned to 256 MiB per
replica so the gates fit the run; the sizing and its consequences were computed in the spec before the run.

- **The chain engaged as re-derived, not as §7.7 documents it**: refusals began 10 minutes into the outage
  (predicted 8–10), `IngestPaused` from +12 min — and **`SealPaused` never fired**. On this stand WAL and live
  partitions dominate the backlog, so the ingest gate (whole budget) trips long before pending parquet alone
  can reach the seal gate (half budget). The documented seal-before-ingest order presupposes a pending-dominant
  backlog; whether production stands are pending- or WAL-dominant decides which protection engages first.
- Agents saw `ACK_ERROR` and reconnect-looped; after the revert the gate cleared within ~40 s and every refusal
  reconciled with `ingest_refused_bytes_total` (107 KB total, each windowed increment logged by the checker).
- **Budget sizing lesson**: 28 minutes after the revert one replica briefly re-crossed the gate (a ~4 KB refusal
  flap outside the settle window) — with the budget at ~3× the steady backlog, residual WAL keeps the collector
  hovering at the threshold. The gate needs to sit well above the steady WAL+partitions level (the 2 GiB
  default is ~11× on this stand).

### S3 slow (`runs/20260717T191126Z-t7-s3-slow`)

1 s latency + 32 KB/s-per-connection bandwidth toxics on the S3 path for 15 minutes, sized to push the drain
rate below the measured parquet production. Runner **completed** the full hold, checker **PASS with zero
violations** — the backlog grew slowly, upload workers sat pinned on crawling PUTs (there is still no per-PUT
timeout, only ambient context — recorded), compaction deadlines shifted by the declared allowance windows, and
the post-revert drain finished inside the settle. The degraded-but-under-threshold regime is absorbed by the
existing machinery.

### Small PV / disk pressure (`runs/20260717T215007Z-t7-small-pv`, probe `20260717T203936Z`)

A deploy-time variant: the segment staging budget shrunk to 32 MiB per replica — below the measured steady
segment footprint (~66 MB per replica at the accelerated seal cadence, from the 256 MiB probe run that never
evicted).

- **Class-aware eviction holds the line exactly**: segments sat at 100.1 MB against a 100.7 MB summed budget
  (99.5% utilization, never exceeded) with ~6.6 segments/min evicted continuously.
- **The cost is visible and counted**: 0.92 truncated rows/s sealed with `truncated_reason=disk_budget` and
  57 429 chunk references pointing at evicted segments — the read side serves those calls without trace blobs.
- Ingest, backpressure, hot lag, and the whole §8 set stayed green (checker PASS, zero violations).
- The true ENOSPC path stays **cluster-pending**: OrbStack hostpath volumes enforce no size, so a full-disk
  write failure (stream teardown → reconnect; no reactive ENOSPC handling exists) cannot be produced locally.

### Agent↔collector network faults (`runs/20260717T231036Z` and `runs/20260717T235336Z-t7-agent-net`)

- **2 s of path latency does not break sessions — it starves them.** The 40 s read deadline held, not one
  reconnect fired; but ingest collapsed from ~440 KB/s to ~11 KB/s (~40×), ack-flush p95 grew to 2.1 s, and
  producers dropped ~30 chunks/s. The wire protocol is latency-bound: 8 KB socket buffers and a synchronous
  per-stream ack drain per 5 s flush cycle turn RTT into a hard throughput ceiling. A WAN-grade agent link is
  effectively unusable — a finding for any multi-region deployment thought.
- **Late data re-opens sealed buckets and floods compaction.** The trickle that does get through arrives after
  its bucket sealed, so seal passes re-emit files for closed buckets: 41–186 objects per (bucket, class)
  against ~20 steady during both injection windows, and maintain digested the burst 10–25 minutes past the
  accelerated deadlines (post-deadline listings — real lateness, not the fixed checker race). The chain was
  undeclared on the first full run and latched as unexpected — exactly the checker's job — and is now a
  declared consequence of agent-path degradation in the spec.
- **A 5-minute full stall tears sessions by deadline, and the revert storm is absorbed.** During the stall
  ingest went to 0 and active connections fell to 10 as read deadlines fired; on the revert the whole fleet
  re-established within ~a minute (0.7 connects/s, tcp-connect p95 steady at ~10 ms — the accept path did not
  notice the front), and ingest overshot to ~950 KB/s (2.2× steady) draining producer backlogs before settling.
- The first run also caught a transient single-pass failure unrelated to the faults: one seal and one upload
  pass on one replica failed with `SQL logic error: no such table: call_index` (a call-partition drop racing an
  in-flight pass), self-healed on the next pass, backlog drained in minutes. The loop-error counters account it;
  a follow-up note, not a stability issue in itself.
- Packet loss proper needs netem and stays **cluster-pending** (the parked `chaos-mesh` release).

## 10. Invariant checker coverage (plan §8) — final for the campaign

| Invariant | Source | Status |
| --- | --- | --- |
| §8.1 hot store not growing | /metrics | implemented (phase 1), latched |
| §8.2 ingest-paused not sticky | /metrics | implemented (phase 1), latched |
| §8.3 no refused bytes | /metrics | implemented (phase 1), latched |
| §8.4 hot-window lag bounded | /metrics | implemented (phase 1), latched |
| §8.5 S3 objects per hour prefix | S3 listing | implemented (phase 4): compaction-keeps-up + small-file share |
| §8.6 RSS under limit, goroutines flat | /metrics | implemented (phase 4) |
| §8.7 sampled UI queries | query API | implemented (phase 4): freshness + markers + optional TTL deletion |
| §8.8 no unexpected restarts | k8s API | implemented (phase 4): restart budget + replacement accounting |

Violations latch: a failure after warm-up fails the run even when the final tick looks healthy
(`doc/checker.md`). Every clause fired at least once for a real cause during the phase-4 bring-up (§5, §7):
§8.5 on the crash-recovery compaction backlog, §8.7 on a probe defect it exposed (405 on HEAD), §8.8 on genuine
OOM and panic restarts, and the scrape-gap rule on real query-API outages — none of the final latches were false
positives.

Phase 5 hardened three rules on the fault runs' own evidence and added the expected-failure layer:

- **§8.6** judges goroutines by a least-squares trend over real scrape timestamps (a healthy 115–130 oscillation
  had latched under the old range rule); **§8.5** judges a compaction group only from a listing that postdates
  the group's deadline (every recurring one-shot latch of the fault runs was a listing-staleness race, and the
  T5.2/T5.3 compaction "misses" retro-resolve to that race); the runner's `monotonic-growth` detector fits a
  trend over at least three plateau windows instead of a first-to-last delta.
- **Expected failures** (`doc/checker.md`): fault runs declare their consequences per injection, the checker
  scopes them to (invariant × subject × window × budget) matched by observation time, and expected latches
  report separately without masking anything undeclared — battle-tested by T5.2 (the lock-collision restart
  surfaced as unexpected, then measured and modeled) and T5.3 (20 declared units matched, zero masked).

## 11. Generator headroom (plan §10)

Every step records the k6 pod's CPU share against its limit (`steps.jsonl`, `generator` field); runs stop as
`invalid` past 70%. Large-cluster runs additionally pin the runner to dedicated nodes.

> Placeholder — headroom observed at the T2/T3 ceilings, and the runner sizing that keeps it. On the local smoke
> levels the generator used 1–2% of a 2-core limit — the guard machinery is verified, the sizing question is not.
> Filled by checklist step 0 (generator sizing validation) and confirmed by every ceiling run after it.

## 12. Follow-ups — dispatched at close-out

Every open follow-up now lives in exactly one of three homes; this section only routes.

- **Fixed during the campaign**: the `CallsPipeReader` panic (`608ce6a9`, verified by the §5 re-run) and the
  collector ack-flush cadence (`82aed788`, phase 2).
- **Backlog** (`load-testing-backlog.md`): the global read-path memory budget (P1), the near-empty pod-restart
  purge fast-path (P1), the per-PUT upload timeout, the partition-drop race, the suspend/params reader
  mis-framing, the recovery-duration metric, and the `collector.lock` collision softening.
- **Deferred with triggers** (`deferred.md`): wire-protocol ack windowing/pipelining, and the per-pod-key
  reconnect rate limit + tracked-restart cap declined by the §7.5.4 decision.
- **Cluster checklist** (`tools/load-generator/doc/cluster-checklist.md`): every placeholder run above (T1–T4,
  T6 safe profile, the cluster-only faults), the accept-side cap decision with its frozen criteria, and the
  runner sizing validation. Revisiting the T6 profile shares against real usage data (plan §7.6) rides with the
  T6 steps.
