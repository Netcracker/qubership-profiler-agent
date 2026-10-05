# Load-testing plan: Go profiler backend

Status: campaign closed 2026-07-20 — phases 1–5 done (§11–§15), phase 6 close-out done (§16). The ceiling,
contract, and real-timer soak numbers wait for the large cluster; their runs are frozen as a mechanical checklist
(`tools/load-generator/doc/cluster-checklist.md`). The harness is in maintenance mode. Owner: @vlsi.

This plan defines the load tests for the Go backend (`backend/apps/profiler-backend` and `backend/libs`): what we
measure, on which stands, with which generator, and what counts as a pass. The outcome is an engineering report
(CPU / RAM / disk I/O curves, discovered ceilings) plus a set of automated invariants for long runs, not a formal
SLO gate.

## 1. Goals

1. Measure resource utilization under load:
   - collector: CPU, RAM, disk I/O on the hot-store PV, S3 traffic;
   - query: CPU, RAM under concurrent UI-style and cold-scan workloads.
2. Confirm the contract-level load: ~6 MB/s of raw trace per cluster (`01-write-contract.md`), modeled as
   ~500 pods at production-like per-pod rates on 3 collector replicas.
3. Find the ceilings of a single collector replica:
   - throughput ceiling (bytes/s and calls/s) — where backpressure engages and where seal/upload stop keeping up;
   - connection-count ceiling — thousands of mostly idle agent connections (goroutine + pod-restart RAM cost).
4. Verify long-run stability (soak): hot store does not grow monotonically, S3 does not accumulate unbounded small
   files, the UI keeps serving old data.
5. Characterize crashloop behavior (agent storms and collector restarts) and decide, from the numbers, whether extra
   protection is needed.

Non-goals: formal SLO certification, multi-region setups, profiling the Java agent itself.

## 2. Key decisions (from the interview)

| Topic | Decision |
| --- | --- |
| Load generator | Extend the existing k6 + xk6 generator (`backend/tools/load-generator`) and the Go emulator; no Java-classloader harness |
| Payload | Synthetic, parameterized generation (not dump replay) |
| Stands | Local k8s (OrbStack or kind) for development; a large k8s cluster for final numbers |
| S3 | MinIO in-cluster on both stands |
| Monitoring | Deployed as part of the harness via [qubership-monitoring-operator](https://github.com/Netcracker/qubership-monitoring-operator) |
| Topology | 1 collector replica for ceiling runs; 3 replicas for contract and soak runs |
| Production reference | ~200–500 profiled pods; contract run at 500, connection ceiling probed from 1000 up |
| Soak | 24–48 h on real timers, plus a short run with accelerated timers |
| Result format | Exploration report + automated invariants (no hard SLO thresholds) |
| Artifacts | This doc + code under `backend/tools/load-generator/` (scenarios, stand manifests, checkers) |
| First step | Stand + observability (pprof, dashboards, monitoring) before generator work |

## 3. Load generator: close the fidelity gaps

The current generator is faithful on the handshake (pod identity), 1 KB `RCV_DATA` framing, and ack reading, but it
cannot exercise backpressure or crashloop paths. Gap analysis against the Java dumper
(`dumper/src/main/java/com/netcracker/profiler/{Dumper,client/DefaultCollectorClient,dump/DumperThread}.java`):

| # | Gap | Size | Needed for |
| --- | --- | --- | --- |
| G1 | No `trace` stream (the dominant-volume stream is commented out) | Large | throughput ceiling |
| G2 | No cross-stream multiplexing: streams sent one at a time, no simulated app threads | Large | realistic ingest shape |
| G3 | No `ACK_ERROR_MAGIC` handling: any non-OK ack is a generic failure | Large | backpressure tests |
| G4 | No agent-style reconnect: no 10 s restart cadence, no `resetRequired=1` dictionary resend | Large | crashloop tests |
| G5 | `resetRequired` hardwired to `false` in `CommandInitStream` | Small | crashloop tests |
| G6 | Flush + ack after every `RCV_DATA` instead of the dumper's 5 s wall-clock flush | Medium | ack-path realism |
| G7 | No duration-class shaping: near-identical durations every burst (the `calls[...]` range files are local-dump-only and never cross the wire; the collector bins the flat `calls` stream itself) | Medium | seal / retention realism |
| G8 | `sql`, `xml`, `params` never sent (`callsDictionary` is local-dump-only; `posDictionary` is V3-only and the collector answers V2 — a faithful agent sends neither) | Medium | full-stream realism |
| G9 | No load-shape knobs (bytes/s, calls/s, distributions); all pods send identical traffic | Medium | parameter sweeps |

Implementation: a "virtual dumper" behavioral layer in Go (shared between `backend/tools/load-generator/pkg/cdt` and
`backend/libs/emulator`) that mirrors the `DumperThread` + `DefaultCollectorClient` state machine. The behavioral
contract — wire rules, state machine, knobs, calibration method — lives in `virtual-dumper.md`:

- N producer goroutines per pod model app threads; each fills a per-thread trace buffer with jittered delays, so
  chunks from different threads interleave on the wire the way real `LocalBuffer` chunks do — this covers the
  "real multiplexing of thread data" requirement directly, without classloaders;
- a 5 s flush loop drains all streams round-robin and validates accumulated acks (G6);
- `ACK_ERROR_MAGIC` triggers drop-window + reconnect with `resetRequired=1` and a configurable restart delay
  (default 10 s, like `DUMPER_RESTART_INTERVAL`) (G3–G5);
- duration-distribution shaping over configurable class thresholds (G7): the collector bins the flat `calls` stream
  itself via `model.ClassifyDuration` (default tiers 100 ms / 1 s / 10 s) plus the `call.red` error marker; the
  agent-side thresholds 100 ms / 500 ms / 3 s / 60 m stay available as a distribution preset.

Fidelity check: one calibration run compares the traffic profile (bytes/s per stream, ack cadence, reconnect
behavior) of the virtual dumper against the real agent from `libs/tests/smoke_realagent`. If the profiles diverge
materially, fix the emulator before trusting ceiling numbers.

## 4. Workload model (synthetic)

Generation is parameterized; every run records its parameter set so runs are comparable. Knobs:

- pods, app threads per pod;
- calls/s per thread; call duration distribution (log-normal, parameterized to hit the duration-class thresholds
  with configurable shares);
- stack depth distribution and dictionary cardinality (new dictionary entries per minute — drives dictionary stream
  and collector RAM);
- share and size of `sql` / `xml` payloads;
- suspension events rate;
- error-call share (feeds the any-error retention class).

Dictionaries and stack shapes may borrow templates from captured dumps later if synthetic compressibility turns out
unrealistic; timings and rates stay synthetic.

## 5. Stands

### 5.1 Local (OrbStack / kind)

For scenario development, harness debugging, and rough profiling. Disk and S3 (MinIO on local SSD) are not
representative; no final numbers from this stand.

### 5.2 Large k8s cluster

For final numbers: real network PVs, real node limits, kubelet crashloop backoff. MinIO in-cluster serves as S3 on
both stands, so cold-read latency numbers carry a caveat: real object storage adds LIST/GET latency on top.

### 5.3 Harness layout

Everything reproducible from the repo, target directory `backend/tools/load-generator/`:

- `deploy/` — a helmfile that composes the stand releases: profiler backend (the existing
  `backend/charts/profiler-backend` chart), MinIO, qubership-monitoring-operator
  (Prometheus/VictoriaMetrics + Grafana + node-exporter + cAdvisor) plus its CRs as a small local chart,
  k6 runner, and (for T7) chaos tooling. Helmfile `environments:` carry the local-vs-large-cluster value
  layers (storage class, PV sizes, limits, replicas); `needs:` orders operator → CRs. Helmfile covers only
  the static stand; run orchestration (scenario parameters, ramp steps, fault injection, artifact
  collection) is a separate script layer designed in phase 1;
- `scenarios/` — k6 scripts per test below, parameterized via env;
- `dashboards/` — Grafana dashboards as code (see §6);
- `checker/` — the invariant checker (see §6);
- `doc/` — runbooks: how to run each test on each stand, one or two commands per run.

k6 writes its own metrics through Prometheus remote-write into the same monitoring stack, so generator-side and
server-side series share a time axis.

## 6. Observability additions (phase 1)

1. **pprof endpoints** in `collect`, `query`, and `maintain`: `net/http/pprof` on the internal/metrics port, behind
   an env flag (default off). Without it, CPU/RAM attribution is guesswork.
2. **Grafana dashboard(s)** committed to the repo, covering:
   - ingest: `profiler_ingest_bytes_total` rate, commands, refused bytes, decoder errors, active connections;
   - backpressure: `backpressure_seal_paused`, `backpressure_ingest_paused`, `pending_parquet_bytes`;
   - pipeline: `seal_queue_depth`, `upload_backlog`, `hot_window_lag_seconds`, janitor activity;
   - resources: per-pod CPU, RSS, PV throughput/IOPS (cAdvisor/node-exporter), MinIO ops;
   - query: request rates, latency percentiles, fan-out partial reasons, S3 LIST/GET counts;
   - k6: sent bytes/s, VU count, reconnects, ack errors.
3. **Invariant checker**: a Go tool polling `/metrics`, S3 (object count/size histogram per prefix), and a few
   `/api/v1` queries during soak. It fails the run with a report when an invariant breaks (see §8).
4. Gap to fill along the way: an explicit gauge for active agent connections and tracked pod-restarts if the current
   `store_pods_size` proves insufficient to attribute RAM growth.

## 7. Test program

Order within each test: start small, scale in steps, hold each step until metrics flatten, record the step where a
saturation signal fires.

### T1. Contract-level run (green-path confirmation)

- 3 collector replicas, 500 pods, per-pod rate sized so the cluster total is ~6 MB/s raw trace; 2–4 h.
- Expect: no `ingest_paused`, `upload_backlog` bounded, `hot_window_lag_seconds` stable, CPU/RAM well under limits.
- Output: baseline utilization table (goal (a) of the effort) and headroom estimate.

### T2. Single-replica throughput ceiling

- 1 replica; ramp bytes/s (more pods, then more calls/s per pod) until either `ingest_paused` fires, seal/upload
  stop keeping up (`upload_backlog` grows without bound), or ack latency degrades.
- Sweep along one axis at a time: total bytes/s, calls/s (small calls vs large), dictionary churn.
- Output: ceiling in MB/s and calls/s, the limiting stage (ingest decode, SQLite index, seal, upload), CPU/heap
  profiles at 70% and 100% of ceiling.

### T3. Connection-count ceiling

- 1 replica; thousands of nearly idle connections (keep-alive traffic only), ramp from 1000 upward.
- Measure RAM and goroutine cost per connection and per tracked pod-restart; find where accept latency, RAM
  (`PROFILER_MEM_BUDGET` pressure), or file-descriptor limits bite.
- Note: there is no accept-side connection cap today (`libs/server/services.go`); record what failure looks like.

### T4. Soak

- 3 replicas, contract-level load, 24–48 h on real timers, plus background query load (see T6).
- A separate short soak (2–4 h) with accelerated timers (retention, TTL, compaction, WAL purge grace scaled to
  minutes) to exercise several full data-lifecycle cycles: seal → upload → janitor eviction → maintain compaction →
  TTL deletion.
- Invariant checker runs throughout (§8).

### T5. Crashloop and restarts

1. **Agent reconnect storm**: N pods in a tight connect → dictionary → little data → disconnect loop (simulating
   CrashLoopBackOff of profiled apps). Measure growth of `store_pods_size`, RAM, pod-restart directories and WAL
   sets on the PV, small-file production in S3; verify the 1 h `WAL_PURGE_GRACE` backlog stays bounded.
2. **Collector restart under load**: kill one of 3 replicas mid-ingest. Measure WAL recovery time vs accumulated
   data volume, time to READY, agent failover behavior, data loss (must stay within the documented unacked-window
   loss), query `partial_reasons` during the outage.
3. **Collector crashloop**: repeated kills (every 1–2 min, 10+ cycles). Verify recovery time does not grow cycle
   over cycle, no orphan parquet/WAL accumulation, startup gate holds the TCP listener down until READY.
4. Based on the results, decide whether protections are warranted (per-pod-key reconnect rate limit, cap on tracked
   pod-restarts, aggressive purge of near-empty pod-restarts) — design them as a separate follow-up, not in this
   campaign.

### T6. Query load

- **UI profile** (during soak and contract runs): background 2–5 virtual users — `/api/v1/calls` over the last
  hour, open a call (`/calls/{pk}/trace` + `/calls/{pk}/tree`; there is no bare `/calls/{pk}` endpoint); plus an
  "incident" burst of 20 users on wide ranges. (Defaults chosen by us; adjust when real usage data appears.)
- **Cold-heavy profile**: wide ranges up to the 6 h guard, deep pagination (every page re-scans S3), against a
  bucket state with many small pre-compaction files.
- Measure query CPU/RAM (goal (b)), fan-out tail latency vs replica count, S3 LIST/GET volume, guard behavior
  (`MAX_SCAN_FILES` / `MAX_SCAN_BYTES` / wide-range limit), and the reverse effect: does hot-read traffic on a
  loaded replica push ingest into backpressure.

### T7. Fault injection

- **S3 unavailable / slow**: stop or throttle MinIO for 5–30 min under contract load. Expected chain:
  `pending_parquet_bytes` grows → `SealPaused` at half budget → `IngestPaused` at `PENDING_UPLOAD_MAX_BYTES` →
  agents get `ACK_ERROR` and reconnect-loop; after recovery the backlog drains and losses stay within the counted
  `ingest_refused_bytes_total`. Requires generator gaps G3–G4 closed. (Measured: the gate order inverts on
  WAL-dominant backlogs — `IngestPaused` fires with `SealPaused` silent; §15, report §9.
  `01-write-contract.md` §4.6 now documents both orders.)
- **Slow / small PV**: throttle IOPS or shrink the PV below the 10 GB segment budget; verify janitor class-aware
  eviction and behavior at real disk pressure (ENOSPC path).
- **Agent↔collector network faults**: latency, loss, and connection resets (tc or chaos-mesh); verify socket
  deadlines (read 40 s, write 2 s) and the reconnect storm after a network blip.

## 8. Soak invariants (automated)

The checker fails the run when any of these break:

1. Hot-store PV usage oscillates but does not grow monotonically over any 2 h window (after warm-up).
2. `backpressure_ingest_paused` never sticks: total paused time < 1% of the run at contract load.
3. `ingest_refused_bytes_total` stays 0 at contract load (nonzero only in T2/T7 by design).
4. `hot_window_lag_seconds` stays below the hot-retention + seal/upload-chain budget. (The gauge is the age of the
   oldest row still in the hot index, so its healthy level is hot retention + eviction cadence; sustained growth
   past the budget means the hot→cold handoff is stuck — found in phase 4 when the original "seal interval +
   grace" reading of this clause fired on every healthy stand.)
5. S3 object count per hour prefix stays bounded after compaction; small-file (< 1 MB) share trends down once
   maintain has run.
6. Collector RSS stays below the pod limit with no monotonic growth (leak signal); goroutine count flat at constant
   connection count.
7. Sampled UI queries keep answering: fresh data visible within the hot window, old data (pre-soak marker calls)
   still retrievable from cold until its TTL.
8. No pod restarts of backend components other than those injected by the scenario.

## 9. Phasing

1. **Stand + observability** (first, per interview): harness manifests, monitoring-operator deployment, pprof
   endpoints, dashboards, checker skeleton. Exit: a contract-shaped run on the local stand is fully observable.
2. **Generator fidelity**: virtual-dumper layer (G1–G6), duration classes and shape knobs (G7–G9), calibration
   against the real agent.
3. **Ceiling campaign**: T2, T3 on the large cluster; first report draft.
4. **Contract + soak**: T1, T4, T6 background load; invariant checker hardened.
5. **Crashloop + faults**: T5, T7; protection-mechanism decision.
6. **Report**: consolidated numbers for goals (a) and (b), headroom statements, follow-up list (including the
   deferred read-side items from `02-read-contract.md` §5.4/§7.1 if cold-scan numbers demand them).

## 10. Risks and open questions

- MinIO understates real object-storage latency; cold-read conclusions need a caveat or a later spot-check against
  a real S3 endpoint.
- Synthetic payloads may compress differently from production traffic; the dictionary/stack template fallback (§4)
  is the mitigation.
- k6 runner resources on the large cluster: at 1000+ virtual pods with trace streams the generator itself needs
  sizing (several CPUs, pinned nodes) so it does not become the bottleneck being measured.
- Accelerated-timer soak can mask slow leaks; that is why the real-timer 24–48 h run stays mandatory.

## 11. Phase 1 status (done 2026-07-16)

Everything landed on the `feat/load-tests` branch; the exit criterion — a contract-shaped run on the local stand is
fully observable — was verified live on OrbStack.

Shipped:

- **pprof** (§6.1): `net/http/pprof` in `collect`/`query`/`maintain` behind `PROFILER_PPROF_ENABLED` (default off),
  on the internal/metrics port; on `query` it rides the dedicated metrics port (`PROFILER_METRICS_PORT`), off the
  external listener the ingress publishes (`04-storage-layout.md` §12, reports2#15).
- **Stand** (§5.3): `backend/tools/load-generator/deploy/` — helmfile with `local` / `cluster` environments.
  qubership-monitoring-operator v0.88.0 comes straight from its git tag via helmfile `git::` charts (the helm-git
  plugin 1.3.0 is broken with helm 4); CRDs are a separate first release, `needs:` orders CRDs → operator → CRs.
- **Dashboards** (§6.2): six JSON dashboards under `dashboards/` (ingest, backpressure, pipeline, resources, query,
  k6), shipped as `GrafanaDashboard` CRs by the `monitoring-crs` release.
- **Checker** (§6.3): `checker/` polls `/metrics`; §8.1–§8.4 implemented, §8.5–§8.8 are declared TODO stubs (need S3
  credentials, pod limits, and the k8s API).
- **Query HTTP histogram**: `profiler_query_http_request_seconds` (code, method) — no series covered the external
  API round-trip, and the query dashboard needs rates and percentiles.
- **Feeder**: `feeder/` holds N emulated agent connections and sends contract-shaped bursts; it exists only to light
  up phase-1 observability and deliberately skips the fidelity gaps (§3).

Verified on the local stand (20 feeder pods, 5 s cadence): ingest → seal → upload visible end to end in
VictoriaMetrics (97 sealed rows, 40 uploaded files, matching MinIO server-side PUTs), all six dashboards imported by
Grafana with live panel queries, `/debug/pprof` answers 200 on all three subcommands, `/api/v1/calls` serves the fed
rows, and a checker run passes §8.1–§8.4 against the live collector.

Carried into later phases:

- The k6 runner release is parked (`k6.installed: false`): its image needs captured wire dumps the repo must not
  carry. The phase-2 virtual dumper replaces that input; the k6 dashboard is unverified against live k6 series until
  then.
- No explicit active-connections gauge yet (§6.4); the ingest dashboard uses the goroutine count as a proxy.
- Run orchestration (ramp steps, artifact collection) remains to be designed as the script layer of §5.3.

## 12. Phase 2 status (done 2026-07-16)

The feeder stub is replaced by the virtual dumper (`backend/libs/emulator/vdumper`), a behavioral layer mirroring
the `DumperThread` + `Dumper` + `DefaultCollectorClient` state machine; the contract is `virtual-dumper.md`. All of
G1–G9 are closed:

- **G1–G2**: producer goroutines model app threads; logical trace chunks interleave on the wire and calls records
  carry the (file index, buffer offset, record index) linkage, verified by decoding the wire through
  `libs/parser/pipe` in the package tests.
- **G3–G6**: the transport (`libs/emulator`) matches the agent ack protocol (+1 pending ack per `RCV_DATA`, no
  per-payload flush, opportunistic drains via FIONREAD, typed `ACK_ERROR_MAGIC`); the lifecycle reconnects after
  10 s with a full dictionary resend under `resetRequired=1`. Lifecycle tests run against the `emutest` scripted
  collector on a fake clock.
- **G7–G9**: `vdumper.Workload` parameterizes every §4 knob; shape tests pin the class shares, error share, dedup
  ratio, dictionary growth, and suspend rate statistically.

Calibration (§3 exit criterion) against the real agent (`test-app` `LoadMain` via the
`tools/load-generator/calibrate` tap; runbook in `tools/load-generator/doc/calibration.md`):

- bytes/s ratios A/B: calls 1.01×, dictionary 1.06×, trace 1.27× (tolerance 1.5×); params/sql/suspend sit under the
  20 B/s noise floor;
- flush cadence: 6.7 vs 6.1 `REQUEST_ACK_FLUSH` per 5 s;
- injected `ACK_ERROR_MAGIC` mid-run: both sides reconnect and re-open all seven streams with the dictionary reset
  (the virtual dumper after exactly 10.0 s; the real agent stalls on the half-dead socket first, then restarts).

Calibration drove three emulator fixes rather than threshold tuning: the dumper-injected per-call tags
(`common.started` / `node.name` / `java.thread` / counter tags), realistic request-id value sizes, and
sleep-shaped default cpu/wait/memory counters (`virtual-dumper.md` §2.5, §4).

Found along the way, tracked separately: the collector never flushes buffered acks on its own cadence (06 §5
violation — every real-agent stream rotation stalls 30 s into a reconnect), and the suspend/params pipe readers
mis-frame multi-phrase streams the real agent produces.

Carried into phase 3: the k6 runner stays parked; wiring `pkg/cdt` onto the virtual dumper is the first step of the
ceiling campaign.

## 13. Phase 3 status (harness done 2026-07-16; numbers pending)

The large cluster (§5.2) was not available, so this phase delivered the complete ceiling harness, validated it with
smoke runs on OrbStack, and stopped before taking numbers. Nothing measured locally counts as a ceiling; the report
draft (`load-testing-report.md`) carries the placeholders and cites every smoke run.

Shipped:

- **k6 on the virtual dumper**: `pkg/cdt` rewritten as the fleet module — `runFleet` drives N vdumpers per VU
  (1 for T2, ~100 for T3), StatsListener maps to `k6_vdumper_*` series with three latency trends of fixed semantics
  (`tcp_connect_time`, `session_ready_time` — dial to all seven streams open, `ack_flush_time` — flush-cycle drains
  only). The dump-replay path is gone (`libs/generator`, captured-dump scenarios, docs, wireshark.lua); the image
  builds from synthetic traffic only, via a plain-`go build` custom k6 binary (`k6runner/`, no xk6 CLI), with
  `go-metrics` folded into the root Go module. The k6 dashboard was reworked for the new series and checked against
  live series (carried item from §11); k6 exports Time trends in seconds over remote write.
- **Run orchestration** (§5.3 script layer): contract in `tools/load-generator/doc/run-orchestration.md`, engine in
  `tools/load-generator/runner`. Externally-controlled k6 scaled over the REST API (connections survive steps),
  level confirmation before every hold (`k6_vus`, and the connection gauge for T3), plateau detection by relative
  slope, detectors with the `pending_parquet_bytes`-primary seal/upload rule, the §10 generator-CPU guard, pprof at
  70%/100% of the ceiling, and frozen-spec artifacts (`spec.yaml`, `steps.jsonl`, series exports, `result.json`)
  under gitignored `runs/`. Spec templates: `specs/t2-bytes.yaml`, `specs/t3-connections.yaml`; runbook:
  `doc/ceiling-runs.md`.
- **Connection gauge** (§6.4): decided the goroutine proxy is not enough for T3 RAM attribution. Added
  `profiler_ingest_active_connections` plus `connects_total` / `disconnects_total` with locked-together semantics
  (connects on successful RegisterPod; disconnects only for registered connections, shutdown included), tests for
  normal close / failed handshake / collector stop, and an ingest-dashboard panel next to the goroutine proxy.

Smoke-validated on OrbStack (details and run ids in the report draft): a 3-step T2 ramp with plateaus, linear
ingest, and all six pprof artifacts; a forced-backpressure run (8 MiB pending budget) firing `ingest-paused` +
`refused-bytes` + generator-side `ack-errors` on one step; a T3 ramp of idle fleets confirming through the gauge
(300/600 connections, ~12 goroutines and ~8 fds per connection locally).

Waiting on the large cluster: the actual T2 sweeps (bytes/s, calls/s small/large, dictionary churn), the T3 ramp
from 1000 connections up with the failure-shape record, runner node pinning + sizing (§10), and the report numbers.
§8.5–§8.8 checker stubs stayed untouched (none of the smoke runs hit S3/PV limits) — closed in phase 4 (§14).

## 14. Phase 4 status (contract + soak; done 2026-07-16, cluster runs pending)

The large cluster is still unavailable, so this phase closed everything locally validatable and froze the
cluster-only work as ready-to-run specs. Local numbers are never quoted as contract or soak results; the one local
*result* is functional — the accelerated-timer soak's lifecycle verdict.

Shipped:

- **Checker hardened (§8.5–§8.8)**, contract in `tools/load-generator/doc/checker.md`:
  - violations now **latch**: a failure after warm-up fails the run even when the final tick looks healthy
    (previously the exit code came from a final re-evaluation, so transient breaches could end in PASS);
  - **§8.5**: S3 listing per hour prefix through a new read-only client (`libs/s3.NewReadOnlyClient` — `NewClient`
    creates buckets and is unusable for an observer). Compaction-keeps-up judges only (bucket, class) groups past a
    deadline derived from the stand's own timers (seal → upload visibility chain, `COMPACTION_MIN_AGE`, maintain
    cadence, delete grace, slack); the small-file share is judged over a sliding window so an early drop cannot
    mask later growth;
  - **§8.6**: RSS under the pod limit (`-rss-limit-bytes`; the limit is not on `/metrics`) with no monotonic
    growth, and goroutine flatness at a constant connection count, both series taken from the same scrapes.
    A target silent for more than `-max-scrape-gap` polls latches `target-unavailable` — absence no longer passes;
  - **§8.7**: freshness (`/api/v1/calls` windows sent as integer Unix ms) plus marker calls sampled after warm-up
    and re-fetched until their class TTL (`corrupted` excluded: reserved, writer-less); optional
    `-expect-ttl-deletion` demands a 404 after TTL + settle;
  - **§8.8**: pod-restart watch over the k8s API (client-go, kubeconfig-or-in-cluster) with a total restart-event
    budget (`-allowed-restarts`, default 0), replacement pods counted as one event plus their own restarts,
    disappearance-without-replacement flagged separately.
  Unit tests cover every threshold boundary; an `integration`-tagged test exercises the S3 lister against a
  testcontainers MinIO.
- **Accelerated-timer soak layer (§7.4)**: the `local-soak` helmfile environment
  (`deploy/environments/local-soak.yaml`) shrinks every lifecycle timer to minutes (1 m buckets, 3 m hot retention,
  15–45 m class TTLs, 1 m maintain cadence, 3 m compaction min-age); the values templates gained maintain/query
  env and retention passthroughs the stand previously dropped. Spec: `specs/t4-soak-accelerated.yaml`.
- **T6 query generator (§7.6)**: `scripts/query-scenario.js` on the stock `k6/http` module of the same custom
  binary — `ui` (calls list → trace + tree of a random row), `incident` (phase-based wide-range bursts), `cold`
  (guard-edge ranges paged to the end; each page re-lists S3 by design; guard 400s are counted, not failed).
  A profile with 0 VUs is omitted, so one env knob gates each profile. Deployed as the `k6-query` release with its
  own `global.name` (`cdt-query`) and `TESTID` — two releases of the chart must not share resource names.
- **Fixed-hold specs**: `hold.min == hold.max` holds one level for the whole duration (documented in
  `doc/run-orchestration.md`); detectors gained an optional per-hold `grace` after the first soak attempt read the
  cold-start fill of `pending_parquet_bytes` (0 → first uploads) as saturation. `specs/t1-contract.yaml` (500 pods,
  ~6 MB/s, 2 h) and `specs/t4-soak.yaml` (24–48 h, checker + `k6-query` mandatory) are frozen pending the cluster.
- **Runbook**: `doc/soak-runs.md` (T1/T4/T6 + the accelerated overlay and checker flag sets).

Validated locally (run ids and details in the report): the accelerated soak ran under the full §8 checker with the
T6 UI profile beside it, plus a dedicated cold-heavy probe and a reduced-scale T1 spec-mechanics smoke. The soak's
verdict is FAIL by design of the invariants, and the failure is the phase's main finding: after 52 healthy minutes
(every lifecycle stage cycling cleanly), a collector died on a `CallsPipeReader` panic and the checker latched the
whole cascade (§8.8 restart, §8.5 compaction backlog, pending-parquet growth, query degradation). Two more findings
came from T6: concurrent guard-passing wide queries OOM the query pod (the scan guard is per-request; no global
read-path memory budget) and deep pagination costs a full re-scan per page, as the read contract predicts.

Waiting on the large cluster: `t1-contract` finale (baseline utilization + headroom), the 24–48 h real-timer
`t4-soak` (slow leaks — the accelerated run cannot see them, §10; **blocked on the `CallsPipeReader` fix**), T6
numbers worth quoting, and the T2/T3 ceiling campaign of §13.

## 15. Phase 5 status (crashloop + faults; done 2026-07-18, local)

The full §9.5 program ran on the local stand (the large cluster is still unavailable): T5 (§7.5), T7 (§7.7),
and the §7.5.4 protection decision. Results, run citations, and the decision live in the report
(`load-testing-report.md` §8–§9); the campaign followed step 0 of the phase brief — trust the checker first.

Step 0 (harness trust), all closed:

- **Workload wiring fails loudly** (the 0a defect): the phase-4 verification soak had silently run at 1.9× its
  declared rate. Scenarios carry no workload defaults (a missing knob crash-loops the k6 pod), the environments
  pin complete `k6.workload` maps, the runner verifies the deployment's `k6_workload_info` fingerprint against
  the spec's frozen block both ways, and `confirm.ingest` compares the measured rate against
  `level × bytesPerVU` (re-calibrated: 19.8 KB/s per pod at 8 × 3 calls/s, `runs/20260717T085030Z-t4-cal-3cps`).
- **§8.6 goroutines** (0b) judges a least-squares trend, not a range — the verification run's latch was legal
  worker oscillation (rationale in `doc/checker.md`).
- **The re-run at the declared rate** (0c, `runs/20260717T091106Z-t4-soak-accelerated`) was clean end to end:
  runner completed, zero §8 violations across the 2.5 h hold — the §8.5 pressure of the mis-wired run was
  overload, not a maintain ceiling.

Shipped for the fault campaign (contracts first, in `doc/run-orchestration.md`, `doc/checker.md`,
`doc/fault-runs.md`, `virtual-dumper.md`):

- the **fault-injection layer** in the runner: a `faults:` schedule (pod-delete / scale / toxiproxy toxics)
  anchored to the actual hold start, atomic per-injection events in `faults.jsonl`, durable revert through
  `runs/.active-faults` with `-revert-faults` in every preflight, a stand-lock lease, and ready-gated crashloop
  repeats whose `readyAt` series is a first-class artifact;
- **scoped expected-failure allowances** in the checker: per-injection (invariant × subject × window × budget),
  matched by observation time, expected latches reported separately — plus the §8.5 post-deadline-listing rule
  and the runner's three-window fitted-trend detector, all three hardened on the fault runs' own evidence;
- **churn mode** in the virtual dumper (deliberate abrupt disconnects under a stable pod name, counted apart
  from failure reconnects) and the toxiproxy release + `local-faults` environment (chaos-mesh stays parked for
  cluster-side netem/IOChaos).

Findings (details and citations in the report §8–§9): the storm's pod-restart/WAL backlog is unbounded — purge
eligibility is gated by hot-index aging and degrades under churn (the §7.5.4 decision: design the near-empty
purge fast-path; skip rate limits and caps for now; the accept-side cap stays with T3); a grace-0 kill recovers
to READY in ~10 s through one measured `collector.lock`-collision restart and a crashloop of ten does not
degrade; the S3-outage gate order inverts the documented §7.7 chain on WAL-dominant backlogs; class-aware
eviction holds a shrunken budget to within 0.5% at a counted truncation cost; and 2 s of agent-path RTT costs
~40× throughput — the wire protocol assumes co-location.

Cluster-pending: real ENOSPC, netem packet loss, PV IOPS throttling, and every ceiling/contract number, as
before.

## 16. Phase 6 status (campaign close-out; done 2026-07-20)

§9.6 promised "consolidated numbers for goals (a) and (b)". Those numbers do not exist and cannot be produced
honestly: goals §1(1)–(3) and the real-timer soak are blocked on the large cluster (§13–§15). Phase 6 therefore
closed the campaign instead of the numbers — consolidating what is portable, folding contradictions back into the
contracts, and freezing the cluster work so its arrival means execution, not design. Nothing from the findings was
implemented in this phase, no runs were added, and no number was quoted without its source.

Shipped:

- **Report consolidated** (`load-testing-report.md`): a §1.1 goals scoreboard with the honest per-goal status, a
  §1.2 portable-findings summary lifted out of §5–§9, every cluster placeholder naming the spec (or frozen values
  block) and the checklist step that fills it, the T6 incident rows marked blocked-on-P1, and §12 reduced to a
  router over the three follow-up homes.
- **Findings folded into the contracts** — the contracts, not the report, carry the corrected behavior:
  - `01-write-contract.md` §4.6: the backpressure gate order depends on the backlog mix; a WAL-dominant backlog
    trips `IngestPaused` with `SealPaused` never firing (the §7.7 chain presupposed pending-dominant), plus the
    budget-sizing note;
  - `01-write-contract.md` §3.5 / `03-lifecycle.md` §3.9: the effective WAL purge lag is
    `max(WAL_PURGE_GRACE, hot-index lag)`, which is what makes the storm backlog unbounded;
  - `01-write-contract.md` §6.6: sustained late data multiplies patch files — a designed degradation, now stated;
  - `01-write-contract.md` §8: the `collector.lock` crash cycle on a grace-0 kill is expected, measured behavior;
  - `02-read-contract.md` §2.3.2/§7.4/§9: the scan-byte guard counts compressed bytes, is per-request, and
    multiplies under concurrency; the fail-soft backstop's revisit trigger has fired;
  - `06-wire-protocol-server.md` §5: the ack protocol assumes co-location — RTT is a hard throughput ceiling.
- **Backlog** (`load-testing-backlog.md`): every open finding as a self-contained, prioritized task — two P1
  requirements (global read-path memory budget, near-empty purge fast-path), three P2 defects, two P3 items —
  with the defect/requirement/deferred classification rule stated. The declined protections went to `deferred.md`
  with explicit triggers (ack windowing/pipelining; reconnect rate limit + tracked-restart cap).
- **Cluster checklist** (`tools/load-generator/doc/cluster-checklist.md`): prerequisites, the run order mapped to
  the report placeholders, the T3 ramp-continuation rule with frozen accept-cap decision criteria, the T6
  safe-profile values blocks (both scan guards pinned, worst-case concurrency math, a mandatory preflight that
  measures the decompression factor, incident profile blocked on P1), and the three cluster-only fault scenarios.

The harness is in maintenance mode: no new scenarios; fixes only. The next load-testing work is either a backlog
item in its own session or the checklist run when the cluster arrives.
