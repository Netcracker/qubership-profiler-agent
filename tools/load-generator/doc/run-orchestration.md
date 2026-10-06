# Run orchestration

Contract for the ramp-run layer of the load-testing harness (`load-testing-plan.md` §5.3): how a ceiling run is
specified, executed, judged, and archived. The implementation is `tools/load-generator/runner`; the traffic comes from
the k6 fleet scenario (`scripts/scenario.js`) driven over the k6 REST API. Fault injection for the T5/T7 runs is the
*Fault injection* section below; the fault runbook is `doc/fault-runs.md`.

## Model

One **run** is one k6 process executing one sweep along one axis (`load-testing-plan.md` §7.2–§7.3). The k6 scenario
uses the `externally-controlled` executor and starts at 0 VUs; the runner raises the VU count step by step, so the
connections of earlier steps stay alive — mandatory for T3, where a per-step reconnect storm would poison the
RAM-per-connection numbers.

Each **step** goes through four stages:

1. **Scale**: `PATCH /v1/status` on the k6 REST API with the step's VU target. A 200 response only means the request
   was accepted.
2. **Confirm**: wait until `k6_vus` reports the target, and — when `confirm.connectionsPerVU` is set (T3) — until
   `profiler_ingest_active_connections` reaches `level × connectionsPerVU`. A confirmation timeout marks the step
   `invalid` with the reason recorded, and ends the run.
3. **Hold**: sample the plateau series until every one of them is flat (see *Plateau*) but at least `hold.min`, at most
   `hold.max`. Detectors run on every sample.
4. **Verdict**: `ok` (plateau reached, no detector fired), `saturated` (a detector fired; the previous step is the
   ceiling candidate), or `invalid` (confirmation timeout, generator guard, query failures).

After a `saturated` verdict the runner re-holds at the pprof points (by default 70% and 100% of the last `ok` level,
rounded to whole VUs) and captures CPU, heap, and goroutine profiles at each. The pre-capture settle is
`min(hold.min, 5m)`: a fixed-hold soak sets `hold.min` to hours, and its capture level was just held that long.

**Fixed hold (contract and soak runs).** A single-level spec with `hold.min == hold.max` holds that level for exactly
that duration: the plateau check can only end a hold after `hold.min`, so with the two equal the hold runs to the
full length with detectors live throughout, and ends `ok` with `plateau` recorded as informational. T1/T4 specs
(`doc/soak-runs.md`) use this shape — `ramp.levels: [500]`, a 2–48 h hold, §8-shaped detectors — and set
`pprof.points: [1.0]` so the profile capture re-holds at the run level instead of dropping to 70% after the soak.

## Run spec

The spec is one YAML file; the runner copies it verbatim into the artifact directory before the first scale, together
with everything needed to compare two runs:

- the **run label** (`run.testid`): a unique string, also set as the k6 deployment's `TESTID` env, so k6 series of
  different runs never mix in VictoriaMetrics. Collector series carry no run label; they are separated by the step
  time windows recorded in `steps.jsonl`.
- the **full workload** (`workload:`): every `scripts/scenario.js` workload knob, mirroring `load-testing-plan.md` §4.
  The block is a complete freeze, not a diff from defaults — the scenario has no workload defaults to diff against
  (see *Workload wiring* below). The runner does not push these to the k6 pod; it verifies them against the
  deployment's exported fingerprint and refuses to run on any mismatch.
- the **images** (`images:`): backend and runner references with digests.
- the **Helm values snapshot** (`helmValues:`): path to the rendered values (resource limits included); copied into
  the artifacts.
- the **detector queries and thresholds**, PromQL verbatim.

```yaml
run:
  name: t2-bytes            # t2-bytes | t2-calls-small | t2-calls-large | t2-dict-churn | t3-connections
  testid: t2-bytes-20260716a
outputs: runs/              # the runner creates runs/<started>-<name>/

endpoints:
  k6: http://localhost:6565         # k6 REST API (kubectl port-forward svc/cdt-loader-service 6565:6565)
  vm: http://localhost:8429         # VictoriaMetrics HTTP API (port-forward vmsingle-k8s 8429:8429)
  collector: http://localhost:8081  # collector internal port: /metrics and /debug/pprof

images:
  backend: profiler-backend@sha256:...
  runner: cdt-load-generator@sha256:...
helmValues: ./values-snapshot.yaml

workload:                   # complete freeze: every scenario workload knob, verbatim
  PODS_PER_VU: "1"
  THREADS_PER_POD: "8"
  CALLS_PER_SEC: "5"
  # ... all remaining knobs; the runner refuses to start when this set
  # differs from the deployment's k6_workload_info fingerprint

ramp:
  levels: [10, 20, 40, 80, 160]     # VUs per step, one axis at a time
  confirm:
    timeout: 3m
    connectionsPerVU: 0             # T3: pods per VU; 0 disables the connection check
    ingest:                         # actual-vs-declared ingest check; omit to disable (T3 idle fleets)
      bytesPerVU: 19650             # steady ingest bytes/s each VU is expected to add
      tolerance: 0.25               # relative deviation that still confirms
  hold:
    min: 3m
    max: 15m
    sample: 15s                     # detector/plateau sampling cadence
    plateau:
      window: 2m
      slopeTolerance: 0.05          # relative growth per window that still counts as flat
      series:
        ingest-bytes: sum(rate(profiler_ingest_bytes_total{namespace="profiler-load"}[1m]))
        collector-rss: sum(container_memory_working_set_bytes{namespace="profiler-load", pod=~".*collector.*", container!=""})

detectors:                          # any hit => the step verdict is `saturated`
  - name: ingest-paused
    kind: sticky-share              # fires when the gauge is nonzero > `share` of the hold so far
    share: 0.05
    query: max(profiler_backpressure_ingest_paused{namespace="profiler-load"})
  - name: pending-parquet-growth    # seal/upload primary signal; see the note below
    kind: monotonic-growth
    minGrowth: 0.10                 # relative growth over the hold that counts as real
    query: sum(profiler_hotstore_pending_parquet_bytes{namespace="profiler-load"})
  - name: refused-bytes
    kind: nonzero
    query: sum(rate(profiler_ingest_refused_bytes_total{namespace="profiler-load"}[1m]))
  - name: ack-flush-degraded
    kind: baseline-ratio            # first `ok` step's mean is the baseline
    ratio: 5
    query: max(k6_vdumper_ack_flush_time_p95{testid="t2-bytes-20260716a"})
  - name: ack-errors
    kind: nonzero
    query: sum(rate(k6_vdumper_ack_errors_total{testid="t2-bytes-20260716a"}[1m]))

context:                            # sampled and archived, never a trigger
  upload-lag: max(profiler_upload_lag_seconds{namespace="profiler-load"})
  upload-backlog: sum(profiler_upload_backlog{namespace="profiler-load"})
  hot-window-lag: max(profiler_hotstore_hot_window_lag_seconds{namespace="profiler-load"})
  active-connections: sum(profiler_ingest_active_connections{namespace="profiler-load"})
  collector-goroutines: sum(go_goroutines{namespace="profiler-load", pod=~".*collector.*"})

guard:                              # plan §10: never trust a ceiling set by the generator
  generator-cpu:
    query: sum(rate(container_cpu_usage_seconds_total{namespace="profiler-load", pod=~"cdt-loader.*", container!=""}[1m]))
    limitCores: 2.0                 # the k6 pod's CPU limit from the Helm values
    maxShare: 0.7                   # above this the step is `invalid`, not a ceiling

pprof:
  points: [0.7, 1.0]                # shares of the ceiling level
  seconds: 30
  profiles: [profile, heap, goroutine]
```

## Workload wiring: no silent defaults

The methodology's core promise — the run spec freezes the workload — failed once in phase 4: the stand left a knob
unset, the scenario fell back to a built-in default, and a soak ran at 1.9× its declared rate with every artifact
claiming otherwise. Three rules now make that impossible:

1. **The scenario has no workload defaults.** Every workload knob of `scripts/scenario.js` (and every profile knob of
   `scripts/query-scenario.js`) must be set in the k6 deployment's environment — the `k6.workload` map in the helm
   values. A missing knob makes the scenario throw during init: k6 exits nonzero and the pod crash-loops, so a
   misconfigured stand fails loudly instead of sending a silently different load. Plumbing (endpoints, `TESTID`,
   `MAX_VUS`, `DURATION`) keeps defaults — it caps or labels the run but does not shape the traffic.
2. **The deployment exports its fingerprint.** On test start the scenario emits one `workload_info` sample per knob
   (`k6_workload_info{knob, value}` in VictoriaMetrics, under the run's `testid`), with the raw env string as the
   value.
3. **The runner verifies the fingerprint in preflight.** Within `confirm.timeout` it waits for every spec knob to
   appear under `run.testid`, then requires exact string equality in both directions: a spec knob missing from the
   deployment, a deployment knob missing from the spec, or any value difference refuses the run and names the knobs.
   Two different values for one knob under the same `testid` mean a reused test id — also a refusal. Comparison is
   env-level string equality (`"3"` ≠ `"3.0"`); write spec values exactly as the deployment env carries them.

**Ingest confirm.** `ramp.confirm.ingest` closes the remaining gap: the fingerprint proves the knobs, this proves the
load. The runner samples the ingest query (default: the `ingest-bytes` plateau series — collector-side
`sum(rate(profiler_ingest_bytes_total{...}[1m]))` in bytes/s; collector series carry no run label, so the stand must
run one k6 deployment at a time; `rate()` absorbs counter resets) over the first plateau window after the level
confirms, and compares the window mean against `level × bytesPerVU`. A deviation beyond `tolerance` marks the step and
the run `invalid`: the numbers would describe a load the spec does not. Omit the block for fleets with no meaningful
ingest (T3 idle connections) and for ramp-to-saturation sweeps (T2) — past the ceiling the measured ingest
legitimately diverges from `level × bytesPerVU`, and that divergence is the detectors' verdict to make, not a
validity failure. The fingerprint check protects the knobs on every run shape.

**Why `pending_parquet_bytes` is the seal/upload primary.** `profiler_upload_backlog` counts *files* and parquet files
vary in size, so the count alone misreads mixed workloads; a hard AND across three series would miss saturation when
bytes hit the budget before the file count moves. The byte gauge growing monotonically through the whole hold is the
trigger; `upload_lag_seconds` and `upload_backlog` are archived as confirming context for the report.

## Detector kinds

All detectors evaluate over the samples of the current hold only. An optional per-detector `grace: 5m` additionally
drops the first samples of every hold, covering two cold-start artifacts: a hold against an empty store fills it
(pending-parquet bytes grow from zero until the first uploads drain), and a stand idle before the hold reports
stale-data hot-window lag until the first new bucket seals. Growth-shaped and absolute-bound detectors would read
either as saturation.

- `sticky-share`: the instant value is nonzero in more than `share` of the samples so far.
- `monotonic-growth`: the least-squares-fitted growth of the series exceeds `minGrowth` (relative to the fitted mean)
  over the hold *and* the last plateau window shows no flattening (its relative slope stays above `slopeTolerance`).
  The fit — not a first-to-last delta — is what keeps a sawtooth oscillating around a level from firing on whichever
  edge aligns with the window; the T5 storm run proved the delta form wrong on a healthy purge-cycle plateau. The
  detector is judged only once its (post-grace) samples span three plateau windows: one window is the flatness scale
  of the tail, not the trend scale — the storm's second attempt fired at exactly one window of span, where the fit
  covered a single trough-to-crest arc of a sawtooth whose period exceeded the window. With three windows the tail
  is a minority of the evidence and a cycle fits flat; a genuine climb just fires a little later. An optional
  absolute `minValue` keeps the detector silent while the last sample is below it — a gauge that oscillates down to
  zero (pending-parquet bytes between upload cycles) stays out of judgment while empty.
- `nonzero`: any sample above zero.
- `baseline-ratio`: the mean over the last plateau window exceeds `ratio ×` the baseline; the baseline is the mean of
  the same query over the first `ok` step's hold. Until a baseline exists the detector stays silent.

**Plateau**: a series is flat when the relative growth of a least-squares fit over the last `plateau.window` is within
`slopeTolerance`. The hold ends at the first sample where every plateau series is flat (but not before `hold.min`);
`hold.max` ends the hold with verdict `ok` and `plateau: false` recorded — a hint the tolerance is too tight, not a
saturation signal by itself.

## Fault injection (T5/T7)

A fault run is an ordinary fixed-hold run plus a `faults:` schedule the runner executes during the hold. The layer
has three jobs: inject on a reproducible timeline, leave an event log the checker can consume live, and never leave
the stand faulted — whatever happens to the runner process.

```yaml
endpoints:
  toxiproxy: http://localhost:8474   # required only when a fault uses action: toxics

faults:
  - name: kill-collector             # unique across the spec (validation)
    at: 30m                          # from the ACTUAL hold start; required on every fault
    action: pod-delete               # instant action: client-go delete, grace 0
    target: {namespace: profiler-load, pod: profiler-backend-collector-1}
    repeat: {every: 90s, count: 10, readyTimeout: 5m}   # instant actions only
    expects: [restarts, scrape-gap, freshness, ack-errors]
    restartBudget: 2                 # §8.8 units one injection legitimately produces (default 1); a grace-0
                                     # collector kill measures at 2 — replacement + collector.lock collision
    settle: 5m                       # expected-effects tail after the fault / its revert
  - name: s3-outage
    at: 60m
    action: scale                    # stateful: client-go scale, durable revert
    target: {namespace: profiler-load, kind: statefulset, name: minio}
    to: 0
    duration: 15m                    # stateful actions only; repeat and duration exclude each other
    expects: [ingest-paused, refused-bytes, ack-errors, compaction-lag, freshness]
  - name: s3-slow
    at: 20m
    action: toxics                   # stateful: toxiproxy REST; several toxics land together
    target: {proxy: s3}
    toxics:
      - {type: latency, attributes: {latency: 2000, jitter: 500}}
      - {type: bandwidth, attributes: {rate: 64}}
    duration: 10m
    expects: [compaction-lag]
```

**Timeline.** Every `at` counts from the actual start of the (single) hold — after scale and the whole confirm phase.
The runner records `holdStartedAt` in `steps.jsonl` and stamps it into the fault log, so the spec offset and the
wall-clock timeline reconcile in the artifacts.

**Actions.**

- `pod-delete` — instant: client-go pod delete with grace 0 (the crash shape, not a drain). With `repeat`, the next
  injection is scheduled only after the target is observed READY again plus `every`; a target that misses
  `readyTimeout` is a failed recovery — the run turns `invalid` and injections stop. Every injection records
  `readyAt`, so recovery time per cycle (`readyAt − at`) is a first-class artifact series.
- `scale` — stateful: scale a statefulset/deployment to `to` replicas, revert to the observed prior count after
  `duration`.
- `toxics` — stateful: create the listed toxics on the named toxiproxy proxy, delete them after `duration`. All
  toxics of one injection are named `<faultId>-<idx>`, so the revert (and any cleanup) deletes by prefix,
  idempotently.

**Event log.** `faults.jsonl` in the run directory carries atomic events, one JSON object per line, written the
moment they happen: `{faultId, name, event, at, scheduledAt, action, target, expects, settle, detail}` with
`event ∈ scheduled | started | ended | revert-started | reverted | ready | error`. `faultId` identifies one concrete
injection — repeats get `kill-collector-001`, `-002`, … (no `#` or other characters unsafe in toxic names, URL
paths, or k8s names) — so a per-injection budget and the recovery-time series stay unambiguous. `scheduledAt` is the
plan, `at` is the fact, `error` events carry the failure; they are never folded into one field. A reader must
tolerate a torn last line (re-read it next tick); an injection whose `started` has no `reverted` yet is active, and
its window extends to now.

**Durable revert.** Before a stateful action executes, the runner writes the prior state (replica count; toxic
names) to the active-faults registry — `runs/.active-faults/<testid>.json`, outside the per-run directory, plus a
copy in the run artifacts. The entry is removed after a successful revert. `runner -revert-faults` restores
everything the registry still holds (idempotently; toxics deleted by faultId prefix) and runs automatically in every
preflight, so a SIGKILL-ed or crashed runner cannot leave MinIO scaled down or a proxy poisoned past the next run.
A failed injection **or** a failed revert marks the run `invalid` — a fault run whose faults misfired proves
nothing.

**Stand lock.** One run per stand is a hard rule, not a convention: the runner takes a lease
(`runs/.stand-lock`: `{testid, pid, acquiredAt, renewedAt, ttl}`) in preflight, renews it through the run, and
releases it at exit. A live foreign lease refuses the start — and preflight recovery then does NOT touch the
registry, because reverting a running run's active fault would corrupt it. `-revert-faults` only processes state
whose lease is dead or expired.

**Detectors during faults.** The spec's `expects` list is the single mechanism for expected failures. The checker
maps it to scoped allowances (`doc/checker.md`); the runner applies the same windows to its own detectors — a
detector whose name appears in an active fault's `expects` (window + settle) has those samples excluded. Runner
detectors are aggregate PromQL queries, so subject attribution is impossible at this layer; the exclusion can in
principle mask an unrelated firing inside the window, which is why the checker — whose allowances are scoped by
invariant × subject × window × budget — stays the authoritative judge of a fault run. Outside fault windows every
detector behaves exactly as before.

## Artifacts

Everything lands under `runs/<started-UTC>-<name>/` (gitignored; runs never enter the repository):

```text
spec.yaml               the spec, copied before the first scale
values-snapshot.yaml    the Helm values referenced by helmValues
steps.jsonl             one line per step (see below)
series/<name>.json      query_range export of every plateau/detector/context/guard series over the whole run
pprof/<profile>-<point>pct.pb.gz   e.g. cpu-70pct.pb.gz, heap-100pct.pb.gz, goroutine-100pct.pb.gz
result.json             run verdict: ceiling level, firing detector, invalid flags, step index
```

A `steps.jsonl` line:

```json
{"level": 80, "startedAt": "...", "confirmedAt": "...", "endedAt": "...", "verdict": "saturated",
 "reasons": ["pending-parquet-growth"], "plateau": true,
 "measurements": {"ingest-bytes": 5.2e6, "collector-rss": 4.1e8, "upload-lag": 42.0},
 "generator": {"cpuCores": 0.9, "cpuShare": 0.45, "valid": true}}
```

`measurements` carries the mean of each series over the last plateau window — the numbers the report quotes.
`generator.valid: false` (CPU share above `maxShare`) marks the step `invalid`: the run stops and no ceiling is
reported, because past that point the harness measures the generator, not the collector.

## Runner CLI

```bash
go run ./tools/load-generator/runner -spec t2-bytes.yaml
```

The runner takes ready URLs and never manages port-forwards; the runbook (`doc/ceiling-runs.md`) lists the
`kubectl port-forward` lines per stand. Exit code 0 means the run completed with a verdict (including `saturated` —
finding the ceiling is the point); nonzero means the run aborted (`invalid` step, endpoint failure, spec error).
