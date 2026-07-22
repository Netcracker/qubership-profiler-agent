# Cluster run checklist

Frozen at the phase-6 close-out (`load-testing-plan.md` §16). When the large cluster (`load-testing-plan.md` §5.2)
materializes, executing this checklist top to bottom fills every placeholder in `load-testing-report.md` — no new
design work. The run model lives in [run-orchestration.md](run-orchestration.md), the per-test runbooks in
[ceiling-runs.md](ceiling-runs.md), [soak-runs.md](soak-runs.md), and [fault-runs.md](fault-runs.md); spec templates
in `../specs/`.

Source discipline (`load-testing-report.md`): measurements cite `runs/<timestamp>-<name>/`; configuration numbers
cite the spec, values file, or contract that pins them; decisions cite the plan or report section that records them.

## 1. Prerequisites

All of these before the first run; the runs themselves are then mechanical.

| # | Prerequisite | Where it lands |
| --- | --- | --- |
| 1 | Fill `deploy/environments/cluster.yaml`: `storageClassName` for monitoring, MinIO, and the collector; a registry and immutable image tags (digests go into every run spec) | `environments/cluster.yaml` |
| 2 | Node capacity: 3 collector pods (chart resources), query, maintain, MinIO (4 CPU / 8 Gi), the monitoring stack — plus **dedicated nodes for the k6 runner** with several CPUs (plan §10); add a `nodeSelector` to the k6 chart and pin it | k6 chart values |
| 3 | A storage class that **enforces PV size** (for the ENOSPC run) and offers **IOPS control** (for the throttle run); collector PVs sized ≥ the §9 budgets: 10 GB segments + 2 GB pending backlog + WAL headroom (`01-write-contract.md` §9) | `environments/cluster.yaml` |
| 4 | Re-measure `bytesPerVU` on the cluster stand (the local figure is 19,798 B/s for the 8 × 3 calls/s shape, `runs/20260717T085030Z-t4-cal-3cps`): one short fixed-hold run per workload shape, then write the measured value into every `confirm.ingest` block | run specs |
| 5 | Kube access for the runner (pod-delete, scale) and the checker (§8.8 pod watch), kubeconfig or in-cluster | runner/checker flags |
| 6 | `chaos-mesh` unparked (`chaosMesh.installed: true`) with the runtime socket wired for the cluster's container runtime — needed only for step 7 (netem, IOChaos) | `environments/cluster.yaml` |
| 7 | Port-forwards per [ceiling-runs.md](ceiling-runs.md) (k6 REST, VictoriaMetrics, collector metrics); toxiproxy only if a fault run uses toxics | shell |

## 2. Run order

One stand, one run at a time (the runner's stand lock enforces it). Ceilings run first on one replica — cheapest to
reset; the soak and faults come last because they hold the stand longest and faults leave the most state to drain.

| # | Run | Spec / frozen block | Replicas | Closes | Decision it feeds |
| --- | --- | --- | --- | --- | --- |
| 0 | Generator sizing validation | any T2 spec at the two lowest levels | 1 | report §11 (runner sizing + headroom) | trust every later ceiling (plan §10) |
| 1 | T2 sweeps ×4: bytes/s, calls/s small, calls/s large, dictionary churn | `specs/t2-bytes.yaml` + the three derivations ([ceiling-runs.md](ceiling-runs.md)); scale `ramp.levels` up from the local smoke values | 1 | report §3 table, §11 | seal/upload limiting stage |
| 2 | T3 connection ramp from 1,000 up | `specs/t3-connections.yaml` + the continuation rule (§3 below) | 1 | report §4 table | **accept-side cap** (§3 below) |
| 3 | T1 contract, 2 h fixed hold | `specs/t1-contract.yaml` (500 pods, ~6 MB/s) | 3 | report §6 table — plan goal (a) | headroom statement |
| 4 | T4 real-timer soak, 24–48 h, checker + `k6-query` **ui companion only** | `specs/t4-soak.yaml` + the T6 companion block (§4 below) | 3 | report §5, §7 (UI rows), §10 verdict — plan goal (b), slow leaks | — |
| 5 | T6 dedicated cold probe (safe profile) | frozen block in §4 below | 3 | report §7 (cold/pagination rows) | pagination cost model at scale |
| 6 | T7 cluster-only faults: real ENOSPC, netem packet loss, PV IOPS throttle | §5 below | 3 | report §9 pending items | — |

## 3. T3: continuation rule and the accept-cap decision

`specs/t3-connections.yaml` ends at 50 VU × 100 = 5,000 connections, which may not reach a ceiling.

**Continuation rule.** If the last level ends `ok`, append the next level at a fixed +10 VU (+1,000 connections) step
and continue until a detector fires, a resource ceiling appears (collector RSS near its limit, fds near the ulimit),
or the cluster itself limits the ramp (generator guard §10, node capacity). If the ramp exhausts the cluster without
saturating, record the outcome explicitly as "ceiling not found at ≥ N connections; accept-cap decision deferred" in
report §4, and add the revisit trigger to `deferred.md`.

**Accept-cap decision** (owed since plan §7.3; the T5 review deferred it here, `load-testing-report.md` §8). Record
the signals: RAM / goroutines / fds per connection, the first limit hit, and the failure shape at the ceiling —
`session-ready` p95, connect errors, OOMKill or fd exhaustion, and above all the fate of already-established
sessions. Then decide:

- **Introduce the cap** if the failure at the ceiling is destructive to established sessions (replica OOMKill,
  fd exhaustion tearing live streams, READY flapping). Set it at ~0.8 × the measured ceiling via the per-connection
  cost formula, with a floor of ≥ 1.5 × the steady per-replica connection count — a lower cap makes the failover of
  a dead neighbor replica refuse its own fleet.
- **Decline the cap** if overload degrades only new accepts (connect errors, rising connect latency) while
  established sessions stay healthy — the agent's 10 s reconnect loop absorbs that.

The trade being weighed: a cap converts overload into a visible, bounded refusal of new agents; no cap risks losing
the whole replica. Outcome: a decision paragraph in report §4, plus either a backlog item carrying the sizing
formula or a `deferred.md` entry with its trigger.

## 4. T6: safe profile only (blocked-on-P1 boundary)

**Decision (phase 6).** Until the P1 global read-path memory budget lands (`load-testing-backlog.md`), the cluster
runs only the safe T6 profile: the `ui` companion during T4 and a dedicated `cold` probe. The incident profile and
any concurrent-wide load stay **blocked-on-P1** — their report-§7 rows remain placeholders. The per-request scan
guard counts compressed bytes and multiplies under concurrency (`02-read-contract.md` §2.3.2); the local campaign
OOM-killed a 3 Gi pod in 34 s that way (`load-testing-report.md` §7). Safety is computed, then verified:

```text
MAX_SCAN_BYTES ≤ (query memory limit − baseline RSS) / (C × E)

C = max concurrent guard-passing queries, worst case = all profile VUs on ONE pod
    (two query replicas do NOT halve C: long-lived k6 connections balance unevenly)
E = compressed→heap expansion factor; use 10 until the preflight measures it
```

**Preflight (mandatory, before either T6 step):** with the block below applied, run one cold VU issuing a single
guard-passing query at the pinned budget. Record the query pod's baseline and peak RSS and compute the actual
`E = ΔRSS / estimated_bytes` (the estimate is in the guard-rejection body or the query log). Proceed only if peak
RSS < 50% of the limit; otherwise recompute the budget with the measured E and repeat. Optionally sample the
per-pod request split (`profiler_query_http_request_seconds` per pod) — using `max(per-pod share)` instead of the
worst case is a relaxation, never a requirement.

**Frozen block — T4 ui companion** (hot-window UI only; wide UI is blocked-on-P1):

```yaml
profiler:
  query:
    replicas: 2                      # chart default, frozen for the C math above
    resources:                       # chart default made explicit; the budget derives from it
      requests: {cpu: 200m, memory: 512Mi}
      limits: {cpu: "1", memory: 2Gi}
    extraEnv:
      PROFILER_MAX_SCAN_BYTES: "50331648"   # 48 MiB ≈ (2048 − 256 MiB baseline) / (C=3 × E=10), rounded down
      PROFILER_MAX_SCAN_FILES: "10000"      # contract default, pinned (02-read-contract.md §9)
k6query:
  installed: true
  workload:                          # complete map — the scenario has no defaults
    UI_VUS: "3"
    INCIDENT_VUS: "0"                # cluster.yaml defaults to "20"; the override IS the safe profile
    COLD_VUS: "0"
    UI_RANGE_MINUTES: "15"           # ≈ the hot window: cold estimates stay near zero, so the 48 MiB budget holds
    INCIDENT_PERIOD_MINUTES: "30"
    INCIDENT_DURATION_MINUTES: "5"
    WIDE_RANGE_MINUTES: "350"
    LIST_LIMIT: "50"
    THINK_SECONDS: "5"
    COLD_MAX_PAGES: "1000"
```

**Frozen block — dedicated cold probe** (runs after T4, its own `TESTID`; UI off, so C = 2):

```yaml
profiler:
  query:
    replicas: 2
    resources:
      requests: {cpu: 200m, memory: 1Gi}
      limits: {cpu: "1", memory: 4Gi}       # raised so a meaningful budget fits the C×E math
    extraEnv:
      PROFILER_MAX_SCAN_BYTES: "134217728"  # 128 MiB ≤ (4096 − 512 MiB baseline) / (C=2 × E=10)
      PROFILER_MAX_SCAN_FILES: "10000"
k6query:
  installed: true
  workload:
    UI_VUS: "0"
    INCIDENT_VUS: "0"
    COLD_VUS: "2"
    UI_RANGE_MINUTES: "15"
    INCIDENT_PERIOD_MINUTES: "30"
    INCIDENT_DURATION_MINUTES: "5"
    WIDE_RANGE_MINUTES: "<computed>"        # see below
    LIST_LIMIT: "50"
    THINK_SECONDS: "5"
    COLD_MAX_PAGES: "1000"
```

**Sizing `WIDE_RANGE_MINUTES`.** Guards scale with data density (`load-testing-report.md` §7, the
accelerated-density lesson), so the window is computed, not guessed: measure the stand's S3 volume per hour for the
probed classes (the checker's §8.5 listing log, or the MinIO dashboard), then pick the window so
`estimated_bytes ≤ 0.8 × MAX_SCAN_BYTES`. At contract density an unfiltered window that fits 128 MiB is minutes
wide; to probe *deep* pagination instead, narrow by class — `error_only=true` cuts the estimate to the error share
(~1% of volume at the T1 workload's `ERROR_SHARE: "0.01"`, `specs/t1-contract.yaml`) and widens the affordable
window proportionally.

**Stop criterion (cold probe).** A probe pass is complete only when the cursor is exhausted (`next_cursor == null`)
before `COLD_MAX_PAGES`: the scenario truncates the walk at that cap (`scripts/query-scenario.js`), and a capped
walk is labeled **truncated** in the report — it bounds page cost but is not a complete pagination-cost model. If
truncation happens, raise `COLD_MAX_PAGES` or narrow the window and rerun.

**What to record** is unchanged from [soak-runs.md](soak-runs.md): query CPU/RSS, request-seconds percentiles,
fan-out tail, LIST/GET volume, guard counters, partials, and the read-vs-ingest interaction — into report §7.

## 5. T7: cluster-only faults

Three scenarios the local stand cannot produce (`load-testing-report.md` §9); all reuse the fault-run machinery
([fault-runs.md](fault-runs.md)) with the checker's scoped allowances.

1. **Real ENOSPC.** Deploy-time variant like `specs/t7-small-pv.yaml`, but on a size-enforcing storage class with
   the PV sized below the steady footprint (segments + WAL + pending). Expectation to verify: the write failure
   tears the stream down and the agent reconnects — there is no reactive ENOSPC handling — and recovery after space
   frees is clean. Record the failure shape; it feeds the report's small-PV section.
2. **Netem packet loss.** chaos-mesh `NetworkChaos` (1–5% loss) on the agent path, hold and expectations derived
   from `specs/t7-agent-net.yaml`. Verifies the socket deadlines under loss rather than latency; the latency and
   stall results are already measured (`runs/20260717T235336Z-t7-agent-net`).
3. **PV IOPS throttle.** chaos-mesh `IOChaos` (or storage-class QoS) against the collector PV under the soak shape:
   watch seal/upload lag, WAL fsync stalls, and whether the backpressure gates engage in the order the backlog mix
   predicts (`01-write-contract.md` §4.6).

## 6. Caveats that ride into the report

- MinIO understates real object-storage latency (plan §10): cold-read latency conclusions carry that caveat unless
  the stand uses a real S3 endpoint.
- Absolute T5/T7 numbers from the local campaign stay *(local)*; only the mechanisms and orderings carry
  (`load-testing-report.md` §8–§9).
