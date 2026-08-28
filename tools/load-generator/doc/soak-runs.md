# Contract and soak runs (T1, T4, T6)

Runbook for the contract run, the soaks, and the background query load of `load-testing-plan.md` §7.1, §7.4, §7.6.
The run model and spec format live in [run-orchestration.md](run-orchestration.md); the invariant checker's contract
lives in [checker.md](checker.md); spec templates live in `../specs/`.

Final T1/T4 numbers come from the large cluster only (plan §5.2). What the local stand *does* deliver as results:
the accelerated-timer soak validates the data-lifecycle invariants end to end (a functional verdict, portable to the
cluster), and T6 establishes the qualitative guard and pagination behavior. Absolute latency, CPU, and S3-volume
numbers from the local stand are harness validation only and are labeled *(local)* in the report.

## Fixed-hold specs

T1 and T4 reuse the ramp engine with a single level and a fixed hold: `ramp.levels: [N]` and
`hold.min == hold.max == <run duration>` (see the fixed-hold note in run-orchestration.md). Detectors run through the
whole hold; `pprof.points: [1.0]` profiles the level itself without the pointless 70% downshift after a soak.

## T1: contract run (pending cluster)

`specs/t1-contract.yaml`: 3 collector replicas, 500 pods (1 pod/VU), per-pod rate sized so the cluster total is
~6 MB/s of raw trace, fixed 2 h hold. Expected green path: no `ingest_paused`, `refused_bytes` = 0, bounded
`pending_parquet_bytes`, `hot_window_lag` under budget — each of those is a detector, so a contract violation ends
the run as `saturated` with the reason named.

```bash
cd backend/tools/load-generator/deploy
helmfile -e cluster apply --state-values-set profiler.collector.replicas=3
cd .. && go run ./runner -spec specs/t1-contract.yaml   # after the usual port-forwards (ceiling-runs.md)
```

Local smoke: scale `ramp.levels` down (e.g. `[50]`) and the hold to 20–30 m; the result validates the spec
mechanics, not the contract.

## T4: soak (real timers pending cluster; accelerated timers local)

### Real-timer soak (pending cluster)

`specs/t4-soak.yaml`: the T1 shape held for 24 h (extendable to 48 h), plus two mandatory companions started before
the runner:

1. the checker with every source enabled (`checker.md`) — metrics targets, `-s3`, `-query-url`, `-kube-namespace`,
   `-rss-limit-bytes` from the Helm values;
2. the background query load (`k6-query` release, UI profile) — the soak measures ingest under realistic read
   traffic, not in silence.

### Accelerated-timer soak (local)

A 2–4 h run that compresses every lifecycle timer to minutes, so the data cycles through
seal → upload → janitor eviction → maintain compaction → TTL deletion several times while the checker watches. The
`local-soak` helmfile environment carries the overlay (`deploy/environments/local-soak.yaml`):

| Stage | Knob | Overlay value | Default |
| --- | --- | --- | --- |
| seal | `PROFILER_TIME_BUCKET` (must divide the hour) | `1m` | `5m` |
| seal | `PROFILER_TIME_BUCKET_GRACE` | `10s` | `30s` |
| seal | `PROFILER_SEAL_CHECK_INTERVAL` | `5s` | `15s` |
| upload | `PROFILER_UPLOAD_CHECK_INTERVAL` | `10s` | `30s` |
| janitor | `PROFILER_JANITOR_CHECK_INTERVAL` | `10s` | `30s` |
| janitor | `PROFILER_HOT_RETENTION` | `3m` | `15m` |
| janitor | `PROFILER_WAL_PURGE_GRACE` | `2m` | `1h` |
| query | `PROFILER_OVERLAP_MARGIN` | `1m` | `5m` |
| maintain | `PROFILER_TIME_BUCKET` | `1m` (must mirror the collector) | `5m` |
| maintain | `PROFILER_MAINTAIN_CHECK_INTERVAL` | `1m` | `5m` |
| maintain | `PROFILER_COMPACTION_MIN_AGE` | `3m` | `30m` |
| maintain | `PROFILER_COMPACTION_DELETE_GRACE` | `1m` | `5m` |
| retention | short/normal/long/huge clean TTL | `15m` / `25m` / `35m` / `45m` | `2d` / `7d` / `30d` / `180d` |
| retention | any-error TTL | `45m` | `180d` |
| retention | corrupted TTL | `25m` (set explicitly; the class is reserved and writer-less, but leaving it at 7 d would exempt it from the accelerated run) | `7d` |
| retention | pods-manifest TTL | `2h` (must exceed the longest class TTL) | `185d` |

The overlay keeps the un-enforced invariant `hot_retention ≥ time_bucket + grace + overlap_margin`
(3m ≥ 1m + 10s + 1m) — breaking it gaps hot reads.

Run it:

```bash
cd backend/tools/load-generator/deploy && helmfile -e local-soak apply
# port-forwards: ceiling-runs.md, plus the query service for the checker probe
cd .. && go run ./runner -spec specs/t4-soak-accelerated.yaml &
go run ./checker \
  -targets http://localhost:8081/metrics,... \
  -warmup 5m -window 30m -max-hot-lag 7m \
  -max-scrape-gap 8 \
  -s3 -s3-interval 1m -time-bucket 1m -time-bucket-grace 10s -seal-check-interval 5s \
  -upload-check-interval 10s -maintain-check-interval 1m -compaction-min-age 3m -compaction-delete-grace 1m \
  -query-url http://localhost:8080 -expect-ttl-deletion \
  -kube-namespace profiler-load -rss-limit-bytes <collector limit from values>
```

`PROFILER_RETENTION_*` must be exported to the checker's environment with the same values the overlay sets — the
marker TTLs (§8.7) read them.

Stop the checker (SIGINT) as soon as the runner exits. Two invariants presuppose a live feed — §8.7 freshness and
the §8.5 small-file trend — and turn meaningless once the generator scales to 0: a checker left running past the
hold latches "no calls in the last N minutes" and drain-shaped share growth that are post-run artifacts, not
findings. The phase-5 step-0 re-run demonstrated exactly that tail.

The accelerated run cannot see slow leaks (plan §10); the real-timer 24–48 h soak stays mandatory on the cluster.

## T6: background query load

`scripts/query-scenario.js` (plain `k6/http` in the same custom k6 image) drives the read side. Windows are computed
in JS and sent as integer Unix milliseconds. Every profile knob must be pinned in `k6query.workload` — the scenario
has no workload defaults and refuses to start on a missing knob (doc/run-orchestration.md, "Workload wiring"); the
values below are the stand baselines from the environment files, per plan §7.6 (chosen by us, to be revisited with
real usage data):

- **`ui`** — `UI_VUS` (baseline 3) virtual users looping the UI journey: `GET /api/v1/calls` over the last hour →
  pick a random row → `GET /calls/{pk}/trace?ts_ms=…` → `GET /calls/{pk}/tree` → think-time. The plan's
  "open a call" step maps to trace + tree: there is no bare `/calls/{pk}` endpoint.
- **`incident`** — phase-based bursts: `INCIDENT_VUS` (20) users hammer wide ranges for
  `INCIDENT_DURATION_MINUTES` (5) out of every `INCIDENT_PERIOD_MINUTES` (30); idle between phases.
- **`cold`** — `COLD_VUS` (baseline 0: this is a dedicated probe, not a soak companion) users issue ranges just
  under the 6 h wide-range guard and page through every `next_cursor` to the end; every page re-lists S3 by
  design. Guard rejections (HTTP 400 with a problem body) are counted in the `query_guard_rejected` custom
  metric, not treated as request failures — probing the guard is the point.

A profile with 0 VUs is omitted from the k6 options entirely, so one env knob switches a profile off. The cluster
soak companion runs `ui` + `incident`; the cold-heavy probe runs `COLD_VUS>0` with the others at 0. On the local
stand the soak companion is `ui` only (`INCIDENT_VUS=0`): concurrent guard-passing wide-range queries multiply the
per-request scan budget and OOM the single query replica — a recorded T6 finding, not a tuning knob to hide.

Deployment: the `k6-query` helmfile release (off by default) runs the same image with `SCENARIO=query-scenario.js`,
its own `TESTID`, and its own resource names (`global.name: cdt-query`), so it never conflicts with the write-side
`cdt-loader` release:

```bash
helmfile -e local-soak apply --state-values-set k6query.installed=true
```

What to record (report section T6): query CPU/RSS, `profiler_query_http_request_seconds` percentiles, fan-out tail
(`profiler_query_fanout_replica_request_seconds`), S3 LIST/GET volume (`cdt_minio_operation_*`,
`profiler_query_cold_lists_total`), guard behavior (`profiler_query_guard_rejections_total{layer}`), partial
responses, and the reverse effect — whether hot-read traffic pushes ingest toward backpressure
(`backpressure` dashboard over the same window).

## Pending-cluster checklist

Moved to [cluster-checklist.md](cluster-checklist.md) at the phase-6 close-out: prerequisites, the full run order
(T1, T2, T3, T4, T6, T7, and runner sizing), the frozen T6 safe-profile blocks, and the T3 accept-cap decision
criteria.
