# Ceiling runs (T2, T3)

Runbook for the single-replica ceiling tests of `load-testing-plan.md` §7.2–§7.3. The run model, spec format, and
artifact layout live in [run-orchestration.md](run-orchestration.md); spec templates live in `../specs/`.

Final numbers come from the large cluster only (plan §5.2). The local stand (OrbStack / kind) is for harness
development and smoke runs — label anything measured there accordingly.

## Stand

One collector replica is the point of a ceiling run. `local` already pins `collector.replicas: 1`; on `cluster`
override it per run:

```bash
cd backend/tools/load-generator/deploy
helmfile -e local apply                                                # local stand
helmfile -e cluster apply --state-values-set profiler.collector.replicas=1   # large cluster
```

Set the k6 deployment's run label and workload through the environment file or `--state-values-set`
(`k6.testid`, `k6.maxVUs`, `k6.workload.*`); the k6 pod restarts with the new env, idle at 0 VUs. The workload map
must match the spec's workload block exactly and the scenario refuses to start on a missing knob — the runner
verifies the fingerprint and the `TESTID` in preflight (doc/run-orchestration.md, "Workload wiring").

To make a smoke run hit backpressure at local load levels, shrink the pending-upload budget so `ingest_paused`
engages early, e.g. add to the profiler values: `PENDING_UPLOAD_MAX_BYTES: "16777216"` (16 MB).

## Port-forwards

The runner takes ready URLs and never manages port-forwards:

```bash
kubectl -n profiler-load port-forward svc/cdt-loader-service 6565:6565 &      # k6 REST API
kubectl -n monitoring    port-forward svc/vmsingle-k8s 8429:8429 &            # VictoriaMetrics
kubectl -n profiler-load port-forward profiler-backend-collector-0 8081:8081 &  # /metrics + /debug/pprof
```

## T2: throughput ceiling

Sweep one axis at a time (plan §7.2), one spec per sweep. `specs/t2-bytes.yaml` is the bytes/s template; derive the
other axes from it by changing only the workload block:

- **calls/s, small calls**: keep `PODS_PER_VU: 1`, raise `CALLS_PER_SEC`, set `SQL_SHARE: 0`, `XML_SHARE: 0`,
  `STACK_DEPTH: 3`.
- **calls/s, large calls**: raise `SQL_SHARE` / `XML_SHARE` / `XML_BYTES` instead.
- **dictionary churn**: raise `DICT_GROWTH_PER_MIN` (drives dictionary stream and collector RAM).

```bash
cd backend/tools/load-generator
cp specs/t2-bytes.yaml /tmp/run.yaml   # set testid, images (digests), values snapshot, levels
go run ./runner -spec /tmp/run.yaml
```

The run directory (`runs/<ts>-t2-bytes/`) holds the frozen spec, `steps.jsonl`, the exported series, the CPU/heap/
goroutine profiles at 70% and 100% of the ceiling, and `result.json` with the ceiling level and the firing detector.
Convert the ceiling level to MB/s and calls/s with the `ingest-bytes` measurement of the last `ok` step.

## T3: connection ceiling

`specs/t3-connections.yaml`: fleets of 100 idle pods per VU, `THREADS_PER_POD: 0`. The confirm phase waits for
`profiler_ingest_active_connections` to reach `level × 100`, so a step's hold never starts before the collector
actually carries the connections.

The idle profile is not empty. Every pod runs the handshake, opens seven streams, and sends its initial dictionary
and params once — with `DICT_INITIAL: 100` that is a fixed, known setup cost — then settles into keep-alive flush
cycles (six `REQUEST_ACK_FLUSH` per 5 s per pod). Separate the two phases when reading RAM numbers: the per-step RSS
delta right after confirm contains the setup burst; the plateau value is the steady per-connection cost.

Per-connection cost at each step, from `steps.jsonl` measurements:

- RAM: Δ`collector-rss` / Δ`active-connections` between consecutive `ok` steps;
- goroutines: Δ`collector-goroutines` / Δ`active-connections`;
- per pod-restart: `pod-restarts` grows with every reconnect — correlate with `inram-bytes`.

**Failure shape.** There is no accept-side connection cap today (`libs/server/services.go`); expected bite points are
RAM (`PROFILER_MEM_BUDGET` pressure — watch `mem-budget` vs `inram-bytes`), the fd limit (`open-fds` against the pod's
ulimit; exhaustion surfaces as accept errors in the collector log and climbing `tcp-connect-p95`), and accept latency
(`session-ready-degraded` detector). Record which one fires first and what the agent side sees — that is a report
deliverable, not something to fix mid-run.

## Generator guard (plan §10)

Every step records the k6 pod's CPU against its limit; a step past `guard.generator-cpu.maxShare` (70%) is `invalid`
and stops the run — the harness would be measuring the generator, not the collector. On the large cluster also pin
the runner to dedicated nodes and size it with several CPUs before trusting any ceiling.

## Checklist per run

1. Fresh `testid`; k6 deployment env matches the spec's workload block.
2. Images pinned by digest in the spec; Helm values snapshot saved next to it.
3. Port-forwards up; `go run ./runner -spec ...`.
4. After the run: `result.json` verdict, `steps.jsonl` sanity (confirmed timestamps, generator shares), dashboards
   (`k6-load-generator`, `profiler-ingest`, `profiler-backpressure`, `profiler-pipeline`) over the run window.
