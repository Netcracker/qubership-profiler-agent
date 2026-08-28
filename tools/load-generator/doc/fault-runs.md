# Fault runs (T5, T7)

Runbook for the crashloop and fault-injection tests of `load-testing-plan.md` §7.5 and §7.7. The fault-layer
contract — spec format, event log, durable revert, stand lock — lives in
[run-orchestration.md](run-orchestration.md) ("Fault injection"); the checker's expected-failure allowances live in
[checker.md](checker.md). Spec templates live in `../specs/`.

All T5/T7 runs are fixed-hold runs on the accelerated-timer base (`local-soak` or `local-faults` environment):
the shrunken lifecycle timers let one run cycle WAL purge, compaction, and TTL deletion through the fault and its
recovery. Where a conclusion depends on the production-scale timer (the 1 h `WAL_PURGE_GRACE` backlog bound), the
report says so explicitly.

## Stands

- **T5 (reconnect storm, restart, crashloop)** and **T7 S3-outage / small-PV** run on `local-soak` — no topology
  change, faults are pod deletes and scale-to-0 through the k8s API.
- **T7 S3-slow and agent-network faults** run on `local-faults`: toxiproxy fronts MinIO (proxy `s3`, :19000) and
  the collector's agent port (proxy `agent`, :11715), and the environment reroutes `S3_ENDPOINT` and the k6
  `COLLECTOR_HOST` through it. Ordinary runs keep the direct topology — never leave a proxy in the path of a
  non-fault run.

```bash
cd backend/tools/load-generator/deploy
helmfile -e local-soak apply     # T5, t7-s3-outage, t7-small-pv
helmfile -e local-faults apply   # t7-s3-slow, t7-agent-net
```

## Port-forwards

The ceiling-run set (`ceiling-runs.md`) plus, for toxics runs, the toxiproxy API:

```bash
kubectl -n profiler-load port-forward svc/toxiproxy 8474:8474 &
```

The runner needs kube access for `pod-delete` and `scale` (kubeconfig or in-cluster, same resolution as the
checker).

## Running

**Start from a steady baseline.** A stand redeploy (collector env change, topology switch) rolls every collector
through WAL recovery, and the re-seal burst that follows looks exactly like backlog growth to the run's own
detectors — a t7-s3-slow attempt died on `pending-parquet-growth` twelve minutes into its baseline, before the
toxics even engaged. After any redeploy, wait until `pending_parquet_bytes` is back to its near-zero sawtooth and
the WAL band is flat before starting the runner; the drain typically takes 5–15 minutes on the accelerated
timers.

1. Deploy the stand, set a fresh `k6.testid`, and start the checker with every source enabled plus the fault log
   and the target-to-pod mapping (the scrape-gap allowance is scoped to mapped targets only, `checker.md`):

   ```bash
   go run ./checker ... \
     -faults-log runs/<run-dir>/faults.jsonl \
     -target-pods http://localhost:8081/metrics=profiler-backend-collector-0,http://localhost:8082/metrics=profiler-backend-collector-1,http://localhost:8083/metrics=profiler-backend-collector-2
   ```

   The run directory name is printed by the runner at start; start the checker right after (its warm-up covers
   the gap — no fault fires before the first `at`).

2. Start the runner with the fault spec. Preflight acquires the stand lock, reverts anything a dead run left
   (`runs/.active-faults/`), and verifies the workload fingerprint before the first scale.

3. After the run: `result.json` verdict, `faults.jsonl` timeline (per-injection `readyAt` for crashloops),
   `steps.jsonl`, the checker's expected/unexpected report, dashboards over the fault windows.

A crashed or SIGKILL-ed runner cannot leave the stand faulted: the next preflight (or a manual
`go run ./runner -revert-faults`) restores scaled-down replicas and deletes leftover toxics, skipping state owned
by a live stand lease.

## What each spec probes

- `t5-reconnect-storm.yaml` — generator-side churn (vdumper `CHURN_INTERVAL`), no faults block: pod-restart
  accumulation on the PV, `store_pods_size` / RAM growth, WAL purge under the accelerated grace, small-file
  production in S3.
- `t5-restart.yaml` — one collector kill mid-ingest: WAL recovery (from the collector log), time to READY
  (`faults.jsonl` `ready` event), agent failover (`k6_vdumper_reconnects_total`), loss accounting
  (`refused_bytes` vs the unacked window), query `partial_reasons` during the outage.
- `t5-crashloop.yaml` — ready-gated repeated kills: the `readyAt − at` series must not grow cycle over cycle,
  no orphan parquet/WAL after stabilization, the startup gate keeps the agent port closed until READY
  (k6 `tcp_connect` errors, not accepted-then-stalled sessions).
- `t7-s3-outage.yaml` — MinIO scaled to 0: the full backpressure chain in order
  (`pending_parquet_bytes` → `SealPaused` at budget/2 → `IngestPaused` at the budget → agent `ACK_ERROR` →
  reconnect loop), then drain after revert; losses reconcile with `ingest_refused_bytes_total`. The spec pins the
  shrunken `PENDING_UPLOAD_MAX_BYTES`, the measured ingest rate, and the computed gate deadlines — sizing is
  decided before the run, not during it.
- `t7-s3-slow.yaml` — latency + bandwidth toxics on the `s3` proxy sized so the drain rate falls below the
  measured accumulation rate: slow backlog growth, pinned upload workers (no per-PUT timeout — a recorded
  finding), gates engaging late.
- `t7-small-pv.yaml` — a deploy-time variant, not an injection: the PV (or `CHUNKS_STAGING_MAX_BYTES`) sized
  below the segment budget; class-aware janitor eviction (`janitor_segments_evicted_total`,
  `seal_truncated_rows_total{reason="disk_budget"}`) and the ENOSPC path (failed stream → reconnect; there is no
  reactive ENOSPC handling).
- `t7-agent-net.yaml` — toxics on the `agent` proxy: latency against the 40 s read deadline, stalls against the
  2 s write deadline, `reset_peer` storms, and the reconnect storm when a toxic lifts. Packet loss needs netem
  and stays cluster-pending (the parked `chaos-mesh` release).
