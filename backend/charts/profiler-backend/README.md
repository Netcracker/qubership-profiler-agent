# profiler-backend Helm chart

Deploys the Go profiler backend (`backend/docs/design/04-storage-layout.md`):

- **collector** — StatefulSet with one RWO PVC per replica (`volumeClaimTemplates`), a governing headless Service for query fan-out, and an L4 Service for agent TCP (port 1715).
- **query** — stateless Deployment + ClusterIP Service (`/api/v1`, port 8080).
- **maintain** — CronJob (default) or a singleton Deployment loop.
- **minio** (optional) — in-cluster dev MinIO for smoke runs; storage is an emptyDir.
- **monitoring** (optional) — ServiceMonitor/PodMonitor objects and a PrometheusRule with the baseline alerts.

All three workloads read the S3 credentials from a Secret **mounted as a volume** (`S3_ACCESS_KEY_FILE` / `S3_SECRET_KEY_FILE`); the values never appear in pod specs or env blocks (`01-write-contract.md` §9, `04` §6).

## Quick start (kind)

```bash
cd backend
docker build -f apps/profiler-backend/Dockerfile -t profiler-backend:dev .
kind create cluster --name profiler
kind load docker-image profiler-backend:dev --name profiler
helm install profiler charts/profiler-backend -f deploy/values-kind.yaml
kubectl rollout status statefulset/profiler-profiler-backend-collector --timeout=180s
```

`make kind-smoke` automates the same flow end to end: build, load, install, wait for READY, then run the in-cluster smoke (agent emulator → collector → seal → MinIO → `query /calls` + `/tree`, a cold-only phase with the collector scaled to zero, and recovery).

## Quick start (OrbStack)

OrbStack's k8s shares the host Docker daemon — no image loading step — and ships a built-in LoadBalancer, so the agent Service gets a host-reachable IP:

```bash
cd backend
docker build -f apps/profiler-backend/Dockerfile -t profiler-backend:dev .
kubectl config use-context orbstack
helm install profiler charts/profiler-backend -f deploy/values-orbstack.yaml
kubectl rollout status statefulset/profiler-profiler-backend-collector --timeout=180s
kubectl get svc profiler-profiler-backend-collector-agent   # EXTERNAL-IP serves agent TCP :1715
```

The same smoke runs against it: `make kind-smoke KIND_CONTEXT=orbstack KIND_SKIP_LOAD=1`.

Point a real agent at the LoadBalancer IP (`-Dprofiler.host=<EXTERNAL-IP>`), or port-forward the query Service for the API:

```bash
kubectl port-forward svc/profiler-profiler-backend-query 8080:8080
curl "localhost:8080/api/v1/calls?from=...&to=..."
```

## Values

| Key | Default | Notes |
|---|---|---|
| `image.registry` / `image.repository` / `image.tag` | `"" / profiler-backend / dev` | One image, one binary; the subcommand is the container arg. |
| `s3.endpoint` | `""` | Required unless `minio.enabled`; `https://` enables TLS. |
| `s3.bucket` | `profiler-data` | Created by the services on first connect. |
| `s3.auth.existingSecret` | `""` | Secret with keys `access-key` / `secret-key`; wins over the inline pair. Production should use this. |
| `s3.auth.accessKey` / `secretKey` | `""` | Renders a chart-managed Secret — dev/smoke only. |
| `s3.auth.mountPath` | `/etc/profiler/s3` | Where every workload mounts the Secret. |
| `s3.tls.caCert` | `""` | PEM CA bundle for a private/internal CA on the S3 endpoint. Renders a chart-managed Secret — dev/smoke only. |
| `s3.tls.existingSecret` | `""` | Secret with the CA bundle under key `ca.crt`; wins over the inline `caCert`. Production should use this. |
| `s3.tls.insecureSkipVerify` | `false` | Skips TLS certificate verification for the S3 endpoint entirely. Dev/smoke only — never set in production. |
| `collector.replicas` | `2` | Scale to the expected agent fan-in (04 §3.5). |
| `collector.storage.size` | `20Gi` | **Must exceed `chunksStagingMaxBytes`** — the budget bounds only segment files; WALs, SQLite partitions, seal scratch, and hot-retention parquet share the PV outside it (04 §3.5). |
| `collector.storage.storageClassName` | `""` | Empty = the cluster's default class (kind: local-path; OrbStack: its own). The field is omitted, not set to `""`. |
| `collector.chunksStagingMaxBytes` | `10GB` | `PROFILER_CHUNKS_STAGING_MAX_BYTES` (01 §4.6). |
| `collector.hotRetention` | `15m` | `PROFILER_HOT_RETENTION` (01 §6.3). |
| `collector.agentService.type` | `ClusterIP` | `NodePort` / `LoadBalancer` (OrbStack) for host-reachable agents. |
| `collector.terminationGracePeriodSeconds` | `100` | ≥ the 03 §5.4 shutdown budget. |
| `collector.env`, `query.env`, `maintain.env` | `{}` | Extra env (name → value) for the remaining `PROFILER_*` knobs. |
| `query.replicas` / `query.port` | `2` / `8080` | Stateless; scale horizontally. |
| `maintain.mode` | `cronjob` | `deployment` runs the singleton loop — the only mode with `/metrics`. |
| `maintain.schedule` | `0 * * * *` | CronJob mode. |
| `retention.*TTL` | contract defaults | Per-class TTLs (01 §6.4) passed to maintain. |
| `minio.enabled` | `false` | Dev/smoke MinIO (emptyDir storage, same credentials Secret). |
| `metrics.serviceMonitor.enabled` | `false` | Requires the prometheus-operator CRDs. |
| `metrics.prometheusRule.enabled` | `false` | Ships `files/prometheus-rules.yaml` as a PrometheusRule. |

## Metrics contract

Series names are stable — dashboards and the shipped alerts reference them; renames are breaking changes (a Go test, `pkg/metrics/collect_test.go`, pins them). Labels stay low-cardinality: `reason`, `kind`, `layer`, `result` — never pod, PK, or replica.

`collect` serves `/metrics` on the internal port (`:8081`), scrapable through LOADING/RECOVERY; `query` on the external port (`:8080`); `maintain` on `PROFILER_METRICS_PORT` in deployment mode (CronJob pods exit too fast to scrape).

| Series | Type | Meaning |
|---|---|---|
| `profiler_seal_rows_total` | counter | Calls sealed into parquet rows. |
| `profiler_seal_files_total` | counter | Parquet files produced by seal passes. |
| `profiler_seal_truncated_rows_total{reason}` | counter | Rows sealed with a NULL blob: `dict_miss`, `disk_budget`, `idle_timeout`, `mem_pressure`. |
| `profiler_upload_uploaded_files_total` | counter | Parquet files confirmed in S3. |
| `profiler_upload_put_failures_total` | counter | Failed S3 PUT attempts (feeds the upload-failure alert). |
| `profiler_upload_retried_puts_total` | counter | PUT attempts a retry followed. |
| `profiler_upload_quarantined_files_total` | counter | Parquet files moved to `upload-failed/`. |
| `profiler_upload_quarantined_objects_total` | counter | Manifest bodies parked under `upload-failed/`. |
| `profiler_upload_manifest_puts_total` | counter | `pods/v1` manifest upserts. |
| `profiler_upload_swept_segments_total` | counter | Refcount-0 segments unlinked after upload. |
| `profiler_janitor_parquet_deleted_total` | counter | Aged local parquet deleted past hot retention. |
| `profiler_janitor_partitions_dropped_total` | counter | Call-index partitions dropped from the hot tier. |
| `profiler_janitor_wals_purged_total` | counter | Pod-restarts whose WALs were purged. |
| `profiler_janitor_segments_evicted_total` | counter | Segments evicted under the disk budget. |
| `profiler_janitor_evicted_bytes_total` | counter | Bytes freed by evictions. |
| `profiler_hotstore_segments_disk_bytes` | gauge | Segment bytes on disk (measured each janitor pass). |
| `profiler_hotstore_segments_disk_budget_bytes` | gauge | The configured budget. |
| `profiler_hotstore_hot_window_lag_seconds` | gauge | Age of the oldest hot-index row; sustained growth = stuck hot→cold handoff. |
| `profiler_hotstore_quarantine_objects{kind}` | gauge | Stuck quarantined objects: `parquet`, `snapshot`. Shrinks only manually. |
| `profiler_hotstore_quarantine_oldest_age_seconds{kind}` | gauge | Age of the oldest quarantined object. |
| `profiler_hotstore_evicted_segment_chunk_refs` | gauge | In-RAM chunk refs pointing at evicted segments (risk B-3). |
| `profiler_query_fanout_replica_request_seconds{result}` | histogram | Per-replica fan-out round-trip. |
| `profiler_query_cold_lists_total` | counter | S3 LIST requests from cold discovery. |
| `profiler_query_partial_responses_total` | counter | Responses with `partial: true`. |
| `profiler_query_guard_rejections_total{layer}` | counter | Wide-query rejections: `span`, `cost`. |
| `profiler_maintain_passes_total` | counter | Completed maintenance passes. |
| `profiler_maintain_compacted_groups_total` | counter | Fresh compactions written. |
| `profiler_maintain_compacted_input_files_total` | counter | Inputs consumed by compactions. |
| `profiler_maintain_compacted_rows_total` / `deduped_rows_total` | counter | Rows written / duplicate PKs dropped. |
| `profiler_maintain_deleted_input_files_total` | counter | Inputs deleted after the grace. |
| `profiler_maintain_skipped_groups_total{reason}` | counter | `small`, `unsettled`, `oversized`. |
| `profiler_maintain_ttl_deleted_objects_total{kind}` | counter | TTL expiries: `parquet`, `snapshot`. |
| `profiler_maintain_pass_errors_total` | counter | Failures a pass logged and skipped. |

## Alerts

`files/prometheus-rules.yaml` (shipped via the PrometheusRule) carries: `ProfilerStuckQuarantine`, `ProfilerQuarantineAgeHigh`, `ProfilerDiskBudgetNearFull`, `ProfilerHotWindowLagHigh`, `ProfilerUploadFailures`. `make rules-test` runs `promtool test rules` over the same file (`tests/prometheus/rules_test.yaml`), including the forced stuck-quarantine scenario.

## Validation

```bash
make charts-build   # helm lint over every chart
make helm-lint      # helm template (monitoring enabled) | kubeconform -strict
make rules-test     # promtool test rules (docker)
make kind-smoke     # full in-cluster smoke on kind
```
