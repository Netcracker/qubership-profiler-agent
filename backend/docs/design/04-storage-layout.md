# 04 — Storage layout and k8s manifests

> Status: **draft**, awaiting review. k8s manifests, PVC templates, headless service, Helm structure and values diff against the current `backend/charts/profiler-stack/`. Tied closely to `03-lifecycle.md` (probe wiring, termination grace) and `01-write-contract.md` §8 (on-disk layout).

## 1. Scope

This document is the operational shape:

- One Docker image, one binary, four subcommands (`profiler-plan.md`, single-binary decision).
- Three k8s workloads: collector (StatefulSet), query (Deployment), maintain (Deployment or CronJob).
- Three services: collector-headless (no ClusterIP), query (ClusterIP), and an optional ingress.
- One umbrella Helm chart (`profiler-stack`) wires the sub-charts.

Out of scope here: monitoring/Grafana dashboards, network policies, service mesh integration. Touched only where the manifest needs a hook.

## 2. Single container image

```
FROM gcr.io/distroless/static-debian12
COPY profiler-backend /usr/local/bin/profiler-backend
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/profiler-backend"]
```

One image; mode is the first positional arg passed by the manifest. Image tag follows semver (e.g. `profiler-backend:1.2.3`), with `:latest` reserved for CI/dev builds and never used in production manifests.

The same image runs `collect`, `query`, `maintain`, and `all`. Distroless + static binary keeps the image under 50 MB.

## 3. `collect` workload — StatefulSet + Headless Service + PVC template

### 3.1 Why StatefulSet (not Deployment)

- **Per-replica RWO PV** — each collector needs its own private PV for the dictionary/calls WAL, trace segments, sealed parquet, and `metadata.sqlite` (`01-write-contract.md` §8). `volumeClaimTemplates` on a StatefulSet provisions one PVC per pod ordinal; Deployment cannot do this.
- **Stable DNS identity** — each pod gets `collector-0`, `collector-1`, … with a stable A-record under the headless service. `<replica>` is encoded in the parquet object key (`01-write-contract.md` §7), so pod identity must be stable across restarts.
- **Ordered rollout** — by default, StatefulSet rolls pods sequentially, which means at most one collector is in the `LOADING/RECOVERY` state at a time. Cluster never loses more than one replica's worth of hot data simultaneously.

### 3.2 StatefulSet manifest sketch

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: profiler-collector
spec:
  replicas: 2
  podManagementPolicy: OrderedReady     # default; explicit for clarity
  serviceName: profiler-collector-headless
  selector:
    matchLabels:
      app: profiler-collector
  template:
    metadata:
      labels:
        app: profiler-collector
    spec:
      terminationGracePeriodSeconds: 100  # >= shutdown budget in 03-lifecycle.md §5.4
      containers:
        - name: profiler-backend
          image: profiler-backend:1.0.0
          args: ["collect"]
          ports:
            - name: agent
              containerPort: 1715
              protocol: TCP
            - name: internal
              containerPort: 8081
              protocol: TCP
          env:
            - name: PROFILER_DATA_DIR
              value: /data
            - name: PROFILER_AGENT_PORT
              value: "1715"
            - name: PROFILER_INTERNAL_API_PORT
              value: "8081"
            - name: PROFILER_HOT_RETENTION
              value: "15m"
            - name: STATEFULSET_ORDINAL
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name      # "profiler-collector-0", parsed by binary
            - name: S3_ENDPOINT
              valueFrom:
                configMapKeyRef: { name: profiler-backend-config, key: s3.endpoint }
            - name: S3_BUCKET
              valueFrom:
                configMapKeyRef: { name: profiler-backend-config, key: s3.bucket }
            # S3 credentials are read from the mounted Secret volume, not from
            # env (01-write-contract.md §9): the values never appear in the pod
            # spec, and a Secret rotation updates the mounted files in place.
            - name: S3_ACCESS_KEY_FILE
              value: /etc/profiler/s3/access-key
            - name: S3_SECRET_KEY_FILE
              value: /etc/profiler/s3/secret-key
            # full env catalog: 03-lifecycle.md §10 + 01-write-contract.md §9
          readinessProbe:
            httpGet:
              path: /internal/v1/health/ready
              port: internal
            initialDelaySeconds: 5
            periodSeconds: 5
            failureThreshold: 3
          # Liveness is recovery-independent: /health/live answers 200 through
          # LOADING and RECOVERY and fails only on FATAL (03-lifecycle.md §4).
          # Gating liveness on READY would kill the pod mid-recovery and loop
          # it forever; only readiness waits for READY.
          livenessProbe:
            httpGet:
              path: /internal/v1/health/live
              port: internal
            initialDelaySeconds: 30
            periodSeconds: 30
            failureThreshold: 5
          volumeMounts:
            - name: data
              mountPath: /data
            - name: s3-credentials
              mountPath: /etc/profiler/s3
              readOnly: true
          resources:
            requests:
              cpu: "500m"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
      volumes:
        - name: s3-credentials
          secret:
            secretName: profiler-backend-s3
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ReadWriteOnce]      # RWO is the whole point
        storageClassName: ""              # default StorageClass; override in values
        resources:
          requests:
            storage: 20Gi
```

### 3.3 Headless Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: profiler-collector-headless
spec:
  clusterIP: None                          # required for headless
  selector:
    app: profiler-collector
  ports:
    - name: agent
      port: 1715
      targetPort: agent
    - name: internal
      port: 8081
      targetPort: internal
  publishNotReadyAddresses: false          # default; only Ready pods in DNS
```

DNS resolution: `profiler-collector-headless.<ns>.svc.cluster.local` returns one A-record per Ready pod (`02-read-contract.md` §7.1, `03-lifecycle.md` §4).

**`publishNotReadyAddresses: false`** is the default. This means a pod in `LOADING`, `RECOVERY`, `DRAINING`, or `TERMINATING` does NOT appear in DNS, so `query` won't fan out to it.

### 3.4 Sticky TCP routing

Headless services don't load-balance — they return DNS records and the client (agent) picks one. Sticky-per-TCP-connection is implicit: all streams over one TCP connection land on whichever pod the agent dialed. (One TCP conn ↔ one pod-restart, per `01-write-contract.md` §1 V6.)

Agents do not need to be aware of the topology. The collector binary stamps `restart_time_ms` at TCP accept (`01-write-contract.md` §3.4), which is the only thing the data model needs.

### 3.5 PVC sizing

Default: `20Gi` per replica. The trace segments dominate; the seal-model metadata (`calls.wal` + `metadata.sqlite`) is a distant second. Figures below assume a per-replica ingest rate `r` (with `R` replicas sharing the 100 K calls/s target, `r = 100000 / R`) and ~250 B per Call record.

- `dictionary.wal` + `params.wal` + `suspend.wal` — tens of MB typically; dominated by long-running pod-restarts.
- Trace segments — the largest component, capped by `PROFILER_CHUNKS_STAGING_MAX_BYTES` (default `10GB`, `01-write-contract.md` §9).
- `calls.wal` — full Call records for the un-sealed window (≈ one bucket plus grace, ~6 min): `r × 360 s × 250 B`. At `r = 25 K/s` (R = 4) ≈ 2.2 GB; at `r = 50 K/s` (R = 2) ≈ 4.5 GB.
- `metadata.sqlite` — the call index. Calls drop out of the hot index at seal and are then served from the sealed local parquet (`02-read-contract.md` §3), so the index spans ~6 min, not the full `hot_retention`: ≈ `r × 360 s × 150 B` including secondary indexes. At `r = 25 K/s` ≈ 2 GB; at `r = 50 K/s` ≈ 4 GB. (Keeping the index for the whole 15 min hot window instead is roughly 2.5× this.)
- Seal scratch (`parquet-sealing/`) — one bucket's parquet in flight; transient, cleared on each seal.
- Hot-retention parquet — bounded by `(uploaded parquet rate) × HOT_RETENTION` ≈ 3 × `parquet_max_size` per retention class ≈ 1 GB.

Fit against `20Gi`: at `r = 25 K/s` (R = 4) the sum is ≈ 10 + 2.2 + 2 + 1 ≈ 15 GB — comfortable. At `r = 50 K/s` (R = 2) it is ≈ 10 + 4.5 + 4 + 1 ≈ 20 GB — at the edge. So the 100 K/s target wants **R ≥ 3–4 collectors**, or a larger PVC or a smaller segment cap.

**The PVC must exceed the segment budget.** `PROFILER_CHUNKS_STAGING_MAX_BYTES` bounds only the trace/`sql`/`xml` segment files (`01-write-contract.md` §4.6); the WALs, the call-index SQLite files, the seal scratch, and the hot-retention parquet listed above share the same PV and are outside the budget. A PVC sized equal to the budget fills up with the unbudgeted components and hits `ENOSPC` before the eviction path ever engages. Size the PVC as segment budget + headroom for the rest (the `20Gi` default carries a `10GB` budget). The RAM budget (`PROFILER_MEM_BUDGET`, `01-write-contract.md` §4.6) is a separate lever: `chunk_index` and seal-pass buffers are bounded by eviction, not by this PVC. Operators override per-environment via Helm values.

## 4. `query` workload — Deployment + ClusterIP

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: profiler-query
spec:
  replicas: 2
  selector:
    matchLabels:
      app: profiler-query
  template:
    metadata:
      labels:
        app: profiler-query
    spec:
      terminationGracePeriodSeconds: 45  # shutdown budget 03-lifecycle.md §7.3
      containers:
        - name: profiler-backend
          image: profiler-backend:1.0.0
          args: ["query"]
          ports:
            - name: external
              containerPort: 8080
              protocol: TCP
          env:
            - name: PROFILER_EXTERNAL_API_PORT
              value: "8080"
            - name: COLLECTOR_HEADLESS_SVC
              value: "profiler-collector-headless"
            - name: PROFILER_OVERLAP_MARGIN
              value: "5m"
            - name: PROFILER_FANOUT_TIMEOUT
              value: "2s"
            - name: S3_ENDPOINT
              valueFrom:
                configMapKeyRef: { name: profiler-backend-config, key: s3.endpoint }
            - name: S3_BUCKET
              valueFrom:
                configMapKeyRef: { name: profiler-backend-config, key: s3.bucket }
            # ... S3 creds from the mounted Secret volume, same as collector (§3.2, §6)
          readinessProbe:
            httpGet: { path: /api/v1/health/ready, port: external }
            initialDelaySeconds: 2
            periodSeconds: 5
          livenessProbe:
            httpGet: { path: /api/v1/health/live, port: external }
            initialDelaySeconds: 10
          resources:
            requests:
              cpu: "200m"
              memory: "512Mi"
            limits:
              cpu: "1"
              memory: "2Gi"
---
apiVersion: v1
kind: Service
metadata:
  name: profiler-query
spec:
  type: ClusterIP
  selector:
    app: profiler-query
  ports:
    - name: external
      port: 8080
      targetPort: external
```

Stateless. No PVC. Scales horizontally on CPU; HPA can be added later (out of scope MVP).

External exposure (UI, MCP, CLI) is via Ingress / Route / LoadBalancer per cluster convention — left to operator values.

## 5. `maintain` workload

Both modes from `03-lifecycle.md` §8 ship. Operator picks via Helm values.

### 5.1 Cron mode (long-running Deployment)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: profiler-maintain
spec:
  replicas: 1
  selector:
    matchLabels: { app: profiler-maintain }
  template:
    metadata:
      labels: { app: profiler-maintain }
    spec:
      containers:
        - name: profiler-backend
          image: profiler-backend:1.0.0
          args: ["maintain", "--cron"]
          env:
            - name: PROFILER_MAINTAIN_INTERVAL
              value: "1h"
            # S3 creds from the mounted Secret volume, same as collector (§3.2, §6)
          resources:
            requests: { cpu: "100m", memory: "256Mi" }
            limits:   { cpu: "1",    memory: "1Gi" }
```

One replica is enough — retention sweeps and S3 LIST scans are not parallelizable in the MVP.

### 5.2 One-shot mode (k8s CronJob)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: profiler-maintain
spec:
  schedule: "0 * * * *"      # hourly
  concurrencyPolicy: Forbid  # never overlap with the previous run
  jobTemplate:
    spec:
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: profiler-backend
              image: profiler-backend:1.0.0
              args: ["maintain", "--run-now"]
              env: # ... same as cron mode
```

Helm values flag picks between the two: `maintain.mode: cron | cronjob`. Default: `cronjob` (lighter-weight, one pod per execution; Deployment mode is mostly for environments that don't support `batch/v1` properly).

## 6. ConfigMap and Secret

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: profiler-backend-config
data:
  s3.endpoint: "https://minio.profiler.svc.cluster.local:9000"
  s3.bucket:   "profiler-data"
  s3.path-prefix: "parquet/v1"
---
apiVersion: v1
kind: Secret
metadata:
  name: profiler-backend-s3
type: Opaque
stringData:
  access-key: "..."
  secret-key: "..."
```

Both shared across `collect`, `query`, and `maintain` workloads.

**The Secret is consumed as a volume, never as env.** Every workload mounts `profiler-backend-s3` at `/etc/profiler/s3` and points `S3_ACCESS_KEY_FILE` / `S3_SECRET_KEY_FILE` at the mounted files (`01-write-contract.md` §9). This keeps the values out of pod specs, `kubectl describe`, and crash dumps of the env block, and a Secret rotation propagates to the mounted files without re-rendering the workload. (The process reads the files at startup; picking up a rotation without a restart is a future improvement.)

## 7. RBAC

The collector, query, and maintain pods need no RBAC permissions in the MVP — they don't query the k8s API. DNS resolution is automatic via CoreDNS.

If `/internal/v1/pods` targeting (`02-read-contract.md` §7.3) is activated later and replaced with k8s API-based `EndpointSlice` watch for sub-second discovery, RBAC will need a small role bound to query: `get`/`list`/`watch` on `endpointslices.discovery.k8s.io` scoped to the collector's namespace. Tracked in `02-read-contract.md` §7.3.

## 8. Resource defaults (summary)

| Workload | Replicas | CPU req | CPU lim | Mem req | Mem lim | PVC |
|---|---|---|---|---|---|---|
| `collect` (StatefulSet) | 2 | 500m | 2 | 2 Gi | 4 Gi | 20 Gi RWO |
| `query` (Deployment) | 2 | 200m | 1 | 512 Mi | 2 Gi | — |
| `maintain` (Deployment cron) | 1 | 100m | 1 | 256 Mi | 1 Gi | — |
| `maintain` (CronJob) | per-run | 100m | 1 | 256 Mi | 1 Gi | — |

Operators override per-environment. Numbers are baseline for typical workloads (~10 instrumented pods, ~100 root calls/sec total).

## 9. Helm chart structure

**Stage 1 ships a new self-contained chart, `backend/charts/profiler-backend/`,** holding the collector StatefulSet, the query Deployment, the maintain workload, the shared ConfigMap/Secret, an optional in-cluster MinIO for dev/smoke, and the monitoring objects (ServiceMonitor / PrometheusRule). The legacy sub-charts below keep serving the Java stack untouched until their Stage 4/5 retirement — the same reasoning that placed the Go binary at `apps/profiler-backend` instead of over the legacy `apps/` paths. The table records the eventual end-state for the legacy charts, not Stage 1 work:

| Chart | Old shape | New shape (this contract) |
|---|---|---|
| `collector/` | Quarkus Deployment, ClusterIP, no PVC | Go StatefulSet + Headless Service + RWO PVC template |
| `query/` | (TS/React build assets only) | Go Deployment + ClusterIP Service |
| `compactor/` | (legacy) | **deprecated**, removed after Stage 4 |
| `maintenance/` | (legacy Go maintenance) | Repurposed for new retention/compaction model (Deployment cron OR CronJob) |
| `migration/` | Postgres schema migration | **removed**; no Postgres in the new architecture |
| `dumps-collector/` | (independent app) | **untouched** until Stage C5 |
| `profiler-stack/` (umbrella) | Wires Postgres + MinIO + collector + query + compactor + maintenance | Wires MinIO (optional) + collector + query + maintenance + dumps-collector |

### 9.1 Umbrella values (sketch)

```yaml
# backend/charts/profiler-stack/values.yaml

global:
  image:
    registry: <override>
    repository: profiler-backend
    tag: 1.0.0
  s3:
    endpoint: <required>
    bucket:   <required>
    pathPrefix: parquet/v1
    accessKey: <required>
    secretKey: <required>
  retention:
    hotMinutes: 15
    overlapMarginMinutes: 5
    classes:
      shortClean:  { ttl: 1d }
      normalClean: { ttl: 7d }
      longClean:   { ttl: 30d }
      anyError:    { ttl: 30d }
      corrupted:   { ttl: 7d }
      dictionary:  { ttl: 35d }

collector:
  replicas: 2
  storage:
    size: 20Gi
    storageClassName: ""
  chunksStagingMaxBytes: 10Gi
  resources: { ... }

query:
  replicas: 2
  resources: { ... }

maintain:
  mode: cronjob              # or: cron
  interval: 1h               # for cron mode
  schedule: "0 * * * *"      # for cronjob mode
  resources: { ... }

minio:
  enabled: true              # set false if external S3
  # ... minio chart values
```

## 10. Diff against current `backend/charts/profiler-stack/values.yaml`

| Removed | Reason |
|---|---|
| `INFRA_POSTGRES_*` | No Postgres in the new architecture (`profiler-plan.md`). |
| `pg-*` sub-chart wiring | Same. |
| Quarkus-specific env (`QUARKUS_*`, `JAVA_OPTS`) | Go binary. |
| `compactor` sub-chart values | Sub-chart deprecated after Stage 4. |
| `migration` sub-chart | No DB schema migration in MVP. |

| Added | Reason |
|---|---|
| `global.retention.classes` map | Per-class TTL configuration (`01-write-contract.md` §6.4). |
| `global.retention.hotMinutes` / `overlapMarginMinutes` | Hot/cold tuning (`02-read-contract.md` §4). |
| `collector.storage.size` | PVC sizing (§3.5). |
| `collector.chunksStagingMaxBytes` | Per `01-write-contract.md` §4.6. |
| `maintain.mode` | Toggle between cron Deployment and CronJob. |

| Modified | Reason |
|---|---|
| `collector` chart from Deployment to StatefulSet | §3.1. |
| `collector` Service from ClusterIP to Headless | §3.3. |
| Probe paths from `/q/health/*` (Quarkus) to `/internal/v1/health/*` | `03-lifecycle.md` §4. |
| `query` chart from a static-asset bundle to a Go Deployment | New backend service replaces UI-only build. |

## 11. Environment-specific overrides

### 11.1 Dev: docker-compose (single-node)

The `all` subcommand runs all three workloads in one process; dev `docker-compose.yaml` mounts a local directory at `/data` and either runs MinIO as a sidecar or uses the filesystem-S3 emulator (`backend/libs/s3/`, deferred — see `01-write-contract.md` decisions). Single Go binary, no k8s, no PVCs.

### 11.2 Dev: kind / minikube (k8s-flavored dev)

Run the full umbrella chart with `collector.replicas: 1`, `query.replicas: 1`, `minio.enabled: true`, default PVC size 5Gi. `PROFILER_ALLOW_K8S_ALL_MODE` stays `false`; even dev k8s uses the three-workload split to exercise the real lifecycle.

### 11.3 Production

`collector.replicas: 2+` (sized to expected agent fan-in), MinIO either internal (`minio.enabled: true`) or external (`minio.enabled: false`, S3 endpoint set explicitly).

## 12. What this contract does NOT cover

- **Ingress / external routing** to `/api/v1` — varies per cluster (Ingress controller, OpenShift Route, AWS ALB, etc.). The Helm chart exposes an `ingress.enabled / className / host` block; specific values are operator-supplied.
- **Grafana dashboards.** The binary exposes `/metrics` (Prometheus format): `collect` on the internal port, `query` on the external port (it has no internal one), `maintain` on its metrics port in loop mode. The `profiler-backend` chart ships ServiceMonitor/PodMonitor objects and a PrometheusRule with the baseline alerts; dashboards land later.
- **Network policies** — supplied per-cluster.
- **Service mesh** integration (Istio sidecar, Linkerd) — orthogonal to this contract; sidecars work as long as they don't terminate the agent's TCP stream.
- **Backup of `metadata.sqlite`** — not needed: the database is rebuildable from PV contents (`03-lifecycle.md` §3.2).
- **Image registry policy** — operator-supplied via `global.image.registry`.

## 13. Review checklist

- [x] PVC default size `20Gi` — accepted.
- [x] Collector `replicas: 2` default — accepted.
- [x] `terminationGracePeriodSeconds: 100` for collector — accepted; matches `03-lifecycle.md` §5.4 budget.
- [x] Probe paths (`/internal/v1/health/{ready,live}` for `collect`; `/api/v1/health/{ready,live}` for `query`) — accepted.
- [x] Helm sub-chart deprecation plan (`compactor/`, `migration/`) — kept in `profiler-stack` umbrella with `enabled: false` default; removed in Stage 4 cleanup.
- [x] `maintain.mode: cronjob` as default — accepted.
- [x] Resource defaults (§8) — accepted as starting baseline; per-environment overrides via Helm values.
- [x] `MinIO` chart enabled by default — accepted.
