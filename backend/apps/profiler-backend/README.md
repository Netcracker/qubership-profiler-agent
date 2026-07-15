# profiler-backend

One static Go binary for the new profiler backend, one subcommand per workload
(`docs/design/04-storage-layout.md` §2). The service logic lives in
`libs/collector` and `libs/query`; this app adds env parsing, lifecycle
states, and the process wiring.

| Subcommand | Workload | Composes |
|---|---|---|
| `collect` | Collector write path: agent TCP (`:1715`), seal and upload loops, `/internal/v1` (`:8081`) | `libs/collector` |
| `query` | Read path: `/api/v1` (`:8080`) over the hot fan-out and cold S3 tiers | `libs/query` |
| `maintain` | S3 compaction + per-class retention TTL; loop by default, `--run-now` for a CronJob | `libs/maintain` |

`all` (`03-lifecycle.md` §9) is not implemented yet.

Every subcommand serves Prometheus `/metrics`: `collect` on the internal port
(scrapable through LOADING/RECOVERY), `query` on the external port, `maintain`
on `PROFILER_METRICS_PORT` in loop mode (`--run-now` exits too fast to
scrape). The series names are a stable contract — the catalogue lives in
`charts/profiler-backend/README.md`.

## Quick start (docker-compose)

From `backend/`:

```bash
docker compose up --build -d     # MinIO + collector + query
make smoke                       # end-to-end proof: agent → collector → seal → MinIO → query
```

The services create the `profiler-data` bucket themselves on first connect.

## Configuration

Everything comes from the environment. The authoritative catalogues are
`01-write-contract.md` §9, `02-read-contract.md` §9, and `03-lifecycle.md`
§10; the wiring parses the subset the composed services honour.

### Both subcommands

| Env | Default | Description |
|---|---|---|
| `PROFILER_LOG_LEVEL` | `info` | Log level. |
| `S3_ENDPOINT` | — (required) | S3/MinIO endpoint URL; an `https://` scheme enables TLS. |
| `S3_BUCKET` | — (required) | Target bucket, created if missing. |
| `S3_PATH_PREFIX` | (empty) | Per-deployment key prefix applied to every object the backend writes and reads (01 §7), so several deployments can share one bucket. Set the same value on all subcommands. |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | — | Credentials from the environment (dev, compose). |
| `S3_ACCESS_KEY_FILE` / `S3_SECRET_KEY_FILE` | — | Path to a file holding the credential (k8s mounts the Secret as a volume); trailing whitespace is trimmed. Set exactly one source per credential. |
| `PROFILER_SHUTDOWN_DRAIN_GRACE` | `30s` | DRAINING hold after SIGTERM (`03` §5.1, §7.3). |

### `collect`

| Env | Default | Description |
|---|---|---|
| `PROFILER_DATA_DIR` | `/data` | PV root; taken exclusively via `collector.lock`. |
| `PROFILER_AGENT_PORT` | `1715` | Agent TCP listener. |
| `PROFILER_INTERNAL_API_PORT` | `8081` | `/internal/v1` and the health probes. |
| `PROFILER_TIME_BUCKET` | `5m` | Parquet time bucket length. |
| `PROFILER_TIME_BUCKET_GRACE` | `30s` | Wait after bucket end before sealing. |
| `PROFILER_DICT_FSYNC_RECORDS` | `256` | Dictionary WAL fsync trigger by record count. |
| `PROFILER_DICT_FSYNC_INTERVAL` | `100ms` | Dictionary WAL fsync trigger by time. |
| `PROFILER_DURATION_THRESHOLDS` | `100ms,1s,10s` | Clean-tier boundaries of the retention tier table (01 §6.4). Unset keeps the table defaults; set the same value on `query`. |
| `PROFILER_SEGMENT_ROTATION_SIZE` | `4MB` | Segment size requested from the agent. |
| `PROFILER_SEAL_CHECK_INTERVAL` | `15s` | Seal-loop poll cadence (implementation knob). |
| `PROFILER_UPLOAD_CHECK_INTERVAL` | `30s` | Upload-loop poll cadence (implementation knob). |
| `STATEFULSET_ORDINAL` | `$HOSTNAME` | Replica name in sealed-file names and S3 keys. |

Not wired yet (their features belong to later Stage 1 tasks):
`PROFILER_PARQUET_MAX_SIZE`, `PROFILER_IDLE_ACCUMULATOR_TIMEOUT`, and
`PROFILER_STARTUP_LOCK_WAIT`.

### `query`

| Env | Default | Description |
|---|---|---|
| `PROFILER_EXTERNAL_API_PORT` | `8080` | `/api/v1` and the health probes. |
| `COLLECTOR_HEADLESS_SVC` | — | Headless-Service DNS name for replica discovery; empty serves the cold tier only. |
| `PROFILER_INTERNAL_API_PORT` | `8081` | The replicas' internal API port. |
| `PROFILER_OVERLAP_MARGIN` | `5m` | Hot/cold overlap window. |
| `PROFILER_FANOUT_TIMEOUT` | `2s` | Per-replica read timeout. |
| `PROFILER_S3_LIST_CONCURRENCY` | `16` | Parallel S3 LIST cap. |
| `PROFILER_CURSOR_TTL` | `15m` | Pagination-cursor validity. |
| `PROFILER_WIDE_RANGE_LIMIT` | `6h` | Span above which `/calls` requires a narrowing filter. |
| `PROFILER_MAX_SCAN_FILES` | `10000` | Candidate-object ceiling per `/calls` scan. |
| `PROFILER_MAX_SCAN_BYTES` | `2GB` | Estimated-scan-byte ceiling per `/calls` scan. |
| `PROFILER_DURATION_THRESHOLDS` | `100ms,1s,10s` | Must mirror the collector's value: the cold class pruning derives from the same tier table (01 §6.4). |

## Lifecycle

`collect` walks the `03-lifecycle.md` §2 states. The internal port binds at
process start, so `GET /internal/v1/health/ready` reports
`LOADING`/`RECOVERY` (503) during startup and `READY` (200) once recovery
finishes; the agent TCP listener and both loops start only after recovery.
SIGTERM flips readiness to `DRAINING`, holds `PROFILER_SHUTDOWN_DRAIN_GRACE`,
then finalizes open pod-restarts. A second signal skips the grace.

`query` follows §7: verify S3, bind, `READY`; SIGTERM drains, in-flight
requests get 15 s to finish.

## Smoke test

`make smoke` (from `backend/`) recreates the compose stack and runs
`libs/tests/smoke` (build tag `smoke`): a synthetic agent sends
dictionary + trace + calls + suspend streams over TCP, the hot phase asserts
`/api/v1/calls` and `/tree` answer before anything reaches MinIO, the cold
phase ages a bucket into S3, stops the collector container, and asserts the
wide range and `/tree` answer from S3 alone; a final phase restarts the
collector and checks recovery. The test needs a fresh stack and the `docker`
CLI (it stops and starts the collector container itself).
