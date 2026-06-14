# Profiler backend: design documents

Contract documents for the profiler-backend migration to the new architecture: no Postgres, in-memory call assembly + WAL + spill + parquet in S3, query fan-out over a Headless Service.

The rationale and the high-level roadmap live in `/profiler-plan.md` at the repo root.

## Layout

| # | Document | Scope |
|---|---|---|
| 01 | [write-contract.md](./01-write-contract.md) | What collector writes: dictionary WAL, spill SQLite, parquet schema, local PV layout, S3 layout, flush semantics |
| 02 | [read-contract.md](./02-read-contract.md) | Query API endpoints, collector hot-read API, hot/cold cutoff, deduplication, S3 LIST-based discovery |
| 03 | [lifecycle.md](./03-lifecycle.md) | Readiness probe, recovery sequence, flush triggers, shutdown |
| 04 | [storage-layout.md](./04-storage-layout.md) | k8s manifests, StatefulSet + volumeClaimTemplates, Headless Service, Helm values, env vars |
| 05 | [diagrams.md](./05-diagrams.md) | Mermaid diagrams: data flow, deployment, lifecycle state |

## Decisions (summary)

- **Language: Go.** All the building blocks (`libs/parser/`, `libs/storage/parquet/`, `libs/s3/`) are already in Go. The Java collector becomes legacy.
- **Inter-service transport: JSON over HTTP.** Both for the external client API and for query-to-collector fan-out.
- **S3 discovery: LIST by prefix.** Manifest files are deferred until LIST becomes a bottleneck.
- **Dev S3: MinIO in docker-compose.** A filesystem emulator stays only for unit tests.
- **Spill: SQLite.** Triggers: primary is memory pressure, secondary is idle timeout per call.
- **Parquet schema: redesigned.** The existing `CallParquet` is a starting point, not a fixed contract. There is no migration constraint and no existing client we must keep compatible.

## Stage 0 progress

Tracked in [stage0-progress.md](./stage0-progress.md).
