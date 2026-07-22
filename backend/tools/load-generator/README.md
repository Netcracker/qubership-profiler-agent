# CDT load generator

Load-testing harness for the Go profiler backend (`load-testing-plan.md`). Traffic comes from the virtual dumper
(`backend/libs/emulator/vdumper`), a behavioral copy of the Java agent's remote-dump pipeline — fully synthetic, no
captured dumps or other binary fixtures.

## Layout

- `k6runner/` — custom k6 binary (plain `go build`, no xk6 CLI): k6 plus the `k6/x/cdt` module and the
  `go-prometheus-exporter` module.
- `pkg/cdt/` — the `k6/x/cdt` module: one VU drives a fleet of virtual dumpers and maps their stats to k6 metrics.
- `go-metrics/` — the `go-prometheus-exporter` module: serves the runner's own Go runtime metrics on `:5656`.
- `scripts/` — the k6 scenarios: `scenario.js` (write fleet, externally-controlled executor; the run orchestrator
  scales VUs over the k6 REST API) and `query-scenario.js` (T6 read load on `k6/http`); `SCENARIO` picks one.
- `runner/` — the run orchestrator: ramp steps, plateau/saturation detection, pprof capture, artifact collection.
  Contract: `doc/run-orchestration.md`.
- `feeder/` — standalone CLI that drives virtual dumpers without k6; handy for local debugging.
- `calibrate/` — the decoding TCP tap used for the phase-2 calibration (`doc/calibration.md`).
- `checker/` — the soak invariant checker, §8.1–§8.8 with latched violations. Contract: `doc/checker.md`.
- `deploy/` — helmfile for the stand (backend, MinIO, monitoring, k6 runner, T6 `k6-query`); see `deploy/README.md`.
- `dashboards/` — Grafana dashboards as code, shipped as `GrafanaDashboard` CRs by the `monitoring-crs` release.
- `doc/` — runbooks: `calibration.md`, `run-orchestration.md`, `ceiling-runs.md`, `soak-runs.md`, `checker.md`.

## Build

The deliverable is the Docker image; build it from the module root (`backend/`) so the k6 module can import `libs/`:

```bash
make image                          # docker buildx, PLATFORM=linux/arm64 (OrbStack) by default
make image PLATFORM=linux/amd64    # match the target stand; use a multi-arch manifest for shared registries
```

`make build` produces a local `bin/k6` for development; it is not a substitute for verifying the image.

## Run

See `doc/ceiling-runs.md` for the T2/T3 ceiling runbooks and `deploy/README.md` for bringing up the stand.
