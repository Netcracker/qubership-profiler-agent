# Profiler gateway extensions

OpenTelemetry Collector extensions that let the diagnostic gateway carry profiler traffic and switch it off per source.
They ship in the `qubership-otec` distribution, built in `Netcracker/qubership-open-telemetry-collector`, and run
under the `deploy/charts/gateway` chart. The design is in `docs/plans/2026-10-09-otel-gateway.md`.

| Extension | Traffic | Status |
|---|---|---|
| `profiler_dumps` | diagtools dump uploads, HTTP | development |

Both surfaces use their own listeners, not Collector pipelines: proxying the profiler protocols through `pdata` would
mean decoding and re-encoding them byte for byte. So `memory_limiter` and the filter processors do not apply here; each
extension has its own caps.

## Layout

```text
internal/units/           ByteSize, for sizes such as 10GiB in config
internal/rules/           modes, per-source rules, the rules file watcher
internal/dumps/           the dump-upload proxy and sink
extension/profilerdumps/  Collector wiring: factory, config, status reporting
```

The `internal/` packages import nothing from the Collector. The extension packages import only `component`,
`componentstatus`, and `extension`, plus `confmap` and the Collector test helpers in tests. A small Collector surface
keeps upgrades of the distribution from breaking on this module.

## `profiler_dumps`

```yaml
extensions:
  profiler_dumps:
    endpoint: 0.0.0.0:8080
    upstream: http://cloud-profiler-dumps-collector.profiler.svc:8080
    mode: passthrough
    max_body_size: 10GiB
    max_body_size_per_type: { td: 64MiB }
    max_concurrent_uploads: 16
    rules_file: /etc/gateway/rules/rules.yaml
service:
  extensions: [profiler_dumps]
```

| Key | Default | Meaning |
|---|---|---|
| `endpoint` | `0.0.0.0:8080` | Listen address. |
| `upstream` | none | Dumps collector base URL. Required unless `mode` is `discard`. |
| `mode` | `passthrough` | `passthrough` or `discard`, when no rule matches. |
| `path_prefix` | `/diagnostic` | Other paths get 404. |
| `allowed_methods` | `[PUT, POST, DELETE]` | Other methods get 405. |
| `connect_timeout` | `1s` | Upstream dial deadline. |
| `response_header_timeout` | `10m` | Wait for the upstream's answer after the body is sent. |
| `read_header_timeout` | `10s` | Wait for the client's request header. The body has no deadline. |
| `max_body_size` | `10GiB` | Size cap. Over it: abort upstream, drain, answer 204. |
| `max_body_size_per_type` | none | Cap per dump type: `hprof`, `td`, `gc`, `other`. |
| `max_concurrent_uploads` | `16` | Over it, an upload is discarded with 204, never queued. |
| `rules`, `rules_file` | none | Per-source overrides; see below. |
| `drain_timeout` | `30s` | Shutdown grace for uploads in flight. |

Behavior that follows from the diagtools client, which resends a file from byte zero on any answer other than 200,
201, or 204:

- `discard`, the caps, and the concurrency limit all read the body to the end and answer 204, so the client deletes
  its copy. The gateway never sheds load with 413, 429, or 503.
- In `passthrough` the upstream's status reaches the client unchanged, including 405 on `DELETE`. An unreachable
  upstream answers 502, so the client keeps the file and retries.
- Uploads stream: memory per upload is bounded by buffers, about 90 KB measured, not by file size. The multipart body
  is forwarded as raw bytes.
- A body over the cap is detected while it streams, because most uploads have no `Content-Length`. The cap saves
  storage, not network.

The dump type comes from the file name: `*.hprof*` is `hprof`, `*.td.txt` is `td`, `gc.log` and `gc.log.N` are `gc`,
and anything else (core files, `hs_err` logs) is `other`.

## Rules

```yaml
rules:
  - match: { namespace: "load-test-*", dump_type: [hprof] }
    mode: discard
  - match: { pod: "billing-*" }
    mode: passthrough
```

- `namespace` and `pod` are globs in `path.Match` syntax; `dump_type` is a list. An omitted key matches anything.
  `service` is a key for profiler TCP only: the upload URL carries no service.
- The first matching rule wins. Rules from `rules_file` go before inline `rules`; no match falls back to `mode`.
- The pod is the URL's pod segment up to the first `_`. diagtools appends `_<start timestamp>` to it.
- `rules_file` is watched. An edit applies to the next upload with no restart. A file that fails to parse is rejected,
  the previous rules stay active, and the extension reports a recoverable error until the file is fixed. Profiler
  surfaces may share one file; each skips rules that use keys only the other one knows.

## Metrics

On the Collector's own Prometheus endpoint:

| Metric | Labels |
|---|---|
| `profiler_gateway_uploads_in_flight` | |
| `profiler_gateway_upload_bytes_total` | `dump_type`, `outcome`: `forwarded`, `discarded`, `over_cap`, `over_concurrency`, `upstream_error`, `client_aborted` |
| `profiler_gateway_bytes_forwarded_total` | `surface` |
| `profiler_gateway_bytes_discarded_total` | `surface` |
| `profiler_gateway_upstream_errors_total` | `surface` |
| `profiler_gateway_rules_reload_total` | `surface`, `result` |

Alert on a non-zero rate of `profiler_gateway_bytes_discarded_total`: discard answers success, so a forgotten switch is
otherwise silent data loss.

## Development

```bash
cd apps/gateway
go test -race ./...
```

To run the extension inside a Collector, build one with the
[OpenTelemetry Collector builder](https://opentelemetry.io/docs/collector/custom-collector/) and a `replaces` entry
that points at this directory:

```yaml
extensions:
  - gomod: github.com/Netcracker/qubership-profiler-agent/apps/gateway v0.0.0
    import: github.com/Netcracker/qubership-profiler-agent/apps/gateway/extension/profilerdumps
replaces:
  - github.com/Netcracker/qubership-profiler-agent/apps/gateway => /path/to/apps/gateway
```

## Releasing

The distribution pins this module by tag and fetches it through the Go proxy:

1. Tag `apps/gateway/vX.Y.Z` on `main`.
2. In `qubership-open-telemetry-collector`, bump the version in `builder-config.yaml`, regenerate the collector, and
   release an image.
3. Pin that image in the gateway chart's `image.tag`.
