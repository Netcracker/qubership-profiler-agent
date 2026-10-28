# Profiler gateway extensions

OpenTelemetry Collector extensions that let the diagnostic gateway carry profiler traffic and switch it off per source.
They ship in the `qubership-otec` distribution, built in `Netcracker/qubership-open-telemetry-collector`, and run
under the `deploy/charts/gateway` chart. The design is in `docs/plans/2026-10-09-otel-gateway.md`.

| Extension | Traffic | Status |
|---|---|---|
| `profiler_dumps` | diagtools dump uploads, HTTP | development |
| `profiler_tcp` | profiler agent connections, TCP | development |

Both extensions use their own listeners, not Collector pipelines: proxying the profiler protocols through `pdata` would
mean decoding and re-encoding them byte for byte. So `memory_limiter` and the filter processors do not apply here; each
extension has its own caps.

## Layout

```text
internal/units/           ByteSize, for sizes such as 10GiB in config
internal/rules/           modes, per-source rules, the rules file watcher
internal/dumps/           the dump-upload proxy and sink
internal/tcp/             the agent connection proxy, handshake reader, and protocol sink
extension/profilerdumps/  Collector wiring for dumps: factory, config, status reporting
extension/profilertcp/    Collector wiring for agent connections
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

## `profiler_tcp`

```yaml
extensions:
  profiler_tcp:
    endpoint: 0.0.0.0:1715
    upstream: profiler-collector-agent.profiler.svc:1715
    mode: passthrough
    max_connections: 256
    rules_file: /etc/gateway/rules/rules.yaml
service:
  extensions: [profiler_tcp]
```

| Key | Default | Meaning |
|---|---|---|
| `endpoint` | `0.0.0.0:1715` | Listen address. |
| `upstream` | none | Collector `host:port`. Required unless `mode` is `discard`. |
| `mode` | `passthrough` | `passthrough` or `discard`, when no rule matches. |
| `connect_timeout` | `1s` | Upstream dial deadline. |
| `max_connections` | `256` | Over it, a connection is closed on arrival. |
| `copy_buffer_size` | `32KiB` | Splice buffer per direction in `passthrough`. |
| `handshake_timeout` | `10s` | Wait for the agent's handshake. |
| `discard.rotation_period` | `0` | Stream rotation period a discarding connection asks the agent for. |
| `discard.required_rotation_size` | `4MiB` | Stream rotation size a discarding connection asks the agent for. |
| `rules`, `rules_file` | none | Per-source overrides; see below. |

Every connection opens with the agent's `GET_PROTOCOL_VERSION_V2` handshake, which names its namespace, service,
and pod. The extension reads it with the collector's own reader, picks the mode, and then:

- in `passthrough`, dials the upstream, replays the handshake bytes unchanged, and splices the two sockets. The
  collector answers the handshake and every ack, and the ack byte's command channel (heap and thread dump requests
  from the UI) keeps working.
- in `discard`, runs the collector protocol itself through `libs/wire/server` with a listener that stores nothing.
  The agent gets real stream handles and acks, logs nothing, and keeps its dumper alive. It still instruments and
  sends, and it cannot receive dump requests from the UI.

A connection with no valid handshake within `handshake_timeout` is closed; the legacy `0x08` handshake is not
supported. An unreachable upstream and a full `max_connections` also close the connection: the agent marks its
dumper dead, logs one warning, and retries every 10 seconds.

A rules file change is applied to open connections too: each one whose mode changes is closed, and the agent
reconnects within 10 seconds into the new mode. That is the only way to move an agent out of `discard`, because the
upstream never saw that connection's handshake. Shutdown closes every connection at once.

Measured on a laptop: a gateway hop adds about 21 µs to a 1 KB request and its answer, and an idle passthrough
connection costs about 40 KB.

## Rules

```yaml
rules:
  - match: { namespace: "load-test-*", dump_type: [hprof] }
    mode: discard
  - match: { pod: "billing-*" }
    mode: passthrough
```

- `namespace`, `service`, and `pod` are globs in `path.Match` syntax; `dump_type` is a list. An omitted key matches
  anything. `service` is a key for `profiler_tcp` only, because the upload URL carries no service. `dump_type` is
  for `profiler_dumps` only.
- The first matching rule wins. Rules from `rules_file` go before inline `rules`; no match falls back to `mode`.
- For dumps, the pod is the URL's pod segment up to the first `_`. diagtools appends `_<start timestamp>` to it.
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
| `profiler_gateway_connections_active` | `mode` |
| `profiler_gateway_connections_total` | `mode` |
| `profiler_gateway_connections_refused_total` | |
| `profiler_gateway_connections_closed_by_reload_total` | `mode`, the mode before the change |

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
  - gomod: github.com/Netcracker/qubership-profiler-agent/apps/gateway v0.0.0
    import: github.com/Netcracker/qubership-profiler-agent/apps/gateway/extension/profilertcp
replaces:
  - github.com/Netcracker/qubership-profiler-agent/apps/gateway => /path/to/apps/gateway
  - github.com/Netcracker/qubership-profiler-agent/libs/wire => /path/to/libs/wire
```

## Releasing

The distribution pins this module by tag and fetches it through the Go proxy. This module requires `libs/wire` at
the placeholder `v0.0.0` and replaces it with `../../libs/wire`; an outside build ignores the replace, so a release
needs a real `libs/wire` version first:

1. Tag `libs/wire/vX.Y.Z` on `main` if `libs/wire` changed, and set this module's `libs/wire` requirement to it.
2. Tag `apps/gateway/vX.Y.Z` on `main`.
3. In `qubership-open-telemetry-collector`, bump the version in `builder-config.yaml`, regenerate the collector, and
   release an image.
4. Pin that image in the gateway chart's `image.tag`.
