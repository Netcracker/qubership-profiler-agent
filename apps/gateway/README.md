# Profiler gateway extensions

OpenTelemetry Collector extensions that let the diagnostic gateway carry profiler traffic and switch it off per source.
They ship in the `qubership-otec` distribution, built in `Netcracker/qubership-open-telemetry-collector`, and run
under the `deploy/charts/gateway` chart. The design is in `docs/plans/2026-10-09-otel-gateway.md`.

| Extension        | Traffic                         | Status      |
| ---------------- | ------------------------------- | ----------- |
| `profiler_dumps` | diagtools dump uploads, HTTP    | development |
| `profiler_tcp`   | profiler agent connections, TCP | development |

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

| Key                       | Default               | Meaning                                                         |
| ------------------------- | --------------------- | --------------------------------------------------------------- |
| `endpoint`                | `0.0.0.0:8080`        | Listen address.                                                 |
| `upstream`                | none                  | Dumps collector base URL. Required unless `mode` is `discard`.  |
| `mode`                    | `passthrough`         | `passthrough` or `discard`, when no rule matches.               |
| `path_prefix`             | `/diagnostic`         | Other paths get 404.                                            |
| `allowed_methods`         | `[PUT, POST, DELETE]` | Other methods get 405.                                          |
| `connect_timeout`         | `1s`                  | Upstream dial deadline, and TLS handshake deadline for `https`. |
| `response_header_timeout` | `10m`                 | Wait for the upstream's answer after the body is sent.          |
| `read_header_timeout`     | `10s`                 | Wait for the client's request header.                           |
| `idle_timeout`            | `60s`                 | Longest pause in body progress, and keep-alive idle limit.      |
| `max_body_size`           | `10GiB`               | Size cap. Over it: abort upstream, drain, answer 204.           |
| `max_body_size_per_type`  | none                  | Cap per dump type: `hprof`, `td`, `gc`, `other`.                |
| `max_concurrent_uploads`  | `16`                  | Over it, an upload is discarded with 204, never queued.         |
| `rules`, `rules_file`     | none                  | Per-source overrides; see below.                                |
| `drain_timeout`           | `30s`                 | Shutdown grace for uploads in flight.                           |

Sizes are a plain number of bytes, such as `4096`, optionally followed by `B` or by one of the IEC suffixes `KiB`,
`MiB`, `GiB`, and `TiB`. Every size must be positive. `KB`, `MB`, `GB`, `TB`, and the one-letter `K`, `M`, `G`, `T`
are rejected, so a profiler-backend value such as `64MB` fails validation; write `64MiB`. A `max_body_size_per_type`
entry replaces `max_body_size` for its dump type, and it can be larger than `max_body_size`.

Keep `response_header_timeout` longer than the client's upload deadline, `DIAGNOSTIC_UPLOAD_TIMEOUT` in diagtools (5
minutes by default), so the client gives up first.

Behavior that follows from the diagtools client, which resends a queued dump file from byte zero on any answer other
than 200, 201, or 204, until `DIAGNOSTIC_UPLOAD_MAX_AGE` passes (48 hours by default):

- `discard`, the caps, and the concurrency limit all read the body to the end and answer 204, so the client deletes
  its copy. The gateway never sheds load with 413, 429, or 503.
- A body has no overall deadline, so a slow 10 GB upload completes. A pause longer than `idle_timeout` fails it: a
  client that stops sending is dropped without an answer, so it keeps the file, and an upstream that stops reading
  answers 502. Either way the upload slot is freed.
- In `passthrough` the upstream's status reaches the client unchanged, including 405 on `DELETE`. An unreachable
  upstream answers 502. diagtools keeps a queued dump file and retries it, but a thread dump that it sends straight
  from memory has no file, and a 502 loses that dump.
- Uploads stream: memory per upload is bounded by buffers, about 90 KB measured, not by file size. The multipart body
  is forwarded as raw bytes.
- A body over the cap is detected while it streams, because most uploads have no `Content-Length`. The cap saves
  storage, not network.
- A path that nginx would normalize to a different one gets 400 before any rule runs: an empty, `.`, or `..`
  segment, or percent-encoding such as `%2F`. The rules and the upstream then read the same source. diagtools never
  builds such a path.

The dump type comes from the file name: `*.hprof*` is `hprof`, `*.td.txt` is `td`, `gc.log` and `gc.log.N` are `gc`,
and anything else (core files, `hs_err` logs) is `other`.

## `profiler_tcp`

```yaml
extensions:
  profiler_tcp:
    endpoint: 0.0.0.0:1715
    upstream: profiler-profiler-backend-collector-agent.profiler.svc:1715
    mode: passthrough
    max_connections: 256
    rules_file: /etc/gateway/rules/rules.yaml
service:
  extensions: [profiler_tcp]
```

| Key                              | Default        | Meaning                                                            |
| -------------------------------- | -------------- | ------------------------------------------------------------------ |
| `endpoint`                       | `0.0.0.0:1715` | Listen address.                                                    |
| `upstream`                       | none           | Collector `host:port`. Required unless `mode` is `discard`.        |
| `mode`                           | `passthrough`  | `passthrough` or `discard`, when no rule matches.                  |
| `connect_timeout`                | `1s`           | Upstream dial deadline.                                            |
| `max_connections`                | `256`          | Over it, a connection is closed on arrival.                        |
| `copy_buffer_size`               | `32KiB`        | Copy buffer per direction in `passthrough`.                        |
| `handshake_timeout`              | `10s`          | Wait for the agent's handshake.                                    |
| `discard.rotation_period`        | `0`            | Stream rotation period a discarding connection asks the agent for. |
| `discard.required_rotation_size` | `4MiB`         | Stream rotation size a discarding connection asks the agent for.   |
| `rules`, `rules_file`            | none           | Per-source overrides; see below.                                   |

Every connection opens with the agent's `GET_PROTOCOL_VERSION_V2` handshake, which names its namespace, service,
and pod. The extension reads it with the collector's own reader, picks the mode, and then:

- in `passthrough`, dials the upstream, replays the handshake bytes unchanged, and copies bytes between the two
  sockets in both directions. The collector answers the handshake and every ack, and the ack byte's command channel
  (heap and thread dump requests from the UI) keeps working.
- in `discard`, runs the collector protocol itself through `libs/wire/server` with a listener that stores nothing.
  The agent gets real stream handles and acks, logs nothing, and keeps its dumper alive. It still instruments and
  sends, and it cannot receive dump requests from the UI.

A connection with no valid handshake within `handshake_timeout` is reset; the legacy `0x08` handshake is not
supported. An unreachable upstream and a full `max_connections` also reset the connection: the agent marks its
dumper dead, logs one warning, and retries after its restart interval, 10 seconds by default
(`-Dcom.netcracker.profiler.Dumper.RESTART_INTERVAL`, in seconds). The gateway sends a reset, not a FIN, because the
agent reads an EOF as "no data yet" and waits out its 30-second read timeout before it retries.

A rules file change is applied to open connections too: each one whose mode changes is reset, and the agent
reconnects into the new mode after its restart interval. That is the only way to move an agent out of `discard`,
because the upstream never saw that connection's handshake. Shutdown resets every connection at once.

How the gateway ends the agent's side of a connection:

| End                                                       | Agent sees | Why                                               |
| --------------------------------------------------------- | ---------- | ------------------------------------------------- |
| The agent finished sending first, then upstream closed    | FIN        | A clean end the agent asked for.                  |
| The agent finished first, upstream still open after 30 s  | Reset      | The session must not hold its slot forever.       |
| The upstream closed first                                 | Reset      | Nothing can answer the agent any more.            |
| A read or write error in either direction                 | Reset      | The stream is broken.                             |
| Mode changed by a rules reload, or shutdown               | Reset      | The agent reconnects after its restart interval.  |
| Upstream unreachable, `max_connections`, bad handshake    | Reset      | The agent retries after its restart interval.     |
| `discard`: read timeout, protocol error, `COMMAND_CLOSE`  | FIN        | The same end the collector gives such a session.  |

Outside an established `discard` session, every end the agent did not start is a reset, because a FIN would make the
agent wait out its 30-second read timeout first.

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
- A `passthrough` rule needs an `upstream`, also when `mode` is `discard`. Without one, inline `rules` with a
  `passthrough` rule fail validation, and the extension rejects a rules file update that has a `passthrough` rule it
  evaluates.
- For dumps, the pod is the URL's pod segment up to the first `_`. diagtools appends `_<start timestamp>` to it.
- `rules_file` is watched. An edit applies to the next upload with no restart. A file that fails to parse is rejected,
  the previous rules stay active, and the extension reports a recoverable error until the file is fixed. Both
  extensions may share one file; each skips rules that use keys only the other one knows.

## Metrics

On the Collector's own Prometheus endpoint:

| Metric                                                | Labels                                                                                                                                    |
| ----------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `profiler_gateway_uploads_in_flight`                  |                                                                                                                                           |
| `profiler_gateway_upload_bytes_total`                 | `dump_type`, `outcome`: `forwarded`, `discarded`, `over_cap`, `over_concurrency`, `upstream_error`, `client_aborted`, `upstream_rejected` |
| `profiler_gateway_bytes_forwarded_total`              | `surface`: `dumps`, `tcp`                                                                                                                 |
| `profiler_gateway_bytes_discarded_total`              | `surface`: `dumps`, `tcp`                                                                                                                 |
| `profiler_gateway_upstream_errors_total`              | `surface`: `dumps`, `tcp`                                                                                                                 |
| `profiler_gateway_rules_reload_total`                 | `surface`: `dumps`, `tcp`; `result`: `success`, `failure`                                                                                 |
| `profiler_gateway_connections_active`                 | `mode`: `passthrough`, `discard`                                                                                                          |
| `profiler_gateway_connections_total`                  | `mode`: `passthrough`, `discard`                                                                                                          |
| `profiler_gateway_connections_refused_total`          |                                                                                                                                           |
| `profiler_gateway_connections_closed_by_reload_total` | `mode`: `passthrough`, `discard`, the mode before the change                                                                              |

Alert on a non-zero rate of `profiler_gateway_bytes_discarded_total`: discard answers success, so a forgotten switch is
otherwise silent data loss. A discard switch is not the only cause: on the dumps surface, an upload over the size or
concurrency cap counts too. The `outcome` attribute of `profiler_gateway_upload_bytes_total` tells the causes apart.

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
