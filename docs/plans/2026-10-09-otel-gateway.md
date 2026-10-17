# Diagnostic gateway on the OpenTelemetry Collector: final plan

> Status: **final**, merged from the draft (`otel-gateway.md`) and its review (`otel-gateway-review.md`) on
> October 9, 2026. Facts are checked against this repository (`src/nginx/`, `charts/diag-proxy/`) and against the
> `qubership-profiler-agent` monorepo (`apps/agent/`, `libs/server/`, `libs/protocol/`, `apps/dumps-collector/`,
> `deploy/charts/`, `docs/design/`), and against `qubership-open-telemetry-collector` (`builder-config.yaml`,
> `Dockerfile`, chart). Section 13 lists what the review got wrong or left unverified.

## 1. Decisions

| Question | Decision |
|---|---|
| Packaging | The existing `qubership-otec` distribution from `qubership-open-telemetry-collector`. No second build. |
| Extension code | `apps/gateway` in `qubership-profiler-agent`, a Go module the distribution's manifest pins by tag. |
| Chart | `deploy/charts/gateway` in `qubership-profiler-agent`, running the `qubership-otec` image. |
| Components | Two extension types, `profiler_dumps` and `profiler_tcp`, sharing one rules package. |
| Topology | Per-namespace drop-in for diag-proxy first. Central mode is designed for and deferred (§10). |
| TCP modes | `passthrough` and `discard`. `reject` is out of v1. |
| Over a TCP cap | Refuse the connection. The agent retries every 10 s on its own. |
| Policy changes | Hot-reloaded rules file. A mode change closes the affected TCP connections. |
| Stage order | Traces and dumps first, TCP after. |

The gateway replaces the nginx-based diag-proxy with one image that carries three traffic classes:

| Traffic | Today (nginx) | In the gateway | Mechanism |
|---|---|---|---|
| OTLP 4317 / 4318 | `stream` L4 forward | `otlp` receiver → processors → exporter | stock components (§6) |
| Jaeger 14250 / 14268 | `stream` L4 forward | `jaeger` receiver | stock components (§6) |
| Jaeger 14267 | `stream` L4 forward | none | gap, see §6.3 |
| Zipkin 9411 | `stream` L4 forward | `zipkin` receiver | stock components (§6) |
| Dump upload, HTTP 8080 | `http` L7 proxy | `profiler_dumps` extension | own HTTP listener, streaming (§4) |
| Profiler TCP 1715 | `stream` L4 forward | `profiler_tcp` extension | own TCP listener, two modes (§5) |

## 2. Why a gateway, and what each surface gains

The goal is a drop switch: stop delivering a signal without touching the services that emit it, so the backend can
scale to zero. The three surfaces benefit from that switch to very different degrees, and that sets the stage order.

**Dumps gain the most.** `checkStatus` in `apps/agent/diagtools/utils/sender.go` accepts only 200, 201, and 204. On
any other answer the file stays on the pod's disk and is sent again, from byte zero, on the next scan tick. Only
`*.hprof*` files are retained, bounded by `DIAGNOSTIC_UPLOAD_MAX_AGE` (48 h) and `DIAGNOSTIC_PENDING_MAX_BYTES`
(10 GiB). Thread dumps are lost at once. A gateway that answers 204 and discards is the only way to stop that.

**Traces gain a real switch.** With a dead upstream, application exporters retry with backoff and log every failed
attempt. A receiver that accepts and drops is silent.

**Profiler TCP gains little.** The draft assumed a refused connection causes a reconnect storm. It does not:

- `DumperThread.run` catches `ProfilerProtocolException`, sets `ProfilerData.dumperDead = true`, sleeps
  `DUMPER_RESTART_INTERVAL` (10 s by default), and retries. It logs "Unable to connect to remote collector" once
  until the next successful connect.
- While `dumperDead` is set, the agent drops its buffers itself, so its memory stays bounded.
- When the backend is scaled to zero today, nginx accepts, fails the upstream dial after 1 s, and closes. That
  already is a quiet drop, at the cost of one connect per pod every 10 s and one log line.

For TCP, `discard` buys two things only: no warning in the agent log, and a dumper that reports itself alive. It also
costs something (§5.2). The plan keeps it, late, and does not present it as the safe default.

## 3. Architecture

### 3.1 Why extensions

A receiver must produce `pdata` and an exporter must consume it. Proxying the profiler protocol through a pipeline
would mean decoding the agent's binary streams into `pdata` and re-encoding them byte for byte, ack state machine
included. A pipeline is also asynchronous, so the gateway would have to ack before it knows the backend accepted
the data. Dump uploads have the same mismatch.

An extension has no pipeline contract: `Start(ctx, host)`, `Shutdown(ctx)`, and everything between is ours.
`extension/httpforwarderextension` in contrib has this shape already.

The consequence: `memory_limiter`, `filterprocessor`, and OTTL do not apply to the two profiler surfaces. Both
extensions carry their own caps (§4.4, §5.4).

### 3.2 Layout: two repositories

The extensions live with the protocol code they wrap. The distribution lives where the Collector is already built.

```text
qubership-profiler-agent/
  libs/wire/                    new module: protocol, server, and the common, io, log packages they import
  apps/gateway/
    go.mod                      module github.com/Netcracker/qubership-profiler-agent/apps/gateway
    internal/rules/             matching, rules-file watch; no Collector imports
    internal/dumps/             HTTP proxy and sink; no Collector imports
    internal/tcp/               TCP proxy and sink; no Collector imports
    extension/profilerdumps/    factory, config, componentstatus wiring for internal/dumps
    extension/profilertcp/      factory, config, componentstatus wiring for internal/tcp
  deploy/charts/gateway/        Helm chart; image is qubership-otec

qubership-open-telemetry-collector/
  builder-config.yaml           gains the two extensions and three stock components (below)
```

**Why not a second distribution.** `qubership-otec` is lean, about 25 components, and already carries most of
what the gateway needs: the `otlp`, `jaeger`, and `zipkin` receivers, `filter`, `tail_sampling`, and `batch`
processors, the `otlp` exporter, and `health_check`. It has a Makefile-driven upstream bump (`make update-otel`),
Renovate, and image CI. A second manifest in the profiler repo would duplicate all of that.

**Manifest additions** in `builder-config.yaml`:

```yaml
extensions:
  - gomod: github.com/Netcracker/qubership-profiler-agent/apps/gateway v0.1.0
    import: github.com/Netcracker/qubership-profiler-agent/apps/gateway/extension/profilerdumps
  - gomod: github.com/Netcracker/qubership-profiler-agent/apps/gateway v0.1.0
    import: github.com/Netcracker/qubership-profiler-agent/apps/gateway/extension/profilertcp
exporters:
  - gomod: go.opentelemetry.io/collector/exporter/nopexporter v0.158.0
processors:
  - gomod: go.opentelemetry.io/collector/processor/memorylimiterprocessor v0.158.0
```

Both extensions come from one module, so each entry names its package with `import`. G2 adds the first entry and
G3 the second. The distribution has `memorylimiterextension` but not the processor, and no `nop` exporter.
`opampextension` is added in G5.

The in-repo components there are pinned to `main` and resolved by local `replaces`. The profiler extensions are
external, so they are pinned to a tag (`apps/gateway/vX.Y.Z`) and fetched through the Go proxy. The Dockerfile
copies only local directories, which is fine for a public module.

**The module-path problem, and the fix.** The monorepo's root module is named
`github.com/Netcracker/qubership-profiler-backend`. A separate repository with that name exists and holds an older
copy of `libs/`. Go ignores `replace` directives in dependencies, so an extension module that imports
`…/qubership-profiler-backend/libs/server` would make the distribution build fetch the stale repository, with no
error. The root also carries `v4.x` tags without a `/v4` module suffix, which Go cannot use as versions.

`libs/server` imports only `libs/common`, `libs/io`, `libs/log`, `libs/protocol`, and `github.com/pkg/errors`.
Move those five packages into a nested module, `github.com/Netcracker/qubership-profiler-agent/libs/wire`, tagged
`libs/wire/vX.Y.Z`. The backend's imports change mechanically, the gateway depends on a small module with a
correct path, and the distribution build never sees the backend's parquet, PostgreSQL, and S3 requirements.
`libs/emulator` is needed only by tests; it stays in the root module, and the gateway's protocol tests live there
too, or the emulator moves into `libs/wire` if its imports allow.

The fallback, if moving packages is unwelcome: a `replaces` entry in `builder-config.yaml` that maps the backend
module name to a pseudo-version of `qubership-profiler-agent`. It needs no refactoring, it is untested, and it
drags the root module's requirement graph into the distribution's version selection.

**Thin adapters.** The `internal/` packages depend on nothing from the Collector. The extension packages use only
`component`, `extension`, `confmap`, and `componentstatus`. That keeps the Collector version coupling narrow (§13),
and the same code runs under a short `main.go` if the gateway ever has to leave the distribution.

### 3.3 Two types, not one

A component ID is `type[/name]`, and the type cannot contain a slash. The draft modeled both surfaces as instances
of one `profiler` type with a union config. That union needs fields documented as "ignored on the other surface"
(`dump_type`, the discard reply fields), and dumps ship two stages before TCP exists. Two types give each surface a
clean schema and an independent factory, health status, and shutdown.

### 3.4 Config sketch

```yaml
extensions:
  health_check:                        # the probe port diag-proxy uses today, see §6.4
    endpoint: 0.0.0.0:8888
    path: /probes/ready

  profiler_dumps:
    endpoint: 0.0.0.0:8080
    upstream: http://cloud-profiler-dumps-collector.profiler.svc:8080
    mode: passthrough                  # passthrough | discard
    max_concurrent_uploads: 16
    rules_file: /etc/gateway/rules.yaml

  profiler_tcp:
    endpoint: 0.0.0.0:1715
    upstream: profiler-collector-agent.profiler.svc:1715
    mode: passthrough                  # passthrough | discard
    max_connections: 256
    rules_file: /etc/gateway/rules.yaml

receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }
  jaeger:
    protocols:
      grpc: { endpoint: 0.0.0.0:14250 }
      thrift_http: { endpoint: 0.0.0.0:14268 }
  zipkin:
    endpoint: 0.0.0.0:9411

processors:
  memory_limiter: { check_interval: 1s, limit_percentage: 80 }
  batch:

exporters:
  otlp/upstream:
    endpoint: jaeger-collector.tracing.svc:4317
    tls: { insecure: true }
  nop:

service:
  telemetry:
    metrics:                           # moved off 8888, which the probes own
      readers:
        - pull: { exporter: { prometheus: { host: 0.0.0.0, port: 8889 } } }
  extensions: [health_check, profiler_dumps, profiler_tcp]
  pipelines:
    traces/otlp:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp/upstream]
    traces/jaeger:
      receivers: [jaeger]
      processors: [memory_limiter, batch]
      exporters: [otlp/upstream]
    traces/zipkin:
      receivers: [zipkin]
      processors: [memory_limiter, batch]
      exporters: [otlp/upstream]
```

Upstream names follow the OSS charts in `deploy/charts/`. `esc-collector-service` and `esc-static-service` are the
legacy Java collector's names, and both the Java agent (`DefaultCollectorClient.java:209`) and the diagtools
bootstrap script (`scripts/diag-bootstrap.sh`) rewrite `esc-static-service` to `esc-collector-service` in the host
they are given. The gateway's own Service name must not contain either string.

## 4. Dump uploads: `profiler_dumps`

### 4.1 What the client sends

```text
PUT    /diagnostic/{namespace}/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}/{pod}/{file}   application/octet-stream
POST   /diagnostic/{namespace}/{yyyy}/{MM}/{dd}/{HH}/{mm}/{ss}/{pod}/{file}   multipart/form-data
DELETE /diagnostic/...
```

- diagtools addresses `NC_DIAGNOSTIC_AGENT_SERVICE` (default `nc-diagnostic-agent`) on port 8080. With
  `TLS_ENABLED=true` it goes straight to `https://…:8443`, where diag-proxy does not listen. The client's TLS mode
  bypasses the proxy today, and the gateway does not change that.
- **Large uploads are chunked.** `SendSingleFile` and `SendMultiPart` feed the body through `io.Pipe`, so Go sends
  `Transfer-Encoding: chunked` with no `Content-Length`. Only thread dumps, sent from a `bytes.Reader`, declare a
  length.
- Heap dumps go as `PUT` on the scan tick (`scan.go:109`; the multipart call next to it is commented out) and as
  multipart `POST` from `heapdump.go`. The gateway forwards both verbatim.
- Dump types are `hprof`, `td`, and `gc`. `topdump.go` is a disabled stub.
- For `DELETE`, the client treats 200, 204, 404, and 405 as fine. The WebDAV upstream answers 405.
- The upload deadline is `DIAGNOSTIC_UPLOAD_TIMEOUT`, 5 minutes by default, set as `http.Client.Timeout`.
- An upload error ends the whole scan tick, so one stuck file delays every file behind it until the next tick.
  Gateway timeouts on this surface must be longer than the client's.

### 4.2 Modes

**`passthrough`** streams the request to the upstream with `httputil.ReverseProxy` and returns the upstream's
status unchanged, including a 405 on `DELETE`.

**`discard`** reads the body to the end into `io.Discard` and answers 204, so the client marks the file uploaded and
deletes its local copy. `DELETE` in `discard` answers 204 without contacting the upstream.

Three rules follow from the client's retry behavior:

1. **Never answer 413, 429, or 503 to shed load.** Any non-2xx answer makes the client resend the same file every
   tick for up to 48 h. Shedding on this surface means accept, discard, answer 204.
2. **A size cap saves storage, not network.** Because large bodies are chunked, the size is unknown until the bytes
   have crossed the network. An oversize upload is detected by a counting reader, the upstream request is aborted,
   the rest of the body is drained, and the client gets 204. For bodies that do declare `Content-Length`, the
   decision is made before the upstream is dialed. Saving network would take a client change (for example, treating
   413 as "delete locally"); that is out of scope here and noted in §12.
3. **Always drain before answering, until G0 proves otherwise.** Whether the diagtools client accepts an early 204
   on a chunked body or reports a broken pipe is a G0 check (§11). Draining is the default.

### 4.3 Streaming, and the traps

Memory per upload is bounded by buffers, never by file size: about 64 KB plain, about 100 KB with upstream TLS.

1. **Never call `r.ParseMultipartForm`.** It buffers parts in memory and spills to temp files. Forward the raw body
   with the original `Content-Type`; every routing input is in the URL path.
2. **Never set `http.Server.ReadTimeout`.** It covers the body. Use `ReadHeaderTimeout`.
3. **Never set a whole-request timeout on the upstream client.** Bound the dial and the response header instead.
4. **Do not rely on `Content-Length` for caps.** See rule 2 in §4.2.

nginx buffers each request body to `/tmp` before forwarding (`proxy_request_buffering` is on by default) and, with
no `client_max_body_size` in `src/nginx/`, caps bodies at 1 MB. If G0 confirms that dumps over 1 MB get 413 through
diag-proxy today, the gateway's streaming path is a behavior fix, and the cutover notes must say so.

### 4.4 Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `endpoint` | `0.0.0.0:8080` | Bind address. |
| `upstream` | none | Base URL of the dumps collector. Required unless `mode` is `discard`. |
| `mode` | `passthrough` | Default mode: `passthrough` or `discard`. |
| `path_prefix` | `/diagnostic` | Requests outside the prefix get 404. |
| `allowed_methods` | `[PUT, POST, DELETE]` | Everything diagtools uses. |
| `upstream_tls` | off | mTLS to the upstream (§7). |
| `connect_timeout` | `1s` | Upstream dial deadline. Matches nginx `proxy_connect_timeout`. |
| `response_header_timeout` | `10m` | Upstream header deadline. Longer than the client's 5 min on purpose. |
| `read_header_timeout` | `10s` | Client header deadline. |
| `max_body_size` | `10GiB` | Storage cap. Over the cap: abort upstream, drain, answer 204. |
| `max_body_size_per_type` | none | Per type, for example `{ hprof: 10GiB, td: 64MiB, gc: 256MiB }`. |
| `max_concurrent_uploads` | `16` | Memory knob. Over the cap the upload is discarded with 204, never queued. |
| `rules`, `rules_file` | none | Per-source overrides (§8). |
| `drain_timeout` | `30s` | Shutdown grace for in-flight uploads. |

## 5. Profiler TCP: `profiler_tcp`

### 5.1 Protocol facts that matter

`docs/design/06-wire-protocol-server.md` is the contract.

- The connection opens with `COMMAND_GET_PROTOCOL_VERSION_V2` (`0x14`): `long clientVersion`, then length-prefixed
  `pod`, `service`, `namespace`, in cleartext. The agent offers `PROTOCOL_VERSION_V3` (100705) and accepts V2 or V3
  in reply. The legacy `0x08` handshake is not used by the agent or the Go server and is not supported.
- Every `RCV_DATA` and `REQUEST_ACK_FLUSH` expects one ack byte. `ACK_OK` is `0x00`.
- **The ack byte is also a command channel.** The agent reads any non-negative ack as the number of
  collector-to-agent commands that follow (`DefaultCollectorClient.java:419`). The UI uses this to request heap and
  thread dumps.
- The agent drains acks synchronously under a 30 s read timeout (`PLAIN_SOCKET_READ_TIMEOUT` is 30000; the comment
  beside it says 10 seconds and is wrong).
- One connection is one pod-restart. There is no resumption; a dropped socket means a full dictionary resend.

### 5.2 Modes

**`passthrough`** splices the socket to the upstream with `io.Copy` in both directions. No parsing; byte-identical
to nginx. With rules configured, the gateway reads the handshake first, decides, dials, and replays the bytes. The
dial and the upstream's reply must both fit inside the agent's 30 s read timeout; `connect_timeout: 1s` leaves room.

**`discard`** terminates the protocol with `libs/server` and a `Listener` that stores nothing. The handshake reply
is whatever `libs/server` answers, not a hard-coded V2. `Listener` has nine methods (`RegisterPod`, `AppendData`,
`RegisterStream`, `PodDisconnected`, `ReceivedCommand`, `Read`, `Write`, `PrintDebug`, `Close`); the null
implementation is small, and `Read`/`Write` feed the gateway's byte counters.

What `discard` costs:

- **The command channel is gone.** A discarded agent cannot be asked for a heap or thread dump from the UI.
- The agent still instruments, serializes, and sends. `discard` relieves the backend and the gateway-to-backend
  network, not the profiled service.
- The data is not spooled anywhere. `localDumpEnabled` is `forceLocalDump || !remoteConfigured`, so an agent with a
  remote host configured writes nothing locally. The switch is not a pause button.

**`reject` is not built.** `BLACK_LISTED_RESP` sets `isBlacklistedNS` at `Dumper.java:488` and nothing clears it
short of recreating the `Dumper`: a JVM restart, or stop/start from the profiler UI. A switch that needs a service
restart to undo defeats the purpose of the gateway. Revisit only with a named consumer.

| Mode | Backend load | Agent log | Dumper status | UI commands | Reversible |
|---|---|---|---|---|---|
| `passthrough` | full | quiet | alive | work | n/a |
| `discard` | none | quiet | alive | lost | yes, on reconnect (§8.2) |
| refused (cap, or upstream down) | none | one warning | dead, retry every 10 s | lost | yes, within 10 s |

### 5.3 What is deliberately absent

- **Byte-rate throttling.** Backpressure stalls the agent's synchronous ack drain; past 30 s the agent reconnects
  and resends its dictionary, so throttling costs more traffic than it saves.
- **Per-stream drop** (`trace` off, `calls` on). It needs protocol termination on both sides, with `libs/emulator`
  as the client half, and it must relay ack-byte commands in both directions. Deferred until a consumer appears.

### 5.4 Parameters

| Parameter | Default | Purpose |
|---|---|---|
| `endpoint` | `0.0.0.0:1715` | Bind address. Needs the `libs/server` change below. |
| `upstream` | none | `host:port` of the collector. Required unless `mode` is `discard`. |
| `mode` | `passthrough` | Default mode: `passthrough` or `discard`. |
| `connect_timeout` | `1s` | Upstream dial deadline. Matches nginx. |
| `upstream_tls` | off | mTLS to the upstream (§7). |
| `handshake_timeout` | `10s` | Deadline to read the handshake before closing an unidentified connection. |
| `max_connections` | `256` | Admission cap. Over the cap the connection is refused. |
| `copy_buffer_size` | `32KiB` | Splice buffer per direction in `passthrough`. |
| `discard.rotation_period` | `0` | `INIT_STREAM_V2` reply field in `discard`. |
| `discard.required_rotation_size` | `4MiB` | `INIT_STREAM_V2` reply field; the `libs/server` default. |
| `rules`, `rules_file` | none | Per-source overrides (§8). |
| `drain_timeout` | `10s` | Shutdown grace. |

Per connection, `passthrough` costs two goroutines, two descriptors, and about 80–100 KB; `discard` costs one
descriptor and about 40 KB. A per-namespace instance sees tens of connections, so the default cap of 256 is a
safety net, not a tuning target. Keep `max_connections` below half the pod's `nofile` limit.

**One change in `libs/server`.** `Service.Start` binds `net.Listen("tcp4", fmt.Sprintf(":%d", port))`
(`services.go:34`): IPv4 only, port only. Add `Endpoint string` to `ConnectionOpts` and fall back to the current
bind when it is empty.

## 6. Traces

### 6.1 The switch

Each protocol has its own pipeline, so the switch is per protocol. Three tools, cheapest first:

**Whole protocol off: swap the exporter to `nop`.** The receiver still accepts and answers success. Remove `batch`
from a dropping pipeline; batching on the way to `nop` costs memory for nothing.

```yaml
service:
  pipelines:
    traces/otlp:
      receivers: [otlp]
      processors: [memory_limiter]
      exporters: [nop]
```

**Selected sources off: `filterprocessor`.**

```yaml
processors:
  filter/drop_sources:
    error_mode: ignore
    traces:
      span:
        - 'resource.attributes["service.name"] == "chatty-service"'
```

A filter on `k8s.namespace.name` works only if the application's SDK sets that resource attribute. The gateway
sees the client pod's IP, so `k8sattributesprocessor` with connection-based association can add it. That is an
option for central mode, not a v1 item.

**Volume capped: `tailsamplingprocessor` with `bytes_limiting`.** The parameter is `bytes_per_second` (plus
`burst_capacity`):

```yaml
processors:
  tail_sampling:
    decision_wait: 5s
    policies:
      - name: cap-volume
        type: bytes_limiting
        bytes_limiting: { bytes_per_second: 1048576 }
```

Tail sampling holds every span in memory for `decision_wait` and needs all spans of a trace on one instance. With
`NUMBER_OF_PODS` above 1 behind a plain Service it is wrong without `loadbalancingexporter` in front. It is
documented as an option and left out of the default config.

Changing a pipeline is a Collector config change. The Collector applies it with a reload that restarts the
extensions too (§9), so flipping the trace switch also drops profiler TCP connections for one retry interval.

### 6.2 Behavior changes against the L4 forward

- The client now gets its answer from the Collector, not from Jaeger. Protocol versions or payloads the receiver
  rejects fail at the gateway.
- The exporter's queue and retry hide a short upstream outage from clients. For the drop goal that is an advantage;
  for debugging "where did my spans go" it is a new place to look, so the exporter's queue and failure metrics go
  on the dashboard.

### 6.3 Port 14267

diag-proxy forwards 14267 (Jaeger agent, TChannel) in the script, the chart, and the Service. The `jaeger`
receiver has no TChannel protocol, and its `thrift_compact` (6831) and `thrift_binary` (6832) are UDP, which
diag-proxy never carried. The gateway drops 14267. G0 checks whether anything still sends to it.

### 6.4 Probes and metrics

diag-proxy's chart probes `/probes/live` and `/probes/ready` on port 8888, exposed on the Service as
`agent-probes`. Port 8888 is also the Collector's default for its own metrics. The gateway chart keeps 8888 for
probes, so the Service stays compatible, and moves Collector metrics to 8889.

`qubership-otec` ships `healthcheckextension`, which serves one path. The gateway chart is new, so both probes
point at that one path. Separate liveness and readiness answers would need `healthcheckv2extension` in the
manifest; that is not worth a manifest change in v1.

### 6.5 Profiles signal

The Collector's profiles signal is not a path for profiler data. It is alpha and its model is pprof-shaped, not
call-tree-shaped.

## 7. TLS

diag-proxy has no server-side TLS. It listens in plaintext on every port. With `ESC_SSL_ENABLED=true` it
originates mTLS to the upstream with the client certificate and CA from `tlsConfig`; 1717 is only the default
upstream port in that mode.

- Both extensions get `upstream_tls` (`ca_file`, `cert_file`, `key_file`, `server_name`). There is no listener TLS
  in v1.
- The Go backend has no TLS listener for agents. Upstream mTLS matters only for installations that still run the
  legacy Java collector; G0 checks whether any exist. If none do, `upstream_tls` drops out of v1.
- nginx sets `proxy_ssl_name` on the stream forward but not on the HTTP forward, so the dump path sends no SNI
  today. The gateway reproduces that by default (`server_name` empty means no SNI on `profiler_dumps`) so a
  working installation does not change behavior silently.

## 8. Rules and hot reload

### 8.1 Syntax

```yaml
rules:
  - match:
      service: "chatty-*"            # glob; an omitted key matches anything
      dump_type: [hprof]             # profiler_dumps only; not a valid key in profiler_tcp rules
    mode: discard
  - match: { service: "billing" }
    mode: passthrough                # first match wins; no match falls back to the top-level mode
```

Match keys are `namespace`, `service`, and `pod` on both surfaces, plus `dump_type` on dumps. `service` is the key
to write rules on; pod names are ephemeral, and `pod` exists for debugging. On the dump path the URL carries
namespace and pod but no service, so `service` is not a valid key in `profiler_dumps` rules. Each extension
validates its own key set and rejects unknown keys.

`rules_file` entries are evaluated before inline `rules`. Both extensions may point at the same file; each reads
the keys it understands and ignores rules that use keys of the other surface only.

### 8.2 Reload

The Collector reloads its config in-process on `SIGHUP` and on a `confmap` provider's watch event. Any change to
an extension still shuts down and restarts every component, which drops all agent connections. A policy switch
should not do that, so the rules live in a separate mounted file that the extension watches with `fsnotify`.
Kubernetes updates a ConfigMap mount by swapping a symlink, so the watch is on the directory. A file that fails to
parse is rejected, the previous rules stay active, and a recoverable `componentstatus` event is reported.

**A mode change closes the affected TCP connections.** A profiler connection lives as long as the application pod,
so a rule that applied only to new connections would never take effect during an incident. On reload, the
extension re-evaluates every open connection against the new rules and closes those whose effective mode changed.
Each agent reconnects within 10 s into the new mode and resends its dictionary. `discard → passthrough` is
impossible any other way, because the upstream never saw that connection's handshake.

Dumps need nothing: every request is evaluated on arrival.

## 9. Remote management with OpAMP

- `cmd/opampsupervisor` wraps the Collector, receives remote config, writes it, and restarts the Collector. It
  delivers the whole config file, so both extensions are remote-managed like any stock component. Rollback to the
  last good config is off by default; set `automatic_config_rollback: true`.
- `extension/opampextension` runs in-process, reports effective config and health, and does not accept config.

Both extensions report status through `componentstatus` on bind failure, upstream unreachable, and rules reload
failure. Without that, OpAMP shows a healthy Collector while a listener is dead.

| What | Where | Change mechanism | Drops TCP connections |
|---|---|---|---|
| Structure: endpoints, upstreams, TLS, pipelines, caps | Collector config | OpAMP push, or ConfigMap and rollout | yes, all |
| Policy: modes and per-source rules | `rules_file` | ConfigMap edit, file watch | only those whose mode changed |

In per-namespace topology, a fleet-wide switch means editing N ConfigMaps. That is acceptable for v1 through the
deploy tooling. If it proves too slow in practice, OpAMP moves from optional to required, with policy inline in the
Collector config at the cost of a full restart per push.

## 10. Topology

### 10.1 Per-namespace (v1)

One instance per application namespace, as diag-proxy is deployed today. The chart keeps the Service name
(`SERVICE_NAME`), the port list minus 14267, and the existing values (`ESC_COLLECTOR_HOST`, `ESC_STATIC_HOST`,
`JAEGER_COLLECTOR_HOST`, `ZIPKIN_COLLECTOR_HOST`, `ESC_SSL_ENABLED`, `NUMBER_OF_PODS`, and the rest), so migration
is an image and chart swap with no agent or service change.

**Memory is the open cost.** diag-proxy requests 20Mi. The `qubership-otec` chart requests 100Mi for the same
binary in its usual role, and the difference is multiplied by the number of application namespaces. The shared
image also links components the gateway never configures (Prometheus, Graylog, Sentry). Linked but unconfigured
components cost image size more than resident memory, but that is an expectation, not a measurement. G1 measures
the idle and loaded RSS of the image under the gateway config and sets the chart's request and limit from the
numbers.

### 10.2 Central (deferred)

One gateway in the profiler namespace in front of the backend. The same binary supports it; what it adds:

- `namespace` rules and `max_connections_per_namespace`, `max_concurrent_uploads_per_namespace`, which are
  pointless when the namespace is fixed.
- Sizing for thousands of connections: about 100 MB and 2,048 descriptors per 1,024 `passthrough` agents, a raised
  `nofile`, and more than one replica.
- `k8sattributesprocessor` for trace filtering by namespace.
- Re-pointing every agent, which makes it a new product, not a diag-proxy replacement.

The per-namespace caps are not implemented in v1. The `namespace` match key is, because it costs nothing.

## 11. Implementation plan

Seven stages, each PR-sized per `docs/design/WORKFLOW.md` §2, each shippable on its own.

### Stage G0: stand checks

No code. Each answer closes an assumption in this plan.

1. Upload a heap dump larger than 1 MB through diag-proxy. Expected: 413. Decides whether §4.3 is a behavior fix.
2. Answer 204 before the body ends on a chunked upload from the real diagtools client. Does it report success or a
   broken pipe? Decides whether rule 3 in §4.2 can relax.
3. Find clients of port 14267. Decides whether §6.3 is a gap or a non-event.
4. Find installations with `ESC_SSL_ENABLED=true` or a legacy Java collector. Decides the scope of §7.

**Acceptance:** four recorded answers, and this plan updated where an answer contradicts it.

### Stage G1: traces on the shared distribution

- In `qubership-open-telemetry-collector`: add `nopexporter` and `memorylimiterprocessor` to `builder-config.yaml`,
  regenerate with `make install-builder build-collector`, and release an image.
- In `qubership-profiler-agent`: `deploy/charts/gateway/` running that image with the trace config from §3.4, minus
  the two extensions. Probes on 8888; Collector metrics on 8889.
- Carve `libs/wire` out of the root module and tag it (§3.2). No gateway code depends on it yet; doing it here
  keeps G2 and G3 free of repository plumbing.
- **Acceptance:** the chart starts and passes both probes; a synthetic span on each of OTLP gRPC, OTLP HTTP,
  Jaeger gRPC, Jaeger Thrift HTTP, and Zipkin reaches the upstream; switching one pipeline to `nop` drops that
  protocol while the client still sees success. Idle and loaded RSS are recorded against the 20Mi baseline. The
  backend builds and passes its tests against `libs/wire`.

### Stage G2: `profiler_dumps`

- The `apps/gateway` module: `internal/rules` (matching and file watch) and `internal/dumps`; the `profilerdumps`
  extension with `Validate()`, `Start`/`Shutdown`, and `componentstatus`.
- Tag `apps/gateway/v0.1.0` and add it to `builder-config.yaml`. Every later stage that changes gateway code ends
  the same way: tag, then a manifest bump and image release in the distribution repository.
- `PUT`, multipart `POST`, and `DELETE` forwarded with the request URI and `Content-Type` preserved.
- Streaming forward; `discard` with drain and 204; size and concurrency caps per §4.2; rules on `namespace`, `pod`,
  and `dump_type`; hot reload.
- A test for each of the four traps in §4.3.
- Dump metrics from §12.
- **Acceptance:** a 10 GB synthetic chunked upload transits in both modes with RSS growth under 128 KB per
  concurrent upload, measured, and finishes inside the client's 5 min deadline. The real diagtools binary uploads
  a heap dump, a GC log, and a thread dump end to end. In `discard`, and when over a cap, the client reports
  success and deletes its local copy. A rules-file edit changes the mode of the next request with no restart.

### Stage G3: `profiler_tcp` passthrough

- `Endpoint` added to `ConnectionOpts` in `libs/wire/server`, defaulting to today's behavior.
- `internal/tcp` with the bidirectional splice; the `profilertcp` extension; `max_connections` with refusal;
  `connect_timeout`; `componentstatus`.
- **Acceptance:** `libs/emulator` drives a synthetic agent through the gateway into a real `libs/server`, and the
  data arrives intact. With the upstream down, the agent side sees a close and retries. A benchmark records added
  latency and per-connection memory; `load-testing-report.md` §9 measured a ~40× ingest collapse at 2 s of added
  path latency, so the gateway's budget is milliseconds.

### Stage G4: TCP discard and rules

- Handshake sniffing with replay; rule matching on `namespace`, `service`, `pod`; `handshake_timeout`.
- `discard` through a null `Listener` from `libs/wire/server`.
- Reload that closes connections whose effective mode changed (§8.2).
- **Acceptance:** in `discard`, a synthetic agent completes the handshake, opens streams, sends several `RCV_DATA`,
  and flushes, with every ack drained and no `ACK_ERROR_MAGIC`, while nothing reaches the upstream. A rules-file
  edit moves a connected agent `passthrough → discard → passthrough` with one reconnect per step and no gateway
  restart. A replay test proves the upstream receives the handshake byte-identical.

### Stage G5: OpAMP and observability

- `opampextension` added to `builder-config.yaml`; supervisor config with `automatic_config_rollback: true`.
- Dashboard and an alert on a non-zero discard rate.
- Operator documentation for the structure and policy split (§9).
- **Acceptance:** a config push through the supervisor changes an upstream and the Collector returns with it; a
  broken push rolls back; a dead TCP listener shows as unhealthy in OpAMP.

### Stage G6: trace policy and cutover

- Documented `filterprocessor` and `tail_sampling` recipes, with the replica caveat from §6.1.
- Chart values mapped from the diag-proxy values (§10.1); `upstream_tls` parity if G0 found users; `nofile` set in
  the pod spec.
- Cutover notes: port 14267, changed trace error semantics (§6.2), dump size behavior (§4.3), memory request.
- **Acceptance:** one namespace runs on the gateway with no agent or service change, and trace, profiler, and dump
  volumes match the nginx baseline within measurement noise.

## 12. Metrics

Exposed on the Collector's Prometheus endpoint, labeled by surface, mode, and `service` where known.

| Metric | Type | Purpose |
|---|---|---|
| `profiler_gateway_connections_active` | gauge | Live agent connections, by mode. |
| `profiler_gateway_connections_total` | counter | Accepted connections, by mode. |
| `profiler_gateway_connections_refused_total` | counter | Refused by `max_connections`. |
| `profiler_gateway_connections_closed_by_reload_total` | counter | Closed because a rule change altered the mode. |
| `profiler_gateway_bytes_forwarded_total` | counter | Bytes relayed upstream. |
| `profiler_gateway_bytes_discarded_total` | counter | Bytes thrown away. The alarm for a forgotten switch. |
| `profiler_gateway_uploads_in_flight` | gauge | Concurrent uploads, against `max_concurrent_uploads`. |
| `profiler_gateway_upload_bytes_total` | counter | By dump type and outcome: forwarded, discarded, over cap. |
| `profiler_gateway_upstream_errors_total` | counter | Dial and transport failures, by surface. |
| `profiler_gateway_rules_reload_total` | counter | Reloads, by success or parse failure. |

## 13. Risks and open items

- **`discard` hides an outage.** Clients report success and the backend receives nothing. The discard counter must
  be alerted on, or a forgotten switch is silent data loss.
- **Memory per namespace.** See §10.1. If G1 shows the footprint is unacceptable across the fleet, the fallback is
  already in the layout: run `internal/dumps` and `internal/tcp` under a plain `main.go` and keep traces on a
  stock Collector or on nginx.
- **Two-repository releases.** A gateway fix is a tag in the profiler repository, then a manifest bump and an image
  release in the distribution repository. An incident fix takes two releases.
- **Collector version coupling.** The distribution bumps upstream with `make update-otel`, which rewrites its local
  modules only. If a bump breaks an API the extensions use, the distribution build fails until the profiler
  repository tags a fix, so the profiler extensions can block another team's upgrade. Keep the imports to
  `component`, `extension`, `confmap`, and `componentstatus`, and add the gateway module to that repository's
  Renovate rules so the bump and the extension update arrive together.
- **Shared image, shared blast radius.** The extensions ship in every `qubership-otec` image, including
  installations that never configure them. They must do nothing at init time and allocate nothing until started.
- **Handshake replay.** The one place where a bug corrupts framing instead of dropping a connection. Covered by
  the byte-identical test in G4.
- **Upload throughput.** A 10 GB dump needs about 34 MB/s to clear the client's 5 min deadline. Measured in G2.
- **Fleet-wide switch.** N ConfigMaps in per-namespace topology (§9).

Follow-ups outside this plan:

- The dumps collector's own "discard" (`apps/dumps-collector/src/nginx/entrypoint.sh:27`) answers 403, so clients
  keep their files and retry. Changing it to 204 gives a backend-side sink without the gateway, though without
  scale-to-zero.
- A diagtools change that treats 413 as "delete locally" would let a size cap save network as well as storage.
- What the agent does with buffered data after `BLACK_LISTED_RESP` is unverified. It matters only if `reject`
  returns.

Notes on the review:

- Its path corrections (`backend/libs/server` and so on) describe an older layout. The monorepo has
  `apps/agent/{dumper,diagtools,proto-definition}` and `libs/server` at the root, as the draft cited. Charts are
  under `deploy/charts/`.
- Its recommendation of a standalone binary plus a stock Collector sidecar was considered and not taken. The
  layout in §3.2 keeps that route open.
- Its point that the backend module cannot be fetched from another repository is right, and matters more now that
  the distribution is built elsewhere. The `libs/wire` module in §3.2 is the answer.
