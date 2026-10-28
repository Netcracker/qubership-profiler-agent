# gateway Helm chart

Runs the `qubership-otec` OpenTelemetry Collector as a diagnostic gateway: a drop-in replacement for the nginx-based
diag-proxy that can forward or discard each trace protocol without touching the services that send it. The design is in
`docs/plans/2026-10-09-otel-gateway.md`.

Traces are on by default. Dump uploads (8080) and profiler agent connections (1715) are behind `dumps.enabled` and
`tcp.enabled`, because they need a `qubership-otec` image that includes the extensions from `apps/gateway`.

## Install

```bash
helm install gateway deploy/charts/gateway \
  --set serviceName=nc-diagnostic-agent \
  --set traces.upstream.endpoint=jaeger-collector.tracing.svc:4317
```

The image must include the `nop` exporter and the `memory_limiter` processor. qubership-otec 1.4.0 has neither; use a
build of `qubership-open-telemetry-collector` that adds them, and pin its tag in `image.tag`.

## Traffic

| Port | Service port name | Protocol | Values switch |
|---|---|---|---|
| 4317 | `otel-grpc` | OTLP gRPC | `traces.otlp` |
| 4318 | `otel-http` | OTLP HTTP | `traces.otlp` |
| 14250 | `jaeger-grpc` | Jaeger gRPC | `traces.jaeger` |
| 14268 | `jaeger-thrift` | Jaeger Thrift over HTTP | `traces.jaeger` |
| 9411 | `zipkin` | Zipkin HTTP | `traces.zipkin` |
| 8888 | `agent-probes` | health check, `/probes/ready` | `ports.probes` |
| 8889 | `metrics` | Collector metrics, Prometheus format | `ports.metrics` |

The port names and numbers are diag-proxy's. Port 14267 (Jaeger TChannel) is gone: the Collector has no receiver for it.

Every protocol is decoded and sent on as OTLP gRPC to `traces.upstream.endpoint`. diag-proxy forwarded raw bytes
instead, so a client now gets its answer from the gateway, not from Jaeger, and a payload the receiver rejects fails at
the gateway.

## Switching a protocol off

Each protocol has its own pipeline and its own switch:

```yaml
traces:
  zipkin:
    mode: discard
```

| Setting | Effect |
|---|---|
| `mode: passthrough` | Spans go to `traces.upstream`. The default. |
| `mode: discard` | Spans are accepted, answered with success, and dropped. Clients stop retrying and logging failures. |
| `enabled: false` | The receiver, the pipeline, and the Service ports are removed. Clients get connection refused. |

A mode change is a config change, and the Collector restarts on it. The pod template carries a checksum of the config,
so `helm upgrade` rolls the pods.

`discard` hides an outage: clients report success and the upstream receives nothing. Watch the receivers' accepted-span
metrics against the exporter's sent-span metrics on port 8889.

## Dump uploads

```yaml
dumps:
  enabled: true
  upstream: http://cloud-profiler-dumps-collector.profiler.svc:8080
  mode: passthrough
rules:
  - match: { namespace: "load-test-*", dump_type: [hprof] }
    mode: discard
```

This adds the `cdt-http` port (8080) and the `profiler_dumps` extension. In `discard`, and for an upload over a cap, the
gateway reads the upload to the end and answers 204, so diagtools deletes its local copy. The extension's behavior,
keys, and metrics are in `apps/gateway/README.md`.

`rules` renders into a separate ConfigMap that the extension watches. Editing that ConfigMap changes the mode of the
next upload without a restart, once the kubelet updates the mounted file (up to a minute). `dumps.mode` is part of the
Collector config, so changing it restarts the pods.

## Profiler agent connections

```yaml
tcp:
  enabled: true
  upstream: profiler-collector-agent.profiler.svc:1715
  mode: passthrough
rules:
  - match: { service: "chatty-*" }
    mode: discard
```

This adds the `cdt-tcp` port (1715) and the `profiler_tcp` extension. In `discard` the gateway answers the agent
protocol itself and drops the data, so agents stay quiet and alive; they cannot receive heap or thread dump requests
from the UI meanwhile. A `rules` change closes the connections whose mode it changes, and those agents reconnect
within 10 seconds into the new mode. Each passthrough connection takes two file descriptors; keep
`tcp.maxConnections` below half the pod's open-file limit.

Both extensions read the one `rules` list. Each skips the rules that use a key only the other one knows.

## Custom config

`config` is merged over the rendered Collector config, last. Maps merge key by key; lists are replaced whole. For
example, to drop the spans of one service:

```yaml
config:
  processors:
    filter/drop_sources:
      error_mode: ignore
      traces:
        span:
          - 'resource.attributes["service.name"] == "chatty-service"'
  service:
    pipelines:
      traces/otlp:
        processors: [memory_limiter, filter/drop_sources, batch]
```

## Resources

The defaults (100Mi request, 500Mi limit) are placeholders until the idle and loaded footprint is measured. diag-proxy
requested 20Mi. `memory_limiter` works in percentages of the container memory limit, so keep
`resources.limits.memory` set.

## Tests

```bash
helm unittest deploy/charts/gateway
make helm-lint
```
