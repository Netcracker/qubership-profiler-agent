{{/* Base name */}}
{{- define "gateway.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified name */}}
{{- define "gateway.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Service name; agents rewrite the two legacy collector names in a host */}}
{{- define "gateway.serviceName" -}}
{{- $name := default (include "gateway.fullname" .) .Values.serviceName -}}
{{- range list "esc-collector-service" "esc-static-service" -}}
{{- if contains . $name -}}
{{- fail (printf "serviceName %q contains %q, which the Java agent and diagtools rewrite in the host they connect to; choose another name" $name .) -}}
{{- end -}}
{{- end -}}
{{- $name -}}
{{- end -}}

{{/* Common labels */}}
{{- define "gateway.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{ include "gateway.selectorLabels" . }}
{{- end -}}

{{/* Selector labels (stable across upgrades) */}}
{{- define "gateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "gateway.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: gateway
{{- end -}}

{{/* Container image reference */}}
{{- define "gateway.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}

{{/*
Trace protocols, each with its receiver config and Service ports. The ports
keep the names and numbers diag-proxy used.
*/}}
{{- define "gateway.protocols" -}}
otlp:
  receiver:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  ports:
    - { name: otel-grpc, port: 4317 }
    - { name: otel-http, port: 4318 }
jaeger:
  receiver:
    protocols:
      grpc:
        endpoint: 0.0.0.0:14250
      thrift_http:
        endpoint: 0.0.0.0:14268
  ports:
    - { name: jaeger-grpc, port: 14250 }
    - { name: jaeger-thrift, port: 14268 }
zipkin:
  receiver:
    endpoint: 0.0.0.0:9411
  ports:
    - { name: zipkin, port: 9411 }
{{- end -}}

{{/* Whether any profiler extension is on, so the rules ConfigMap is needed */}}
{{- define "gateway.profilerEnabled" -}}
{{- if or .Values.dumps.enabled .Values.tcp.enabled -}}true{{- end -}}
{{- end -}}

{{/* Where the extensions read the rules file */}}
{{- define "gateway.rulesFile" -}}/etc/gateway/rules/rules.yaml{{- end -}}

{{/* Service and container ports of the profiler extensions */}}
{{- define "gateway.profilerPorts" -}}
ports:
{{- if .Values.tcp.enabled }}
  - { name: cdt-tcp, port: 1715 }
{{- end }}
{{- if .Values.dumps.enabled }}
  - { name: cdt-http, port: 8080 }
{{- end }}
{{- end -}}

{{/* Names of the enabled protocols, in a fixed order, as a JSON list */}}
{{- define "gateway.enabledProtocols" -}}
{{- $enabled := list -}}
{{- range $name := list "otlp" "jaeger" "zipkin" -}}
{{- $p := index $.Values.traces $name -}}
{{- if $p.enabled -}}
{{- if not (has $p.mode (list "passthrough" "discard")) -}}
{{- fail (printf "traces.%s.mode is %q; use passthrough or discard" $name (toString $p.mode)) -}}
{{- end -}}
{{- $enabled = append $enabled $name -}}
{{- end -}}
{{- end -}}
{{- if not $enabled -}}
{{- fail "every trace protocol is disabled; enable at least one of traces.otlp, traces.jaeger, traces.zipkin" -}}
{{- end -}}
{{- toJson $enabled -}}
{{- end -}}

{{/*
The Collector config. A discarding pipeline exports to `nop` and skips
`batch`, which would only hold spans in memory on their way to nowhere. The
`nop` and `otlp_grpc` exporters are declared only when a pipeline uses them.
*/}}
{{- define "gateway.config" -}}
{{- $protocols := include "gateway.protocols" . | fromYaml -}}
{{- $enabled := include "gateway.enabledProtocols" . | fromJsonArray -}}
{{- $receivers := dict -}}
{{- $exporters := dict -}}
{{- $pipelines := dict -}}
{{- range $name := $enabled -}}
{{- $_ := set $receivers $name (index $protocols $name "receiver") -}}
{{- if eq (index $.Values.traces $name "mode") "discard" -}}
{{- $_ := set $exporters "nop" dict -}}
{{- $_ := set $pipelines (printf "traces/%s" $name) (dict "receivers" (list $name) "processors" (list "memory_limiter") "exporters" (list "nop")) -}}
{{- else -}}
{{- $_ := set $exporters "otlp_grpc/upstream" (deepCopy $.Values.traces.upstream) -}}
{{- $_ := set $pipelines (printf "traces/%s" $name) (dict "receivers" (list $name) "processors" (list "memory_limiter" "batch") "exporters" (list "otlp_grpc/upstream")) -}}
{{- end -}}
{{- end -}}
{{- $extensions := dict
  "health_check" (dict
    "endpoint" (printf "0.0.0.0:%d" (int .Values.ports.probes))
    "path" "/probes/ready") -}}
{{- $extensionIDs := list "health_check" -}}
{{- with .Values.dumps -}}
{{- if .enabled -}}
{{- if not (has .mode (list "passthrough" "discard")) -}}
{{- fail (printf "dumps.mode is %q; use passthrough or discard" (toString .mode)) -}}
{{- end -}}
{{- $d := dict
  "endpoint" "0.0.0.0:8080"
  "mode" .mode
  "max_body_size" .maxBodySize
  "max_concurrent_uploads" (int .maxConcurrentUploads)
  "rules_file" (include "gateway.rulesFile" $) -}}
{{- if .upstream -}}
{{- $_ := set $d "upstream" .upstream -}}
{{- end -}}
{{- with .maxBodySizePerType -}}
{{- $_ := set $d "max_body_size_per_type" . -}}
{{- end -}}
{{- $_ := set $extensions "profiler_dumps" $d -}}
{{- $extensionIDs = append $extensionIDs "profiler_dumps" -}}
{{- end -}}
{{- end -}}
{{- with .Values.tcp -}}
{{- if .enabled -}}
{{- if not (has .mode (list "passthrough" "discard")) -}}
{{- fail (printf "tcp.mode is %q; use passthrough or discard" (toString .mode)) -}}
{{- end -}}
{{- $c := dict
  "endpoint" "0.0.0.0:1715"
  "mode" .mode
  "max_connections" (int .maxConnections)
  "rules_file" (include "gateway.rulesFile" $) -}}
{{- if .upstream -}}
{{- $_ := set $c "upstream" .upstream -}}
{{- end -}}
{{- $_ := set $extensions "profiler_tcp" $c -}}
{{- $extensionIDs = append $extensionIDs "profiler_tcp" -}}
{{- end -}}
{{- end -}}
{{- $ml := .Values.memoryLimiter -}}
{{- $config := dict
  "extensions" $extensions
  "receivers" $receivers
  "processors" (dict
    "memory_limiter" (dict
      "check_interval" $ml.checkInterval
      "limit_percentage" $ml.limitPercentage
      "spike_limit_percentage" $ml.spikeLimitPercentage)
    "batch" dict)
  "exporters" $exporters
  "service" (dict
    "telemetry" (dict
      "metrics" (dict
        "readers" (list (dict
          "pull" (dict
            "exporter" (dict
              "prometheus" (dict
                "host" "0.0.0.0"
                "port" (int .Values.ports.metrics))))))))
    "extensions" $extensionIDs
    "pipelines" $pipelines)
-}}
{{- $config = mergeOverwrite $config (deepCopy .Values.config) -}}
{{- toYaml $config -}}
{{- end -}}
