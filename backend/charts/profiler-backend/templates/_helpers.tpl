{{/* Base name */}}
{{- define "profiler-backend.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified name */}}
{{- define "profiler-backend.fullname" -}}
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

{{/* Common labels; pass a dict with .root and .component */}}
{{- define "profiler-backend.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
app.kubernetes.io/version: {{ .root.Chart.AppVersion | quote }}
{{ include "profiler-backend.selectorLabels" . }}
{{- end -}}

{{/* Selector labels (stable across upgrades); pass a dict with .root and .component */}}
{{- define "profiler-backend.selectorLabels" -}}
app.kubernetes.io/name: {{ include "profiler-backend.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Container image reference */}}
{{- define "profiler-backend.image" -}}
{{- if .Values.image.registry -}}
{{- printf "%s/%s:%s" .Values.image.registry .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the S3 credentials */}}
{{- define "profiler-backend.s3SecretName" -}}
{{- if .Values.s3.auth.existingSecret -}}
{{- .Values.s3.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-s3" (include "profiler-backend.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Effective S3 endpoint: explicit value, or the in-cluster MinIO */}}
{{- define "profiler-backend.s3Endpoint" -}}
{{- if .Values.s3.endpoint -}}
{{- .Values.s3.endpoint -}}
{{- else if .Values.minio.enabled -}}
{{- printf "http://%s-minio:9000" (include "profiler-backend.fullname" .) -}}
{{- else -}}
{{- fail "set s3.endpoint, or enable the dev MinIO with minio.enabled=true" -}}
{{- end -}}
{{- end -}}

{{/* Headless service name (collector discovery + governing service) */}}
{{- define "profiler-backend.collectorHeadless" -}}
{{- printf "%s-collector-headless" (include "profiler-backend.fullname" .) -}}
{{- end -}}

{{/*
Gateway API parentRefs shared by HTTPRoute objects (04 §12; platform
convention set by PR #798). Blue/green (BGD2): when PEER_NAMESPACE is set,
the parentRef targets the edge-router Gateway in CONTROLLER_NAMESPACE
instead of the cluster's default external gateway.
*/}}
{{- define "gateway.parentRefs" -}}
{{- if (default "" .Values.PEER_NAMESPACE) -}}
- group: gateway.networking.k8s.io
  kind: Gateway
  name: edge-router
  namespace: {{ .Values.CONTROLLER_NAMESPACE | default "bluegreen-controller" }}
{{- else -}}
- group: gateway.networking.k8s.io
  kind: Gateway
  name: {{ .Values.GATEWAY_SYSTEM_NAME | default "default-external-gateway" }}
  namespace: {{ .Values.GATEWAY_SYSTEM_NAMESPACE | default "gateway-system" }}
{{- end -}}
{{- end -}}

{{/* Default query Ingress host, unless query.ingress.host is set explicitly.
Fails fast rather than render an invalid "…-query-." host when the platform
globals are absent (PR 708 review #11). */}}
{{- define "profiler-backend.queryIngressHost" -}}
{{- if .Values.query.ingress.host -}}
{{ .Values.query.ingress.host }}
{{- else if .Values.CLOUD_PUBLIC_HOST -}}
{{- printf "%s-query-%s.%s" (include "profiler-backend.fullname" .) (.Values.NAMESPACE | default .Release.Namespace) .Values.CLOUD_PUBLIC_HOST -}}
{{- else -}}
{{- fail "query.ingress is enabled but no host can be built: set query.ingress.host, or provide CLOUD_PUBLIC_HOST (the cluster public domain, injected on Qubership-managed clusters)" -}}
{{- end -}}
{{- end -}}

{{/* Default query HTTPRoute hostname, unless query.httpRoute.host is set
explicitly. Same fail-fast as the Ingress host above (PR 708 review #11). */}}
{{- define "profiler-backend.queryHttpRouteHost" -}}
{{- if .Values.query.httpRoute.host -}}
{{ .Values.query.httpRoute.host }}
{{- else if .Values.CLOUD_PUBLIC_HOST -}}
{{- printf "%s-query-%s.eg.%s" (include "profiler-backend.fullname" .) (.Values.NAMESPACE | default .Release.Namespace) .Values.CLOUD_PUBLIC_HOST -}}
{{- else -}}
{{- fail "query.httpRoute is enabled but no host can be built: set query.httpRoute.host, or provide CLOUD_PUBLIC_HOST (the cluster public domain, injected on Qubership-managed clusters)" -}}
{{- end -}}
{{- end -}}

{{/*
S3 connection env shared by all three workloads: endpoint, bucket, and the
per-deployment key prefix, all from the ConfigMap (04 §6).
*/}}
{{- define "profiler-backend.s3ConfigEnv" -}}
- name: S3_ENDPOINT
  valueFrom:
    configMapKeyRef:
      name: {{ include "profiler-backend.fullname" . }}-config
      key: s3-endpoint
- name: S3_BUCKET
  valueFrom:
    configMapKeyRef:
      name: {{ include "profiler-backend.fullname" . }}-config
      key: s3-bucket
- name: S3_PATH_PREFIX
  valueFrom:
    configMapKeyRef:
      name: {{ include "profiler-backend.fullname" . }}-config
      key: s3-path-prefix
{{- end -}}

{{/*
S3 credential wiring shared by all three workloads: the Secret mounts as a
volume and the *_FILE envs point into it — the values never appear in the pod
spec (01-write-contract.md §9, 04 §6).
*/}}
{{- define "profiler-backend.s3CredentialEnv" -}}
- name: S3_ACCESS_KEY_FILE
  value: {{ printf "%s/access-key" .Values.s3.auth.mountPath }}
- name: S3_SECRET_KEY_FILE
  value: {{ printf "%s/secret-key" .Values.s3.auth.mountPath }}
{{- end -}}

{{- define "profiler-backend.s3CredentialMount" -}}
- name: s3-credentials
  mountPath: {{ .Values.s3.auth.mountPath }}
  readOnly: true
{{- end -}}

{{- define "profiler-backend.s3CredentialVolume" -}}
- name: s3-credentials
  secret:
    secretName: {{ include "profiler-backend.s3SecretName" . }}
{{- end -}}

{{/* Name of the Secret holding the S3 CA bundle */}}
{{- define "profiler-backend.s3TlsSecretName" -}}
{{- if .Values.s3.tls.existingSecret -}}
{{- .Values.s3.tls.existingSecret -}}
{{- else -}}
{{- printf "%s-s3-ca" (include "profiler-backend.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* "true" when a private CA bundle is configured, empty otherwise */}}
{{- define "profiler-backend.s3TlsEnabled" -}}
{{- if or .Values.s3.tls.caCert .Values.s3.tls.existingSecret -}}
true
{{- end -}}
{{- end -}}

{{/*
S3 TLS env shared by all three workloads: S3_CA_FILE points into the CA
volume when one is configured, S3_INSECURE_SKIP_VERIFY is the dev/smoke
escape hatch. Both default off — an https:// endpoint with a
publicly-trusted certificate needs neither.
*/}}
{{- define "profiler-backend.s3TlsEnv" -}}
{{- if eq (include "profiler-backend.s3TlsEnabled" .) "true" }}
- name: S3_CA_FILE
  value: {{ printf "%s/ca.crt" .Values.s3.tls.mountPath }}
{{- end }}
{{- if .Values.s3.tls.insecureSkipVerify }}
- name: S3_INSECURE_SKIP_VERIFY
  value: "true"
{{- end }}
{{- end -}}

{{- define "profiler-backend.s3TlsMount" -}}
{{- if eq (include "profiler-backend.s3TlsEnabled" .) "true" }}
- name: s3-ca
  mountPath: {{ .Values.s3.tls.mountPath }}
  readOnly: true
{{- end }}
{{- end -}}

{{- define "profiler-backend.s3TlsVolume" -}}
{{- if eq (include "profiler-backend.s3TlsEnabled" .) "true" }}
- name: s3-ca
  secret:
    secretName: {{ include "profiler-backend.s3TlsSecretName" . }}
{{- end }}
{{- end -}}

{{/*
Pod securityContext shared by all three workloads; pass a dict with .root and
.component (values key: collector, query, or maintain). fsGroup is what makes
a PVC (the collector's /data) writable by the non-root container — without it
the volume mounts owned by root and the container can't write to it. 65532 is
gcr.io/distroless/static-debian12:nonroot's built-in uid:gid (the image's
USER, apps/profiler-backend/Dockerfile) — keep this in sync with the
Dockerfile if that base image ever changes. Values overrides win over these
defaults; skip runAsUser/fsGroup on OpenShift, which assigns both from the
namespace's SCC range.
*/}}
{{- define "profiler-backend.podSecurityContext" -}}
{{- $override := deepCopy ((index .root.Values .component).securityContext | default dict) -}}
{{- $defaults := dict "seccompProfile" (dict "type" "RuntimeDefault") -}}
{{- if not (.root.Capabilities.APIVersions.Has "apps.openshift.io/v1") -}}
{{- $defaults = merge $defaults (dict "runAsUser" 65532 "fsGroup" 65532) -}}
{{- end -}}
{{- $merged := merge $override $defaults -}}
{{/* merge treats explicit "false" as unset, so runAsNonRoot needs its own pass */}}
{{- $_ := set $merged "runAsNonRoot" (ne ($override.runAsNonRoot | toString) "false") -}}
{{- toYaml $merged }}
{{- end -}}

{{/*
Container securityContext shared by all three workloads; pass a dict with
.root and .component. Values overrides win over these defaults.
*/}}
{{- define "profiler-backend.containerSecurityContext" -}}
{{- $override := deepCopy ((index .root.Values .component).containerSecurityContext | default dict) -}}
{{- $defaults := dict "allowPrivilegeEscalation" false "capabilities" (dict "drop" (list "ALL")) -}}
{{- toYaml (merge $override $defaults) }}
{{- end -}}

{{/* Extra env from a name → value map */}}
{{- define "profiler-backend.extraEnv" -}}
{{- range $name, $value := . }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end }}
{{- end -}}

{{/*
The clean-tier duration thresholds shared by the collector's write-time
classification and the query's read pruning (01 §6.4). ONE values key renders
into BOTH workloads, so they can never drift — a collect/query mismatch
silently drops rows from /calls. Empty keeps the built-in tier-table defaults
(100ms,1s,10s) in both binaries, which are consistent by construction.
*/}}
{{- define "profiler-backend.durationThresholdsEnv" -}}
{{- with .Values.retention.durationThresholds }}
- name: PROFILER_DURATION_THRESHOLDS
  value: {{ . | quote }}
{{- end }}
{{- end -}}

{{/*
Env shared by both maintain modes: S3 + per-class retention TTLs (01 §9).
The TTL values mirror the tier table of 01 §6.4 (№10): thresholds classify at
the collector, these expire at the maintainer — override them together.
*/}}
{{- define "profiler-backend.maintainEnv" -}}
{{ include "profiler-backend.s3ConfigEnv" . }}
{{ include "profiler-backend.s3CredentialEnv" . }}
{{ include "profiler-backend.s3TlsEnv" . }}
- name: PROFILER_RETENTION_SHORT_CLEAN_TTL
  value: {{ .Values.retention.shortCleanTTL | quote }}
- name: PROFILER_RETENTION_NORMAL_CLEAN_TTL
  value: {{ .Values.retention.normalCleanTTL | quote }}
- name: PROFILER_RETENTION_LONG_CLEAN_TTL
  value: {{ .Values.retention.longCleanTTL | quote }}
- name: PROFILER_RETENTION_HUGE_CLEAN_TTL
  value: {{ .Values.retention.hugeCleanTTL | quote }}
- name: PROFILER_RETENTION_ANY_ERROR_TTL
  value: {{ .Values.retention.anyErrorTTL | quote }}
- name: PROFILER_RETENTION_CORRUPTED_TTL
  value: {{ .Values.retention.corruptedTTL | quote }}
- name: PROFILER_RETENTION_PODS_TTL
  value: {{ .Values.retention.podsTTL | quote }}
{{- include "profiler-backend.extraEnv" .Values.maintain.env }}
{{- end -}}
