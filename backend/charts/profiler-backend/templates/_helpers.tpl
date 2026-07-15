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

{{/* Extra env from a name → value map */}}
{{- define "profiler-backend.extraEnv" -}}
{{- range $name, $value := . }}
- name: {{ $name }}
  value: {{ $value | quote }}
{{- end }}
{{- end -}}

{{/* Env shared by both maintain modes: S3 + per-class retention TTLs (01 §9) */}}
{{- define "profiler-backend.maintainEnv" -}}
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
{{ include "profiler-backend.s3CredentialEnv" . }}
- name: PROFILER_RETENTION_SHORT_CLEAN_TTL
  value: {{ .Values.retention.shortCleanTTL | quote }}
- name: PROFILER_RETENTION_NORMAL_CLEAN_TTL
  value: {{ .Values.retention.normalCleanTTL | quote }}
- name: PROFILER_RETENTION_LONG_CLEAN_TTL
  value: {{ .Values.retention.longCleanTTL | quote }}
- name: PROFILER_RETENTION_ANY_ERROR_TTL
  value: {{ .Values.retention.anyErrorTTL | quote }}
- name: PROFILER_RETENTION_CORRUPTED_TTL
  value: {{ .Values.retention.corruptedTTL | quote }}
- name: PROFILER_RETENTION_DICTIONARY_TTL
  value: {{ .Values.retention.dictionaryTTL | quote }}
{{- include "profiler-backend.extraEnv" .Values.maintain.env }}
{{- end -}}
