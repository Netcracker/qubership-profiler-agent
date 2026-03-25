{{/* vim: set filetype=mustache: */}}

{{/******************************************************************************************************************/}}

{{/*
Create common labels for each resource which is creating by this chart.
*/}}
{{- define "common.namedLabels" -}}
app: {{ .name }}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .name }}
{{- if .serviceMonitor }}
  {{- print "app.kubernetes.io/component: monitoring" | nindent 0 }}
{{- else }}
  {{- printf "%s: %s" "app.kubernetes.io/component" .name | nindent 0 }}
{{- end }}
{{- if .labels }}
{{ .labels | toYaml }}
{{- end }}
{{- end -}}

{{/*
Create common labels for each resource which is creating by this chart.
*/}}
{{- define "common.commonLabels" -}}
app.kubernetes.io/part-of: cloud-profiler
app.kubernetes.io/managed-by: helm
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Find a compactor image in various places.
Image can be found from:
* .Values.compactor.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler-compactor" "image"
*/}}
{{- define "compactor.image" -}}
  {{- if .Values.compactor.image -}}
    {{- printf "%s" .Values.compactor.image -}}
  {{- else -}}
    {{- if .Values.global -}}
      {{- if .Values.global.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-compactor" "image") -}}
      {{- end -}}
    {{- else -}}
      {{- if .Values.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-compactor" "image") -}}
      {{- else -}}
        {{- print "product/prod.platform.cloud.infra_profiler_cdt-cloud-profiler-compactor:master_latest" -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}


{{/*
Template to insert envs for S3 storage
*/}}
{{- define "s3.envs" -}}
- name: MINIO_ENDPOINT
  {{- if .Values.cloud.s3.endpoint }}
  value: {{ .Values.cloud.s3.endpoint }}
  {{- else if .Values.INFRA_S3_MINIO_ENDPOINT }}
  value: {{ .Values.INFRA_S3_MINIO_ENDPOINT }}
  {{- end }}
- name: MINIO_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.compactor.name }}-s3-credentials
      key: accessKey
- name: MINIO_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.compactor.name }}-s3-credentials
      key: secretKey
- name: MINIO_BUCKET
  value: {{ .Values.cloud.s3.bucket }}
{{- end -}}

{{/*
Template to insert envs for Postgres
*/}}
{{- define "pg.envs" -}}
- name: POSTGRES_URL
  {{- if and .Values.cloud.postgres.host .Values.cloud.postgres.port }}
  value: {{ .Values.cloud.postgres.host }}:{{ .Values.cloud.postgres.port }}
  {{- else if .Values.INFRA_POSTGRES_HOST }}
  value: {{ .Values.INFRA_POSTGRES_HOST }}:{{ .Values.INFRA_POSTGRES_PORT }}
  {{- end }}
- name: POSTGRES_USER
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.compactor.name }}-pg-credentials
      key: username
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.compactor.name }}-pg-credentials
      key: password
  {{- if .Values.cloud.postgres.database }}
- name: POSTGRES_DB
  value: {{ .Values.cloud.postgres.database }}
  {{- end }}
{{- end -}}

{{/*
Template to insert pg credentials username
*/}}
{{- define "pg.creds.username" -}}
{{- if .Values.cloud.postgres.username -}}
{{ .Values.cloud.postgres.username }}
{{- else if .Values.INFRA_POSTGRES_ADMIN_USERNAME -}}
{{ .Values.INFRA_POSTGRES_ADMIN_USERNAME }}
{{- end -}}
{{- end -}}

{{/*
Template to insert pg credentials password
*/}}
{{- define "pg.creds.password" -}}
{{- if .Values.cloud.postgres.password -}}
{{ .Values.cloud.postgres.password }}
{{- else if .Values.INFRA_POSTGRES_ADMIN_PASSWORD -}}
{{ .Values.INFRA_POSTGRES_ADMIN_PASSWORD }}
{{- end -}}
{{- end -}}

{{/*
Template to insert s3 credentials access key
*/}}
{{- define "s3.creds.accessKey" -}}
{{- if .Values.cloud.s3.accessKey -}}
{{ .Values.cloud.s3.accessKey }}
{{- else if .Values.INFRA_S3_MINIO_ACCESSKEY -}}
{{ .Values.INFRA_S3_MINIO_ACCESSKEY }}
{{- end -}}
{{- end -}}

{{/*
Template to insert s3 credentials secret key
*/}}
{{- define "s3.creds.secretKey" -}}
{{- if .Values.cloud.s3.secretKey -}}
{{ .Values.cloud.s3.secretKey }}
{{- else if .Values.INFRA_S3_MINIO_SECRETKEY -}}
{{ .Values.INFRA_S3_MINIO_SECRETKEY }}
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to insert envs for CDT
*/}}
{{- define "cdt.envs" -}}
  {{- if .Values.cloud.cdt.invertedIndex.granularity }}
- name: INVERTED_INDEX_GRANULARITY
  value: "{{ .Values.cloud.cdt.invertedIndex.granularity }}"
  {{- end }}
   {{- if .Values.cloud.cdt.invertedIndex.lifetime }}
- name: INVERTED_INDEX_LIFETIME
  value: "{{ .Values.cloud.cdt.invertedIndex.lifetime }}"
  {{- end }}
  {{- if .Values.cloud.cdt.invertedIndex.params }}
- name: INVERTED_INDEX_PARAMS
  value: "{{ join "," .Values.cloud.cdt.invertedIndex.params }}"
  {{- end }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to insert args for command line arguments
*/}}
{{- define "compactor.args" -}}
- "run"
- "--run.cron"
{{- if .Values.cloud.s3.tls.insecure }}
- "--minio.insecure"
{{- end -}}
{{- if .Values.cloud.s3.tls.useSSL }}
- "--minio.use_ssl"
{{- end -}}
{{- if .Values.cloud.s3.tls.ca }}
- "--minio.ca_file=/tls/s3/ca.crt"
{{- end }}
- "--pg.ssl_mode={{ .Values.cloud.postgres.tls.sslMode }}"
{{- if .Values.cloud.postgres.tls.ca }}
- "--pg.ca_file=/tls/pg/ca.crt"
{{- end -}}
{{- end -}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "compactor.envs" -}}
- name: CRON_SCHEDULE
  value: {{ .Values.compactor.cron | quote }}
- name: LOG_LEVEL
  value: {{ .Values.compactor.log.level }}
- name: OUTPUT_DIR
  value: '/output'
{{- include "s3.envs" . | nindent 0 }}
{{- include "pg.envs" . | nindent 0 }}
{{- include "cdt.envs" . | nindent 0 }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to generate Pod SecurityContext
*/}}
{{- define "compactor.podSecurityContext" -}}
  {{- if .Values.compactor.securityContext }}
    {{ toYaml .Values.compactor.securityContext | indent 2 }}
  {{- end }}
  {{- if not (.Capabilities.APIVersions.Has "apps.openshift.io/v1") }}
    {{- if not .Values.compactor.securityContext.runAsUser }}
      runAsUser: 2000
    {{- end }}
    {{- if not .Values.compactor.securityContext.fsGroup }}
      fsGroup: 2000
    {{- end }}
  {{- end }}
  {{- if (eq (.Values.compactor.securityContext.runAsNonRoot | toString) "false") }}
      runAsNonRoot: false
  {{- else }}
      runAsNonRoot: true
  {{- end }}
  {{- if not .Values.compactor.securityContext.seccompProfile }}
      seccompProfile:
        type: "RuntimeDefault"
  {{- end }}
{{- end -}}

{{/*
Template to generate Container SecurityContext
*/}}
{{- define "compactor.containerSecurityContext" -}}
{{- if .Values.compactor.containerSecurityContext }}
{{- toYaml .Values.compactor.containerSecurityContext -}}
{{- else -}}
allowPrivilegeEscalation: false
capabilities:
  drop:
  - ALL
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Return resources for monitoring-operator by HWE profile.
*/}}
{{- define "compactor.resources" -}}
  {{- if .Values.compactor.resources }}
    {{- toYaml .Values.compactor.resources | nindent 4 }}
  {{- else if eq .Values.profile "small" }}
    limits:
      cpu: "200m"
      memory: "160Mi"
    requests:
      cpu: "100m"
      memory: "75Mi"
  {{- else if eq .Values.profile "medium" }}
    limits:
      cpu: "200m"
      memory: "160Mi"
    requests:
      cpu: "100m"
      memory: "75Mi"
  {{- else if eq .Values.profile "large" }}
    limits:
      cpu: "200m"
      memory: "160Mi"
    requests:
      cpu: "100m"
      memory: "75Mi"
  {{- else -}}
    limits:
      cpu: "200m"
      memory: "160Mi"
    requests:
      cpu: "100m"
      memory: "75Mi"
  {{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}
