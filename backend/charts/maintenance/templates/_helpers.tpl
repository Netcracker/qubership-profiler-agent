{{/* vim: set filetype=mustache: */}}

{{/*
Create common labels for each resource which is creating by this chart.
*/}}
{{- define "common.namedLabels" -}}
app: {{ .name }}
app.kubernetes.io/name: {{ .name }}
app.kubernetes.io/instance: {{ .name }}
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
Find a maintenance job image in various places.
Image can be found from:
* .Values.maintenanceJob.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler-maintenance-job" "image"
*/}}
{{- define "maintenanceJob.image" -}}
  {{- if .Values.maintenanceJob.image -}}
    {{- printf "%s" .Values.maintenanceJob.image -}}
  {{- else -}}
    {{- if .Values.global -}}
      {{- if .Values.global.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-maintenance-job" "image") -}}
      {{- end -}}
    {{- else -}}
      {{- if .Values.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-maintenance-job" "image") -}}
      {{- else -}}
        {{- print "product/prod.platform.cloud.infra_profiler_cdt-cloud-profiler-maintenance-job:master_latest" -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Find a maintenance job image in various places.
Image can be found from:
* .Values.maintenanceJob.migrateSchema.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler-maintenance-job" "image"
*/}}
{{- define "maintenanceJob.migrateSchema.image" -}}
  {{- if .Values.maintenanceJob.migrateSchema.image -}}
    {{- printf "%s" .Values.maintenanceJob.migrateSchema.image -}}
  {{- else -}} 
    {{- if .Values.maintenanceJob.image -}}
      {{- printf "%s" .Values.maintenanceJob.image -}}
    {{- else -}}
      {{- if .Values.global -}}
        {{- if .Values.global.deployDescriptor -}}
          {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-maintenance-job" "image") -}}
        {{- end -}}
      {{- else -}}
        {{- if .Values.deployDescriptor -}}
          {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-maintenance-job" "image") -}}
        {{- else -}}
          {{- print "product/prod.platform.cloud.infra_profiler_cdt-cloud-profiler-maintenance-job:master_latest" -}}
        {{- end -}}
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
      name: {{ .Values.maintenanceJob.name }}-s3-credentials
      key: accessKey
- name: MINIO_SECRET_ACCESS_KEY
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.maintenanceJob.name }}-s3-credentials
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
      name: {{ .Values.maintenanceJob.name }}-pg-credentials
      key: username
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef: 
      name: {{ .Values.maintenanceJob.name }}-pg-credentials
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
{{- define "maintenanceJob.args" -}}
- "run"
{{- if .Values.cloud.s3.tls.insecure }}
- "--minio.insecure"
{{- end }}
{{- if .Values.cloud.s3.tls.useSSL }}
- "--minio.use_ssl"
{{- end }}
{{- if .Values.cloud.s3.tls.ca }}
- "--minio.ca_file=/tls/s3/ca.crt"
{{- end }}
- "--pg.ssl_mode={{ .Values.cloud.postgres.tls.sslMode }}"
{{- if .Values.cloud.postgres.tls.ca }}
- "--pg.ca_file=/tls/pg/ca.crt"
{{- end }}
- "--run.config=/config/config.yaml"
{{- end -}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "maintenanceJob.envs" -}}
- name: LOG_LEVEL
  value: {{ .Values.maintenanceJob.log.level }}
{{- include "s3.envs" . | nindent 0 }}
{{- include "pg.envs" . | nindent 0 }}
{{- include "cdt.envs" . | nindent 0 }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to insert args for command line arguments
*/}}
{{- define "maintenanceJob.migrateSchema.args" -}}
- "migrate"
- "--pg.ssl_mode={{ .Values.cloud.postgres.tls.sslMode }}"
{{- if .Values.cloud.postgres.tls.ca }}
- "--pg.ca_file=/tls/pg/ca.crt"
{{- end -}}
{{- end -}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "maintenanceJob.migrateSchema.envs" -}}
- name: LOG_LEVEL
  value: {{ .Values.maintenanceJob.migrateSchema.log.level }}
{{- include "pg.envs" . | nindent 0 }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to generate Pod SecurityContext
*/}}
{{- define "maintenanceJob.migrateSchema.podSecurityContext" -}}
{{- if .Values.maintenanceJob.securityContext -}}
    {{ toYaml .Values.maintenanceJob.securityContext | nindent 6 }}
  {{- end }}
  {{- if not (.Capabilities.APIVersions.Has "apps.openshift.io/v1") }}
    {{- if not .Values.maintenanceJob.securityContext.runAsUser }}
      runAsUser: 2000
    {{- end }}
    {{- if not .Values.maintenanceJob.securityContext.fsGroup }}
      fsGroup: 2000
    {{- end }}
  {{- end }}
  {{- if (eq (.Values.maintenanceJob.securityContext.runAsNonRoot | toString) "false") }}
      runAsNonRoot: false
  {{- else }}
      runAsNonRoot: true
  {{- end }}
  {{- if not .Values.maintenanceJob.securityContext.seccompProfile }}
      seccompProfile:
        type: "RuntimeDefault"
  {{- end }}
{{- end -}}

{{/*
Template to generate Container SecurityContext
*/}}
{{- define "maintenanceJob.migrateSchema.containerSecurityContext" -}}
{{- if .Values.maintenanceJob.containerSecurityContext }}
{{- toYaml .Values.maintenanceJob.containerSecurityContext -}}
{{- else -}}
allowPrivilegeEscalation: false
capabilities:
  drop:
  - ALL
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to generate Pod SecurityContext
*/}}
{{- define "maintenanceJob.podSecurityContext" -}}
{{- if .Values.maintenanceJob.securityContext -}}
    {{ toYaml .Values.maintenanceJob.securityContext | nindent 6 }}
  {{- end }}
  {{- if not (.Capabilities.APIVersions.Has "apps.openshift.io/v1") }}
    {{- if not .Values.maintenanceJob.securityContext.runAsUser }}
      runAsUser: 2000
    {{- end }}
    {{- if not .Values.maintenanceJob.securityContext.fsGroup }}
      fsGroup: 2000
    {{- end }}
  {{- end }}
  {{- if (eq (.Values.maintenanceJob.securityContext.runAsNonRoot | toString) "false") }}
      runAsNonRoot: false
  {{- else }}
      runAsNonRoot: true
  {{- end }}
  {{- if not .Values.maintenanceJob.securityContext.seccompProfile }}
      seccompProfile:
        type: "RuntimeDefault"
  {{- end }}
{{- end -}}

{{/*
Template to generate Container SecurityContext
*/}}
{{- define "maintenanceJob.containerSecurityContext" -}}
{{- if .Values.maintenanceJob.containerSecurityContext }}
{{- toYaml .Values.maintenanceJob.containerSecurityContext -}}
{{- else -}}
allowPrivilegeEscalation: false
capabilities:
  drop:
  - ALL
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}


{{/*
Return resources for maintenance job by HWE profile.
*/}}
{{- define "maintenanceJob.resources" -}}
  {{- if .Values.maintenanceJob.resources }}
    {{- toYaml .Values.maintenanceJob.resources | nindent 4 }}
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

{{/*
Return resources for maintenance job migrate schema by HWE profile.
*/}}
{{- define "maintenanceJob.migrateSchema.resources" -}}
  {{- if .Values.maintenanceJob.migrateSchema.resources }}
    {{- toYaml .Values.maintenanceJob.migrateSchema.resources | nindent 4 }}
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
