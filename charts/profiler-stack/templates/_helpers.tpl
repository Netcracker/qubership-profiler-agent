{{/* vim: set filetype=mustache: */}}

{{/*
Template to insert envs for CDT Inverted Index Params
*/}}
{{- define "cdt.envs" -}}
  {{- if .Values.cloud.cdt.invertedIndex.granularity }}
- name: INVERTED_INDEX_GRANULARITY
  value: {{ .Values.cloud.cdt.invertedIndex.granularity | quote}}
  {{- end }}
  {{- if .Values.cloud.cdt.invertedIndex.lifetime }}
- name: INVERTED_INDEX_LIFETIME
  value: {{ .Values.cloud.cdt.invertedIndex.lifetime | quote}}
  {{- end }}
  {{- if .Values.cloud.cdt.invertedIndex.params }}
- name: INVERTED_INDEX_PARAMS
  value: {{ join "," (.Values.cloud.cdt.invertedIndex.params | default (list)) | quote }}
  {{- end }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Return the appropriate apiVersion for deployment.
*/}}
{{- define "deployment.apiVersion" -}}
  {{- if semverCompare "<1.9-0" .Capabilities.KubeVersion.GitVersion -}}
    {{- print "apps/v1beta2" -}}
  {{- else -}}
    {{- print "apps/v1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for rbac.
*/}}
{{- define "rbac.apiVersion" -}}
  {{- if .Capabilities.APIVersions.Has "rbac.authorization.k8s.io/v1" -}}
    {{/* This API supports since k8s 1.18 */}}
    {{- print "rbac.authorization.k8s.io/v1" -}}
  {{- else -}}
    {{/* Deprecated this API since k8s 1.18 and remove since k8s 1.22 */}}
    {{- print "rbac.authorization.k8s.io/v1beta1" -}}
  {{- end -}}
{{- end -}}

{{/*
Return the appropriate apiVersion for ingress.
*/}}
{{- define "ingress.apiVersion" -}}
  {{- if .Capabilities.APIVersions.Has "networking.k8s.io/v1" -}}
    {{/* This API supports since k8s 1.19 */}}
    {{- print "networking.k8s.io/v1" -}}
  {{- else if .Capabilities.APIVersions.Has "networking.k8s.io/v1beta1" -}}
    {{/* Deprecated this API since k8s 1.19 and remove since k8s 1.22 */}}
    {{- print "networking.k8s.io/v1beta1" -}}
  {{- else -}}
    {{/* Deprecated this API since k8s 1.18 and remove since k8s 1.22 */}}
    {{- print "extensions/v1beta1" -}}
  {{- end -}}
{{- end -}}

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
{{- end -}}

{{/*
Create common labels for each resource which is creating by this chart.
*/}}
{{- define "common.commonLabels" -}}
app.kubernetes.io/part-of: cloud-profiler
app.kubernetes.io/managed-by: helm
app.kubernetes.io/version: {{ .Chart.AppVersion }}
{{- end -}}

{{- define "common.envs" -}}
{{- if .Values.JAVA_DEBUG }}
- name: JAVA_DEBUG
  value: {{ .Values.JAVA_DEBUG | quote }}
{{- if .Values.JAVA_DEBUG_SUSPEND }}
- name: JAVA_DEBUG_SUSPEND
  value: {{ .Values.JAVA_DEBUG_SUSPEND | quote }}
{{- end -}}
{{- if .Values.SLEEP_BEFORE_EXIT }}
- name: SLEEP_BEFORE_EXIT
  value: {{ .Values.SLEEP_BEFORE_EXIT }}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "common.ports" -}}
- name: http
  containerPort: 8080
  protocol: TCP
{{- if .Values.JAVA_DEBUG }}
- name: debug
  containerPort: 5005
  protocol: TCP
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Find a collector image in various places.
Image can be found from:
* .Values.collector.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler" "image"
*/}}
{{- define "collector.image" -}}
  {{- if .Values.collector.image -}}
    {{- printf "%s" .Values.collector.image -}}
  {{- else -}}
    {{- if .Values.global -}}
      {{- if .Values.global.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler" "image") -}}
      {{- end -}}
    {{- else -}}
      {{- if .Values.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler" "image") -}}
      {{- else -}}
        {{- print "product/prod.platform.cloud.infra_profiler_cdt-cloud-profiler:master_latest" -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Find a ui-service image in various places.
Image can be found from:
* .Values.ui.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler" "image"
*/}}
{{- define "ui.image" -}}
  {{- if .Values.ui.image -}}
    {{- printf "%s" .Values.ui.image -}}
  {{- else -}}
    {{- if .Values.global -}}
      {{- if .Values.global.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler" "image") -}}
      {{- end -}}
    {{- else -}}
      {{- if .Values.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler" "image") -}}
      {{- else -}}
        {{- print "product/prod.platform.cloud.infra_profiler_cdt-cloud-profiler:master_latest" -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to insert common envs
*/}}
{{- define "agent.envs" -}}
- name: PERSISTENCE
  value: {{ .Values.storage }}
- name: QUARKUS_PROFILE
  value: {{ .Values.storage }}

- name: NC_DIAGNOSTIC_MODE
  value: '{{ .Values.NC_DIAGNOSTIC_MODE }}'
- name: NC_DIAGNOSTIC_AGENT_SERVICE
  value: '{{ .Values.NC_DIAGNOSTIC_AGENT_SERVICE }}'
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to insert envs for Postgres+S3 (cloud) storage
*/}}
{{- define "cloud.envs" -}}
- name: PERSISTENCE
  value: {{ .Values.storage }}
- name: QUARKUS_PROFILE
  value: {{ .Values.storage }}
- name: POSTGRES_HOST
  {{- if .Values.cloud.postgres.host }}
  value: {{ .Values.cloud.postgres.host }}
  {{- else if .Values.INFRA_POSTGRES_HOST }}
  value: {{ .Values.INFRA_POSTGRES_HOST }}
  {{- end }}
- name: POSTGRES_PORT
  {{- if .Values.cloud.postgres.port }}
  value: {{ .Values.cloud.postgres.port | quote }}
  {{- else if .Values.INFRA_POSTGRES_PORT }}
  value: {{ .Values.INFRA_POSTGRES_PORT | quote }}
  {{- end }}
- name: POSTGRES_DATABASE
  {{- if .Values.cloud.postgres.database }}
  value: {{ .Values.cloud.postgres.database }}
  {{- end }}
- name: POSTGRES_USERNAME
  {{- if .Values.cloud.postgres.username }}
  value: {{ .Values.cloud.postgres.username }}
  {{- else if .Values.INFRA_POSTGRES_ADMIN_USERNAME }}
  value: {{ .Values.INFRA_POSTGRES_ADMIN_USERNAME }}
  {{- end }}
- name: POSTGRES_PASSWORD
  {{- if .Values.cloud.postgres.password }}
  value: {{ .Values.cloud.postgres.password }}
  {{- else if .Values.INFRA_POSTGRES_ADMIN_PASSWORD }}
  value: {{ .Values.INFRA_POSTGRES_ADMIN_PASSWORD }}
  {{- end }}
- name: MINIO_ENDPOINT
  {{- if .Values.cloud.s3.endpoint }}
  value: "{{ .Values.cloud.s3.endpoint }}"
  {{- else if .Values.INFRA_S3_MINIO_ENDPOINT }}
  value: "{{ .Values.INFRA_S3_MINIO_ENDPOINT }}"
  {{- end }}
- name: MINIO_ACCESS_KEY
  {{- if .Values.cloud.s3.accessKey }}
  value: {{ .Values.cloud.s3.accessKey }}
  {{- else if .Values.INFRA_S3_MINIO_ACCESSKEY }}
  value: "{{ .Values.INFRA_S3_MINIO_ACCESSKEY }}"
  {{- end }}
- name: MINIO_SECRET_KEY
  {{- if .Values.cloud.s3.secretKey }}
  value: {{ .Values.cloud.s3.secretKey }}
  {{- else if .Values.INFRA_S3_MINIO_SECRETKEY }}
  value: "{{ .Values.INFRA_S3_MINIO_SECRETKEY }}"
  {{- end }}
- name: MINIO_BUCKET_NAME
  {{- if .Values.cloud.s3.bucket }}
  value: {{ .Values.cloud.s3.bucket }}
  {{- end }}
- name: MINIO_IGNORE_CERT_CHECK
  {{- if .Values.cloud.s3.tls.insecure }}
  value: {{ .Values.cloud.s3.tls.insecure | quote}}
  {{- else }}
  value: "false"
  {{- end }}
{{- end -}}

{{/*
Template to insert envs for S3 storage
*/}}
{{- define "s3.envs" -}}
{{/* Not support now */}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to generate Pod SecurityContext
*/}}
{{- define "collector.podSecurityContext" -}}
{{- print "{}" -}}
{{- end -}}

{{/*
Template to generate Container SecurityContext
*/}}
{{- define "collector.containerSecurityContext" -}}
{{- print "{}" -}}
{{- end -}}

{{/*
Template to insert envs for command line arguments
*/}}
{{- define "collector.args" -}}
{{- print "[]" -}}
{{- end -}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "collector.envs" -}}
{{- include "common.envs" . | nindent 0 }}
- name: NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: CLOUD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: SERVICE_TYPE
  value: {{ .Values.collector.name }}
- name: MICROSERVICE_NAME
  value: {{ .Values.collector.name }}
  {{- if eq .Values.storage "cloud" }}
    {{- include "cloud.envs" . | nindent 0 }}
  {{- else if eq .Values.storage "s3" }}
    {{- include "s3.envs" . | nindent 0 }}
  {{- else }}
    {{- include "cloud.envs" . | nindent 0 }}
  {{- end }}
  {{- include "agent.envs" . | nindent 0 }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to generate Pod SecurityContext
*/}}
{{- define "ui.podSecurityContext" -}}
  {{- if .Values.ui.podSecurityContext }}
    {{- toYaml .Values.ui.podSecurityContext | nindent 8 }}
  {{- else }}
    runAsUser: 10001
    runAsGroup: 2000
    runAsNonRoot: true
    fsGroup: 2000
  {{- end }}
{{- end -}}

{{/*
Template to generate Container SecurityContext
*/}}
{{- define "ui.containerSecurityContext" -}}
{{- print "{}" -}}
{{- end -}}

{{/*
Template to insert envs for command line arguments
*/}}
{{- define "ui.args" -}}
{{- print "[]" -}}
{{- end -}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "ui.envs" -}}
{{- include "common.envs" . | nindent 0 }}
{{- include "cdt.envs" . | nindent 0 }}
- name: S3_DOWNLOAD_CACHE_DIR
  value: {{ .Values.ui.s3.downloadCacheDir }}
{{- if .Values.ui.security.basic.username }}
- name: QUARKUS_HTTP_AUTH_BASIC
  value: {{ "true" | quote }}
{{- else }}
- name: QUARKUS_HTTP_AUTH_BASIC
  value: {{ "false" | quote }}
{{- end }}
{{- if .Values.ui.security.basic.username }}
- name: UI_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.ui.security.basic.credentialsSecretName | quote }}
      key: username
{{- end }}
{{- if .Values.ui.security.basic.password }}
- name: UI_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.ui.security.basic.credentialsSecretName | quote }}
      key: password
{{- end }}
- name: NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: CLOUD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: SERVICE_TYPE
  value: {{ .Values.ui.name }}
- name: MICROSERVICE_NAME
  value: {{ .Values.ui.name }}
  {{- if eq .Values.storage "cloud" }}
    {{- include "cloud.envs" . | nindent 0 }}
  {{- else if eq .Values.storage "s3" }}
    {{- include "s3.envs" . | nindent 0 }}
  {{- else }}
    {{- include "cloud.envs" . | nindent 0 }}
  {{- end }}
  {{- include "agent.envs" . | nindent 0 }}
{{- if .Values.ui.security.oidc.idp_url }}
- name: QUARKUS_OIDC_AUTH_SERVER_URL
  value: {{ .Values.ui.security.oidc.idp_url | quote }}
- name: QUARKUS_OIDC_ENABLED
  value: "true"
- name: QUARKUS_OIDC_CLIENT_ID
  valueFrom:
    secretKeyRef:
      name: ui-oidc-client-secret
      key: client_id
- name: QUARKUS_OIDC_CREDENTIALS_SECRET
  valueFrom:
    secretKeyRef:
      name: ui-oidc-client-secret
      key: client_secret
{{- if hasPrefix "https" .Values.ui.security.oidc.idp_url }}
- name: QUARKUS_OIDC_AUTHENTICATION_FORCE_REDIRECT_HTTPS_SCHEME
  value: "true"
- name: QUARKUS_OIDC_AUTHENTICATION_COOKIE_FORCE_SECURE
  value: "true"
{{- end }}
{{ else }}
- name: QUARKUS_OIDC_ENABLED
  value: "false"
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Set default value for collector ingress host if not specify in Values.
*/}}
{{- define "collector.ingress" -}}
  {{- if not .Values.collector.ingress.host -}}
      "collector-{{ .Values.NAMESPACE }}.{{ .Values.CLOUD_PUBLIC_HOST }}"
  {{- else -}}
      {{ .Values.collector.ingress.host }}
  {{- end -}}
{{- end -}}


{{/*
Set default value for ui ingress host if not specify in Values.
*/}}
{{- define "ui.ingress" -}}
  {{- if not .Values.ui.ingress.host -}}
      "ui-{{ .Values.NAMESPACE }}.{{ .Values.CLOUD_PUBLIC_HOST }}"
  {{- else -}}
      {{ .Values.ui.ingress.host }}
  {{- end -}}
{{- end -}}
