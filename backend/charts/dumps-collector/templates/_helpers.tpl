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
Set default value for dumps-collector ingress host if not specify in Values.
*/}}
{{- define "dumpsCollector.ingress" -}}
  {{- if .Values.dumpsCollector.ingress.host -}}
    {{ .Values.dumpsCollector.ingress.host }}
  {{- else -}}
    {{- printf "dumps-collector-%s.%s" .Values.NAMESPACE .Values.CLOUD_PUBLIC_HOST -}}
  {{- end -}}
{{- end -}}

{{- define "dumpsCollector.containerSecurityContext" -}}
  {{- if ge .Capabilities.KubeVersion.Minor "25" -}}
    {{- if .Values.dumpsCollector.containerSecurityContext -}}
      {{- toYaml .Values.dumpsCollector.containerSecurityContext | nindent 12 }}
    {{- else }}
            allowPrivilegeEscalation: false
            capabilities:
              drop:
                - ALL
    {{- end -}}
  {{- else }}
    {{- if .Values.dumpsCollector.containerSecurityContext -}}
      {{- toYaml .Values.dumpsCollector.containerSecurityContext | nindent 12 }}
    {{- else }}
            {}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/*
Find a dumpsCollector image in various places.
Image can be found from:
* .Values.dumpsCollector.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler-dumps-collector" "image"
*/}}
{{- define "dumpsCollector.image" -}}
  {{- if .Values.dumpsCollector.image -}}
    {{- printf "%s" .Values.dumpsCollector.image -}}
  {{- else -}}
    {{- if .Values.global -}}
      {{- if .Values.global.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-dumps-collector" "image") -}}
      {{- end -}}
    {{- else -}}
      {{- if .Values.deployDescriptor -}}
        {{- printf "%s" (index .Values.deployDescriptor "cloud-profiler-dumps-collector" "image") -}}
      {{- else -}}
        {{- print "ghcr.io/netcracker/qubership-profiler-dumps-collector:latest" -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}


{{/*
Dumps storage class from various places.
*/}}
{{- define "dumpsStorage.storageClassName" -}}
  {{- if and (ne (.Values.STORAGE_RWX_CLASS | toString) "<nil>") .Values.global.cloudIntegrationEnabled -}}
    {{- .Values.STORAGE_RWX_CLASS -}}
  {{- else -}}
    {{- .Values.cloud.dumpsStorage.storageClassName -}}
  {{- end -}}
{{- end -}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "dumpsCollector.envs" -}}
- name: POD_NAME
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: CLOUD_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: MICROSERVICE_NAME
  value: '{{ .Values.dumpsCollector.name }}'
{{- if or (eq (include "isTlsGenerateCertsEnabled" .) "true") (eq (include "isTlsUseExistingCertsEnabled" .) "true") }}
- name: TLS_CERT_DIR
  value: /tmp/cert/cloud-profiler-tls
{{- end }}
{{- if or .Values.cloud.dumpsStorage.name (include "dumpsStorage.storageClassName" .) .Values.cloud.dumpsStorage.emptydir }}
- name: DIAG_PV_MOUNT_PATH
  value: '/diag'
- name: DIAG_PV_HOURS_ARCHIVE_AFTER
  value: {{ .Values.cloud.dumpsStorage.hoursArchiveAfter | default 2 | quote }}
- name: DIAG_PV_DAYS_DELETE_AFTER
  value: {{ .Values.cloud.dumpsStorage.daysDeleteAfter | default 14 | quote }}
- name: DIAG_PV_MAX_HEAP_DUMPS_PER_POD
  value: {{ .Values.cloud.dumpsStorage.maxHeapDumpsPerPod | default 10 | quote }}
- name: DIAG_PV_INSERT_CRON
  value: {{ .Values.cloud.dumpsStorage.insertCron | default "* * * * *" | quote }}
- name: DIAG_PV_ARCHIVE_CRON
  value: {{ .Values.cloud.dumpsStorage.archiveCron | default "6 * * * *" | quote }}
- name: DIAG_PV_DELETE_CRON
  value: {{ .Values.cloud.dumpsStorage.deleteCron | default "30 * * * *" | quote }}
- name: DIAG_DB_PATH
  value: '/diag/profiler_dumps.db'
{{- else if .Values.cloud.dumpsStorage.host }}
- name: DIAG_HTTP_STORAGE_HOST
  value: '{{ .Values.cloud.dumpsStorage.host }}'
{{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}


{{/******************************************************************************************************************/}}

{{/*
Return securityContext section for dumpsCollector
*/}}
{{- define "dumpsCollector.securityContext" -}}
  {{- if .Values.dumpsCollector.securityContext }}
    {{- toYaml .Values.dumpsCollector.securityContext | nindent 8 }}
    {{- if not (.Capabilities.APIVersions.Has "apps.openshift.io/v1") }}
      {{- if not .Values.dumpsCollector.securityContext.runAsUser }}
        runAsUser: 2000
      {{- end }}
      {{- if not .Values.dumpsCollector.securityContext.fsGroup }}
        fsGroup: 2000
      {{- end }}
    {{- end }}
    {{- if (eq (.Values.dumpsCollector.securityContext.runAsNonRoot | toString) "false") }}
        runAsNonRoot: false
    {{- else }}
        runAsNonRoot: true
    {{- end }}
    {{- if and (ge .Capabilities.KubeVersion.Minor "25") (not .Values.dumpsCollector.securityContext.seccompProfile) }}
        seccompProfile:
          type: "RuntimeDefault"
    {{- end }}
  {{- else }}
    {{- if not (.Capabilities.APIVersions.Has "apps.openshift.io/v1") }}
        runAsUser: 2000
        fsGroup: 2000
    {{- end }}
        runAsNonRoot: true
    {{- if ge .Capabilities.KubeVersion.Minor "25" }}
        seccompProfile:
          type: "RuntimeDefault"
    {{- end }}
  {{- end }}
{{- end -}}

{{- define "isTlsGenerateCertsEnabled" -}}
{{- if .Values.tlsConfig -}}
{{- if .Values.tlsConfig.generateCerts -}}
{{- if .Values.tlsConfig.generateCerts.enabled -}}
{{ .Values.tlsConfig.generateCerts.enabled }}
{{- else -}}
false
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}


{{- define "isTlsUseExistingCertsEnabled" -}}
{{- if .Values.tlsConfig -}}
{{- if .Values.tlsConfig.useExistingCerts -}}
{{- if .Values.tlsConfig.useExistingCerts.enabled -}}
{{ .Values.tlsConfig.useExistingCerts.enabled }}
{{- else -}}
false
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "tls.secretName" -}}
  {{- $isTlsGenerateCertsEnabled := include "isTlsGenerateCertsEnabled" . | trim }}
  {{- $isTlsUseExistingCertsEnabled := include "isTlsUseExistingCertsEnabled" . | trim }}
  {{- if eq $isTlsGenerateCertsEnabled "true" -}}
      {{ .Values.tlsConfig.generateCerts.secretName | default "cloud-profiler-tls" }}
  {{- else if eq $isTlsUseExistingCertsEnabled "true" -}}
      {{ .Values.tlsConfig.useExistingCerts.secretName | default "cloud-profiler-tls" }}
  {{- else -}}
    {{- printf "" -}}
  {{- end }}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Return resources for monitoring-operator by HWE profile.
*/}}
{{- define "dumpsCollector.resources" -}}
requests:
 cpu: {{ .Values.dumpsCollector.resources.requests.cpu | default "100m" }}
 memory: {{ .Values.dumpsCollector.resources.requests.memory | default "650Mi" }}
limits:
 cpu: {{ .Values.dumpsCollector.resources.limits.cpu | default "1000m" }}
 memory: {{ .Values.dumpsCollector.resources.limits.memory | default "650Mi" }}
{{- end -}}

{{/******************************************************************************************************************/}}
