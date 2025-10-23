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
* .Values.cleaner.image from values file
* SaaS/App deployer (or groovy.deploy.v3) from .Values.deployDescriptor "cloud-profiler-migration-cleaner" "image"
*/}}
{{- define "cleaner.image" -}}
  {{- if .Values.cleaner.image -}}
    {{- printf "%s" .Values.cleaner.image -}}
  {{- else -}}
    {{- if .Values.global -}}
      {{- if .Values.global.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-migration-cleaner" "image") -}}
      {{- end -}}
    {{- else -}}
      {{- if .Values.deployDescriptor -}}
        {{- printf "%s" (index .Values.global.deployDescriptor "cloud-profiler-migration-cleaner" "image") -}}
      {{- else -}}
        {{- print "product/prod.platform.cloud.infra_profiler_cdt-cloud-profiler-migration-cleaner:master_latest" -}}
      {{- end -}}
    {{- end -}}
  {{- end -}}
{{- end -}}

{{/******************************************************************************************************************/}}

{{/*
Template to generate Pod SecurityContext
*/}}
{{- define "cleaner.podSecurityContext" -}}
{{- if .Values.cleaner.securityContext -}}
    {{ toYaml .Values.cleaner.securityContext | nindent 6 }}
  {{- end }}
  {{- if not (.Capabilities.APIVersions.Has "apps.openshift.io/v1") }}
    {{- if not .Values.cleaner.securityContext.runAsUser }}
      runAsUser: 2000
    {{- end }}
    {{- if not .Values.cleaner.securityContext.fsGroup }}
      fsGroup: 2000
    {{- end }}
  {{- end }}
  {{- if (eq (.Values.cleaner.securityContext.runAsNonRoot | toString) "false") }}
      runAsNonRoot: false
  {{- else }}
      runAsNonRoot: true
  {{- end }}
  {{- if not .Values.cleaner.securityContext.seccompProfile }}
      seccompProfile:
        type: "RuntimeDefault"
  {{- end }}
{{- end -}}

{{/*
Template to generate Container SecurityContext
*/}}
{{- define "cleaner.containerSecurityContext" -}}
{{- if .Values.cleaner.containerSecurityContext }}
{{- toYaml .Values.cleaner.containerSecurityContext -}}
{{- else -}}
allowPrivilegeEscalation: false
capabilities:
  drop:
  - ALL
{{- end -}}
{{- end -}}

    {{/******************************************************************************************************************/}}

{{/*
Template to insert envs for ENVs for selected storage
*/}}
{{- define "cleaner.envs" -}}
- name: LOG_LEVEL
  value: {{ .Values.cleaner.log.level }}
- name: NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: PRIVILEGED_RIGHTS
  value: {{ .Values.privilegedRights | quote }}
- name: ESC_LABEL
  value: {{ .Values.cleaner.escLabelSelector | quote }}
{{- end -}}
