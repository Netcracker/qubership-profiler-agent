{{/* vim: set filetype=mustache: */}}

{{- define "spring-boot-2-undertow.image" -}}
  {{- if .Values.image -}}
    {{- printf "%s" .Values.image -}}
  {{- else -}}
    {{- printf "%s/spring-boot-2-undertow:%s" .Values.global.image.repository .Values.global.image.tag -}}
  {{- end -}}
{{- end -}}
