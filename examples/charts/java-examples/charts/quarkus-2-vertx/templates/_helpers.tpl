{{/* vim: set filetype=mustache: */}}

{{- define "quarkus-2-vertx.image" -}}
  {{- if .Values.image -}}
    {{- printf "%s" .Values.image -}}
  {{- else -}}
    {{- printf "%s/quarkus-2-vertx:%s" .Values.global.image.repository .Values.global.image.tag -}}
  {{- end -}}
{{- end -}}