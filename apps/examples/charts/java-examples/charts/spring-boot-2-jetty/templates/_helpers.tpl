{{/* vim: set filetype=mustache: */}}

{{- define "spring-boot-2-jetty.image" -}}
  {{- if .Values.image -}}
    {{- printf "%s" .Values.image -}}
  {{- else -}}
    {{- printf "%s/spring-boot-2-jetty:%s" .Values.global.image.repository .Values.global.image.tag -}}
  {{- end -}}
{{- end -}}
