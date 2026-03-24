{{/* vim: set filetype=mustache: */}}

{{- define "spring-boot-3-tomcat.image" -}}
  {{- if .Values.image -}}
    {{- printf "%s" .Values.image -}}
  {{- else -}}
    {{- printf "%s/spring-boot-3-tomcat:%s" .Values.global.image.repository .Values.global.image.tag -}}
  {{- end -}}
{{- end -}}
