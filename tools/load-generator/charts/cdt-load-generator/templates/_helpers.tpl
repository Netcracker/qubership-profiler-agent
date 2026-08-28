{{- define "deployment.image" -}}
{{- required "CDT Load Generator docker image should be specified." .Values.deployment.image -}}
{{- end -}}
 
{{- define "ingress.host" -}}
{{- if .Values.ingress.enabled -}}
{{- required "Ingress enabled, so host should be specified" .Values.ingress.host -}}
{{- end -}}
{{- end -}}

{{- define "prometheus-rw.server-url" -}}
{{- if .Values.global.prometheusRW -}}
{{- required "Prometheus remote write enabled, so server url should be specified" .Values.deployment.envRW.K6_PROMETHEUS_RW_SERVER_URL -}}
{{- end -}}
{{- end -}}