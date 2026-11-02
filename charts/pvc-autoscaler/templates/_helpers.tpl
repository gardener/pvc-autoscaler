{{- define "pvc-autoscaler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pvc-autoscaler.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "pvc-autoscaler.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pvc-autoscaler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pvc-autoscaler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "pvc-autoscaler.labels" -}}
helm.sh/chart: {{ include "pvc-autoscaler.chart" . }}
{{ include "pvc-autoscaler.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "pvc-autoscaler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "pvc-autoscaler.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "pvc-autoscaler.webhookServiceName" -}}
{{- printf "%s-webhook" (include "pvc-autoscaler.fullname" .) }}
{{- end }}

{{- define "pvc-autoscaler.certificateName" -}}
{{- printf "%s-serving-cert" (include "pvc-autoscaler.fullname" .) }}
{{- end }}

{{- define "pvc-autoscaler.webhookSecretName" -}}
{{- if .Values.webhook.certManager.enabled }}
{{- printf "%s-webhook-server-cert" (include "pvc-autoscaler.fullname" .) }}
{{- else }}
{{- required "webhook.tls.secretName is required when webhook.certManager.enabled is false" .Values.webhook.tls.secretName }}
{{- end }}
{{- end }}

{{- define "pvc-autoscaler.webhookAnnotations" -}}
{{- if .Values.webhook.certManager.enabled }}
annotations:
  cert-manager.io/inject-ca-from: {{ .Release.Namespace }}/{{ include "pvc-autoscaler.certificateName" . }}
{{- end }}
{{- end }}

{{- define "pvc-autoscaler.webhookClientConfig" -}}
{{- $ctx := .context -}}
service:
  name: {{ include "pvc-autoscaler.webhookServiceName" $ctx }}
  namespace: {{ $ctx.Release.Namespace }}
  path: {{ .path }}
  port: 443
{{- if not $ctx.Values.webhook.certManager.enabled }}
caBundle: {{ required "webhook.tls.caBundle is required when webhook.certManager.enabled is false" $ctx.Values.webhook.tls.caBundle }}
{{- end }}
{{- end }}
