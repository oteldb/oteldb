{{/*
Expand the name of the chart.
*/}}
{{- define "oteldb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "oteldb.fullname" -}}
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

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "oteldb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "oteldb.labels" -}}
helm.sh/chart: {{ include "oteldb.chart" . }}
{{ include "oteldb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "oteldb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "oteldb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Local state directory backed by the persistent volume: the data directory for the "file"
backend (parts + WAL), the write-ahead log directory for the stateless "s3" backend.
Empty when the backend keeps nothing locally, in which case no volume is mounted.
*/}}
{{- define "oteldb.dataDir" -}}
{{- $storage := .Values.config.storage | default dict }}
{{- if eq (default "memory" $storage.backend) "s3" }}
{{- $storage.wal_dir | default "" }}
{{- else }}
{{- $storage.dir | default "" }}
{{- end }}
{{- end }}

{{/*
Whether a local volume is mounted at all: only when persistence is meaningful for the
selected backend (see oteldb.dataDir).
*/}}
{{- define "oteldb.persistent" -}}
{{- if include "oteldb.dataDir" . }}true{{- end }}
{{- end }}

{{/*
Whether oteldb serves its own metrics in Prometheus format, i.e. the effective
OTEL_METRICS_EXPORTER (which .Values.env may override) includes the prometheus exporter.
*/}}
{{- define "oteldb.metricsEnabled" -}}
{{- if .Values.telemetry.metrics.enabled }}
{{- $exporters := .Values.telemetry.metrics.exporters | toString }}
{{- range .Values.env }}
{{- if eq .name "OTEL_METRICS_EXPORTER" }}
{{- $exporters = .value | toString }}
{{- end }}
{{- end }}
{{- if has "prometheus" (splitList "," ($exporters | nospace)) }}true{{- end }}
{{- end }}
{{- end }}

{{/*
Container environment: self-telemetry defaults first, then .Values.env. Defaults whose
name is already set in .Values.env are dropped, so user values always win.
*/}}
{{- define "oteldb.env" -}}
{{- $defaults := list }}
{{- if .Values.telemetry.metrics.enabled }}
{{- $port := .Values.telemetry.metrics.port | int }}
{{- $defaults = list
  (dict "name" "OTEL_METRICS_EXPORTER" "value" (.Values.telemetry.metrics.exporters | toString))
  (dict "name" "OTEL_EXPORTER_PROMETHEUS_HOST" "value" "0.0.0.0")
  (dict "name" "OTEL_EXPORTER_PROMETHEUS_PORT" "value" (printf "%d" $port)) }}
{{- end }}
{{- $taken := dict }}
{{- range .Values.env }}
{{- $_ := set $taken .name true }}
{{- end }}
{{- $env := list }}
{{- range $defaults }}
{{- if not (hasKey $taken .name) }}
{{- $env = append $env . }}
{{- end }}
{{- end }}
{{- range .Values.env }}
{{- $env = append $env . }}
{{- end }}
{{- toYaml $env }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "oteldb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "oteldb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
