{{- define "jobprocessor.fullname" -}}
{{ .Release.Name }}
{{- end }}

{{- define "jobprocessor.labels" -}}
app.kubernetes.io/name: jobprocessor
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Connection math (spec: PgBouncer + KEDA). Fails template rendering when
     the fleet at max scale could exceed PgBouncer's client ceiling, or
     PgBouncer's server pool could exceed Postgres capacity. */}}
{{- define "jobprocessor.validateConnections" -}}
{{- $workerMax := ternary .Values.keda.maxReplicas .Values.worker.replicas .Values.keda.enabled | int -}}
{{- $demand := add (mul $workerMax (.Values.worker.dbPoolSize | int)) (mul (.Values.api.replicas | int) (.Values.api.dbPoolSize | int)) (.Values.ticker.dbPoolSize | int) -}}
{{- if gt $demand (.Values.pgbouncer.maxClientConn | int) -}}
{{- fail (printf "connection math: max client demand %d (workers %d x %d + api %d x %d + ticker %d) exceeds pgbouncer.maxClientConn %d" $demand $workerMax (.Values.worker.dbPoolSize | int) (.Values.api.replicas | int) (.Values.api.dbPoolSize | int) (.Values.ticker.dbPoolSize | int) (.Values.pgbouncer.maxClientConn | int)) -}}
{{- end -}}
{{- $serverBudget := sub (.Values.postgres.maxConnections | int) (.Values.postgres.reservedConnections | int) -}}
{{- if gt (.Values.pgbouncer.defaultPoolSize | int) $serverBudget -}}
{{- fail (printf "connection math: pgbouncer.defaultPoolSize %d exceeds postgres budget %d (maxConnections - reservedConnections)" (.Values.pgbouncer.defaultPoolSize | int) $serverBudget) -}}
{{- end -}}
{{- end }}

{{- define "jobprocessor.postgresHost" -}}
{{ include "jobprocessor.fullname" . }}-postgres.{{ .Release.Namespace }}.svc
{{- end }}

{{- define "jobprocessor.pgbouncerHost" -}}
{{ include "jobprocessor.fullname" . }}-pgbouncer.{{ .Release.Namespace }}.svc
{{- end }}

{{- define "jobprocessor.redisHost" -}}
{{ include "jobprocessor.fullname" . }}-redis.{{ .Release.Namespace }}.svc
{{- end }}

{{/* URLs embed $(DB_PASSWORD)/$(REDIS_PASSWORD): kubelet env-var expansion
     substitutes them from secretKeyRef env vars declared earlier in the
     container spec, keeping passwords out of rendered manifests. */}}
{{- define "jobprocessor.appDatabaseUrl" -}}
postgresql+psycopg://{{ .Values.postgres.user }}:$(DB_PASSWORD)@{{ include "jobprocessor.pgbouncerHost" . }}:6432/{{ .Values.postgres.database }}{{- if .Values.tls.appToPgbouncer -}}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt{{- end -}}
{{- end }}

{{- define "jobprocessor.directDatabaseUrl" -}}
postgresql+psycopg://{{ .Values.postgres.user }}:$(DB_PASSWORD)@{{ include "jobprocessor.postgresHost" . }}:5432/{{ .Values.postgres.database }}?sslmode=verify-full&sslrootcert=/etc/pki/service-ca/ca.crt
{{- end }}

{{- define "jobprocessor.redisUrl" -}}
rediss://:$(REDIS_PASSWORD)@{{ include "jobprocessor.redisHost" . }}:6379/0?ssl_ca_certs=/etc/pki/service-ca/ca.crt
{{- end }}

{{/* Shared env block for app containers (api/worker/ticker). */}}
{{- define "jobprocessor.appEnv" -}}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "jobprocessor.fullname" . }}-credentials
      key: db-password
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "jobprocessor.fullname" . }}-credentials
      key: redis-password
- name: DATABASE_URL
  value: {{ include "jobprocessor.appDatabaseUrl" . | quote }}
- name: REDIS_URL
  value: {{ include "jobprocessor.redisUrl" . | quote }}
- name: DB_DISABLE_PREPARED_STATEMENTS
  value: "true"
{{- if .Values.otel.enabled }}
{{- if not .Values.otel.exporterEndpoint }}
{{- fail "otel.exporterEndpoint must be set (to the cluster's existing OpenTelemetry collector) when otel.enabled=true" }}
{{- end }}
- name: OTEL_ENABLED
  value: "true"
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.otel.exporterEndpoint | quote }}
{{- end }}
{{- end }}

{{/* Service-CA bundle mount, paired volume in jobprocessor.caVolume. */}}
{{- define "jobprocessor.caVolumeMount" -}}
- name: service-ca
  mountPath: /etc/pki/service-ca
  readOnly: true
{{- end }}

{{- define "jobprocessor.caVolume" -}}
- name: service-ca
  configMap:
    name: {{ include "jobprocessor.fullname" . }}-service-ca
    items:
      - key: service-ca.crt
        path: ca.crt
{{- end }}
