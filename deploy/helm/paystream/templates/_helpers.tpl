{{- define "paystream.labels" -}}
app.kubernetes.io/part-of: paystream
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{- define "paystream.image" -}}
{{ .root.Values.image.registry }}/{{ .name }}:{{ .root.Values.image.tag }}
{{- end }}

{{/* librdkafka settings for a Python service connecting as Strimzi user .user (KAFKA_CFG_* convention). */}}
{{- define "paystream.kafkaEnv" -}}
- name: KAFKA_BOOTSTRAP
  value: {{ .root.Values.kafka.bootstrap | quote }}
- name: KAFKA_CFG_SECURITY_PROTOCOL
  value: SASL_SSL
- name: KAFKA_CFG_SASL_MECHANISM
  value: SCRAM-SHA-512
- name: KAFKA_CFG_SASL_USERNAME
  value: {{ .user }}
- name: KAFKA_CFG_SASL_PASSWORD
  valueFrom: { secretKeyRef: { name: {{ .user }}, key: password } }
- name: KAFKA_CFG_SSL_CA_LOCATION
  value: /etc/kafka-ca/ca.crt
{{- end }}

{{- define "paystream.redisEnv" -}}
- name: REDIS_HOST
  value: {{ .Values.redis.host | quote }}
- name: REDIS_PORT
  value: {{ .Values.redis.port | quote }}
- name: REDIS_TLS
  value: {{ .Values.redis.tls | quote }}
- name: REDIS_PASSWORD
  valueFrom: { secretKeyRef: { name: {{ .Values.redis.authSecret }}, key: auth-token } }
{{- end }}

{{- define "paystream.kafkaCaVolume" -}}
- name: kafka-ca
  secret: { secretName: {{ .Values.kafka.caSecret }}, items: [{ key: ca.crt, path: ca.crt }] }
{{- end }}

{{- define "paystream.securityContext" -}}
securityContext:
  runAsNonRoot: true
  runAsUser: 10001
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  capabilities: { drop: [ALL] }
{{- end }}
