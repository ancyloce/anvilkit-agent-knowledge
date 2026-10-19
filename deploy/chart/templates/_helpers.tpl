{{/* The stable service identity (architecture.md naming): Deployment, Service,
ServiceAccount and Helm release share it. */}}
{{- define "anvilkit-agent-knowledge.name" -}}
anvilkit-agent-knowledge
{{- end -}}

{{- define "anvilkit-agent-knowledge.fullname" -}}
{{- if eq .Release.Name (include "anvilkit-agent-knowledge.name" .) -}}
{{- .Release.Name -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "anvilkit-agent-knowledge.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-knowledge.labels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-knowledge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/component: knowledge
app.kubernetes.io/part-of: anvilkit
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "anvilkit-agent-knowledge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "anvilkit-agent-knowledge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "anvilkit-agent-knowledge.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "anvilkit-agent-knowledge.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-knowledge.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-knowledge.forwarderImage" -}}
{{- if .Values.forwarder.image.digest -}}
{{- printf "%s@%s" .Values.forwarder.image.repository .Values.forwarder.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.forwarder.image.repository (default .Chart.AppVersion .Values.forwarder.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "anvilkit-agent-knowledge.relayImage" -}}
{{- if .Values.relay.image.digest -}}
{{- printf "%s@%s" .Values.relay.image.repository .Values.relay.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.relay.image.repository (default .Chart.AppVersion .Values.relay.image.tag) -}}
{{- end -}}
{{- end -}}

{{/* Required environment values, checked once for every template. */}}
{{- define "anvilkit-agent-knowledge.require" -}}
{{- $kube := eq .Values.secrets.provider "kubernetes" -}}
{{- if not (has .Values.secrets.provider (list "kubernetes" "csi")) }}
{{- fail "secrets.provider must be kubernetes or csi" }}
{{- end }}
{{- if and (not $kube) (or (not .Values.secrets.csi.address) (not .Values.secrets.csi.path)) }}
{{- fail "secrets.csi.address and secrets.csi.path are required under secrets.provider csi (the OpenBao address and the service's KV v2 data path)" }}
{{- end }}
{{- if and $kube (not .Values.development.enabled) (or .Values.forwarder.enabled .Values.relay.enabled) (not .Values.nats.credentials.secret.name) }}
{{- fail "nats.credentials.secret.name is required outside development: the NATS server admits no anonymous client (or use secrets.provider csi)" }}
{{- end }}
{{- if and $kube (not .Values.database.secret.name) }}
{{- fail "database.secret.name is required: an existing Secret holding the application-role URL, mounted as the file ANVILKIT_KNOWLEDGE_DATABASE_URL_FILE names" }}
{{- end }}
{{- if not .Values.nats.url }}
{{- fail "nats.url is required: the JetStream placement of the forwarder and the relay" }}
{{- end }}
{{- if and $kube .Values.forwarder.enabled (not .Values.forwarder.database.secret.name) }}
{{- fail "forwarder.database.secret.name is required while forwarder.enabled is true: the forwarder role's database URL" }}
{{- end }}
{{- if and $kube .Values.relay.enabled (or (not .Values.relay.database.secret.name) (not .Values.relay.queue.secret.name)) }}
{{- fail "relay.database.secret.name and relay.queue.secret.name are required while relay.enabled is true: the relay role's database URL and the queue Valkey URL" }}
{{- end }}
{{- if not (has .Values.identity.mode (list "mtls" "development")) }}
{{- fail "identity.mode must be mtls or development" }}
{{- end }}
{{- if and (eq .Values.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true (a plaintext listener authenticates and authorizes no caller)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.trustDomain) (not .Values.development.enabled) }}
{{- fail "identity.trustDomain is required outside development (the development default anvilkit.local applies only with development.enabled: true)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") .Values.identity.certificate.create (not .Values.identity.certificate.issuerRef.name) }}
{{- fail "identity.certificate.issuerRef.name is required: the cert-manager issuer of the workload certificate (or set identity.certificate.create false and identity.secretName)" }}
{{- end }}
{{- if and (eq .Values.identity.mode "mtls") (not .Values.identity.certificate.create) (not .Values.identity.secretName) }}
{{- fail "identity.secretName is required while identity.certificate.create is false" }}
{{- end }}
{{- if and (eq .Values.control.identity.mode "development") (not .Values.development.enabled) }}
{{- fail "control.identity.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (eq .Values.control.identity.mode "mtls") (ne .Values.identity.mode "mtls") }}
{{- fail "control.identity.mode mtls presents the workload certificate and needs identity.mode mtls" }}
{{- end }}
{{- if not (has .Values.nats.tls.mode (list "tls" "mtls" "development")) }}
{{- fail "nats.tls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.nats.tls.mode "development") (not .Values.development.enabled) }}
{{- fail "nats.tls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.nats.tls.mode "development") (not .Values.nats.tls.caSecret.name) }}
{{- fail "nats.tls.caSecret.name is required under nats.tls.mode tls or mtls: the Secret holding the JetStream CA bundle (key ca.crt)" }}
{{- end }}
{{- if .Values.inference.url }}
{{- if not (has .Values.inference.tls.mode (list "tls" "mtls" "development")) }}
{{- fail "inference.tls.mode must be tls, mtls or development" }}
{{- end }}
{{- if and (eq .Values.inference.tls.mode "development") (not .Values.development.enabled) }}
{{- fail "inference.tls.mode development is DEVELOPMENT_ONLY: it renders only with development.enabled: true" }}
{{- end }}
{{- if and (ne .Values.inference.tls.mode "development") (not .Values.inference.tls.caSecret.name) }}
{{- fail "inference.tls.caSecret.name is required under inference.tls.mode tls or mtls" }}
{{- end }}
{{- end }}
{{- if and (dig "retained" "url" "" .Values.inference) (not .Values.inference.url) }}
{{- fail "inference.retained.url places the earlier model of an embedding-model change and needs inference.url" }}
{{- end }}
{{- end -}}

{{/* The identity Secret: the rendered Certificate's or the environment's. */}}
{{- define "anvilkit-agent-knowledge.identitySecret" -}}
{{- if .Values.identity.certificate.create -}}
{{- printf "%s-identity" (include "anvilkit-agent-knowledge.fullname" .) -}}
{{- else -}}
{{- .Values.identity.secretName -}}
{{- end -}}
{{- end -}}

{{/* The rendered configuration: the reviewed sections plus the chart-owned
identity, guard and transport settings, so every value is wired to the
loader's keys. */}}
{{- define "anvilkit-agent-knowledge.config" -}}
{{- $cfg := deepCopy .Values.config -}}
{{- $_ := set $cfg "development" (dict "enabled" .Values.development.enabled) -}}
{{- $id := dict "mode" .Values.identity.mode "max_connection_age" .Values.identity.maxConnectionAge "reload_interval" (dig "identity" "reload_interval" "5s" $cfg.grpc) -}}
{{- if .Values.identity.trustDomain }}{{- $_ = set $id "trust_domain" .Values.identity.trustDomain }}{{- end -}}
{{- if eq .Values.identity.mode "mtls" }}
{{- $_ = set $id "cert_file" "/etc/anvilkit/identity/tls.crt" }}
{{- $_ = set $id "key_file" "/etc/anvilkit/identity/tls.key" }}
{{- $_ = set $id "ca_file" "/etc/anvilkit/identity/ca.crt" }}
{{- end -}}
{{- $_ = set $cfg.grpc "identity" $id -}}
{{- $_ = set $cfg.control "identity" (dict "mode" .Values.control.identity.mode "server_name" .Values.control.identity.serverName) -}}
{{- if .Values.inference.url }}
{{- $it := dict "mode" .Values.inference.tls.mode -}}
{{- if ne .Values.inference.tls.mode "development" }}
{{- $_ = set $it "ca_file" "/etc/anvilkit/inference-ca/ca.crt" }}
{{- if .Values.inference.tls.serverName }}{{- $_ = set $it "server_name" .Values.inference.tls.serverName }}{{- end }}
{{- end -}}
{{- $_ = set $cfg.inference "tls" $it -}}
{{- end -}}
{{- toYaml $cfg -}}
{{- end -}}

{{/* P0.6: where each credential file is inside the Pod. */}}
{{- define "anvilkit-agent-knowledge.credentialFile" -}}
{{- if eq .root.Values.secrets.provider "csi" -}}
/var/run/secrets/anvilkit/csi/{{ .csi }}
{{- else -}}
{{ .kube }}
{{- end -}}
{{- end -}}
