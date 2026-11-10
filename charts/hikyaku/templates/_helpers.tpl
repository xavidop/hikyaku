{{/*
The chart name.
*/}}
{{- define "hikyaku.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
The full name, truncated to 63 characters (a DNS label).
*/}}
{{- define "hikyaku.fullname" -}}
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
A suffixed name (<fullname>-<suffix>) that still fits in 63 characters.
Call it with (dict "root" $ "suffix" "admin").
*/}}
{{- define "hikyaku.componentName" -}}
{{- $max := int (sub 62 (len .suffix)) }}
{{- printf "%s-%s" (include "hikyaku.fullname" .root | trunc $max | trimSuffix "-") .suffix }}
{{- end }}

{{- define "hikyaku.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels. The component label is added by each object.
*/}}
{{- define "hikyaku.labels" -}}
helm.sh/chart: {{ include "hikyaku.chart" . }}
{{ include "hikyaku.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "hikyaku.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hikyaku.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Selector of the relay pods (not the bundled valkey, not the test pod).
*/}}
{{- define "hikyaku.relaySelectorLabels" -}}
{{ include "hikyaku.selectorLabels" . }}
app.kubernetes.io/component: relay
{{- end }}

{{- define "hikyaku.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "hikyaku.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "hikyaku.image" -}}
{{- if .Values.image.digest }}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else }}
{{- printf "%s:%s" .Values.image.repository (toString (.Values.image.tag | default .Chart.AppVersion)) }}
{{- end }}
{{- end }}

{{- define "hikyaku.configMapName" -}}
{{- if .Values.existingConfigMap }}
{{- .Values.existingConfigMap }}
{{- else }}
{{- include "hikyaku.componentName" (dict "root" . "suffix" "config") }}
{{- end }}
{{- end }}

{{- define "hikyaku.secretName" -}}
{{- include "hikyaku.componentName" (dict "root" . "suffix" "secrets") }}
{{- end }}

{{- define "hikyaku.redisName" -}}
{{- include "hikyaku.componentName" (dict "root" . "suffix" "redis") }}
{{- end }}

{{/*
The workload kind: a StatefulSet with persistence, a Deployment without.
*/}}
{{- define "hikyaku.workloadKind" -}}
{{- ternary "StatefulSet" "Deployment" (.Values.persistence.enabled | default false) }}
{{- end }}

{{/*
The container port of the extraPorts entry called .name, or "".
Call it with (dict "root" $ "name" "http").
*/}}
{{- define "hikyaku.containerPort" -}}
{{- $found := "" }}
{{- range .root.Values.extraPorts }}
{{- if eq .name $.name }}
{{- $found = int .port }}
{{- end }}
{{- end }}
{{- $found }}
{{- end }}

{{/*
The Service port of the extraPorts entry called .name, or "".
*/}}
{{- define "hikyaku.servicePort" -}}
{{- $found := "" }}
{{- range .root.Values.extraPorts }}
{{- if eq .name $.name }}
{{- $found = int (.servicePort | default .port) }}
{{- end }}
{{- end }}
{{- $found }}
{{- end }}

{{/*
The address the spec reaches Redis at, from redis.enabled or redis.external.addr.
*/}}
{{- define "hikyaku.redisAddr" -}}
{{- if .Values.redis.enabled }}
{{- printf "%s:6379" (include "hikyaku.redisName" .) }}
{{- else }}
{{- .Values.redis.external.addr }}
{{- end }}
{{- end }}

{{/*
What `config` holds: "string" (a non-blank spec text), "map" (a non-empty
mapping), or "" (empty: the built-in example).
*/}}
{{- define "hikyaku.configKind" -}}
{{- $c := .Values.config }}
{{- if kindIs "string" $c }}
{{- if trim $c }}string{{ end }}
{{- else if kindIs "map" $c }}
{{- if $c }}map{{ end }}
{{- end }}
{{- end }}

{{/*
True when the chart's built-in example spec is in use.
*/}}
{{- define "hikyaku.usesDefaultConfig" -}}
{{- if not (or .Values.existingConfigMap (include "hikyaku.configKind" .)) }}true{{ end }}
{{- end }}

{{/*
The built-in example spec: one HTTP source that logs every message to stdout,
with dead letters in a file under /var/lib/hikyaku.
*/}}
{{- define "hikyaku.defaultConfig" -}}
{{- $port := include "hikyaku.containerPort" (dict "root" . "name" "http") | default "8080" -}}
version: 1
name: hikyaku
sources:
  - name: webhooks
    type: http
    listen: ":{{ $port }}"
    path: /ingest
destinations:
  - name: log
    type: stdout
routes:
  - name: all
    from: [webhooks]
    to: [log]
dead_letter:
  type: file
  path: /var/lib/hikyaku/dlq.ndjson
  fallback: [stderr]
server:
  admin:
    listen: ":{{ int .Values.adminPort }}"
{{- end }}

{{/*
The spec text that goes into the ConfigMap: config as a string is used as is,
config as a map goes through toYaml, and an empty config means the example.
*/}}
{{- define "hikyaku.configYaml" -}}
{{- $kind := include "hikyaku.configKind" . }}
{{- if eq $kind "string" }}
{{- .Values.config }}
{{- else if eq $kind "map" }}
{{- toYaml .Values.config }}
{{- else }}
{{- include "hikyaku.defaultConfig" . }}
{{- end }}
{{- end }}

{{/*
The spec as a dict, to check it against the chart's values. Empty for
existingConfigMap, which the chart cannot read.
*/}}
{{- define "hikyaku.configDict" -}}
{{- if not .Values.existingConfigMap }}
{{- include "hikyaku.configYaml" . | fromYaml | toJson }}
{{- else }}
{{- dict | toJson }}
{{- end }}
{{- end }}

{{/*
The port of a listen address such as ":8080" or "0.0.0.0:8080", or "" when it
has none or is templated.
*/}}
{{- define "hikyaku.listenPort" -}}
{{- $port := regexFind ":[0-9]+$" (toString .) | trimPrefix ":" }}
{{- $port }}
{{- end }}

{{/*
Fails on values that would render a broken release.
*/}}
{{- define "hikyaku.validate" -}}
{{- $v := .Values }}
{{- if and $v.redis.enabled $v.redis.external.addr }}
{{- fail "redis.enabled (the bundled development valkey) and redis.external.addr are both set: use one of them" }}
{{- end }}
{{- $names := list }}
{{- $ports := list (int $v.adminPort) }}
{{- $servicePorts := list }}
{{- range $v.extraPorts }}
{{- if eq .name "admin" }}
{{- fail "extraPorts: the name admin is reserved for the admin API port (adminPort)" }}
{{- end }}
{{- if has .name $names }}
{{- fail (printf "extraPorts: the name %s is used twice" .name) }}
{{- end }}
{{- if has (int .port) $ports }}
{{- fail (printf "extraPorts: port %d is used twice (adminPort is %d)" (int .port) (int $v.adminPort)) }}
{{- end }}
{{- $servicePort := int (.servicePort | default .port) }}
{{- if has $servicePort $servicePorts }}
{{- fail (printf "extraPorts: Service port %d is used twice" $servicePort) }}
{{- end }}
{{- $names = append $names .name }}
{{- $ports = append $ports (int .port) }}
{{- $servicePorts = append $servicePorts $servicePort }}
{{- end }}
{{- if and $v.ingress.enabled (not (has $v.ingress.servicePort $names)) }}
{{- fail (printf "ingress.servicePort is %q, which is not the name of an extraPorts entry" $v.ingress.servicePort) }}
{{- end }}
{{- if $v.gatewayAPI.enabled }}
{{- if not $v.gatewayAPI.parentRefs }}
{{- fail "gatewayAPI.enabled needs gatewayAPI.parentRefs: the Gateways the HTTPRoute attaches to" }}
{{- end }}
{{- if not (has $v.gatewayAPI.servicePort $names) }}
{{- fail (printf "gatewayAPI.servicePort is %q, which is not the name of an extraPorts entry" $v.gatewayAPI.servicePort) }}
{{- end }}
{{- end }}
{{- if and $v.autoscaling.enabled (gt (int $v.autoscaling.minReplicas) (int $v.autoscaling.maxReplicas)) }}
{{- fail "autoscaling.minReplicas is greater than autoscaling.maxReplicas" }}
{{- end }}
{{- $cfg := include "hikyaku.configDict" . | fromJson }}
{{- if hasKey $cfg "Error" }}
{{- fail (printf "config is not a valid YAML mapping: %s" (get $cfg "Error")) }}
{{- end }}
{{- if not $v.existingConfigMap }}
{{- $admin := include "hikyaku.specAdmin" $cfg | fromJson }}
{{- if get $admin "disabled" }}
{{- fail "the spec sets server.admin.disabled: the chart's probes, admin Service and ServiceMonitor need the admin API" }}
{{- end }}
{{- $listen := get $admin "listen" | default ":9090" }}
{{- $port := include "hikyaku.listenPort" $listen }}
{{- if and $port (ne (int $port) (int $v.adminPort)) }}
{{- fail (printf "the spec's server.admin.listen is %q but adminPort is %d: set adminPort to %s (the probes, the admin Service and the ServiceMonitor use it)" (toString $listen) (int $v.adminPort) $port) }}
{{- end }}
{{- end }}
{{- end }}

{{/*
server.admin of a spec dict, as JSON ({} when absent or not a mapping).
*/}}
{{- define "hikyaku.specAdmin" -}}
{{- $admin := dict }}
{{- $server := get . "server" }}
{{- if kindIs "map" $server }}
{{- $a := get $server "admin" }}
{{- if kindIs "map" $a }}
{{- $admin = $a }}
{{- end }}
{{- end }}
{{- $admin | toJson }}
{{- end }}

{{/*
The items of a list in a spec dict that are mappings, as JSON.
Call it with (dict "spec" $cfg "key" "sources").
*/}}
{{- define "hikyaku.specItems" -}}
{{- $out := list }}
{{- $items := get .spec .key }}
{{- if kindIs "slice" $items }}
{{- range $items }}
{{- if kindIs "map" . }}
{{- $out = append $out . }}
{{- end }}
{{- end }}
{{- end }}
{{- $out | toJson }}
{{- end }}

{{/*
Warnings for NOTES.txt, one per line: listen addresses of an inline spec that
extraPorts does not expose, and a TLS mismatch with redis.external.tls.
*/}}
{{- define "hikyaku.warnings" -}}
{{- $v := .Values }}
{{- $cfg := include "hikyaku.configDict" . | fromJson }}
{{- $exposed := list }}
{{- range $v.extraPorts }}
{{- $exposed = append $exposed (toString (int .port)) }}
{{- end }}
{{- $listeners := list }}
{{- range (include "hikyaku.specItems" (dict "spec" $cfg "key" "sources") | fromJsonArray) }}
{{- if .listen }}
{{- $listeners = append $listeners (dict "what" (printf "source %s" (toString .name)) "listen" .listen) }}
{{- end }}
{{- end }}
{{- range (include "hikyaku.specItems" (dict "spec" $cfg "key" "destinations") | fromJsonArray) }}
{{- if .listen }}
{{- $listeners = append $listeners (dict "what" (printf "destination %s" (toString .name)) "listen" .listen) }}
{{- end }}
{{- end }}
{{- range $listeners }}
{{- $port := include "hikyaku.listenPort" .listen }}
{{- if and $port (not (has $port $exposed)) }}
- {{ .what }} listens on {{ .listen }}, but no extraPorts entry has port {{ $port }}, so neither the pod nor the Service exposes it.
{{- end }}
{{- end }}
{{- if and $v.redis.external.addr $v.redis.external.tls }}
{{- range (include "hikyaku.specItems" (dict "spec" $cfg "key" "stores") | fromJsonArray) }}
{{- $tls := get . "tls" }}
{{- $tlsOn := false }}
{{- if kindIs "map" $tls }}
{{- $tlsOn = get $tls "enabled" }}
{{- end }}
{{- if and (eq (toString .type) "redis") (contains "HIKYAKU_REDIS_ADDR" (toString .addr)) (not $tlsOn) }}
- redis.external.tls is true, but store {{ .name }} does not set tls: {enabled: true}, so hikyaku would reach {{ $v.redis.external.addr }} without TLS.
{{- end }}
{{- end }}
{{- end }}
{{- if and $v.existingConfigMap (include "hikyaku.configKind" .) }}
- config is ignored because existingConfigMap is set.
{{- end }}
{{- if and $v.pdb.enabled (not $v.autoscaling.enabled) (lt (int $v.replicaCount) 2) }}
- pdb is enabled with replicaCount {{ int $v.replicaCount }}: it blocks node drains until there are more replicas.
{{- end }}
{{- end }}
