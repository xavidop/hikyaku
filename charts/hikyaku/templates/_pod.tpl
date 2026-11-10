{{/*
The relay pod template, shared by the Deployment and the StatefulSet.
*/}}
{{- define "hikyaku.podTemplate" -}}
metadata:
  annotations:
    checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
    {{- if .Values.prompts }}
    checksum/prompts: {{ include (print $.Template.BasePath "/configmap-prompts.yaml") . | sha256sum }}
    {{- end }}
    {{- if .Values.secrets.create }}
    checksum/secrets: {{ include (print $.Template.BasePath "/secret.yaml") . | sha256sum }}
    {{- end }}
    {{- with .Values.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  labels:
    {{- include "hikyaku.labels" . | nindent 4 }}
    app.kubernetes.io/component: relay
    {{- with .Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  serviceAccountName: {{ include "hikyaku.serviceAccountName" . }}
  automountServiceAccountToken: {{ .Values.serviceAccount.automount }}
  # Service links would add HIKYAKU_*_PORT variables for this release's
  # Services, next to the HIKYAKU_* variables the chart sets.
  enableServiceLinks: false
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.priorityClassName }}
  priorityClassName: {{ . }}
  {{- end }}
  terminationGracePeriodSeconds: {{ int .Values.terminationGracePeriodSeconds }}
  {{- with .Values.podSecurityContext }}
  securityContext:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  containers:
    - name: hikyaku
      image: {{ include "hikyaku.image" . | quote }}
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      args:
        - run
        - -f
        - /etc/hikyaku/hikyaku.yaml
        {{- range .Values.extraArgs }}
        - {{ . | quote }}
        {{- end }}
      ports:
        {{- range .Values.extraPorts }}
        - name: {{ .name }}
          containerPort: {{ int .port }}
          protocol: {{ .protocol | default "TCP" }}
        {{- end }}
        - name: admin
          containerPort: {{ int .Values.adminPort }}
          protocol: TCP
      {{- $redisAddr := include "hikyaku.redisAddr" . }}
      {{- if or $redisAddr .Values.extraEnv }}
      env:
        {{- if $redisAddr }}
        - name: HIKYAKU_REDIS_ADDR
          value: {{ $redisAddr | quote }}
        - name: HIKYAKU_REDIS_TLS
          value: {{ and (not .Values.redis.enabled) .Values.redis.external.tls | toString | quote }}
        {{- end }}
        {{- with .Values.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- end }}
      {{- if or .Values.secrets.create .Values.secrets.existingSecret .Values.redis.external.existingSecret .Values.extraEnvFrom }}
      envFrom:
        {{- if .Values.secrets.create }}
        - secretRef:
            name: {{ include "hikyaku.secretName" . }}
        {{- end }}
        {{- with .Values.secrets.existingSecret }}
        - secretRef:
            name: {{ . }}
        {{- end }}
        {{- with .Values.redis.external.existingSecret }}
        - secretRef:
            name: {{ . }}
        {{- end }}
        {{- with .Values.extraEnvFrom }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- end }}
      {{- with .Values.startupProbe }}
      startupProbe:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.livenessProbe }}
      livenessProbe:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.readinessProbe }}
      readinessProbe:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.lifecycle }}
      lifecycle:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.resources }}
      resources:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.securityContext }}
      securityContext:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      volumeMounts:
        - name: config
          mountPath: /etc/hikyaku
          readOnly: true
        - name: tmp
          mountPath: /tmp
        - name: data
          mountPath: /var/lib/hikyaku
        {{- with .Values.extraVolumeMounts }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
    {{- if .Values.ollama.enabled }}
    - name: ollama
      image: {{ printf "%s:%s" .Values.ollama.image.repository (toString .Values.ollama.image.tag) | quote }}
      imagePullPolicy: {{ .Values.ollama.image.pullPolicy }}
      {{- if .Values.ollama.models }}
      # `ollama serve`, then the models pulled in the background, so the pod
      # does not wait for multi-gigabyte downloads.
      command:
        - /bin/sh
        - -c
        - |
          /bin/ollama serve &
          pid=$!
          trap 'kill -TERM "$pid" 2>/dev/null' TERM INT
          (
            until /bin/ollama list >/dev/null 2>&1; do sleep 1; done
            for model in $OLLAMA_PULL_MODELS; do
              /bin/ollama pull "$model" || echo "ollama: could not pull $model" >&2
            done
          ) &
          wait "$pid"
          status=$?
          if [ "$status" -gt 128 ]; then wait "$pid"; status=$?; fi
          exit "$status"
      {{- else }}
      args:
        - serve
      {{- end }}
      env:
        # Loopback only: the relay is the sidecar's only client.
        - name: OLLAMA_HOST
          value: 127.0.0.1:11434
        - name: HOME
          value: /ollama
        - name: OLLAMA_MODELS
          value: /ollama/models
        {{- with .Values.ollama.models }}
        - name: OLLAMA_PULL_MODELS
          value: {{ join " " . | quote }}
        {{- end }}
        {{- with .Values.ollama.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      ports:
        - name: ollama
          containerPort: 11434
          protocol: TCP
      {{- with .Values.ollama.resources }}
      resources:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with (.Values.ollama.securityContext | default .Values.securityContext) }}
      securityContext:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      volumeMounts:
        - name: ollama
          mountPath: /ollama
        - name: ollama
          mountPath: /tmp
          subPath: tmp
    {{- end }}
  volumes:
    - name: config
      projected:
        sources:
          - configMap:
              name: {{ include "hikyaku.configMapName" . }}
              items:
                - key: hikyaku.yaml
                  path: hikyaku.yaml
          {{- if .Values.prompts }}
          - configMap:
              name: {{ include "hikyaku.componentName" (dict "root" . "suffix" "prompts") }}
              items:
                {{- range $file, $_ := .Values.prompts }}
                - key: {{ $file | quote }}
                  path: {{ printf "prompts/%s" $file | quote }}
                {{- end }}
          {{- end }}
    - name: tmp
      emptyDir: {}
    {{- if not .Values.persistence.enabled }}
    - name: data
      emptyDir: {}
    {{- end }}
    {{- if .Values.ollama.enabled }}
    - name: ollama
      emptyDir: {}
    {{- end }}
    {{- with .Values.extraVolumes }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .Values.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.topologySpreadConstraints }}
  topologySpreadConstraints:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
