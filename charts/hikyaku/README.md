# hikyaku Helm chart

[hikyaku 飛脚](https://xavidop.github.io/hikyaku/) is a message relay. It reads from one or more endpoints and delivers to one or more endpoints, over almost any protocol. A YAML spec configures it. It has an AI router (Genkit Go), and retries and fallbacks at every layer. This chart runs the relay on Kubernetes. The chart's values are the spec itself, the secrets it refers to, and the usual workload settings.

## Install

Install the chart from the OCI registry or from the chart repository. Both carry the same chart.

The chart is published as an OCI artifact on ghcr.io:

```bash
helm install hikyaku oci://ghcr.io/xavidop/charts/hikyaku --version <version>
```

It is also published in a chart repository on GitHub Pages:

```bash
helm repo add hikyaku https://xavidop.github.io/hikyaku
helm repo update
helm install hikyaku hikyaku/hikyaku
```

Chart versions follow hikyaku's releases. The chart for `vX.Y.Z` has version and appVersion `X.Y.Z`. So by default it runs `ghcr.io/xavidop/hikyaku:X.Y.Z`. Kubernetes 1.25 or later is required.

With no values, the chart runs a built-in example spec and needs nothing else. The example has an HTTP source on `:8080/ingest` that logs every message to stdout. Its dead-letter queue (DLQ, where a message goes when every delivery attempt failed) is a file under `/var/lib/hikyaku`. Try it:

```bash
kubectl port-forward svc/hikyaku 8080:8080
curl -X POST http://127.0.0.1:8080/ingest -H 'Content-Type: application/json' -d '{"hello":"hikyaku"}'
kubectl logs -l app.kubernetes.io/instance=hikyaku,app.kubernetes.io/component=relay -c hikyaku
```

## The spec

The spec is the YAML file that tells hikyaku what to read, where to deliver it, and how. You give it to the chart in one of three ways.

The relay runs `hikyaku run -f /etc/hikyaku/hikyaku.yaml`. That file comes from one of three places:

- **`config` as a map.** The chart renders it with `toYaml` into the ConfigMap `<fullname>-config`.

  ```yaml
  config:
    version: 1
    name: support-intake
    sources:
      - name: webhooks
        type: http
        listen: ":8080"
        path: /ingest
    destinations:
      - name: billing
        type: https
        url: https://billing.internal/api/events
    routes:
      - name: all
        from: [webhooks]
        to: [billing]
  ```

- **`config` as a string.** The chart uses it as is, comments and anchors included. This is the easiest way to keep your `hikyaku.yaml` file unchanged:

  ```bash
  helm install hikyaku oci://ghcr.io/xavidop/charts/hikyaku --set-file config=hikyaku.yaml
  ```

- **`existingConfigMap`.** It names a ConfigMap you manage, holding the spec under the key `hikyaku.yaml`.

The chart never runs the spec through Helm's `tpl`. So load-time references (`{{secrets.X}}`, `{{env.X}}`) and per-message templates (`{{ .json.id }}`) reach hikyaku untouched.

For an inline spec, the chart checks `server.admin.listen` against `adminPort` (default `9090`). The probes, the admin Service and the ServiceMonitor use that port. The chart also warns, in the install notes, about any `listen:` address that `extraPorts` does not expose.

A change to `config`, `prompts` or `secrets.create` rolls the pods: the pod template carries `checksum/config`, `checksum/prompts` and `checksum/secrets` annotations. An `existingConfigMap` is mounted through a projected volume, so its edits reach the file in place. Run with `extraArgs: [--watch]` to reload them without a restart.

`prompts` adds files next to the spec. A `triage.md` key under `prompts` is mounted at `/etc/hikyaku/prompts/triage.md`.

## Secrets

Secret values (passwords, API keys, signing keys) do not go in the spec. The spec names them, and the chart puts their values in the environment.

The spec declares its secrets as mamori refs (a reference such as `env:NAME`, which hikyaku resolves once at load), usually environment variables:

```yaml
config:
  secrets:
    WEBHOOK_SECRET: env:WEBHOOK_SECRET
  sources:
    - name: webhooks
      type: http
      listen: ":8080"
      auth:
        hmac:
          header: X-Signature-256
          secret: "{{secrets.WEBHOOK_SECRET}}"
```

The chart puts them in the environment with `envFrom`:

- **`secrets.existingSecret`** names a Secret you manage. Each of its keys becomes an environment variable. Use this in production.

  ```bash
  kubectl create secret generic hikyaku-env --from-literal=WEBHOOK_SECRET=...
  helm install hikyaku oci://ghcr.io/xavidop/charts/hikyaku --set secrets.existingSecret=hikyaku-env
  ```

- **`secrets.create`** is a map of `NAME: value` pairs that the chart stores in `<fullname>-secrets`. It is handy in development, but the values end up in Helm's release history.

You can use both. You can also add more `envFrom` entries with `extraEnvFrom`, or single variables with `extraEnv`.

## Redis

hikyaku keeps some state between messages and restarts, and it keeps that state in Redis. The chart can point the spec at an external Redis, or run a small one for development.

Redis holds the history, the decision cache, dedupe keys, the retry and AI budgets, and a source's `state_store` progress (Event Hubs checkpoints, the s3 and gcs objects already read). The chart tells the spec where Redis is through the `HIKYAKU_REDIS_ADDR` environment variable, so the store points at it:

```yaml
config:
  secrets:
    REDIS_PASSWORD: env:REDIS_PASSWORD      # with redis.external.existingSecret
  stores:
    - name: redis-main
      type: redis
      addr: "{{env.HIKYAKU_REDIS_ADDR}}"
      password: "{{secrets.REDIS_PASSWORD}}"
      tls:
        enabled: "{{env.HIKYAKU_REDIS_TLS}}"
```

- **`redis.external.addr`** (recommended in production: ElastiCache, Memorystore and the like) sets `HIKYAKU_REDIS_ADDR` to that `host:port`.
  - **`redis.external.existingSecret`** adds that Secret to `envFrom`. A `REDIS_PASSWORD` key, for example, becomes the `REDIS_PASSWORD` variable that the `secrets:` ref above reads.
  - **`redis.external.tls`** (default `true`) is exported as `HIKYAKU_REDIS_TLS`. A value that is exactly one `{{env.X}}` takes the type of what it resolves to. So `enabled: "{{env.HIKYAKU_REDIS_TLS}}"` under the store's `tls:` is the boolean `true` or `false`, as above. A store can also say `enabled: true` itself. The install notes warn when `redis.external.tls` is on but a store that uses `HIKYAKU_REDIS_ADDR` sets no `tls.enabled`, or sets it to `false`.
- **`redis.enabled: true`** runs a bundled single-replica valkey (`valkey/valkey:8-alpine`, a Deployment and the Service `<fullname>-redis`) and sets `HIKYAKU_REDIS_ADDR=<fullname>-redis:6379`. It is for development only: no persistence, no auth, no TLS. `HIKYAKU_REDIS_TLS` is `"false"` there, and the store takes no password. The bundled valkey is not a subchart, so installing the chart never downloads dependencies.

`redis.enabled` and `redis.external.addr` are exclusive: set one or the other, not both.

## Environment variables set by the chart

The chart sets these variables in the relay container. The spec reads them with `{{env.X}}`.

| Variable | When | Value |
|---|---|---|
| `HIKYAKU_REDIS_ADDR` | `redis.enabled` or `redis.external.addr` | `<fullname>-redis:6379`, or `redis.external.addr` |
| `HIKYAKU_REDIS_TLS` | the same | `"false"` for the bundled valkey, else `redis.external.tls` |
| the keys of `secrets.existingSecret`, `secrets.create` and `redis.external.existingSecret` | when set | `envFrom` |

The pods run with `enableServiceLinks: false`, so Kubernetes adds no `<SERVICE>_PORT` variables of its own.

## Ports, Services and traffic

This section covers how traffic reaches the relay: the source ports, the admin API, Ingress, Gateway API, NetworkPolicy and Prometheus scraping.

- **`extraPorts`** lists the source ports. Each one is a container port and a port on the Service `<fullname>`, whose type is `service.type`. Every `listen:` address in the spec needs an entry. The default is `[{name: http, port: 8080}]`. An entry can set `servicePort`, `protocol`, `appProtocol` and `nodePort`.
- **The admin API** (`/healthz`, `/readyz`, `/metrics`, `/reload`...) listens on `adminPort`. It is always on a separate ClusterIP Service, `<fullname>-admin`, so a LoadBalancer for the sources never exposes it. The liveness and startup probes use `/healthz`. The readiness probe uses `/readyz`. That endpoint is false while a required destination and every destination of its fallback chain are down (`server.readiness.destinations`, on by default), or while a listen source is down. With `server.readiness.models: true`, it is also false while every model of an AI route's chain fails its probe. A probe is reused for 30 seconds, so the kubelet does not call the models on every check.
- **`ingress`** routes to the `extraPorts` entry named by `ingress.servicePort` (default `http`). With no `hosts`, every host goes to `/`.
- **`gatewayAPI`** creates a `gateway.networking.k8s.io/v1` HTTPRoute (the Gateway API's routing rule, which plays the part of an Ingress) to the same port. It needs `parentRefs`, the Gateways to attach to. WebSocket upgrades work through HTTPRoute as they are. But long-lived WebSocket and SSE streams outlive most default request timeouts. Set `gatewayAPI.timeouts.request: "0s"` where your implementation supports it.
- **`networkPolicy`** allows ingress to the source ports from `ingressFrom` peers, and to the admin port from `adminFrom` peers. Both are empty by default, which allows any peer. It also allows all egress, unless `egress` lists rules.
- **`serviceMonitor`** creates a Prometheus Operator ServiceMonitor (the object that tells Prometheus what to scrape). It scrapes `/metrics` on the admin Service.

## Persistence and the StatefulSet

Turn persistence on when the relay must keep its files across a pod restart. Those files are the file DLQ and the disk buffer.

hikyaku keeps file DLQs and its disk buffer under `/var/lib/hikyaku`. For a relay that must not lose an accepted message across a pod's crash, give it both a volume and a disk buffer:

```yaml
persistence:
  enabled: true
  size: 5Gi
config:
  buffer:
    type: disk
    path: /var/lib/hikyaku/buffer.db
```

A restarted pod delivers what its buffer held. Every replica has its own volume and buffer file: a buffer file belongs to one relay.

- With `persistence.enabled: false` (the default), the relay is a Deployment. `/var/lib/hikyaku` is an `emptyDir`, so its files go away with the pod.
- With `persistence.enabled: true`, the relay becomes a StatefulSet (a workload whose pods have stable names and a volume each). It runs with `podManagementPolicy: Parallel`, governed by the Service `<fullname>-headless`. A `volumeClaimTemplate` gives each replica its own PersistentVolumeClaim, `data-<fullname>-<n>`, of `persistence.size`. A restarted or rescheduled pod gets its DLQ back.

Keep in mind:

- Kubernetes cannot change `volumeClaimTemplates` after creation. A new `persistence.size` or `storageClass` does not apply to an existing StatefulSet: resize the PVCs by hand, or reinstall.
- Turning persistence on or off swaps the Deployment for a StatefulSet, or the other way round, on the next `helm upgrade`. The pods are replaced, and the files in an `emptyDir` are lost.
- PVCs outlive `helm uninstall` unless `persistence.retentionPolicy` says otherwise, for example `whenDeleted: Delete` with `whenScaled: Retain`.

`/tmp` is always an `emptyDir`. Together with `/var/lib/hikyaku`, that lets the relay run with `readOnlyRootFilesystem: true`, as uid 65532, with every capability dropped.

## Ollama sidecar

`ollama.enabled: true` adds an `ollama/ollama` sidecar (a second container in the relay's pod) for a local decision or generative model fallback. It listens on `127.0.0.1:11434` only. That is the default URL of hikyaku's ollama plugin and of the System One ollama server, so the spec needs no URL. Models are pulled at runtime. `ollama.models: [llama3.2]` pulls them in the background when the sidecar starts, into an `emptyDir`, so every new pod pulls them again. Give the sidecar memory for its models through `ollama.resources`.

## Values

Every value the chart accepts, with its default and what it does.

| Key | Default | Description |
|---|---|---|
| `image.repository` | `ghcr.io/xavidop/hikyaku` | Image repository. |
| `image.tag` | `""` | Image tag. Empty means the chart's appVersion. |
| `image.digest` | `""` | `sha256:...` digest. When set, it is used instead of the tag. |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy. |
| `replicaCount` | `2` | Replicas, when autoscaling is off. |
| `config` | `{}` | The spec, as a map or a string. Empty means the built-in example. |
| `existingConfigMap` | `""` | A ConfigMap with the spec under `hikyaku.yaml`. It replaces `config`. |
| `prompts` | `{}` | Files mounted at `/etc/hikyaku/prompts/<name>`. |
| `secrets.existingSecret` | `""` | A Secret loaded with `envFrom`. |
| `secrets.create` | `{}` | `NAME: value` pairs stored in `<fullname>-secrets` and loaded with `envFrom`. |
| `extraPorts` | `[{name: http, port: 8080}]` | Source ports: `name`, `port`, and optionally `servicePort`, `protocol`, `appProtocol`, `nodePort`. |
| `service.type` | `ClusterIP` | Type of the sources' Service. |
| `service.annotations` / `service.labels` | `{}` | Service metadata. |
| `service.externalTrafficPolicy` | `""` | `Cluster` or `Local`, for NodePort and LoadBalancer. |
| `service.loadBalancerSourceRanges` | `[]` | Client CIDRs allowed through a LoadBalancer. |
| `ingress.enabled` | `false` | Create an Ingress. |
| `ingress.className` | `""` | IngressClass. |
| `ingress.annotations` | `{}` | Ingress annotations. |
| `ingress.hosts` | `[]` | `[{host, paths: [{path, pathType}]}]`. Empty routes every host to `/`. |
| `ingress.servicePort` | `http` | The `extraPorts` entry it routes to. |
| `ingress.tls` | `[]` | Ingress TLS blocks. |
| `gatewayAPI.enabled` | `false` | Create a Gateway API HTTPRoute. |
| `gatewayAPI.parentRefs` | `[]` | The Gateways to attach to. Required when enabled. |
| `gatewayAPI.hostnames` | `[]` | Hostnames. |
| `gatewayAPI.annotations` / `gatewayAPI.labels` | `{}` | HTTPRoute metadata. |
| `gatewayAPI.servicePort` | `http` | The `extraPorts` entry it routes to. |
| `gatewayAPI.matches` | `[{path: {type: PathPrefix, value: /}}]` | Rule matches. |
| `gatewayAPI.filters` | `[]` | Rule filters. |
| `gatewayAPI.timeouts` | `{}` | Rule timeouts, such as `{request: "0s"}` for long-lived streams. |
| `autoscaling.enabled` | `false` | Create a HorizontalPodAutoscaler (autoscaling/v2). Set `resources.requests`. |
| `autoscaling.minReplicas` / `maxReplicas` | `2` / `10` | Replica range. |
| `autoscaling.targetCPUUtilizationPercentage` | `80` | CPU target. |
| `autoscaling.targetMemoryUtilizationPercentage` | unset | Memory target. |
| `autoscaling.behavior` | `{}` | HPA behavior. |
| `pdb.enabled` | `true` | Create a PodDisruptionBudget. |
| `pdb.minAvailable` | `1` | Pods kept available. |
| `pdb.maxUnavailable` | unset | When set, it is used instead of `minAvailable`. |
| `serviceMonitor.enabled` | `false` | Create a ServiceMonitor for `/metrics`. |
| `serviceMonitor.labels` | `{}` | Labels your Prometheus selects ServiceMonitors by. |
| `serviceMonitor.interval` / `scrapeTimeout` | `30s` / `10s` | Scrape settings. |
| `serviceMonitor.relabelings` / `metricRelabelings` | `[]` | Relabeling rules. |
| `networkPolicy.enabled` | `false` | Create a NetworkPolicy. |
| `networkPolicy.ingressFrom` | `[]` | Peers allowed to the source ports. Empty allows any peer. |
| `networkPolicy.adminFrom` | `[]` | Peers allowed to the admin port. Empty allows any peer. |
| `networkPolicy.egress` | `[]` | Egress rules. Empty allows all egress. |
| `persistence.enabled` | `false` | Run a StatefulSet with a PVC per pod at `/var/lib/hikyaku`. |
| `persistence.size` | `1Gi` | PVC size. |
| `persistence.storageClass` | `""` | StorageClass. Empty uses the default, and `-` sets `""`. |
| `persistence.accessModes` | `[ReadWriteOnce]` | Access modes. |
| `persistence.annotations` | `{}` | PVC annotations. |
| `persistence.retentionPolicy` | `{}` | `persistentVolumeClaimRetentionPolicy`. |
| `ollama.enabled` | `false` | Ollama sidecar on `127.0.0.1:11434`. |
| `ollama.image.repository` / `tag` / `pullPolicy` | `ollama/ollama` / `0.40.15` / `IfNotPresent` | Sidecar image. |
| `ollama.models` | `[]` | Models pulled in the background at startup. |
| `ollama.resources` | `{}` | Sidecar resources. |
| `ollama.securityContext` | `{}` | Sidecar securityContext. Empty uses `securityContext`. |
| `ollama.extraEnv` | `[]` | Extra sidecar environment variables. |
| `redis.enabled` | `false` | Bundled development valkey. |
| `redis.image.repository` / `tag` / `pullPolicy` | `valkey/valkey` / `8-alpine` / `IfNotPresent` | Bundled valkey image. |
| `redis.resources` | `{}` | Bundled valkey resources. |
| `redis.external.addr` | `""` | External Redis `host:port`, exported as `HIKYAKU_REDIS_ADDR`. |
| `redis.external.existingSecret` | `""` | A Secret loaded with `envFrom`, such as one with `REDIS_PASSWORD`. |
| `redis.external.tls` | `true` | Exported as `HIKYAKU_REDIS_TLS`. |
| `securityContext` | read-only root, non-root, no privilege escalation, `drop: [ALL]` | Relay container securityContext. |
| `podSecurityContext` | uid/gid/fsGroup 65532, `runAsNonRoot`, `RuntimeDefault` seccomp | Pod securityContext. |
| `adminPort` | `9090` | Admin API port. It must match `server.admin.listen`. |
| `serviceAccount.create` | `true` | Create a ServiceAccount. |
| `serviceAccount.name` | `""` | Its name. Empty means the full name. |
| `serviceAccount.annotations` | `{}` | Its annotations (workload identity, IRSA). |
| `serviceAccount.automount` | `false` | Mount its token. hikyaku calls the Kubernetes API only to resolve `k8s-secret://` secret refs; turn this on for those. |
| `podAnnotations` / `podLabels` | `{}` | Pod metadata. |
| `resources` | `{}` | Relay container resources. |
| `nodeSelector` / `tolerations` / `affinity` | `{}` / `[]` / `{}` | Scheduling. |
| `topologySpreadConstraints` | `[]` | Spread constraints. |
| `priorityClassName` | `""` | PriorityClass. |
| `terminationGracePeriodSeconds` | `30` | Keep it above the spec's `server.shutdown_grace` (default 20s). |
| `imagePullSecrets` | `[]` | Registry pull secrets. |
| `extraArgs` | `[]` | Arguments after `run -f /etc/hikyaku/hikyaku.yaml`, such as `[--watch]`. |
| `extraEnv` / `extraEnvFrom` | `[]` | More environment. |
| `extraVolumes` / `extraVolumeMounts` | `[]` | More volumes. |
| `livenessProbe` / `readinessProbe` / `startupProbe` | `/healthz`, `/readyz`, `/healthz` on `admin` | Probes. |
| `lifecycle` | `{}` | Lifecycle hooks, such as `{preStop: {sleep: {seconds: 5}}}`. The image has no shell. |
| `tests.image.repository` / `tag` / `pullPolicy` | `curlimages/curl` / `8.23.0` / `IfNotPresent` | Image of the `helm test` pod. It curls `/healthz` and, with the built-in example spec, posts one message to its `http` source and expects it delivered. |
| `nameOverride` / `fullnameOverride` | `""` | Name overrides. |

`values.schema.json` validates every value and rejects unknown keys. So a typo fails the install instead of being ignored. The spec under `config` is the exception: hikyaku validates it at startup against its own [schema](https://xavidop.github.io/hikyaku/schema/hikyaku.schema.json).

## Testing the chart

Two Make targets test the chart, one offline and one on a local cluster.

- `make helm-lint` runs `helm lint` with the defaults and each `ci/*-values.yaml`.
- `make helm-test` runs `scripts/helm-test.sh`. It builds `ghcr.io/xavidop/hikyaku:ci` from the repository, loads it into the kind cluster `hikyaku-ct`, and runs `ct lint` and `ct install` from the chart-testing image. `ct install` installs once per `ci/*-values.yaml`, then runs `helm test`. `scripts/helm-test.sh --delete` removes the cluster.

The Helm workflow (`.github/workflows/helm.yml`) runs the same checks on every change to the chart.
