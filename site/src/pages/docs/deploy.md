---
layout: ../../layouts/DocsLayout.astro
title: Deploy (Docker, Helm)
---

# Deploy (Docker, Helm)

This page shows how to run hikyaku as a service: in Docker, and on Kubernetes with the Helm
chart. It also covers what changes when you run more than one replica. One binary runs three
ways: as the CLI on your machine, as the container image, and through the Helm chart on
Kubernetes. The spec is the same file in all three.

## Docker

```sh
docker run --rm -p 8080:8080 -p 9090:9090 \
  -v $PWD/hikyaku.yaml:/etc/hikyaku/hikyaku.yaml \
  -e TYPESAFE_API_KEY -e WEBHOOK_SECRET \
  ghcr.io/xavidop/hikyaku:latest
```

- **Image**: `ghcr.io/xavidop/hikyaku`, tagged with each version and `latest`, for linux/amd64
  and linux/arm64, signed with cosign.
- **Base**: `gcr.io/distroless/static:nonroot`. No shell, no package manager, a non-root user.
- **Entrypoint**: `ENTRYPOINT ["hikyaku"]` and `CMD ["run","-f","/etc/hikyaku/hikyaku.yaml"]`.
  Mount the spec at `/etc/hikyaku/hikyaku.yaml`, or pass another command:
  `docker run ... ghcr.io/xavidop/hikyaku:latest validate -f /etc/hikyaku/hikyaku.yaml`.
- **Ports**: 8080 for sources, 9090 for the admin API (`/healthz`, `/readyz`, `/metrics`).
- **Secrets**: pass the environment variables your `env:` secret refs read, as `-e NAME`.

To keep a file DLQ or file destinations across restarts, mount a volume and point their `path`
at it.

## Helm

```sh
helm install hikyaku oci://ghcr.io/xavidop/charts/hikyaku --set-file config=hikyaku.yaml
```

The chart lives at `charts/hikyaku` in the repository. It is published in two places: as an OCI
artifact at `oci://ghcr.io/xavidop/charts/hikyaku`, and in a chart repository served with these
docs (`helm repo add hikyaku https://xavidop.github.io/hikyaku`). Chart versions follow hikyaku's
releases: the chart of `vX.Y.Z` runs `ghcr.io/xavidop/hikyaku:X.Y.Z` by default.
`values.schema.json` checks the values you pass. With no values at all, the chart runs a built-in
example: an HTTP source on `:8080/ingest` that logs every message to stdout.

### Values

These are the top-level values, with their defaults. `charts/hikyaku/values.yaml` documents every
key:

```yaml
image:
  repository: ghcr.io/xavidop/hikyaku
  tag: ""                       # empty: the chart's appVersion
  digest: ""
  pullPolicy: IfNotPresent

replicaCount: 2

config: {}                      # the hikyaku.yaml spec, a map or a string (--set-file)
existingConfigMap: ""           # or a ConfigMap you manage, with the spec under hikyaku.yaml
prompts: {}                     # extra files, mounted at /etc/hikyaku/prompts/<name>

secrets:                        # environment for the env: secret refs
  existingSecret: ""
  create: {}

extraPorts:                     # every listen address of the spec
  - name: http
    port: 8080

service:
  type: ClusterIP
  annotations: {}
  labels: {}
  externalTrafficPolicy: ""
  loadBalancerSourceRanges: []

ingress:
  enabled: false
  className: ""
  annotations: {}
  hosts: []
  servicePort: http
  tls: []

gatewayAPI:                     # an HTTPRoute
  enabled: false
  parentRefs: []
  hostnames: []
  annotations: {}
  labels: {}
  servicePort: http
  matches:
    - path:
        type: PathPrefix
        value: /
  filters: []
  timeouts: {}

autoscaling:
  enabled: false
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 80
  behavior: {}

pdb:
  enabled: true
  minAvailable: 1

serviceMonitor:
  enabled: false
  labels: {}
  interval: 30s
  scrapeTimeout: 10s
  relabelings: []
  metricRelabelings: []

networkPolicy:
  enabled: false
  ingressFrom: []
  adminFrom: []
  egress: []

persistence:                    # a PVC per pod: the chart becomes a StatefulSet
  enabled: false
  size: 1Gi
  storageClass: ""
  accessModes:
    - ReadWriteOnce
  annotations: {}
  retentionPolicy: {}

ollama:                         # a local model, as a sidecar
  enabled: false
  image:
    repository: ollama/ollama
    tag: 0.40.15
    pullPolicy: IfNotPresent
  models: []
  resources: {}
  securityContext: {}
  extraEnv: []

redis:                          # history, decision cache, dedupe, budgets
  enabled: false                # a bundled Valkey, for development
  image:
    repository: valkey/valkey
    tag: 8-alpine
    pullPolicy: IfNotPresent
  resources: {}
  external:                     # recommended in production
    addr: ""
    existingSecret: ""
    tls: true

securityContext:
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities:
    drop:
      - ALL

adminPort: 9090
```

| Value | What it does |
|---|---|
| `config` | The spec, rendered into a ConfigMap mounted at `/etc/hikyaku/hikyaku.yaml`. `{{secrets.X}}`, `{{env.X}}` and per-message templates reach hikyaku untouched: the chart never runs them through `tpl`. A `checksum/config` annotation rolls the pods when it changes. |
| `secrets` | The environment your `env:` secret refs read, loaded with `envFrom`: `existingSecret` names a Secret you manage, `create` stores name and value pairs in one the chart makes. |
| `extraPorts` | The ports your listen sources use, on the container and the Service. Every listen address of the spec needs an entry. The admin API always gets its own ClusterIP Service, `<fullname>-admin`. |
| `ingress`, `gatewayAPI` | Expose the source ports with an Ingress or a Gateway API `HTTPRoute`. WebSocket upgrades need nothing special, but long-lived WebSocket and SSE streams outlive most default timeouts: raise them with the Ingress annotations or `gatewayAPI.timeouts`. |
| `serviceMonitor` | A Prometheus Operator `ServiceMonitor` for `/metrics` on the admin port. |
| `networkPolicy` | A NetworkPolicy: `ingressFrom` for the source ports, `adminFrom` for the admin port, `egress` rules. |
| `persistence` | A PersistentVolumeClaim per pod at `/var/lib/hikyaku`, for file DLQs and file destinations; with it the chart deploys a StatefulSet. Off, a Deployment gets an `emptyDir` there. |
| `ollama` | An Ollama sidecar on `127.0.0.1:11434`, the default URL of the ollama plugin and the ollama System One server, so a model chain can end with a model that is always there. `ollama.models` are pulled when it starts. |
| `redis` | The shared store, bundled or external (below). |
| `securityContext` | A read-only root filesystem, non-root, no privilege escalation, every capability dropped; hikyaku only writes to `/tmp` and `/var/lib/hikyaku`, which the chart mounts. |

### Redis

A Redis store holds the state that replicas share (see
[Running several replicas](#running-several-replicas)). The chart hands the Redis address to the
pod as the environment variable `HIKYAKU_REDIS_ADDR`:

- **Bundled** (`redis.enabled: true`): a single-replica Valkey (a Redis-compatible server) with
  no persistence, no auth and no TLS. It is for development. `HIKYAKU_REDIS_ADDR` is
  `<fullname>-redis:6379`.
- **External** (`redis.external.addr`): a Redis you run or rent (ElastiCache, Memorystore, your
  own). This is the recommended setup in production. `HIKYAKU_REDIS_ADDR` is that address.
  `redis.external.existingSecret` is loaded with `envFrom`, so a key such as `REDIS_PASSWORD`
  reaches the spec's secrets. `redis.external.tls` is exported as `HIKYAKU_REDIS_TLS`.

The spec reads the address with `{{env.HIKYAKU_REDIS_ADDR}}`. The store itself still has to say
`tls`:

```yaml
secrets:
  REDIS_PASSWORD: env:REDIS_PASSWORD         # from redis.external.existingSecret

stores:
  - name: redis-main
    type: redis
    addr: "{{env.HIKYAKU_REDIS_ADDR}}"
    password: "{{secrets.REDIS_PASSWORD}}"
    tls:
      enabled: true
```

### Probes and shutdown

- **Liveness** (is the process up?): `/healthz` on the admin port. A startup probe on the same
  path gives the spec up to a minute to load and the listeners to open.
- **Readiness** (can the pod take traffic?): `/readyz`. It turns false while a required
  destination and every destination of its fallback chain are down
  (`server.readiness.destinations`, on by default), and while a listen source is down. With
  `server.readiness.models: true`, it also turns false while every model of an AI route's chain
  fails its probe. The probe result is reused for 30 seconds, so the kubelet's checks do not call
  the models each time. A pod that cannot deliver stops getting traffic. See
  [Readiness](/docs/observability/#readiness).
- **Shutdown**: `terminationGracePeriodSeconds` (30 by default) must be longer than
  `server.shutdown_grace` (20s by default). A `lifecycle.preStop` sleep gives Kubernetes time to
  take the pod out of the Service's endpoints before the relay stops. It is off by default
  (`lifecycle: {}`), and it needs Kubernetes 1.30 or newer, because the image has no shell.

CI lints the chart with every `ci/*-values.yaml`, then installs it on kind (Kubernetes in
Docker) with `ct install` and runs `helm test`. `make helm-lint` and `make helm-test` do the
same on your machine.

## Running several replicas

Replicas share nothing in memory. State that must hold across them lives in a shared
[store](/docs/spec/#stores). Point every one of these at the same Redis:

| State | Key | Default | Without a shared store |
|---|---|---|---|
| Routing history | `history.store` | the first store | each replica sees only the calls it routed itself |
| Dedupe and cross-replica idempotency | a source's `dedupe.store` | the first store | each replica catches only its own duplicates |
| AI decision cache | `cache.store` | in-process | each replica asks the model again |
| Retry budgets | `retry_budget.store` | in-process | each replica spends a full budget |
| AI budgets | `budget.store` | in-process | each replica spends a full budget |

With a Redis store shared this way, two replicas behave like one relay. A follow-up routed by one
replica sees the history the other wrote. A message deduped by one replica is not emitted again
by the other.

Other things to know with more than one replica:

- Listen sources scale behind the Service: any replica can take any request.
- Poll and subscribe sources run on every replica, so each replica polls, or holds a connection,
  on its own. To handle each message once, give the source a `dedupe` key on the shared store, or
  run it on a single replica. `dedupe` works on every source type. A replica drops a message
  whose key was seen within the `ttl` before routing it, acknowledges it to its source as
  filtered, and counts it in `hikyaku_deduped_total{source}`.
- `pdb.minAvailable` and `server.shutdown_grace` keep a rollout from losing in-flight messages.
  A stopping pod gets the grace period to finish what it holds. It nacks the rest (tells the
  sources they are not done), so senders can redeliver them.
