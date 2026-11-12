# Contributing to hikyaku

Thanks for your interest. This guide explains how to build and test hikyaku, how the repository
is laid out, what a change needs before it merges, and how to add a protocol. Contributions are
welcome, especially new protocols.

## Ground rules

- Be kind. See the [Code of Conduct](CODE_OF_CONDUCT.md).
- **Never commit real credentials**: not in code, tests, fixtures, example specs or issue reports.
- Open an issue before a large change, so we agree on scope before you build it.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

## Build and test

You need **Go 1.27 or newer**. A plain clone builds on its own: every module points at the
modules it needs in this repository with `replace` directives. So a build with the workspace off
works from a checkout.

```bash
git clone https://github.com/xavidop/hikyaku
cd hikyaku
make build       # build every module; the binary is cmd/hikyaku/hikyaku
make all         # what CI runs: fmt, vet, check-deps, check-claims, schema-check, test, lint, test-llmfake
```

**Run `make all` before each commit.** CI runs the same checks, plus the race detector,
coverage and the suites below.

Other targets:

```bash
make test             # test every module
make race             # test every module with the race detector
make lint             # golangci-lint across modules
make test-llmfake     # the suites that run against the fake model provider (-tags llmfake)
make build-llmfake    # bin/hikyaku with the fake model provider, for the *.llmfake.yaml examples
make lint-integration # golangci-lint -tags integration, for the container suites
make integration      # run the examples end to end with the fake model provider
make chaos-e2e        # the resilience suite: fault sinks, hikyaku chaos, fallbacks and the DLQ
make release-check    # a throwaway Go module release of HEAD, cut in a scratch clone and go-installed from outside
make vuln             # govulncheck every module, minus the IDs in .github/govulncheck-ignore (reasons inside)
make schema           # regenerate schema/hikyaku.schema.json and the spec reference page from the Go structs
make check-claims     # the README and docs against the code: commands, flags, protocols
make notices          # regenerate THIRD_PARTY_LICENSES.csv and THIRD_PARTY_NOTICES.md after a dependency change
make genkit-ui        # run the ai-triage example under `genkit start` to inspect routing flows
make docker           # build the image locally as ghcr.io/xavidop/hikyaku:dev
make helm-lint        # helm lint the chart
make helm-test        # install the chart on a kind cluster with chart-testing (needs Docker)
make site-build       # build the docs site (Node 22 through nvm)
make site-linkcheck   # build the site and fail on a broken internal link
make demo             # record site/public/demo.gif with vhs (needs vhs, ttyd and ffmpeg)
```

Every module is built with the workspace off (`GOWORK=off`), as the Makefile does. That way each
`go.mod` stays correct on its own.

### Build tags

A build tag turns a set of files on at build time. hikyaku uses three:

- `llmfake` compiles the fake model provider into `llm` and the CLI: decision and generative
  models whose answers are deterministic (the same input always gives the same answer). With it,
  the AI routing suites and the `*.llmfake.yaml` examples run with no network and no API key. A
  default build never contains it.
- `integration` marks the container suites. They start real servers (Redis, Kafka, NATS and so
  on) with testcontainers, so they need Docker. `make all` does not run them. CI type-checks
  them with `go vet -tags integration` and lints them with the same tag, so a refactor cannot
  leave them behind; run them yourself with `go test -tags integration ./...` in the module.
- `lite` builds the CLI without the protocols whose SDKs are heavy. The release ships it as the
  `hikyaku-lite_*` archives.

## Repository layout

This is a multi-module repository: several Go modules in one repository, tied together by a
`go.work` file. Each module below is named by its directory.

- `/` (module `github.com/xavidop/hikyaku`): the core, a light library. It holds the public API
  (`hikyaku.Run`); the `envelope` and `plugin` contracts (sources, sinks, routers and the registry
  protocols register with); the engine (dispatch, delivery, retries, breakers, retry budgets,
  fallbacks, groups, the DLQ); the routers; the transforms (CEL, jq, templates); the `store`
  interfaces with the memory store; metrics; `schema/`; and, under `internal/`, the config,
  secrets (mamori), redaction, admin API and CLI packages. It must never depend on Genkit, a model
  SDK, Redis, or a broker or cloud SDK. `make check-deps` fails the build if it does.
- `cmd/hikyaku`: the `hikyaku` binary, its own module. `app/wiring.go` registers the protocols
  every build has (with `app/wiring_web.go` for ws and sse). `app/wiring_full.go`
  (`//go:build !lite`) registers the ones with heavy SDKs, the out-of-process plugin host and the
  cloud secret providers. `app/app.go` hands the CLI its heavy pieces (the AI router, the Redis
  store, the fault wrapper). It is the only module that links everything.
- `llm`: the only place a model is called, its own module built on [Genkit](https://genkit.dev).
  It holds the decision engine (System One decision models) and the generative engine, model
  retries and fallback chains, the routing flows and, behind `-tags llmfake`, the fake provider CI
  runs with.
- `stores/redis`: the Redis history store and KV (standalone, sentinel and cluster), its own
  module on go-redis.
- `grpcplugin`: out-of-process plugins over gRPC, its own module. It holds the host that starts
  the binaries a spec lists under `plugins:` (hashicorp/go-plugin), the Go SDK for writing one
  (`grpcplugin.Serve`), the `hikyaku.plugin.v1` protocol between them (which a plugin in another
  language implements), and an example plugin.
- One module per protocol family under `protocols/`, in alphabetical order:
  - `protocols/amqp`: `amqp` sources (subscribe: a RabbitMQ queue, manual acks) and destinations
    (publisher confirms).
  - `protocols/aws`: `sqs`, `sns` and `eventbridge` sources (subscribe, through an SQS queue) and
    destinations, and `s3` sources (poll) and destinations.
  - `protocols/azure`: `servicebus` and `eventhubs` sources (subscribe; Event Hubs checkpoints and
    leases in a hikyaku store) and destinations.
  - `protocols/fault`: the fault-injecting sink behind `hikyaku chaos` and the resilience suite.
  - `protocols/file`: `stdin`, `stdout`, `stderr` and `file` (NDJSON).
  - `protocols/gcp`: `pubsub` sources (subscribe) and destinations, and `gcs` sources (poll) and
    destinations.
  - `protocols/grpc`: `grpc` sources (listen: the built-in `hikyaku.relay.v1.Relay` and the
    services of a descriptor set; subscribe: a server-streaming method) and destinations (calls to
    unary and streaming methods).
  - `protocols/http`: `http` and `https` sources (listen, poll) and destinations.
  - `protocols/kafka`: `kafka` sources (a consumer group, commits past the settled prefix) and
    destinations (the idempotent producer), on franz-go.
  - `protocols/mqtt`: `mqtt` sources (subscribe: persistent sessions, in-order acks, shared
    subscriptions) and destinations, MQTT 5 and 3.1.1.
  - `protocols/nats`: `nats` and `jetstream` sources (subscribe; JetStream through durable pull
    consumers) and destinations.
  - `protocols/openai`: the `openai` source, an OpenAI-compatible endpoint for the LLM-proxy mode
    (its `model` destinations live in `llm`).
  - `protocols/redis`: `redis` sources (subscribe: a stream through a consumer group, or pub/sub)
    and destinations (XADD, PUBLISH). `stores/redis` is the history store, a different module.
  - `protocols/schedule`: the `schedule` source (a message per cron tick or interval).
  - `protocols/sse`: `sse` sources (listen, subscribe) and destinations.
  - `protocols/tcp`: `tcp` and `udp` sources (listen) and destinations, with newline,
    length-prefix or raw framing, TLS on tcp and DTLS on udp. It uses the standard library and
    pion/dtls, pure Go, so hikyaku-lite has it too.
  - `protocols/ws`: `ws` and `wss` sources (listen, subscribe) and destinations.
- `charts/hikyaku`: the Helm chart.
- `examples/`: runnable specs, each with a `hikyaku.yaml`, a `hikyaku.llmfake.yaml` and a README.
  Three of them are modules of their own. `examples/custom-destination` is a custom build that
  adds a destination type with the plugin SDK (`plugin.RegisterSink` + `app.Main`).
  `examples/crm-plugin` and `examples/postgres-plugin` are out-of-process plugins, binaries
  served with `grpcplugin.Serve`: a CRM destination, and PostgreSQL as a destination and an
  outbox source.
- `site/`: the Astro documentation site.
- `internal/tools`: the repository's own tooling, its own module, never released: `release` pins
  and tags the Go modules of a release.

Every module has its own `go.mod`, a `go.work` entry and a line in the `directories` list of
`.github/dependabot.yml` (one gomod entry covers them all, so an update is one PR across modules).

## Tests

- **Every package gets tests**, with testify (`assert`, `require`), table-driven where it fits.
- **No network in unit tests.** A protocol tests against an in-process fake of the network, the
  AI routers against `llmfake`, and the Redis store against miniredis.
- **Container suites** sit behind `//go:build integration` and start their servers with
  testcontainers.
- **The examples are tests too.** `make integration` runs them with the fake model provider, so
  keep each example's `hikyaku.llmfake.yaml` in step with its `hikyaku.yaml`.
- **Resilience changes** get a case in the resilience suite (`protocols/fault`, `make chaos-e2e`):
  the expected attempt counts, and where the message ends up (the destination, a fallback or the
  DLQ).

## Adding a protocol

A protocol is a Go module under `protocols/<name>`. Its `Register` function registers a source
factory, a sink factory or both with the `plugin` registry. Every protocol of the
[protocols page](https://xavidop.github.io/hikyaku/docs/protocols/) is implemented. For a new one, open a feature request
first, or ship it as an out-of-process plugin (`grpcplugin`) without touching this repository. The checklist:

- **Module.** One module under `protocols/<name>` with `require github.com/xavidop/hikyaku v0.0.0`
  and `replace github.com/xavidop/hikyaku => ../..`. Its SDK stays out of the root module
  (`make check-deps`).
- **Config.** Its options live in `internal/config`, as an inline struct on `Source` and
  `Endpoint`, so `make schema` reflects them without importing the module. The module's
  `config.go` converts and validates its slice of them. Run `make schema` and commit the result.
- **Registration.** Call its `Register` from `cmd/hikyaku/app/wiring.go`, and add a `require` and
  a `replace` line for it to `cmd/hikyaku/go.mod`. If its SDK is heavy, call it from
  `registerFull` in `cmd/hikyaku/app/wiring_full.go` instead (`//go:build !lite`), so `-tags lite`
  leaves it out, and add its types to `fullOnly` in `internal/config/validate.go`.
- **Errors.** A failed delivery returns a `*plugin.Error` with a `Class` and `Tags` (`5xx`, `429`,
  `connect`, `timeout`, ...), so `retry_on` and `never_on` can match it.
- **Secrets.** Credentials arrive as `{{secrets.NAME}}` references resolved through mamori. They
  are revealed only at the point of use, and never logged.
- **Tests.** Unit tests against a fake of the network; container tests behind
  `//go:build integration`.
- **Repository wiring.** A `go.work` entry, and a line in dependabot's `directories` (CI fails
  without it).
- **Docs.** Its docs page under `site/src/pages/docs/`, the protocol tables in the README and the
  docs, and the sidebar. If the docs named the protocol before it shipped, remove its name from
  the guarded words in `.github/scripts/check_readme_claims.sh`; those words keep the docs from
  claiming a protocol before it ships. Add its scope to the Protocols group of the changelog in
  `.goreleaser.yaml`.

A destination or source type that lives outside this repository is a plugin instead. It
implements `plugin.Sink` or `plugin.Source` and registers itself before the CLI runs, with its
settings in the entry's `options:` block.

## Commits and pull requests

- Keep pull requests focused, and reference the issue they resolve.
- Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`,
  `chore:`, `refactor:`, `test:`, `ci:`). Releases and the changelog are generated from them:
  `fix:` makes a patch release, `feat:` a minor one, and `feat!:` or `BREAKING CHANGE:` a major
  one.
- Keep commits small; each one passes `make all`.
- Scopes are `cli`, `config`, `engine`, `router`, `transform`, `store`, `llm`, `redis`, `admin`,
  `secrets`, `schema`, `helm`, `site` and the protocol name (`http`, `ws`, `sse`, `file`,
  `fault`).
- Fill out the pull request template.
- CI must be green before review.
