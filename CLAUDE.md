# CLAUDE.md

Guidance for agents working in this repository. The design document (DESIGN.md) that drove the build is not in the repository; the section numbers (§n) below refer to it. The docs site is the reference for what is built.

## Shape

- A `go.work` multi-module monorepo shaped like [tamesu](https://github.com/xavidop/tamesu). The root module (`github.com/xavidop/hikyaku`) is light. Genkit lives in `llm/`, Redis in `stores/redis/`, each protocol in `protocols/<name>/`, and the binary in `cmd/hikyaku/`. Every non-root module uses `require github.com/xavidop/hikyaku v0.0.0` with a `replace` to the relative path.
- The Makefile runs every target with `GOWORK=off`, module by module, the way CI does. Before each commit, run `make all`.
- `make check-deps` fails when the root module pulls Genkit, a model SDK, Redis, or a broker or cloud SDK (`.github/scripts/check_root_deps.sh`). It also fails when any module other than `llm/` imports Genkit or a model provider (`check_ai_deps.sh`).
  - Unlike tamesu's, the root check reads the modules of the packages actually compiled (`go list -deps -test`), not `go list -m all`. OTel's exporter brings in grpc, whose `go.mod` names `cloud.google.com/go/auth`, but nothing builds it.
- `make schema` regenerates `schema/hikyaku.schema.json` and `site/src/pages/docs/spec-reference.md` from `internal/config`. `make schema-check` and `TestSchemaFileUpToDate` fail when either is stale.

## Conventions

- Commits are conventional (`feat:`, `fix:`, `chore:`, `docs:`, `test:`), small, and each one passes `make all`.
- Every package has tests. Use testify (`assert`/`require`). The fake-model suites build with `-tags llmfake` (`make test-llmfake`). The container suites build with `-tags integration`.
- Errors from sinks are `*plugin.Error`, with a `Class` and `Tags` (`5xx`, `503`, `429`, `connect`, `timeout`, `ws:close_1011`). Retry policies match tags, so the more specific tag wins: `429` in `retry_on` beats `4xx` in `never_on`.
- Secrets are mamori refs, resolved once at load. A secret is never logged: the slog handler goes through `internal/redact`. `/debug` output is redacted too.
- Exit codes follow tamesu: 0 ok, 1 failed (checks, evals, DLQ'd sends), 2 config/usage, 3 infrastructure.

## Adaptations from tamesu and the design

These record where the build departs from the design, and the decisions the design left open.

- **§15 answers (from the owner, 2026-11-09):**
  - `multi: true` sends one System One request with one `NoulQuestion` per candidate.
  - The schema `$id` base is `https://xavidop.github.io/hikyaku/schema/`.
  - History is stored per route.
  - v1 has no generative second opinion and no generative history summarizer.
- **Protocol options live in `internal/config`**, as inline structs on `Source`/`Endpoint`. That way `schema/gen.go` in the root module reflects every key without importing protocol modules. `protocols/<name>/config.go` converts and validates its slice of them. The schema uses the `yaml` tags (`FieldNameTag: "yaml"`).
- **`internal/redact`** is tamesu's secret-value redactor plus `jsonpath.go`, which handles the JSONPath redaction of AI states (`input.redact`).
- **`internal/secrets`** is tamesu's mamori resolution, without the suite-specific collectors. `EnvLookup` returns a plain `func(string) (string, bool)`.
- **Validation is split in two.**
  - `config.Validate` does the structural checks: references, fallback cycles, the protocol matrix, required fields.
  - `engine.Check` compiles every CEL expression, jq program and template.
  - `hikyaku validate` runs both. `config` never imports `transform`.
- **Multi-file specs:**
  - Lists are concatenated, and names must stay unique (the error names both files).
  - `secrets` are unioned.
  - Each other section may be set in one file only.
  - Every file must agree on `name`: one relay per process from the CLI. The library can run several.
- **Store interface changes from §6.5:**
  - `Append` takes `AppendOptions{TTL, Keep}`, because the Redis `LTRIM` length comes from the route.
  - `Feedback` returns the updated `CallRecord`, so the admin API can count `hikyaku_feedback_total{route}`.
  - `Records(ctx, route, since, fn)` backs `history export`.
- **Error tags in YAML:** `retry_on: [5xx, 429]` is a `TagList`, and the schema accepts bare numbers.
- **Custom destinations and sources are plugins** (decided with the owner on 2026-11-09: the Go SDK now, out-of-process gRPC plugins in M3).
  - A plugin implements `plugin.Sink` (or `plugin.Source`) and registers with `plugin.RegisterSink(kind, factory, plugin.WithOptions(MyOptions{}))` before the CLI runs (`cmd/hikyaku/app.Main`).
  - Its settings live in the entry's `options:` block, decoded strictly with `spec.Decode(&opts)`, because code outside this repo cannot import `internal/config`.
  - `hikyaku validate` and `hikyaku schema` learn registered types through `config.Extensions`. Built-in types reject `options:`.
- **Delivery semantics the design leaves open:**
  - **HTTP listen ack.** `ack: delivered` (default) answers once every required destination has the message, or it is dead-lettered, which gets a `202`. A nack gets `503` + `Retry-After`. `ack: accepted` answers `202` once the message is queued. `respond.from` returns that destination's reply (`502` if it failed, `504` on timeout). The caller's `Idempotency-Key` becomes the envelope ID.
  - **The DLQ gets one record per failed required destination.** Each record carries `dlq.destination` (replay delivers there), or `dlq.route` with reason `no_route`/`transform` (replay re-runs that route). A best-effort (`required: false`) failure is counted (`hikyaku_delivery_dropped_total`) and logged, not dead-lettered.
  - **Readiness** fails only while a required destination's whole fallback chain is down.
  - **`multi: true` without `min_confidence`** uses 0.5.
  - **Group members** fail over on transient failures only: a permanent one stops the group.
- **Engine hooks:**
  - The history recorder starts in `engine.New`, so injected messages (send, route, dlq replay) record too.
  - Sinks that serve something of their own (sse) implement `plugin.Starter`. Only `engine.Run` starts them, so one-shot commands never take a running relay's port.
- **Interpolation:** a value that is exactly one `{{env.X}}` takes the type of its value (numbers, booleans). Secrets always stay strings. mamori's `env:` provider reads the process environment; `app.WithEnv` only replaces `{{env.X}}` lookups.
- **CLI commands that read a running relay** (`tail`, `history *`) go through the admin API, using `--admin`/`--token`, then `HIKYAKU_ADMIN_TOKEN`, then the spec. `dlq *` reads the file DLQ offline. `route` without `--dry-run` delivers.
- **Admin protection:** `/reload`, `/debug/*`, `/v1/tail`, `/v1/feedback` and `/v1/history*` need the token, or a loopback client when no token is set. `/healthz`, `/readyz` and `/metrics` are open.
- **`hikyaku chaos`:**
  - With `--data`, it pushes `--count` messages through one destination's path and exits 1 if any is lost.
  - Without `--data`, it runs the relay with the destination wrapped by `protocols/fault`.
- **Retry tags (M2):** a failure whose tags match neither `retry_on` nor `never_on` falls back to its class: it is retried unless it is permanent. A protocol tag nobody listed (`kafka:NOT_LEADER_FOR_PARTITION`, `ws:close_1011`) retries when its error is transient. List the tag in `never_on` to stop that.
- **Lite build:** brokers and clouds are full-build only (`config.FullOnly`). `validate` accepts them; a lite binary's engine fails with "not in this build" instead.
- **Go module releases:** on a branch, every non-root module requires its siblings at `v0.0.0` through `replace` directives, which `go install` refuses.
  - `internal/tools/release` writes a release commit on top of semantic-release's `vX.Y.Z`, on no branch. There, every module requires its siblings at `vX.Y.Z` with no `replace`, and `go mod tidy` completes each `go.sum` in dependency order against a `file://` GOPROXY of the sibling modules.
  - The zips come from the git tree through `golang.org/x/mod/zip`, the way the go command builds them from a tag: the root `LICENSE` goes into every module. `CreateFromDir` on a checkout would not match (a worktree's `.git` file, no `LICENSE`).
  - Tags: `<dir>/vX.Y.Z` on the release commit. `vX.Y.Z` stays where semantic-release put it, since the release commit changes no file of the root module. The release commit takes the identity and date of `vX.Y.Z`'s commit, so a re-run makes the same commit.
  - `internal/tools` is a module of its own (`golang.org/x/mod` stays out of the root), and modules under `internal/` are never released.
  - The release workflow's `modules` job runs the tool, installs from its proxy (`scripts/release-check.sh --proxy`), pushes the tags, and installs again through proxy.golang.org (`--public`). `make release-check` (CI, and the release job before semantic-release) does it all on `HEAD` at `v1.99.99-test` in a scratch clone, and compares the zips with the go command's own (`GOPROXY=direct` reading the clone).
  - The tool refuses a `replace` other than a sibling's directory, an `exclude`, and a version above v1 (the module paths have no `/vN`). The httpcore `replace` is gone: requiring its pseudo-version wins under MVS.
  - A `go install` build has no ldflags: `hikyaku version` reports the module version from the build info.
- **Disk buffer (§15 Q2, bbolt, owner 2026-11-09):**
  - `internal/wal` holds the log. The engine opens it in `Run` only, so one-shot commands never take its lock (one relay per buffer file).
  - Messages are appended in groups, one fsync per group, before `env.Queued()`. A message is deleted once its outcome is final.
  - A failed message is retried from the log with backoff while the relay runs. What is left at shutdown or a crash is delivered by the next start; sources still waiting get a nack.
  - HTTP `ack: accepted` answers 202 once the message is on disk (`plugin.Env.Buffer == "disk"`).
- **Ordering:** a source's `ordering.key` template (or, when empty, the `ordering_key` attribute every broker source sets) serializes messages per key: head-of-line per key, keys in parallel.
- **Batching:** a destination's `batch` collects messages and walks the chain once per batch.
  - At each hop, every member is transformed on its own, then they are combined: a `plugin.BatchSink` gets the members, any other sink gets an NDJSON or JSON-array envelope.
  - A `BatchSink` that cannot take a batch in one call after all answers `plugin.ErrNoBatch`, and gets the combined envelope through `Deliver` (grpc: a unary or server-streaming method, which may only be known after reflection).
  - Members share the outcome, each writes its own DLQ record, and a waiting member gives its worker slot back.
  - At shutdown, open batches finish on their own `max_wait`.
- **Source hooks for brokers:** `plugin.OnSettle` (ack when final, nack on `Failed`; `ack: accepted` acks on `env.OnQueued`), `plugin.Closer` (closed after in-flight acks), `plugin.BatchSink`.
- **LLM proxy (M3):**
  - The `openai` source (`protocols/openai`) serves `/v1/chat/completions` and `/v1/models`.
  - `model` destinations are built by the AI factory through `router.ModelSinks`, so only `llm` touches Genkit.
  - Streaming goes through `env.Stream()`. Once a chunk reached the caller, a failure is permanent, so no other model takes over half an answer. A model destination whose stream another model already started fails at once (`model:stream_broken`).
  - The caller's key is never copied into the envelope.
  - The answer is the served destination's reply (a chat completion first).
  - Function calling: the request's functions become Genkit tools with its JSON Schemas, and `ai.WithReturnToolRequests(true)` hands the model's calls back to the caller. hikyaku runs no tool. A call without a provider ID gets `call_<random>` (a `WrapModel` hook, before Genkit's UUID). Streamed calls are written from the whole answer, one `delta.tool_calls` entry each, because providers stream calls in pieces (OpenAI) or whole (Claude, Gemini, Ollama).
  - A model's declared supports are checked first: one without `tool_choice` (Ollama) is offered no tools for `tool_choice: none` and refuses `required`. A provider's INVALID_ARGUMENT (or the Ollama plugin's plain 400) is tagged `model:bad_request` too: the caller gets 400.
  - `n` is at most 8. Each choice is a generation of its own, run concurrently, with the usage summed.
  - The deprecated `functions` and `function_call` map onto tools, and such a request is answered with `message.function_call`.
- **Protocol modules (M2/M3):** one module per protocol family: `grpc`, `tcp` (tcp and udp), `kafka`, `nats` (nats and jetstream), `mqtt`, `amqp`, `redis`, `aws` (sqs, sns, eventbridge, s3), `gcp` (pubsub, gcs), `azure` (servicebus, eventhubs), and `openai`.
  - Each README is the reference for its keys, acknowledgement and resume semantics, attributes and error tags.
  - Error tags are `<protocol>:<code>`, and several modules also add a generic `connect`, `timeout`, `5xx`, `4xx` or `429`.
  - The brokers and clouds register in `wiring_full.go`. tcp/udp and openai stay in hikyaku-lite.
- **`split: lines`** (s3, gcs) is a line split, not jq: `engine.Check` skips it for those types.
- **Listeners:** grpc, tcp and udp sources need a port of their own (validation rejects a shared one); http, ws, sse and openai share ports by path.
- **DTLS (udp):** pion/dtls v4, pure Go, so hikyaku-lite has it too (about 1.8 MB).
  - A udp source with `tls` serves DTLS 1.2 and 1.3. A destination turns it on with `dtls://`, or any `tls` key on `udp://`, the way tcp does with `tls://`.
  - A destination redials an association unused for 30s, because DTLS does not tell a client that the server or a NAT dropped it. A source closes an association idle for 5 minutes.
- **Out-of-process plugins (`grpcplugin`, full build only):**
  - The CLI starts a spec's `plugins:` in `load`, before validation, through `cli.Deps.Plugins`, so the root never imports go-plugin.
  - Their kinds register like Go SDK plugins, and their options' JSON Schema goes into `config.ExtType.Schema`.
  - A reload closes the old set once the old engine stopped; `Execute` closes every set on exit.
  - Plugins are written in Go with `grpcplugin.Serve` (owner, 2026-11-10: no SDK for other languages; the docs point other languages at `plugin.proto` and the handshake steps in grpcplugin's README). `examples/crm-plugin` is a complete one, a module of its own; `TestCRMPluginExample` builds and runs it. `examples/postgres-plugin` (owner, 2026-11-10) serves a `postgres` destination and a `postgres` outbox source from one binary; `TestPostgresPluginExample` builds and validates it, and the `integration` suites run both against a PostgreSQL container.
- **Hot reload of Genkit and the admin API:**
  - A reload keeps the AI factory while `genkit:`, the model providers and the secret values stay the same (`sameAI`). Otherwise it builds a new factory, a new Genkit instance, next to the running one. The old one's context ends once the old engine has stopped.
  - A new `server.admin.listen` is bound before the swap, and the old server shuts down after it, answering the `POST /reload` in flight. On the same address, `admin.Server.Configure` applies the new token, readiness and metrics options. Every admin server shares the relay's hub.
  - `needs_restart` is left for one case: `GENKIT_REFLECTION_PORT` pins the Dev UI's reflection port, so a second Genkit instance cannot start (`llm/devui.go`).
- **Real-model checks:** the ai-triage example was tuned with `hikyaku ai test` on TypeSafe `jev-1.13.0`. The real-model suites are build-tagged `integration` and skip without their env:
  - `TYPESAFE_API_KEY` for TypeSafe;
  - `HIKYAKU_OLLAMA_URL` for Ollama (`HIKYAKU_OLLAMA_DECISION_MODEL`, since clef is about 20 GB);
  - `HIKYAKU_AZURE_EMULATORS=1` for the Azure emulators.
  No CI job uses a model key (owner, 2026-11-09): the repository has no real-model workflow or secret, and the TypeSafe suite runs only on a machine whose environment has the key. CI evaluates on llmfake.
- **AI spend gate (owner, 2026-11-09):** `router.when`, a CEL condition on any router. A message it is false for, or cannot evaluate, skips that router with no call and goes to the next router of the chain. The chain records an `<type>: skipped (...)` hop and counts `hikyaku_router_skipped_total{route,router}`. Lint warns when the chain's last router has a `when`.
- **The design's §5 example** declares neither `REDIS_PASSWORD` nor `CORP_DECISIONS_KEY`, and it hedges a group that isn't marked `idempotent`. Validation rejects both, as §5 and §7.2 require. `internal/config/testdata/full.yaml` is the corrected copy, and the tests load both versions.
