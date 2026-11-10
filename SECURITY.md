# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately**. Do not open a public issue, discussion, or
pull request for a security problem.

Use GitHub's private vulnerability reporting:
**[Report a vulnerability](https://github.com/xavidop/hikyaku/security/advisories/new)**. This
opens an advisory visible only to you and the maintainers.

Include as much as you can:

- A description of the issue and its impact.
- Steps to reproduce, or a proof of concept (a minimal `hikyaku.yaml` and the message that
  triggers it help most).
- The affected module(s) and version(s), and whether you run the binary, the container image or
  the Helm chart.
- Any suggested remediation.

Please do not include real API keys, tokens, webhook secrets or message bodies with personal data
in your report.

## What to expect

- Acknowledgement within 3 business days.
- An initial assessment and severity rating within 7 days.
- Coordinated disclosure: we will work with you on a fix and a release, and credit you (if you
  wish) in the advisory and release notes.

## Supported versions

hikyaku is a multi-module repository released as a whole: one version tag (`vX.Y.Z`) covers the
CLI, the container image, the Helm chart and the core library, all built from the same commit.
The other modules (`llm`, `stores/redis`, `protocols/*`, `grpcplugin`) are not released on their
own: the CLI and the image build them from that commit. Security fixes land in the latest
release, so upgrade the binary, the image or the chart to the newest version.

| Artifact | Supported |
| --- | --- |
| the CLI (`hikyaku` and `hikyaku-lite`) and the `ghcr.io/xavidop/hikyaku` image | latest release |
| the Helm chart (`charts/hikyaku`) | latest release |
| the core library (`github.com/xavidop/hikyaku`, with `go get` at the release tag) | latest release |

## Scope

In scope:

- The core library, the CLI, the container image and the Helm chart in this repository.
- Secret handling: any way a value resolved through mamori reaches a log, an error message, the
  admin API (`/debug`, `tail`, history), a dead letter, a model prompt or a Genkit trace
  unredacted.
- Egress: a spec or a message that makes hikyaku deliver to a host the spec does not allow (see
  the SSRF guard below).
- AI routing: a message that makes the AI router deliver to a destination its route does not
  declare.
- Authentication: a request that gets past a source's HMAC, bearer token, JWT, API key or IP
  allowlist check, or past the admin API's token.

Out of scope:

- Vulnerabilities in mamori, Genkit, go-redis or the other dependencies themselves. Report those
  upstream; we will update our dependency once a fix is available.
- Issues that require an already-compromised host, or credentials the reporter already controls.
- What destinations do with the messages hikyaku delivers to them. hikyaku relays messages; it
  does not protect the systems at either end.
- A message delivered more than once with the same idempotency key. Delivery is at least once by
  design.

## Security model

- **Secrets are references.** `secrets:` entries are mamori refs (`env:NAME` and the other mamori
  schemes), never values. They are resolved once at load, held as `secret.String` and revealed
  only at the moment of use: an HTTP header, an HMAC check, a model plugin's initialization.
- **Secrets are never logged.** Every resolved value is registered with the redactor, which
  replaces it (and its base64, URL-encoded and JSON-escaped forms) with `‹secret:NAME›` in what
  hikyaku writes: the logs (the log handler goes through it), errors, and the admin API's
  `/debug/config`, `/v1/tail` and `/v1/history` output. Values shorter than three bytes cannot be
  told apart from ordinary text and are not redacted.
- **Callers' credentials stay at the edge.** The http, ws, sse, grpc and openai listen sources
  drop the credential headers (`Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`,
  `Api-Key` and the source's `auth.api_key.header`) before a message exists, so they never reach
  a destination, a file, the DLQ, the routing history or `tail`.
- **What redaction does not cover.** A secret a destination echoes back in a form the redactor
  does not recognize, a secret written into a spec by hand, and message bodies: hikyaku relays
  payloads as they are, so treat dead letters, files and `tail` output like the messages
  themselves.
- **SSRF guard.** Destinations come from the spec only; a message can never add one. When a
  destination's URL host is a per-message template, the rendered host must match `egress.allow`
  (exact hosts, `*.suffix` patterns and CIDRs), and private, loopback and link-local addresses are
  refused unless `egress.allow_private` is set. A redirect of such a request is checked against
  `egress.allow` like the first host.
- **The AI picks only declared destinations.** The decision and generative engines choose among
  the route's candidates. Names a model returns that are not candidates are dropped, output that
  names none is retried like a transport error, and when the model still cannot answer, the
  router's fallback chain (`router.fallback`, typically rules, then static) decides. A model never
  chooses a URL.
- **Models see only what you send.** The state the AI router sends is redacted first (the
  `input.redact` JSONPaths, such as `$.card.number`, plus the secret values above), and the
  message is passed to the model as untrusted data, never as instructions.
- **Inbound authentication.** Sources can verify HMAC signatures, bearer tokens, JWTs (against a
  JWKS) and API keys, restrict client IPs, and terminate TLS or mTLS.
- **The admin API.** `/reload`, `/debug/config`, `/debug/state`, `/v1/tail`, `/v1/feedback` and
  `/v1/history*` require `server.admin.token` as a bearer token; without a token they answer
  loopback clients only. `/healthz`, `/readyz` and `/metrics` are open.
- **The container** runs as distroless's non-root user (UID 65532) and has no shell or package
  manager.
- **Supply chain.** Workflow actions are pinned by commit SHA; govulncheck scans every module in
  CI; every Go module has a Dependabot entry (CI checks it). Releases ship SBOMs, cosign keyless
  signatures for `checksums.txt` and the image, and SLSA provenance; the release notes show how to
  verify them.
