---
layout: ../../layouts/DocsLayout.astro
title: Security
---

# Security

hikyaku sits on the path of your messages, so this page describes its security model. Read it
before you expose a listen source, hand hikyaku a secret, or let a model see message contents.
It covers:

- transport security (TLS) and inbound authentication;
- the SSRF guard, which limits where a templated URL may send a request;
- how secrets are handled;
- what the AI router is and is not allowed to do;
- the admin API, the container and the supply chain.

To report a vulnerability, use GitHub's private reporting:
[Report a vulnerability](https://github.com/xavidop/hikyaku/security/advisories/new). Please do
not open a public issue.
[SECURITY.md](https://github.com/xavidop/hikyaku/blob/main/SECURITY.md) describes the process.

## Transport

TLS encrypts a connection. mTLS (mutual TLS) also makes the client prove who it is, with a
certificate. Both work on both sides of hikyaku:

- **Listen sources** serve TLS with `tls.cert_file` and `tls.key_file`, and require client
  certificates (mTLS) with `tls.client_ca_file`.
- **Destinations, poll sources and subscribe sources** verify the server against the system
  roots, or against `tls.ca_file`. They present a client certificate with `tls.cert_file` and
  `tls.key_file`. `tls.insecure_skip_verify` turns verification off; it exists for development
  only.
- **udp** takes the same keys for DTLS (TLS over UDP): a udp source serves it, and a `dtls://`
  destination dials it.
- **Redis** takes `tls: { enabled: true }` and the same keys.

## Inbound authentication

Listen sources check who is calling with `auth:`. The methods are:

- an **HMAC** signature of the body (a hash made with a shared secret);
- a **JWT** verified against a **JWKS** (the issuer's published keys), with issuer, audience and
  algorithms;
- an **API key** header;
- a **bearer** token;
- an **IP allowlist**.

A listen source without `auth` accepts any caller, and `hikyaku lint` warns about it.
[Protocols](/docs/protocols/#listen) lists the keys. An sse destination takes the same `auth`
(without `hmac`) for the clients that read its stream.

`api_key.query` names a query parameter that may carry the key when the header is absent. It is
for clients that cannot set headers, such as a browser's `EventSource` or `WebSocket`. URLs end
up in proxy and access logs, so prefer the header when the client can send one.

The caller's credentials are for the relay, never for the destinations. The http, ws, sse, grpc
and openai listen sources drop the credential headers before the message exists:
`Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`, `Api-Key` and the source's
`auth.api_key.header`. The http source also drops the `auth.api_key.query` parameter from
`http.query`. So a credential never reaches a destination, a file, the dead-letter queue, the
routing history or `hikyaku tail`.

## Signed deliveries

An http or https destination can sign what it sends, so the receiver can check that a request
came from your relay and was not changed on the way:

```yaml
destinations:
  - name: customer-hook
    type: https
    url: https://customer.example.com/webhooks
    sign:
      secret: "{{secrets.WEBHOOK_SECRET}}"
      prefix: "sha256="
      timestamp_header: X-Timestamp
```

- `sign` adds an HMAC of the request body in `header` (default `X-Signature-256`), with `algo`
  `sha256` (default) or `sha512`, and `encoding` `hex` (default) or `base64`.
- With `timestamp_header`, that header carries the send time in Unix seconds, and the signature
  covers `<timestamp>.<body>`. A receiver that refuses old timestamps cannot be sent a captured
  request again later.
- The signature covers the bytes that go out, and every retry is signed again.

It is the sending side of a source's `auth.hmac`: without `timestamp_header`, a hikyaku source
with the same settings verifies it. [The http module's README](https://github.com/xavidop/hikyaku/tree/main/protocols/http#signing)
has every key and a receiver's check.

## SSRF guard

SSRF (server-side request forgery) is an attack where someone makes a server send a request to a
host of their choosing, such as an internal service. hikyaku guards against it. Destinations
come from the spec only: no message, header or model can add one. A destination URL may still
contain a per-message template, such as `https://{{ .json.region }}.api.example.com/events`.
Then every rendered host is checked before the request:

```yaml
egress:
  allow: ["*.api.example.com", "10.20.0.0/16"]   # exact hosts, *.suffix, CIDRs
  allow_private: false                           # the default
```

`egress` means outgoing traffic. A templated URL may only reach the hosts and CIDRs (address
ranges) in `egress.allow`. Private, loopback and link-local addresses are blocked unless
`allow_private: true`. A host listed by name may resolve to any public address. Any other host
must resolve to an address inside one of the CIDRs. The check runs on the address hikyaku
actually dials, so DNS rebinding (a name that resolves to a safe address first and a private one
later) cannot slip past it. A redirect is checked against `egress.allow` like the first host, and
the chain stops after 10 redirects. URLs without a templated host are yours, written in the
spec, and are not restricted.

## Secrets

- `secrets:` holds [mamori](https://github.com/xavidop/mamori) refs (`env:`, `file://`,
  `vault://` and the other providers), never values. A ref says where to read the secret.
  hikyaku resolves them once, when the spec loads.
- A resolved secret is a mamori `secret.String`. Its value is revealed only at the point of use
  (a header, a TLS key, a model plugin's key), and it is never logged. The slog handler redacts
  every resolved value. `/debug/config` shows the spec with secrets redacted. `/v1/tail` and
  `/v1/history` redact any secret value a message carries.
- `{{secrets.NAME}}` must name a declared secret. `hikyaku validate --check-secrets` resolves
  every secret without printing any. `hikyaku doctor` resolves them too, and prints only how
  many resolved (`✓ secrets 3 resolved`).

## The AI router

- **Redact before sending.** `input.redact` removes JSONPaths (for example card numbers or
  emails) from the state before any model sees it. `input.headers` is an allowlist: no header
  reaches the model unless it is listed. `input.max_bytes` caps what is sent.
- **The state is untrusted data, never instructions.** The route's `instructions` are the system
  message. The message is only the state the model evaluates.
- **The model only chooses among declared candidates.** Every candidate is a destination of the
  spec. When a generative model answers with a name that is not a candidate, that name is
  dropped. The AI never picks a URL. A prompt injection (text in a message that tries to give the
  model orders) can at worst move that message to another declared candidate.
- **History is redacted too.** `history.record.state` stores the redacted state, a summary, or
  nothing. The TTL (how long a record is kept) is mandatory, and keys can be forgotten. See
  [History-aware routing](/docs/history/#privacy).

## The admin API

The admin API is how you watch and reload a running relay. `/reload`, `/debug/config`,
`/debug/state`, `/v1/tail`, `/v1/feedback` and `/v1/history*` need `server.admin.token` as a
bearer token. When no token is set, they answer loopback clients (the same machine) only.
`/healthz`, `/readyz` and `/metrics` are open. The admin port streams envelopes (`/v1/tail`) and
shows the spec (`/debug/config`), so keep it on a private network. See
[Observability](/docs/observability/#admin-api).

## The container

The image is built on `gcr.io/distroless/static:nonroot`: no shell, no package manager, and a
non-root user. The Helm chart runs it with a read-only root filesystem, no privilege escalation
and every capability dropped. It can also add a NetworkPolicy.

## Supply chain

The supply chain is everything that goes into a release: the code, its dependencies and the
build.

- The GitHub Actions of the workflows are pinned by commit SHA.
- `govulncheck` (Go's vulnerability scanner) runs on every module in CI. Another check makes sure
  Dependabot covers every module.
- Every release has SBOMs and keyless cosign signatures of `checksums.txt` and of the container
  image. [Install](/docs/install/#release-archives-linux-macos-windows) shows how to verify them.
- The root module never imports Genkit, model SDKs, Redis, or broker and cloud SDKs. Only the
  `llm` module imports model providers. `make check-deps` enforces both rules.
