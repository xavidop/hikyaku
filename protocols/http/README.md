# protocols/http

The HTTP protocol for hikyaku: the `http` and `https` source and destination types. Use a source to receive webhooks or to read an API on a schedule, and a destination to call an HTTP endpoint. Built on Go's `net/http`.

```go
import httpproto "github.com/xavidop/hikyaku/protocols/http"

httpproto.Register() // registers http and https as sources and destinations
```

The two types differ on the listener only: an `https` listener needs a certificate. A client (a poll source or a destination) uses TLS whenever its URL is `https://`.

## Sources

A source brings requests or responses into the relay. It runs in one of two modes:

- **listen**: hikyaku is the server. Callers send requests to `listen` + `path`, and each request is one message.
- **poll**: hikyaku is the client. It requests `url` at once, then every `interval`, and each response is one message (or several, with `split`).

The mode defaults to `poll` when the source has a `url` and no `listen`, and to `listen` otherwise.

### listen

The listener is shared through hikyaku's HTTP listeners. So other `http`, `ws`, `sse` and `openai` sources, and `sse` destinations, can serve other paths on the same address. Everything on one address must agree on TLS. A path ending in `/` serves everything below it, so the default `/` takes every path no other source serves.

Every request becomes one envelope:

| Field | Value |
|---|---|
| `ID` | the caller's `Idempotency-Key` header (up to 256 characters), else a new ID |
| `Subject` | the request path |
| `ContentType` | the request's `Content-Type` |
| `Headers` | the request's headers, without the caller's credentials (below) |
| `Attributes` | `http.method`; `http.path`; `http.query` (when the URL has a query, without the `auth.api_key.query` parameter); `remote_addr`; `idempotency_key` (when the caller sent one) |

An idempotency key is a value the caller picks to mark one request. A retry that carries the same key is the same message, so it is safe to send twice.

The caller's credentials never enter a message. `Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`, `Api-Key` and the configured `auth.api_key.header` are dropped before the message is built, with or without `auth`. No destination, file, DLQ record, history entry or `hikyaku tail` sees them. A W3C `traceparent` header continues the caller's trace.

| Key | Meaning |
|---|---|
| `listen` | Address, such as `:8080` (required). |
| `path` | Path served, starting with `/` (default `/`; a path ending in `/` serves everything below it). |
| `methods` | Methods accepted (default `POST` and `PUT`). |
| `tls` | `cert_file` + `key_file` (an `https` listener needs them), and `client_ca_file` for mTLS (mutual TLS): a client certificate signed by that CA is then required. TLS 1.2 or newer. |
| `auth` | Inbound authentication, below (401 on failure). |
| `max_body_bytes` | Largest body (default 10MiB). A larger one gets `413`. |
| `ack` | `delivered` (default) or `accepted`, below. |
| `respond` | `from`: a destination whose reply is returned to the caller; `timeout`: how long the caller waits (default 30s). Needs `ack: delivered`. |

`auth` says who may send. Every check set under it must pass:

| Key | Checks |
|---|---|
| `hmac` | An HMAC of the body in `header`, with `secret`. `algo`: `sha256` (default), `sha1` or `sha512`. `encoding`: `hex` (default) or `base64`. `prefix` is cut from the header value before the comparison; without one, a short `name=` prefix such as `sha256=` is cut. |
| `webhook` | The signature of a well-known sender: `provider` is `github`, `stripe`, `slack`, `shopify` or `twilio`, and `secret` is the secret that sender gave you. `tolerance` (stripe, slack; default `5m`) is how old the signed timestamp may be. `url` (twilio) is the public URL Twilio calls, for a relay behind a proxy. |
| `bearer` | `Authorization: Bearer <token>` against `tokens`. |
| `jwt` | A bearer JWT verified with the keys at `jwks_url`, with optional `issuer` and `audience`. `algorithms` defaults to `RS256` and `ES256`. Clock skew of up to 30s is allowed. |
| `api_key` | A key in `header` (default `X-API-Key`) against `keys`. With `query`, a query parameter may carry it when the header is absent (for clients that cannot set headers); it is dropped from `http.query`. |
| `ip_allow` | The client address of the connection against IPs and CIDRs. `X-Forwarded-For` is not read. |

`hikyaku lint` warns about a listen source without `auth`.

#### Answers

This is what a caller gets back. `ack` decides when the answer is sent:

- `ack: delivered` (the default) answers once the message's outcome is final. The table below lists each outcome.
- `ack: accepted` answers `202` as soon as the message is queued. With a disk buffer, it answers once the message is on disk.

hikyaku's own answers are JSON: `{"id": "<envelope ID>", "status": "<status>"}`, plus `"error"` when there is one.

| Situation | Answer |
|---|---|
| The method is not in `methods` | `405`, with `Allow` |
| The body is over `max_body_bytes` | `413` |
| `auth` failed | `401`, with `WWW-Authenticate: Bearer` |
| The buffer is full (backpressure) | `503`, with `Retry-After: 1` |
| `ack: accepted`: the message is queued | `202` |
| `ack: accepted` with a disk buffer: the buffer could not store the message, or not within 30s | `503`, with `Retry-After: 1` |
| A repeat that `dedupe` drops, under any `ack` mode | `200`, status `duplicate` |
| `ack: delivered`: delivered, filtered by every route, or unrouted | `200` |
| `ack: delivered`: dead-lettered | `202` |
| `ack: delivered`: dead-lettered because a route's `validate` step refused the body | `422`, with the schema error |
| `ack: delivered`: failed, because the DLQ failed too or a shutdown cut the delivery | `503`, with `Retry-After: 5` |
| `ack: delivered`: no outcome within 30s (`respond.timeout`) | `202`, status `pending`: the message is still being delivered |

Backpressure means the relay holds as many messages as it can. It then answers `503` instead of dropping the request, and `Retry-After` tells the caller when to try again.

With `respond.from`, the caller gets that destination's reply instead: its status (`200` when it has none), its headers (without hop-by-hop headers, `Authorization` and `Cookie`) and its body. Use it when the caller needs the answer of the system behind hikyaku. When there is no reply to return:

| Situation | Answer |
|---|---|
| The outcome is final, but `from` has no reply (the router did not choose it, or it is best-effort and failed) | `202` |
| Dead-lettered | `502` |
| No outcome within `respond.timeout` | `504` |
| Failed | `503`, with `Retry-After: 5` |

A caller that hangs up does not cancel anything. The message is the relay's once it is queued, and it is still delivered. A caller that retries with the same `Idempotency-Key` sends the same message ID again. Destinations receive it as their idempotency key.

### poll

Each poll sends `method` with `headers` and `body` to `url`. A poll whose attempts all fail is logged at warn and skipped; the next interval tries again. A permanent failure (a `4xx` other than `408` and `429`) ends the poll at once. A `Retry-After` replaces the backoff, capped at `max_backoff`. The source's health is the last poll's error.

Polls are conditional, so an unchanged response makes no message. After a `2xx`, hikyaku keeps the response's `ETag` and `Last-Modified`. It sends them back on the next poll as `If-None-Match` and `If-Modified-Since`. A `304 Not Modified` makes no message. The validators are kept in memory, so the first poll after a start is unconditional.

A response is read up to 10MiB; anything past that is cut off. With `split`, the response must be JSON. The jq expression runs on it, and each output is one message, encoded as JSON. A response that is not JSON fails that poll.

| Field | Value |
|---|---|
| `Subject` | the `url`, without its credentials and query (as the `url` attribute) |
| `ContentType` | the response's `Content-Type` (`application/json` for each output of `split`) |
| `Headers` | none |
| `Attributes` | `url` (without its password and query) |

| Key | Meaning |
|---|---|
| `url` | `http://` or `https://` URL (required). |
| `method` | Request method (default `GET`). |
| `body` | Request body, sent as written (it is not a per-message template). |
| `headers` | Static request headers. `User-Agent` is `hikyaku` unless one is set here. |
| `oauth2` | Fetch an OAuth 2.0 access token and send it as a bearer token on every poll. See [OAuth 2.0](#oauth-20). |
| `interval` | Time between polls (default 30s). |
| `timeout` | Timeout of each request (default 10s). |
| `retry` | Attempts per poll: `attempts` (3), `backoff` (1s), `max_backoff` (10s), `jitter` (`full`), and `retry_on`, `never_on` and `respect_retry_after`, matched as for a destination. |
| `split` | jq expression that splits one JSON response into several messages, such as `.items[]`. |
| `tls` | `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. |

A poll source has nothing to acknowledge: once a message is handed over, the relay owns it, and a failed delivery goes to the DLQ. While the relay applies backpressure, a poll waits to hand over its next message. Polls never overlap: a poll that runs past `interval` delays the next one.

### Dedupe

`dedupe` drops repeats. It works on every source type ([Dedupe](https://xavidop.github.io/hikyaku/docs/protocols/#dedupe)), and the http sources apply it themselves, before a message is queued. A message is a repeat when its `key` (a per-message template, such as `{{ .json.id }}`) was seen within `ttl` (default 1h). A listen source answers a repeat with `200` and `{"id": "...", "status": "duplicate"}`, under any `ack` mode. A poll source skips it, so an item that comes back on the next poll does not become a second message. Both count it in `hikyaku_deduped_total{source}`. The keys live in `store` (default: the relay's default store). With a shared store such as Redis, dedupe holds across replicas. A key that renders empty, or a store that is down, lets the message through.

## Destination

A destination sends one request per attempt to `url`. The URL may be a per-message template, such as `https://{{ .json.region }}.api.example.com/events`. A rendered URL must be `http://` or `https://`.

The request carries:

- the message's headers, without hop-by-hop and framing headers (`Connection`, `Keep-Alive`, `Te`, `Trailer`, `Transfer-Encoding`, `Upgrade`, `Host`, `Content-Length`, `Accept-Encoding`, `Proxy-Authenticate`) and without `Authorization`, `Proxy-Authorization` and `Cookie`;
- `Content-Type` set to the message's content type;
- the `headers` you set, which replace the message's headers of the same name;
- the idempotency key in `Idempotency-Key` (`idempotency_header` renames it): the source's `idempotency_key` attribute, else the message ID. Every retry and every fallback hop sends the same key, so the receiver can tell a retry from a new message;
- `User-Agent: hikyaku`, unless a header sets one;
- the message body, or the rendered `body` template.

A `2xx` answer is a success. Its status, headers and body (up to 10MiB) are the destination's reply, which `respond.from` returns to a caller. A reply with an `X-Hikyaku-Reroute: <destination>` header (several are comma-separated) records a correction for history-aware routing.

| Key | Meaning |
|---|---|
| `url` | `http://` or `https://` URL, or a per-message template. |
| `method` | Request method (default `POST`). |
| `headers` | Headers set on every request. Values may be per-message templates. |
| `body` | Per-message template of the request body (default: the message body). |
| `tls` | Client TLS: `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. An `https` destination uses TLS 1.2 or newer. |
| `idempotency_header` | The header that carries the idempotency key (default `Idempotency-Key`). |
| `sign` | Sign every request with an HMAC of its body. See [Signing](#signing). |
| `oauth2` | Fetch an OAuth 2.0 access token and send it as a bearer token. See [OAuth 2.0](#oauth-20). |
| `compression` | `gzip` compresses the request body and sets `Content-Encoding: gzip`. Default `none`. See [Compression](#compression). |

`timeout`, `retry`, the circuit breaker, groups, hedging, `batch` and the fallback chain work as for every destination. With `batch`, one request carries the whole batch: an NDJSON body (`application/x-ndjson`, the default) or a JSON array (`batch.format: json_array`). In a group, a member with `health_check.path` is probed with a `GET` on its origin plus that path. Otherwise the health check dials the host of a static URL and sends no request, because a webhook endpoint should not see probes. A templated URL's health check always passes.

The client follows up to 10 redirects. It uses `HTTP_PROXY`, `HTTPS_PROXY` and `NO_PROXY`, except for a templated URL.

### Signing

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

`sign` adds an HMAC of the request body, made with a shared secret, so the receiver can check that the request came from you and was not changed on the way. The signature covers the bytes that go out: the rendered `body` template when there is one, or the whole batch with `batch`. Every retry is signed again. A signature header the message came in with is replaced.

| Key | Meaning |
|---|---|
| `secret` | The shared secret. Required. Use `{{secrets.NAME}}`. |
| `header` | The header that carries the signature (default `X-Signature-256`). |
| `algo` | `sha256` (default) or `sha512`. |
| `encoding` | `hex` (default) or `base64`. |
| `prefix` | Text put before the digest, such as `sha256=`. |
| `timestamp_header` | When set, this header carries the send time in Unix seconds, and the signature covers `<timestamp>.<body>`. A receiver that refuses old timestamps cannot be sent a captured request again later. |

Without `timestamp_header`, a hikyaku source with the same `auth.hmac` settings (`header`, `secret`, `algo`, `encoding`, `prefix`) verifies the signature. A source's `auth.hmac` checks the body alone, so it does not verify a request signed with `timestamp_header`.

The receiver computes the same HMAC over what it received and compares the two in constant time:

```python
expected = "sha256=" + hmac.new(secret, timestamp + b"." + body, hashlib.sha256).hexdigest()
ok = hmac.compare_digest(expected, request.headers["X-Signature-256"])
```

### OAuth 2.0

```yaml
destinations:
  - name: crm
    type: https
    url: https://api.crm.example.com/v1/events
    oauth2:
      token_url: https://login.crm.example.com/oauth2/token
      client_id: "{{secrets.CRM_CLIENT_ID}}"
      client_secret: "{{secrets.CRM_CLIENT_SECRET}}"
      scopes: [events.write]
```

With `oauth2`, hikyaku asks the token endpoint for an access token with the client credentials grant, and sends it as `Authorization: Bearer <token>`. It keeps the token and asks for a new one 30 seconds before the old one expires. Deliveries that need a token while one is being fetched wait for that one fetch.

| Key | Meaning |
|---|---|
| `token_url` | The authorization server's token endpoint. Required. |
| `client_id` | The client ID. Required. |
| `client_secret` | The client secret. Required. Use `{{secrets.NAME}}`. |
| `scopes` | The scopes to ask for. |
| `audience` | The `audience` parameter, for servers that need one (Auth0). |
| `params` | More form parameters for the token request, such as `resource`. |
| `client_auth` | Where the client ID and secret go: `basic` (HTTP Basic, the default) or `body` (form fields). |

- When the destination answers `401`, hikyaku drops the token, fetches a new one and sends the request once more. A second `401` is the delivery's failure.
- When no token can be fetched, the delivery fails with the `oauth2` tag and nothing is sent. A token endpoint that cannot be reached, or that answers `5xx` or `429`, is a transient failure, so the retry policy applies. Any other answer (wrong credentials, a scope that is not allowed) is permanent.
- The token is kept in memory, per destination, in this process. It is never logged.
- `oauth2` does not go with an `Authorization` header in `headers`.
- The token endpoint uses the destination's `tls` settings. The [egress](#egress) guard does not apply to it: the spec names it, not a message.

Only the client credentials grant is supported. A flow that needs a person to sign in (authorization code) is not.

A poll source takes the same `oauth2` block, with the same keys, for an API that wants a token to be read:

```yaml
sources:
  - name: open-tickets
    type: https
    mode: poll
    url: https://api.helpdesk.example.com/v1/tickets?status=open
    oauth2:
      token_url: https://login.helpdesk.example.com/oauth2/token
      client_id: "{{secrets.HELPDESK_CLIENT_ID}}"
      client_secret: "{{secrets.HELPDESK_CLIENT_SECRET}}"
      scopes: [tickets.read]
```

- The token is kept in memory, per source, and replaced 30 seconds before it expires.
- When the API answers `401`, hikyaku drops the token, fetches a new one and polls once more.
- When no token can be fetched, the API is not called and the poll fails with the `oauth2` tag. The poll's `retry` applies, and the next `interval` tries again.
- The token is not put on the message.
- `oauth2` applies to poll mode only. A listen source checks its callers with `auth`.

### Compression

```yaml
destinations:
  - name: archive
    type: https
    url: https://logs.example.com/bulk
    compression: gzip
    batch:
      max_messages: 500
```

`compression: gzip` compresses every request body and sets `Content-Encoding: gzip`. It pays off for large bodies and batches; a small body can come out larger. The receiver must accept gzip requests. With `sign`, the signature covers the compressed bytes, because those are the bytes that go out.

A `listen` source decodes a request sent with `Content-Encoding: gzip` on its own, with no setting. The message holds the decoded body, and the header is removed. `max_body_bytes` applies to the request and to the decoded body: a body that grows past it gets a `413`. `auth.hmac` and `auth.webhook` check the bytes as they were sent. Another encoding (`br`, `zstd`) gets a `400`.

### Egress

A templated URL could be steered anywhere by a message. So its host is guarded against SSRF (server-side request forgery: a message that makes hikyaku call an address it should not reach):

- When `egress.allow` is set, the host must match one of its entries: an exact host, a `*.suffix` pattern, an IP or a CIDR. When the list has CIDRs, a name that matches no entry is judged by the address it resolves to.
- The address dialed is checked too. An address inside an `egress.allow` CIDR passes. With a list of CIDRs only, any other address is refused. Otherwise, a private, loopback, link-local or unspecified address is refused unless `egress.allow_private` is set. The check runs on the address actually dialed, so DNS rebinding cannot get past it.
- A templated URL is dialed directly, never through a proxy, so the check sees the real address.
- Each redirect is checked against `egress.allow` like the first host, and its address like the first one.

A refused request is a permanent failure, tagged `egress`: the fallback chain or the DLQ takes the message. A static URL is written in the spec, so it is not guarded.

### Errors

Each delivery is one attempt, because the relay owns retries. A failed attempt is an error with a class and tags. The class says whether hikyaku retries it by default. The tags are what `retry_on` and `never_on` match:

| Failure | Class | Tags |
|---|---|---|
| `429` | rate limited, with `Retry-After` | `429`, `4xx` |
| `408` | timeout | `408`, `4xx`, `timeout` |
| `504` | timeout | `504`, `5xx` |
| Another `5xx` | transient | the code, `5xx` |
| Another `4xx` | permanent | the code, `4xx` |
| Another status | transient | the code |
| The connection failed (refused, reset, DNS, TLS), the response could not be read, or more than 10 redirects | transient | `connect` |
| The attempt's `timeout` ran out | timeout | `timeout` |
| A `url`, `headers` or `body` template failed to render | permanent | `template` |
| The rendered URL is not `http://` or `https://` | permanent | |
| `egress` refused the host or the address | permanent | `egress` |

`Retry-After` is read as seconds or as an HTTP date, and the retry waits that long, capped at `max_backoff`. The default policy (`retry_on: [connect, timeout, 5xx, 429]`, `never_on: [4xx]`) retries `429` and `408`, because a code or a class is more specific than a range. It sends any other `4xx` straight to the fallback chain. A failure whose tags match neither list falls back to its class: it is retried unless it is permanent. List a code in `never_on` to stop retrying it (`never_on: [4xx, 501]`), or in `retry_on` to retry a `4xx` (`retry_on: [connect, timeout, 5xx, 429, 409]`). A list you set replaces the default one.

## Example

```yaml
sources:
  - name: webhooks
    type: https
    mode: listen
    listen: ":8443"
    path: /ingest
    methods: [POST]
    tls:
      cert_file: /etc/hikyaku/tls.crt
      key_file: /etc/hikyaku/tls.key
    auth:
      hmac:
        header: X-Signature-256
        prefix: "sha256="
        secret: "{{secrets.WEBHOOK_SECRET}}"
    max_body_bytes: 1MiB
    respond:
      from: billing
      timeout: 10s

  - name: open-tickets
    type: https
    mode: poll
    url: https://api.example.com/v1/tickets?status=open
    headers:
      Authorization: "Bearer {{secrets.API_TOKEN}}"
    interval: 1m
    split: .items[]
    dedupe:
      key: "{{ .json.id }}"
      ttl: 24h

destinations:
  - name: billing
    type: https
    url: https://billing.internal/api/events
    headers:
      Authorization: "Bearer {{secrets.BILLING_TOKEN}}"
      X-Event-Type: "{{ .json.type }}"
    timeout: 3s
    retry:
      attempts: 5
      retry_on: [connect, timeout, 5xx, 429, 409]

  - name: regional
    type: https
    url: "https://{{ .json.region }}.ingest.example.com/v1/events"
    required: false
```

With a templated URL like `regional`'s, set `egress.allow` at the top of the spec:

```yaml
egress:
  allow: ["*.ingest.example.com"]
```
