---
layout: ../../layouts/DocsLayout.astro
title: Transforms
---

# Transforms

Transforms reshape a message on its way through hikyaku. Use them to drop messages you do not
want, to pick fields out of a body, to render a new body, to edit headers, to change the
body's encoding, to hide sensitive fields, or to refuse a body that does not match a JSON
Schema. Transforms run in two
places:

- A route's `transforms` run before its router, so the router sees the transformed message.
- A destination's `transforms` run right before that destination delivers, so each destination
  can get its own shape.

Both are lists, applied in order. A transform that drops the message stops the list.

```yaml
routes:
  - name: tickets
    from: [webhooks]
    transforms:
      - filter: 'msg.json.type != "ping"'
      - jq: '{id: .ticket_id, customer_id, subject, body: .description}'
      - headers: { set: { X-Relay: "{{ .source }}" }, remove: [X-Signature-256] }
    to: [support, archive]

destinations:
  - name: support
    type: https
    url: https://support.internal/hook
    transforms:
      - template: '{"text": "New ticket {{ .json.id }}: {{ .json.subject }}"}'
        content_type: application/json
```

**Each entry sets exactly one of** `filter`, `jq`, `template`, `headers`, `codec`, `redact`,
`validate` and `enrich`, or `content_type` on its own. `content_type` may also go with a `jq` or `template`
entry, to label what that entry produced. Validation rejects an entry with two operations. It
also compiles every CEL expression, jq program, template and JSON Schema before the relay
starts.

## filter (CEL)

```yaml
- filter: 'msg.json.amount > 100 && msg.headers["X-Env"] == "prod"'
```

A [CEL](https://cel.dev) expression (Common Expression Language, a small language for
conditions) over `msg`. When it is false, the message is dropped from this route or destination.
It is not dead-lettered: dropping is the point. The fields of `msg` are:

- `msg.json`: the parsed body;
- `msg.headers`: each header's name in its standard form, mapped to its first value;
- `msg.bodyText`, `msg.attributes`, `msg.source`, `msg.subject`, `msg.id`, `msg.protocol` and
  `msg.contentType`.

An expression that cannot be evaluated for a message, for example because a key is missing,
**keeps** the message. A filter never drops a message by accident. A route also has a `filter:`
key of its own: the same check, in a shorter form.

## jq

```yaml
- jq: '{id: .ticket_id, priority: (if .vip then "high" else "normal" end)}'
- jq: 'select(.type | startswith("invoice."))'
```

A jq program (run with gojq) over the JSON body. Its first output, encoded as JSON, becomes the
new body. A program with no output, such as a `select` that does not match, drops the message.

## template

```yaml
- template: |
    {"customer": "{{ .json.customer_id }}", "summary": {{ json .json.subject }}, "seen_at": "{{ now }}"}
```

A Go template, rendered as the new body. It gets the same data and functions as every
per-message template in the spec. The data: `.json`, `.headers`, `.bodyText`, `.attributes`,
`.source`, `.subject`, `.id`, `.protocol`, `.contentType` and `.receivedAt`. The functions:
`json`, `header`, `default`, `lower`, `upper`, `trim`, `replace`, `contains`, `hasPrefix`,
`join`, `truncate`, `sha256`, `b64enc`, `b64dec`, `now` and `str`. A missing key renders as
empty. See [The spec: templating](/docs/spec/#templating).

## headers

```yaml
- headers:
    set: { X-Tenant: "{{ .json.tenant }}" }        # replace
    add: { X-Via: hikyaku }                       # append
    remove: [Cookie, X-Signature-256]             # delete
```

Edits the envelope's headers, in this order: `remove`, then `set`, then `add`. Values are
templates. The headers travel with the message to destinations that send headers, such as
`http`.

## codec

```yaml
- codec: form_to_json
```

A codec re-encodes the body and sets the content type to match:

| Codec | Does | Content type |
|---|---|---|
| `json` | parses the body as JSON and writes it back compact; a body that is not JSON is an error | `application/json` |
| `text` | keeps the body and marks it as text | `text/plain; charset=utf-8` |
| `base64_encode` | encodes the body as standard base64 | `text/plain; charset=utf-8` |
| `base64_decode` | decodes a base64 body | `application/octet-stream` |
| `form_to_json` | turns a form body (`a=1&b=2`) into a JSON object; repeated keys become arrays | `application/json` |
| `cloudevents` | wraps the message as a structured CloudEvent: `specversion`, `id`, `source`, `type` (the subject), `time`, `datacontenttype`, and the body as `data` (JSON) or `data_base64` | `application/cloudevents+json` |

## redact

```yaml
destinations:
  - name: archive
    type: s3
    bucket: orders-archive
    transforms:
      - redact: [$.card.number, $..email, "$.items[*].note"]
```

`redact` replaces the values its JSONPaths select with the string `[redacted]`, and leaves the
rest of the JSON body as it is. Use it on a destination to keep sensitive fields away from one
receiver, or on a route to keep them away from every destination of the route.

- The paths are the ones `router.input.redact` takes: `$.a.b`, `$.a[0]`, `$.a[*].b`,
  `$['odd key']`, and `$..email` for a key at any depth.
- A path that selects nothing in a message is fine: the message goes on unchanged.
- A body that is not JSON is a failure, not a pass. Letting it through would deliver what the
  step is there to hide.
- Validation compiles every path before the relay starts.

A `redact` on a destination changes that destination's copy only. When that delivery fails,
the dead letter holds the message as it was before the destination's transforms, so protect the
dead-letter queue like the data it holds.

## validate (JSON Schema)

```yaml
routes:
  - name: orders
    from: [webhooks]
    transforms:
      - validate:
          schema:
            type: object
            required: [id, items]
            properties:
              id: { type: string }
              email: { type: string, format: email }
              items:
                type: array
                minItems: 1
                items:
                  type: object
                  required: [price]
                  properties:
                    price: { type: number, minimum: 0 }
    to: [billing]
```

`validate` checks the JSON body against a [JSON Schema](https://json-schema.org). It never
changes the message. A body that matches goes on to the next transform. A body that does not
match is a failure, not a drop: the bad message is kept, with what is wrong with it.

Write the schema inline under `schema`, or keep it in a file:

```yaml
- validate: { file: schemas/order.json }
```

- The file is JSON or YAML. A relative path is relative to the directory the relay runs in. The
  file is read when the relay starts, and again on a reload.
- A schema without `$schema` is read as draft 2020-12. Older drafts work when `$schema` names
  them.
- `format` is checked (`date-time`, `email`, `uri`, `uuid` and the others), not only noted.
- A `$ref` may point inside the schema. In a schema file, it may also point at another file.
  Nothing is fetched over the network, so a `$ref` to an `http://` or `https://` URL is an
  error.
- `hikyaku validate` compiles every schema, so a schema with a mistake stops the relay from
  starting.

A body that is empty, or that is not JSON, does not match. The error lists up to five problems,
each with the key it is about:

```text
schema: items[0].price: got string, want number; missing property 'id'
```

What happens to a message that does not match depends on where the step runs:

- **On a route**, the message goes to the dead-letter queue with reason `schema`, and
  `hikyaku_schema_invalid_total{route}` counts it. An HTTP source answers `422` with the error,
  so the sender learns what to fix. The `openai` source answers `400`. Other sources ack the
  message, because it is safe in the dead-letter queue.
- **On a destination**, that hop fails as permanent, like any failed transform. The message
  moves on to the next fallback, then to the dead-letter queue with reason `delivery`.

### on_invalid: send bad messages somewhere

```yaml
transforms:
  - validate:
      file: schemas/order.json
      on_invalid: { to: [bad-orders] }
```

On a route, `on_invalid.to` names the destinations that get a message the step refused, in
place of the dead-letter queue: a quarantine topic, an alert webhook, a file. The message goes
there as it was when the step refused it, with what is wrong in the `schema.error` attribute:

```yaml
destinations:
  - name: bad-orders
    type: https
    url: https://hooks.example.com/alerts
    body: '{"text": {{ printf "Bad order from %s: %s" .source (index .attributes "schema.error") | json }}}'
```

- The route's own destinations do not get the message.
- The message counts as delivered once those destinations have it: an HTTP source answers
  `200`, not `422`. A destination in `to` that fails is a failed delivery like any other, with
  its retries, its fallbacks and a `delivery` dead letter.
- `hikyaku_schema_invalid_total{route}` still counts it, and reports name the router `validate`.
- `on_invalid` is for a route's transforms. On a destination, give the destination a
  `fallback`.

Put `validate` first to check what the sender sent, or after a `jq` step to check what you are
about to deliver.

## enrich

```yaml
routes:
  - name: orders
    from: [webhooks]
    transforms:
      - enrich:
          url: "https://crm.internal/customers/{{ .json.customer_id | urlquery }}"
          headers: { Authorization: "Bearer {{secrets.CRM_TOKEN}}" }
          into: customer
          cache: 5m
    router:
      type: rules
      rules:
        - { when: 'msg.json.customer.tier == "gold"', to: [vip-queue] }
      default: [standard-queue]
```

`enrich` calls an HTTP endpoint and puts its JSON answer into the message, under the `into` key.
The steps after it, the router and the destinations then see the answer like any other part of
the body. In the example, an order that arrives as `{"id": "o-1", "customer_id": 42}` goes on as:

```json
{"id": "o-1", "customer_id": 42, "customer": {"tier": "gold", "country": "ES"}}
```

| Key | Meaning |
|---|---|
| `url` | The endpoint to call. It is a per-message template. |
| `method` | `GET` (default), or `POST`, which sends the message body. |
| `headers` | Request headers. The values are templates. |
| `into` | Where the answer goes: a key, or a dotted path such as `meta.customer`. Missing objects on the way are made. A value already there is replaced. |
| `timeout` | How long the call may take (default `2s`). |
| `cache` | Keep each answer this long and skip the call meanwhile (`GET` only). Without it, every message calls. |
| `on_error` | `fail` (default) or `skip`. |

Things to know:

- **The message body must be a JSON object**, and the answer must be JSON with a `2xx` status.
  An answer over 1 MiB is refused.
- **A failed call fails the step** (`on_error: fail`): a wrong status, no answer within
  `timeout`, an answer that is not JSON. On a route, the message is
  [dead-lettered](/docs/resilience/#dead-letters-and-replay) with reason `transform`, and
  `hikyaku dlq replay` sends it through the route again, which calls again. On a destination it
  is a failed hop, so a fallback can take the message. The call is tried once.
- **`on_error: skip`** lets the message go on without the answer. Use it when the answer is nice
  to have. Rules and templates after it must then cope with the missing key.
- **The cache** is kept in memory, per `enrich` step and per relay process, for up to 10,000
  answers. The key is the rendered URL and headers. A failed call is not cached.
- **Put values into the URL with `urlquery`**, as the example does. A value from the message is
  otherwise copied as it is, and a `/` or a `?` in it would change the URL. A missing key renders
  as empty.
- **The [SSRF guard](/docs/security/#ssrf-guard) applies** when the URL has a template in it:
  the call cannot reach a private or loopback address unless `egress` allows it. So an internal
  service such as `crm.internal` needs `egress.allow_private: true` or its network in
  `egress.allow`. A URL with no template in it is not guarded.
- `hikyaku route --dry-run` runs the transforms, so it makes the call too.
- `content_type` does not go with `enrich`: the body stays JSON.

## content_type

```yaml
- content_type: application/x-ndjson
```

Sets the content type without touching the body.

## Dropping is not failing

A drop and a failure are different things. A false `filter`, or a `jq` program with no output,
drops the message on purpose. The message does not continue on that route or to that
destination, and it is not dead-lettered.

A transform that errors on a message is a failure. Examples: a codec that cannot decode the
body, or a template that fails. It is reported as an error, together with the transform, never
as a silent drop. What happens next depends on where the transform runs:

- On a route, the message goes to the dead-letter queue (DLQ) with reason `transform`, or
  `schema` when a `validate` step refused the body. `hikyaku dlq replay` runs it through the
  route again.
- On a destination, that hop fails as permanent. The message moves on to the next fallback, then
  to the dead-letter queue.
