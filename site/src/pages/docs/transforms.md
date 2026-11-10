---
layout: ../../layouts/DocsLayout.astro
title: Transforms
---

# Transforms

Transforms reshape a message on its way through hikyaku. Use them to drop messages you do not
want, to pick fields out of a body, to render a new body, to edit headers, or to change the
body's encoding. Transforms run in two places:

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

**Each entry sets exactly one of** `filter`, `jq`, `template`, `headers` and `codec`, or
`content_type` on its own. `content_type` may also go with a `jq` or `template` entry, to label
what that entry produced. Validation rejects an entry with two operations. It also compiles
every CEL expression, jq program and template before the relay starts.

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

- On a route, the message goes to the dead-letter queue (DLQ) with reason `transform`.
  `hikyaku dlq replay` runs it through the route again.
- On a destination, that hop fails as permanent. The message moves on to the next fallback, then
  to the dead-letter queue.
