# protocols/openai

This module is the `openai` source: an OpenAI-compatible endpoint, which makes hikyaku an LLM proxy. Read this README for the source's keys, the request fields a `model` destination reads, function calling, streaming, the error codes and the envelope it produces.

The endpoint speaks the same HTTP API as OpenAI, so any OpenAI SDK or tool works with it. Point the SDK at the relay (`base_url: http://relay:8080/v1`). Routes then pick the model for each request, and destination retries and fallbacks fail over between models and providers.

```yaml
secrets:
  PROXY_KEY: env:PROXY_KEY

sources:
  - name: api
    type: openai
    listen: ":8080"
    models: [auto, fast, smart]          # what GET /v1/models lists
    auth: { bearer: { tokens: ["{{secrets.PROXY_KEY}}"] } }

destinations:                            # model destinations live in the llm module
  - { name: fast,  type: model, model: googleai/gemini-2.5-flash, fallback: [local] }
  - { name: smart, type: model, model: anthropic/claude-sonnet-4-5, fallback: [fast] }
  - { name: local, type: model, model: ollama/llama3.2 }

routes:
  - name: chat
    from: [api]
    router:
      type: rules
      rules:
        - { when: 'msg.json.model == "smart"', to: [smart] }
      default: [fast]
```

## Endpoints

The source serves two paths under `path`.

| Endpoint | |
|---|---|
| `POST <path>/chat/completions` | One envelope per request: the request JSON is the body, the requested model is the subject (and `msg.json.model` in rules). The caller gets the reply of the destination that served it. |
| `GET <path>/models` | The `models` list (default `[hikyaku]`). |

`path` defaults to `/v1`. Other paths answer 404 with an OpenAI-style error.

## Keys

| Key | |
|---|---|
| `listen` | Address served, such as `:8080` (required). Shared with other listen sources on the same address. |
| `path` | Path prefix (default `/v1`). |
| `models` | Model names `GET /v1/models` lists. |
| `auth` | Inbound auth: `bearer` (OpenAI clients send their key as a bearer token), `api_key`, `jwt`, `ip_allow`. `hmac` does not apply. |
| `tls` | Server TLS (and mTLS with `client_ca_file`). |
| `max_body_bytes` | Request size limit (default 4MiB). |
| `timeout` | How long a request waits for its answer (default 5m). |

The caller's credentials (`Authorization`, `Proxy-Authorization`, `Cookie`, `Api-Key`, `X-Api-Key` and the `auth.api_key` header) and its `OpenAI-Organization` and `OpenAI-Project` headers are never copied into the envelope. A destination never sees the caller's key. An `Idempotency-Key` header becomes the envelope ID.

## Requests

A `model` destination reads these fields of the request:

- `messages`, with the roles `system`, `developer` (taken as a system prompt), `user`, `assistant`, `tool` and the deprecated `function`. A `content` is a string or an array of parts (see [Content parts](#content-parts)). The destination's `generation.system` prompt is added when the request has no `system` or `developer` message.
- `temperature`, `top_p`, `max_completion_tokens` (else `max_tokens`) and `stop`. The destination's `generation` block (`temperature`, `top_p`, `max_tokens`) fills in what the request leaves out.
- `response_format`: `json_object` asks the model for JSON, and `json_schema` passes its `json_schema.schema` to the model as the output schema.
- `tools`, `tool_choice`, `parallel_tool_calls`, and the deprecated `functions` and `function_call` (see [Function calling](#function-calling)).
- `n`, from 1 to 8 (see [Choices](#choices)).
- `stream`, and `stream_options.include_usage`, which adds a last chunk with the token usage.

A request the destination cannot run fails the delivery as permanent, tagged `model:bad_request`, and the caller gets `400`. That is one of:

- an invalid request: an unknown role or content part, `n` outside 1 to 8, or a `tool` message that answers no earlier call;
- a request the model cannot take: tools, `tool_choice` or media on a model that does not support them;
- a request the provider refused as invalid.

## Function calling

Function calling (also called tool calls) lets a model ask the caller to run a function and send the result back. The model gets the request's functions, and its calls go back to the caller, who runs them, as with OpenAI. hikyaku runs no tool.

- `tools` are entries of type `function`: `name` (letters, digits, `_` and `-`, at most 64), `description`, `parameters` (a JSON Schema, which describes the arguments and their types; none means no arguments) and `strict`. `strict` is off unless the function sets it, whatever the provider's default.
- `tool_choice` is `auto`, `none`, `required`, or `{"type": "function", "function": {"name": ...}}`, which offers that function alone and requires a call.
- `parallel_tool_calls: false` keeps the first call of each answer.
- An `assistant` message carries its `tool_calls` (`id`, `function.name`, and `function.arguments` as a JSON string); its `content` may be null. A `tool` message answers one call by `tool_call_id`, with a string or text parts. Content that is JSON reaches the model as JSON, other content as text. The answers to one turn's calls reach the model together.

The model's calls come back as `choices[].message.tool_calls`, each with an `id`, `type: "function"`, `function.name` and `function.arguments` (a JSON string). `content` is null when the model wrote no text, and `finish_reason` is `tool_calls`. The `id` is the provider's. A provider that gives none (Gemini, Ollama) gets `call_<random>`. Send the `id` back unchanged: it matches each answer to its call.

A Gemini model's thought signature comes with its call as `extra_content.google.thought_signature`, as on Gemini's own OpenAI-compatible endpoint. Send it back with the call: Gemini 3 requires it.

Each model declares what it supports. A model without tools refuses `tools`. A model without `tool_choice` (Ollama, OpenAI's text-only models) is offered no tools for `tool_choice: none`, and refuses `required` or a named function.

The deprecated `functions` and `function_call` work as `tools` and `tool_choice`, and a `function` message answers the last call to its `name`. A request with `functions` is answered the deprecated way: `message.function_call` (the first call) and `finish_reason: "function_call"`.

## Choices

`n` asks for up to 8 choices. Each choice is a generation of its own, and they run at the same time. The usage is their sum: the provider bills each one. When one fails, the whole request fails, and is retried or falls back as a whole.

## Content parts

| Part | |
|---|---|
| `text` | Text. |
| `image_url` | An image: `image_url.url` is a URL or a data URL. |
| `file` | A file: `file.file_data` is a data URL, or base64 whose media type comes from `file.filename` (a PDF when it names none). `file.file_id` is refused: hikyaku keeps no files. |
| `input_audio` | Audio: `input_audio.data` is base64, and `input_audio.format` is `wav` or `mp3`. |

A model that takes no media refuses them. Providers read different media: Gemini takes images, PDFs and audio; Claude images, PDFs and plain text; OpenAI images, PDFs and, on its audio models, audio; Ollama images.

## Streaming

Streaming sends the answer in pieces (chunks) while the model writes it, instead of one whole answer at the end. With `"stream": true` the answer is server-sent events:

- A model destination writes each chunk as the model generates it, and the source ends the stream with `data: [DONE]`.
- Tool calls are written once the model's answer is whole: one `delta.tool_calls` entry per call (`index`, `id`, `type`, `function.name`, and `function.arguments` in full), then the last chunk with `finish_reason: "tool_calls"`. Providers stream calls in pieces or whole; this way every provider gives the same chunks.
- With `n` above 1, the choices' chunks interleave. Each chunk names its choice in `choices[].index`, and each choice ends with its own last chunk.
- A model that fails before its first chunk is retried, or falls back, like any destination.
- Once a chunk reached the caller, a failure ends the stream with an error event: no other model may take over half an answer.
- A destination that does not stream is answered with chunks made from its whole answer: per choice, its content, its tool calls, and a last chunk. An `http` upstream that answered with its own event stream is passed through as is.

## Errors

Failures are answered in OpenAI's error shape (`{"error": {"message", "type", "code"}}`):

| Outcome | Status | `code` |
|---|---|---|
| The `auth` check failed | 401 | `invalid_api_key` |
| Another path under `path` | 404 | `unknown_url` |
| A method other than `POST` (chat) or `GET` (models) | 405 | `method_not_allowed` |
| The request is over `max_body_bytes` | 413 | `request_too_large` |
| The body is not JSON, or has no `messages` | 400 | `invalid_json`, `missing_messages` |
| The relay's queue is full | 503 (`Retry-After: 1`) | `overloaded` |
| No route serves the model: no route takes the source, or its routers chose no destination (a `no_route` dead letter) | 404 | `model_not_found` |
| A route's `validate` step refused the request body (a `schema` dead letter) | 400 | `schema` |
| A route transform failed (a `transform` dead letter) | 500 | `transform_failed` |
| Rejected by filters | 403 | `filtered` |
| The model destination cannot run the request (`model:bad_request`): it is invalid, the model cannot take it, or the provider refused it as invalid | 400 | `bad_request` |
| Rate limited | 429 (`Retry-After: 1`) | `rate_limited` |
| Timed out: the model, or the source's `timeout` | 504 | `timeout` |
| A permanent upstream error | 502 | `upstream_error` |
| The destination that served the request sent no reply | 502 | `no_reply` |
| Every model unavailable | 503 (`Retry-After: 1`) | `unavailable` |

The first six rows never reach the relay. The other rows are the outcome of a message like any other. A request whose routing or delivery failed is answered once it is dead-lettered. An unrouted or filtered one gets no dead letter.

## Envelope

| Field | Value |
|---|---|
| Protocol | `openai` |
| Subject | The requested model. |
| Body | The request JSON. |
| Attributes | `openai.model`, `openai.stream`, `openai.endpoint` (`chat.completions`), `remote_addr`. |
