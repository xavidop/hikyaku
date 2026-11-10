---
layout: ../../layouts/DocsLayout.astro
title: LLM proxy
---

# LLM proxy

This page explains how to run hikyaku as an LLM proxy: one endpoint in front of all your models.
Read it when you want your applications to call one URL, and hikyaku to pick the model, retry,
and fail over between models and providers.

hikyaku sits in front of your models as an **OpenAI-compatible endpoint**. It speaks the same
HTTP API as OpenAI, so any OpenAI SDK or tool works with it. Point the SDK at the relay
(`base_url: http://relay:8080/v1`). A router then picks the model for each request, and
destination retries and fallbacks fail over between models and providers.

```yaml
sources:
  - name: api
    type: openai
    listen: ":8080"
    models: [auto, fast, smart]                  # what GET /v1/models lists
    auth: { bearer: { tokens: ["{{secrets.PROXY_KEY}}"] } }

destinations:
  - { name: fast,  type: model, model: googleai/gemini-2.5-flash, fallback: [local] }
  - { name: smart, type: model, model: anthropic/claude-sonnet-4-5, fallback: [fast] }
  - { name: local, type: model, model: ollama/llama3.2, generation: { max_tokens: 512 } }

routes:
  - name: chat
    from: [api]
    router:
      type: rules
      rules:
        - { when: 'msg.json.model == "fast"', to: [fast] }
        - { when: 'msg.json.model == "smart"', to: [smart] }
      fallback:
        - type: ai                               # "auto": a decision model picks
          when: msg.json.model == "auto"         # only "auto" pays for a decision
          engine: decision
          model: typesafe/jev-1.13.0
          state: "{{ .json.messages }}"
          candidates:
            - { name: fast, description: "A short or simple request: a greeting, a lookup, a rewrite" }
            - { name: smart, description: "A hard request: multi-step reasoning, math, code, analysis" }
          on_low_confidence: [smart]
          fallback: [{ type: static, to: [fast] }]
```

In this spec, only requests for `auto` reach the decision model, because of its `when`. Any other
model name falls to the static router, with no decision call. See
[Only the messages that need a model](/docs/routing/#only-the-messages-that-need-a-model).

## The endpoint (type: openai)

The `openai` source is the endpoint your clients call. It serves two paths.

- `POST /v1/chat/completions` turns each request into one envelope. The request JSON is the body.
  The requested model is the subject, and `msg.json.model` in rules.
- `GET /v1/models` lists `models`. `path` changes `/v1`.
- The caller's credentials (`Authorization`, `Proxy-Authorization`, `Cookie`, `X-Api-Key`,
  `Api-Key` and the `auth.api_key` header) are never copied into the envelope. A destination never
  sees the caller's key. Use `auth.bearer` for the relay's own keys.
- An `Idempotency-Key` header becomes the envelope ID.
- Failures are answered in OpenAI's error shape (`{"error": {"message", "type", "code"}}`):

| Outcome | Status | `code` |
|---|---|---|
| the `auth` check failed | 401 | `invalid_api_key` |
| another path under `path` | 404 | `unknown_url` |
| a method other than `POST` (chat) or `GET` (models) | 405 | `method_not_allowed` |
| the request is over `max_body_bytes` (4MiB by default) | 413 | `request_too_large` |
| the body is not JSON, or has no `messages` | 400 | `invalid_json`, `missing_messages` |
| the relay's queue is full | 503, with `Retry-After` | `overloaded` |
| no route serves the model | 404 | `model_not_found` |
| a request the model destination cannot run: an invalid one (an unknown role, `n` above 8), one the model cannot take (tools, `tool_choice` or media it does not support), or one the provider refused as invalid | 400 | `bad_request` |
| a route transform failed | 500 | `transform_failed` |
| the relay's filters rejected it | 403 | `filtered` |
| rate limited | 429, with `Retry-After` | `rate_limited` |
| timed out: a model, or the source's `timeout` (5m by default) | 504 | `timeout` |
| a permanent upstream error | 502 | `upstream_error` |
| the destination that served the request sent no reply | 502 | `no_reply` |
| every model unavailable | 503, with `Retry-After` | `unavailable` |

The first six rows are answered before the request becomes a message. The other rows are the
outcome of a message like any other. A failed message is dead-lettered first, then answered: a
model no route serves (its routers chose nothing) with reason `no_route`, an invalid request with
reason `delivery`. A request that no route takes at all is unrouted. It also gets `404`, but
no dead letter.

## Model destinations (type: model)

A `model` destination takes the chat request, runs it on one model, and answers with an OpenAI
chat completion, usage included.

`model` names a Genkit model as `provider/name`: `googleai/...`, `anthropic/...`, `openai/...`,
`openrouter/<vendor>/<model>`, `ollama/...`. The provider prefix loads the provider's Genkit
plugin. Each provider needs one API key (`GEMINI_API_KEY`, `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`,
`OPENROUTER_API_KEY`), taken from a declared secret of that name or from the environment.
`genkit.openai.base_url` sends `openai/<name>` models to any OpenAI-compatible server (vLLM,
LM Studio, a gateway) instead of OpenAI; such a server may need no key.

The destination reads these fields of the request:

- `messages`, with the roles `system`, `developer` (taken as a system prompt), `user`,
  `assistant`, `tool` and the deprecated `function`. A `content` is a string, or an array of
  `text`, `image_url`, `file` and `input_audio` parts.
- `temperature`, `top_p`, `max_completion_tokens` (else `max_tokens`) and `stop`.
- `response_format`: `json_object` asks the model for JSON, and `json_schema` passes its
  `json_schema.schema` to the model as the output schema.
- `tools`, `tool_choice` and `parallel_tool_calls` (see [Function calling](#function-calling)),
  and the deprecated `functions` and `function_call`.
- `n`, from 1 to 8. Each choice is a generation of its own, and they run at the same time. The
  usage is their sum.
- `stream`, and `stream_options.include_usage`, which adds a last chunk with the token usage.

`generation` sets defaults the request can override (`temperature`, `max_tokens`, `top_p`), and a
`system` prompt for requests without a `system` or `developer` message. Each provider gets them in
the shape its plugin validates.

A request the destination cannot run is refused as a bad request: the caller gets `400`, and the
message is dead-lettered. That is one of:

- an invalid request (an unknown role or content part, `n` outside 1 to 8);
- a request the model cannot take (tools, `tool_choice` or media on a model that does not support
  them);
- a request the provider refused as invalid.

A `file` part's `file.file_data` is a data URL, or base64 whose media type comes from
`file.filename` (a PDF when it names none). `file.file_id` is refused. An `input_audio` part is
base64 `wav` or `mp3`. Providers read different media: Gemini takes images, PDFs and audio; Claude
images, PDFs and plain text; OpenAI images, PDFs and, on its audio models, audio; Ollama images.

Everything else works as on an ordinary destination: `retry`, `timeout`, `circuit_breaker`,
`fallback` (another model, another provider) and groups (round-robin or failover between models).
Validation refuses `batch` on a model destination, or on a group with model members: a model
answers one chat request per message. `hikyaku_model_tokens_total{destination,type}` counts input
and output tokens.

## Function calling

Function calling (also called tool calls) lets a model ask the caller to run a function and send
the result back. hikyaku passes the request's functions to the model, and the model's calls go
back to the caller, who runs them, as with OpenAI. hikyaku runs no tool.

```sh
curl -s localhost:8080/v1/chat/completions -d '{
  "model": "fast",
  "tools": [{"type": "function", "function": {"name": "get_weather",
    "parameters": {"type": "object", "properties": {"city": {"type": "string"}}, "required": ["city"]}}}],
  "messages": [{"role": "user", "content": "Weather in Paris?"}]}'
```

The answer holds the call. `content` is null when the model wrote no text, and `finish_reason` is
`tool_calls`:

```json
{"index": 0, "finish_reason": "tool_calls",
 "message": {"role": "assistant", "content": null, "tool_calls": [{"id": "call_MPI45YS2GTXJ7O6SL2OZKNZ3GD",
   "type": "function", "function": {"name": "get_weather", "arguments": "{\"city\":\"Paris\"}"}}]}}
```

Run the tool. Then send the conversation back with the assistant message and one `tool` message
per call, with the call's `id` as `tool_call_id`.

- A function's `strict` is off unless the function sets it, whatever the provider's default.
- `tool_choice` is `auto`, `none`, `required`, or `{"type": "function", "function": {"name": ...}}`,
  which offers that function alone and requires a call. `parallel_tool_calls: false` keeps the
  first call of each answer.
- The call `id` is the provider's. A provider that gives none (Gemini, Ollama) gets
  `call_<random>`.
- A Gemini thought signature comes with its call as `extra_content.google.thought_signature`, as on
  Gemini's own OpenAI-compatible endpoint. Send it back with the call: Gemini 3 requires it.
- A model without `tool_choice` (Ollama, OpenAI's text-only models) is offered no tools for
  `tool_choice: none`, and refuses `required` or a named function.
- The deprecated `functions` and `function_call` work too. A request with `functions` is answered
  with `message.function_call` and `finish_reason: "function_call"`.

## Streaming

Streaming sends the answer in pieces (chunks) while the model writes it, instead of one whole
answer at the end. With `"stream": true`, the answer is server-sent events, written by the model
as it generates, ending with `data: [DONE]`.

- **Before the first chunk**, a failure is retried, or falls back to another model, like any
  delivery.
- **After the first chunk**, a failure ends the stream with an error event. No other model takes
  over half an answer.
- **Tool calls** are written once the model's answer is whole: one `delta.tool_calls` entry per
  call, with `index`, `id`, `type`, `function.name` and all of `function.arguments`, then the last
  chunk with `finish_reason: "tool_calls"`.
- **With `n` above 1**, the choices' chunks interleave. Each chunk names its choice in
  `choices[].index`, and each choice ends with its own last chunk.
- A destination that does not stream (an `http` upstream that is itself OpenAI-compatible) is
  answered with chunks made from its whole answer. An upstream event stream is passed through.

## Try it

`examples/llm-proxy` runs offline on fake models: the `llmfake` provider, which `make build-llmfake`
compiles into the binary. The example was also run end to end on real services: TypeSafe's
decision model routing between two `ollama/qwen2.5:0.5b` destinations, whole and streamed answers,
and token metrics. Function calling was run on `ollama/qwen2.5:0.5b` with the OpenAI Python SDK:
a call, the model reading the tool's answer, streamed calls, and `n: 2`.
