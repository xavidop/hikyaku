# llm-proxy

This example runs hikyaku as an OpenAI-compatible LLM proxy: clients talk to it with the OpenAI API, and hikyaku picks the model behind each request.

- Clients ask for `fast`, `smart` or `auto`. Rules route `fast` and `smart`.
- For `auto`, a System One decision model picks the model by how hard the request is.
- Each model destination retries, then falls back to another provider. A local Ollama model is the last resort.

Two specs ship with the example:

- `hikyaku.yaml`: Gemini, Claude and Ollama, behind a bearer key (clients send it in the `Authorization` header).
- `hikyaku.llmfake.yaml`: the same shape on fake models, offline. It needs a build with the fake model provider (`-tags llmfake`): `make build-llmfake` writes it to `bin/hikyaku`.

## Offline walkthrough

Build the fake-model binary, run the relay, and send a few requests:

```sh
make build-llmfake            # from the repository root
cd examples/llm-proxy
../../bin/hikyaku run -f hikyaku.llmfake.yaml &
curl -s localhost:8080/v1/models
curl -s localhost:8080/v1/chat/completions -d '{"model":"fast","messages":[{"role":"user","content":"ping"}]}'
curl -s localhost:8080/v1/chat/completions -d '{"model":"smart","messages":[{"role":"user","content":"hard one"}]}'
curl -sN localhost:8080/v1/chat/completions -d '{"model":"fast","stream":true,"messages":[{"role":"user","content":"stream me"}]}'
```

`smart` is down in the fake setup, so its fallback (`fast`) answers. The streaming request prints one `data:` event per word, then `data: [DONE]`.

Function calling works too. The fake model calls every tool the message names. The answer holds the call in `tool_calls`, with `finish_reason: "tool_calls"`:

```sh
curl -s localhost:8080/v1/chat/completions -d '{"model":"fast","tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"get_weather Paris"}]}'
```

The caller runs the tool and sends its answer back with the call's `id`. The fake model then says what the tool said:

```sh
curl -s localhost:8080/v1/chat/completions -d '{"model":"fast","tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"get_weather Paris"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}}]},{"role":"tool","tool_call_id":"call_1","content":"{\"temp\":21}"}]}'
```

Any OpenAI SDK works too:

```python
from openai import OpenAI
client = OpenAI(base_url="http://localhost:8080/v1", api_key="unused-offline")
print(client.chat.completions.create(model="fast", messages=[{"role": "user", "content": "hi"}]).choices[0].message.content)
```

## With real models

Set the keys, pull the local models, check the setup, and run:

```sh
export PROXY_KEY=... GEMINI_API_KEY=... ANTHROPIC_API_KEY=... TYPESAFE_API_KEY=...
ollama pull llama3.2 && ollama pull clef
hikyaku doctor
hikyaku run
```

`hikyaku_model_tokens_total{destination,type}` counts the tokens each model destination used. `hikyaku_fallback_total` shows the failovers between models.
