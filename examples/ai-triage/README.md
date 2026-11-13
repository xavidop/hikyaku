# ai-triage

This example uses an AI router to send each support event to the team or teams that must see it. The router runs on [System One](https://genkit.dev/docs/go/integrations/system-one/) decision models through Genkit Go. It shows:

- **One request, one question per candidate.** With `multi: true`, each request to the model holds one yes/no question per candidate (a team the event could go to). Each answer comes back with a calibrated probability: a confidence meant to match how often the model is right. A candidate is chosen when its probability reaches `min_confidence`.
- **Model fallback.** When the hosted model is down, a local `ollama-decisions/clef` serves instead.
- **Only text costs a call.** `when: has(msg.json.text)` sends free-text customer messages to the model. Typed events, such as `invoice.paid`, skip it and go straight to the rules, so they never cost a call.
- **Router fallback.** If every model fails, CEL rules decide, then a static catch-all.
- **History.** Each customer's past calls (kept in Redis) go to the model as earlier turns of the conversation. A follow-up sticks to where the previous call went, and corrections teach the router per customer.

Two specs ship with the example:

- `hikyaku.yaml`: TypeSafe's `jev`, with Ollama and Redis.
- `hikyaku.llmfake.yaml`: the same router on deterministic fake models and an in-process store. It runs offline. It needs a build with the fake model provider (`-tags llmfake`): `make build-llmfake` writes it to `bin/hikyaku`.

## Offline walkthrough

Build the fake-model binary, then ask the router where it would send a message, without delivering it:

```sh
make build-llmfake            # from the repository root
cd examples/ai-triage
../../bin/hikyaku route --dry-run -f hikyaku.llmfake.yaml --source webhooks --data @msg.json
```

The decision shows the chain of hops: `llmfake-decisions/down` failed (a model hop), then `llmfake-decisions/clef` answered with a probability for each candidate. `billing` is chosen for the double-charge message. Each destination's plan also shows the fallback chain it would walk.

Next, evaluate the router on labeled cases. This is how you tune `min_confidence`:

```sh
../../bin/hikyaku ai test -f hikyaku.llmfake.yaml --route triage --cases cases.yaml
```

This prints accuracy, per-destination precision and recall, a confusion matrix, reliability bins (calibration, with the expected calibration error), and latency.

Now run the relay, send a message, then a follow-up from the same customer:

```sh
../../bin/hikyaku run -f hikyaku.llmfake.yaml &
curl -s localhost:8080/events -d @msg.json
curl -s localhost:8080/events -d '{"customer_id":"cus_42","text":"Still no refund, following up again on the same invoice."}'
../../bin/hikyaku history show -f hikyaku.llmfake.yaml --route triage --key cus_42
```

The second message is recognized as a follow-up and goes where the first one went, with no candidate questions asked. `history show` prints the past calls exactly as the model sees them.

Correct a decision, and the next call for that customer sees the correction:

```sh
../../bin/hikyaku history correct -f hikyaku.llmfake.yaml <msg_id> --to support --note "a bug, not billing"
```

## With real models

Set the key, pull the local fallback model, check the setup, and run:

```sh
export TYPESAFE_API_KEY=...   # and a Redis on localhost:6379
ollama pull clef              # the local fallback decision model
hikyaku doctor                # checks the keys, the models, Redis and every destination
hikyaku run
```

Pin the decision model version (`jev-1.13.0`). Calibration changes between releases, so thresholds tuned on one version do not carry over. `hikyaku lint` warns about `-latest`.

The real spec was tuned that way. With the first candidate descriptions and `min_confidence: 0.6`, `hikyaku ai test` on `jev-1.13.0` scored 3/5: recall was perfect, but a refund also went to support, and a single customer's crash also paged on-call. Sharper descriptions brought it to 4/5, and `min_confidence: 0.9` to 5/5, with an expected calibration error of 0.03. The outage still reaches both support and on-call. The fake models in `hikyaku.llmfake.yaml` keep their own tuning: thresholds never carry over between models.

## On Gemini Decision 1

The same router runs on [Gemini Decision 1](https://aistudio.google.com/docs/models/gemini-decision-1), Google's decision model, through OpenRouter. Change three things in `hikyaku.yaml`:

```yaml
secrets:
  OPENROUTER_API_KEY: env:OPENROUTER_API_KEY

genkit:
  systemone:
    servers: [openrouter, ollama]

models:
  decision: openrouter-decisions/google/gemini-decision-1
```

With the candidates and `min_confidence: 0.9` unchanged, `hikyaku ai test` on this model scored 5/5, with an expected calibration error of 0.03 and about 250 ms per call. Run it again with your own cases before you rely on the thresholds. The [routing page](https://xavidop.github.io/hikyaku/docs/routing/#gemini-decision-1) lists what to know about this model.
