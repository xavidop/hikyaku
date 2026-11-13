# webhook-fanout

This example receives a signed webhook and fans it out: one copy goes to each of three destinations, and one to an archive file. It shows the full set of delivery protections hikyaku has:

- a timeout on every attempt, and retries with full jitter (a random wait between attempts) that honor the `Retry-After` header;
- a circuit breaker (after enough failures, hikyaku stops calling the destination for a while and goes straight to its fallback), a retry budget (a cap on retries as a share of traffic, so an outage does not multiply the load), and a fallback to a disaster-recovery (DR) endpoint;
- a best-effort destination (`required: false`): when it fails, the webhook still succeeds;
- a dead-letter file with a fallback of its own. The dead-letter queue (DLQ) is where a message goes when every delivery attempt failed.

Two specs ship with the example:

- `hikyaku.yaml`: the real thing, talking to internal HTTPS services.
- `hikyaku.llmfake.yaml`: the same fan-out with [fault sinks](../../protocols/fault) in place of the services. A fault sink is a destination that fails the way you tell it to, so every path runs offline on your machine.

## Test it

`hikyaku.test.yaml` holds tests of the real spec: where an event goes, what happens when billing is down, and what is dead-lettered. They run without the services and without the secret, because every destination is replaced by a stand-in:

```sh
hikyaku test
```

To see the spec as a diagram, run `hikyaku graph`. `hikyaku graph --out relay.svg` saves it
as a picture.

## Run it offline

Start the relay with the offline spec. The secret is the key the webhook is signed with:

```sh
export WEBHOOK_SECRET=s3cret
hikyaku run -f hikyaku.llmfake.yaml
```

In another terminal, sign and send a webhook:

```sh
body='{"type":"invoice.paid","id":"evt_1"}'
sig=$(printf '%s' "$body" | openssl dgst -sha256 -hmac "$WEBHOOK_SECRET" -hex | sed 's/^.* //')
curl -i localhost:8080/ingest -H 'Content-Type: application/json' \
  -H "X-Signature-256: sha256=$sig" -H 'Idempotency-Key: evt_1' -d "$body"
```

The call answers `200` once every required destination has the message. This is what happens to each copy:

- `billing` fails twice, then takes the message on the third attempt.
- `flaky` fails every attempt, so its fallback writes the message to `out/spool.ndjson`.
- `analytics` fails, but it is best-effort, so it does not hold up the answer.
- `out/archive.ndjson` has the envelope (the message as hikyaku carries it). Its ID is `evt_1`: the idempotency key the caller sent, which hikyaku sends on every attempt.

Then look at what happened:

```sh
curl -s localhost:9090/metrics | grep -E 'hikyaku_(delivery_attempts|fallback|delivery_dropped)_total'
hikyaku send -f hikyaku.llmfake.yaml --to flaky --data '{"n":1}'   # one message through the retry/fallback path
hikyaku chaos -f hikyaku.llmfake.yaml --destination archive --fail 50% --data '{"n":1}' --count 20
```

The first command prints the attempt, fallback and drop counters. The second sends one message through `flaky`, so you can watch the retry and fallback path on its own. The third is a chaos test: hikyaku pushes 20 messages through `archive` while it makes half of the attempts fail, and exits 1 if any message is lost.

An unsigned or tampered webhook gets `401`.
