---
layout: ../../layouts/DocsLayout.astro
title: CLI reference
---

# CLI reference

This page is the reference for the `hikyaku` command line. Read it when you need the flags of a
command, or the rules every command shares: the global flags, how spec files are found and merged,
and the exit codes.

Everything hikyaku does is a `hikyaku` subcommand. Each command has a section below, with its
flags in a table.

## Quick start

The four commands you use first, in this order:

```sh
hikyaku init --template webhook-fanout     # scaffold hikyaku.yaml
hikyaku validate                           # check it
hikyaku doctor                             # reach every destination, model and secret
hikyaku run                                # start the relay
```

## Commands

| Command | What it does |
|---|---|
| [`hikyaku run`](#hikyaku-run) | Run the relay: local development and the container entrypoint |
| [`hikyaku validate`](#hikyaku-validate) | Check the spec: keys, references, fallback cycles, CEL, jq, templates |
| [`hikyaku lint`](#hikyaku-lint) | Warn about specs that work but probably should not |
| [`hikyaku init`](#hikyaku-init) | Scaffold a spec from a template |
| [`hikyaku schema`](#hikyaku-schema) | Print the JSON Schema of the spec |
| [`hikyaku doctor`](#hikyaku-doctor) | Check every destination, model, secret and plugin |
| [`hikyaku send`](#hikyaku-send) | Push one message through a destination's full retry and fallback path |
| [`hikyaku route`](#hikyaku-route) | Route one message, or show the decision with `--dry-run` |
| [`hikyaku ai test`](#hikyaku-ai-test) | Score an AI route on labeled cases |
| [`hikyaku chaos`](#hikyaku-chaos) | Inject faults into a destination to exercise retries and fallbacks |
| [`hikyaku dev`](#hikyaku-dev) | `run` plus the Genkit reflection server |
| [`hikyaku tail`](#hikyaku-tail) | Stream live envelopes from a running relay |
| [`hikyaku dlq`](#hikyaku-dlq) | List, inspect and replay dead letters |
| [`hikyaku history`](#hikyaku-history) | Show, correct, export and forget routing history |
| [`hikyaku version`](#hikyaku-version) | Print the version, commit and Genkit version |

Two more commands help with the shell. `hikyaku completion bash|zsh|fish|powershell` prints a
[shell completion](#shell-completion) script. `hikyaku help <command>` (or `--help` on any
command) shows that command's help.

## Global flags

Every command takes these flags:

| Flag | Meaning |
|---|---|
| `-f, --file <path>` | The spec. Repeatable; default `hikyaku.yaml`. A directory loads every YAML file in it. See [spec files](#spec-files). |
| `--var KEY=VAL` | A variable for secret refs: `${KEY}` in a ref takes `VAL`. Repeatable. |
| `--env-file <path>` | A dotenv file. hikyaku reads it when an `env:` secret or an `{{env.NAME}}` is not set in the process environment. The environment itself is never changed. |
| `--color auto\|always\|never` | Colors in the output. `auto` (the default) colors a terminal, never a pipe or a file, and honors `NO_COLOR`. |
| `-v, --verbose` | Debug logging to stderr. Secrets are redacted in the logs, as everywhere else. |

## Spec files

Commands that read a spec take it from `-f, --file`. The default is `hikyaku.yaml` in the working
directory. `-f` can repeat. A directory means its `*.yaml` and `*.yml` files, in name order. The
files are merged into one spec, as [The spec](/docs/spec/#multiple-files) describes, and every
file must agree on `name`.

## hikyaku run

```sh
hikyaku run [--watch]
```

`hikyaku run` starts the relay: its sources, its delivery workers and the admin API (on `:9090`
by default). It is the container's entrypoint.

- It stops on `SIGINT` or `SIGTERM`. In-flight messages get `server.shutdown_grace` (20s by
  default) to finish.
- It reloads the spec on `SIGHUP` or `POST /reload`. A reload that changes `genkit:` or the model
  providers starts a new Genkit instance. A reload that changes `server.admin.listen` moves the
  admin API. See [hot reload](/docs/spec/#hot-reload).

| Flag | Meaning |
|---|---|
| `--watch` | Reload when a spec file changes. The new spec is validated and built first, then swapped in. One that does not validate or build is reported, and the running one stays. |

## hikyaku validate

```sh
hikyaku validate [--check-secrets]
```

`hikyaku validate` loads the spec and checks it without starting anything. It reports:

- unknown keys and wrong types, with file and line;
- dangling references (a name that nothing declares);
- fallback cycles;
- AI candidates that are not destinations;
- hedging without `idempotent`;
- the protocol matrix (the modes each protocol supports as a source or a destination);
- every CEL expression, jq program and template.

Exit `0` when the spec is valid, `2` with every error otherwise.

Some protocol keys are checked by the protocol's own module, when the relay builds the source or
destination. Kafka `brokers` written as URLs instead of `host:port` is one example. `hikyaku doctor`
builds every destination, so it catches a destination's bad keys before `hikyaku run` does.

| Flag | Meaning |
|---|---|
| `--check-secrets` | Also resolve every declared secret through mamori, and fail on one without a value. Values are never printed. |

## hikyaku lint

```sh
hikyaku lint [--strict]
```

`hikyaku lint` validates the spec, then warns about settings that work but probably should not:

- an unpinned decision model (its calibration changes between releases);
- an AI router without a router fallback;
- `min_margin` with `multi: true`;
- `min_confidence: 0`, so every answer passes;
- `on_low_confidence` unset;
- a single-model chain;
- a required destination without a fallback;
- a load-balancing group without health checks;
- dead letters to stdout or stderr;
- no admin token;
- a listen source without `auth`;
- `ack: accepted` with the memory buffer;
- a chain whose last router has a `when` condition;
- a fallback destination that batches;
- an ordered source feeding a destination that batches.

Exit `0` with warnings, `2` when the spec is invalid.

| Flag | Meaning |
|---|---|
| `--strict` | Exit `1` when there is any warning, for CI. |

## hikyaku init

```sh
hikyaku init [--template NAME] [--dir DIR] [--force]
```

`hikyaku init` writes a `hikyaku.yaml` from a template. The spec validates as written; edit it
from there.

| Template | Scaffolds |
|---|---|
| `webhook-fanout` | an HTTP webhook source that fans out to several destinations |
| `ai-triage` | an AI route that triages events to teams with a decision model, with rules and a static catch-all as fallbacks |
| `wss-to-http` | a WebSocket subscription delivered to an HTTP endpoint, with reconnects and a fallback |
| `kafka-bridge` | HTTP into a keyed, batched Kafka topic with a file spool, and a topic out to a webhook in partition order |

| Flag | Meaning |
|---|---|
| `--template` | The template: `webhook-fanout` (the default), `ai-triage`, `wss-to-http` or `kafka-bridge`. |
| `--dir` | Where to write the spec (default: the working directory). |
| `--force` | Overwrite an existing spec. |

## hikyaku schema

```sh
hikyaku schema [--out FILE]
```

`hikyaku schema` prints the JSON Schema of the spec. A JSON Schema describes the keys a file may
have and the type of each one. Editors use it to complete and check your YAML, through the
`# yaml-language-server: $schema=...` line; see [The spec](/docs/spec/#editor-support).

The schema is the same one the docs serve at
[`/schema/hikyaku.schema.json`](/schema/hikyaku.schema.json), plus the types and `options:` of
the plugins:

- the types a custom build registers;
- the out-of-process plugins the spec (`-f`) lists under `plugins:`. hikyaku starts them to ask
  for their schema.

| Flag | Meaning |
|---|---|
| `--out` | Write the schema to a file instead of stdout. |

## hikyaku doctor

```sh
hikyaku doctor [--timeout 5s]
```

`hikyaku doctor` loads the spec, resolves its secrets through mamori, builds its destinations and
checks the setup. It prints one line per check:

```
✓ spec hikyaku.yaml: 1 sources, 2 destinations, 1 routes
✓ secrets 2 resolved
✓ plugins
✓ destination billing
✓ destination support[0]
✓ destination support[1]
✓ dead_letter
✓ store default (default)
✓ model typesafe/jev-1.13.0
```

It checks:

- the spec;
- how many secrets resolved (values are never printed);
- that this build has every type the spec uses;
- each destination endpoint and each group member (a member without a `name` is shown as
  `<group>[i]`);
- the dead-letter queue and each fallback of it;
- every store;
- every model of the AI chains and of the model destinations.

Exit `0` when every check passed, `1` when one failed. A spec that does not load or validate
counts as a failed check (see [exit codes](#exit-codes)).

| Flag | Meaning |
|---|---|
| `--timeout` | How long each check may take (for example `5s`). |

## hikyaku send

```sh
hikyaku send --to billing --data @msg.json --header X-Tenant:acme --content-type application/json
```

`hikyaku send` pushes one message to one destination through its full delivery path: transforms,
timeout, retries, the circuit breaker (which stops attempts to a destination that keeps failing),
the fallback chain and, at the end, the dead-letter queue (DLQ). It prints what each hop did.

- Exit `0`: the message was delivered.
- Exit `1`: it was not. It was dead-lettered, or a best-effort destination dropped it.
- Exit `3`: the DLQ could not take it either.

| Flag | Meaning |
|---|---|
| `--to` | The destination, by name. |
| `--data` | The message body: `@msg.json` reads a file, `@-` reads stdin, anything else is the literal body. |
| `--header` | A header for the message, as `Name: value` (repeatable). |
| `--content-type` | The content type of the message (default: `application/json` for JSON, else `text/plain`). |
| `--subject` | The message's subject, as a source would set it (a path, a topic, a routing key): `msg.subject`. |
| `--attr` | An attribute of the message, as `name=value`, a string (repeatable): `msg.attributes`. |
| `--json` | Print the delivery report as JSON. |

## hikyaku route

```sh
hikyaku route --source webhooks --data @msg.json [--subject S] [--attr name=value] [--dry-run] [--json]
```

`hikyaku route` takes one message as if it had arrived on `--source`. It runs the message through
every route of that source: filters, transforms, routers and delivery.

With `--dry-run`, it stops after the routers and prints the decision of each route instead: the
router that answered, the model, the probability of each candidate, the destinations chosen, and
the fallback hops it would take.

Without `--dry-run`, it exits `1` when the message was not delivered: dead-lettered, failed or
unrouted. A message that every route filtered out exits `0`.

| Flag | Meaning |
|---|---|
| `--source` | The source the message comes from. |
| `--data` | The message body: `@msg.json` reads a file, `@-` reads stdin, anything else is the literal body. |
| `--header` | A header for the message, as `Name: value` (repeatable). |
| `--content-type` | The content type of the message (default: `application/json` for JSON, else `text/plain`). |
| `--subject` | The message's subject, as the source would set it (a path, a topic, a routing key): `msg.subject`, for rules and `when` conditions that read it. |
| `--attr` | An attribute of the message, as `name=value`, a string (repeatable): `msg.attributes`. |
| `--dry-run` | Decide, but do not deliver. |
| `--json` | Print the report as JSON. |

## hikyaku ai

`hikyaku ai` groups the tools for AI routes. It has one subcommand, `hikyaku ai test`.

## hikyaku ai test

```sh
hikyaku ai test --route smart-triage --cases cases.yaml [--min-accuracy 0.9] [--bins 10] [--json]
```

`hikyaku ai test` scores an AI route on labeled cases. A case is one message and the destinations
it should reach. The command runs each case through the route's router, without delivering, and
reports:

- the accuracy;
- a confusion matrix (which destinations were chosen for each expected one);
- the calibration, in reliability bins (whether the model's confidence matches how often it is
  right);
- the latency.

Use it to tune `min_confidence` and `min_margin`. `hikyaku history export --labeled` turns
recorded corrections into such cases. Exit `1` when the accuracy is below `--min-accuracy`.

Each case has a `name`, the message as `data` (JSON) or `body` (text), and `expect`, the
destinations it should reach. A case may also set the message's `subject`, `headers` and
`attributes`, for routers and `when` conditions that read them:

```yaml
cases:
  - name: refund
    data:
      text: I was charged twice this month
      customer_id: c-42
    expect: [billing]
  - name: outage
    body: The dashboard has been down since 9am
    subject: tickets
    headers:
      X-Tier: gold
    attributes:
      channel: email
    expect: [support]
```

| Flag | Meaning |
|---|---|
| `--route` | The AI route to test. |
| `--cases` | The labeled cases file. |
| `--min-accuracy` | Fail below this accuracy (for example `0.9`). |
| `--bins` | Reliability bins of the calibration report (default `10`). |
| `--json` | Print the report as JSON. |

## hikyaku chaos

```sh
hikyaku chaos --destination billing [--fail 50%] [--latency 2s] [--status 503] [--data @msg.json] [--count 100]
```

`hikyaku chaos` wraps one declared destination with the fault injector, locally. You see the retry
and fallback policy at work without breaking anything real. It runs in one of two modes:

- **With `--data`**, it pushes `--count` copies of the message (100 by default) through the
  destination's retry and fallback path. No source is started. It prints a summary: the attempts
  per message, how many the destination delivered, how many each fallback delivered, the dead
  letters, the messages lost (neither delivered nor dead-lettered), and the p50 and p95 latency.
  It exits `1` when a message was lost.
- **Without `--data`**, it runs the relay as `hikyaku run` does, with the destination wrapped.
  Real traffic from the sources meets the faults. `--count` does not apply. Stop it with `Ctrl-C`.

```
chaos 100 messages to billing, 163 attempts (1.6 per message)
  delivered by billing     88
  delivered by fallback billing-dr  12
  dead-lettered          0
  lost                   0
  latency p50 212ms  p95 1.4s
```

| Flag | Meaning |
|---|---|
| `--destination` | The destination to inject faults into. |
| `--fail` | The share of attempts that fail, such as `50%`. |
| `--latency` | Latency added to every attempt, such as `2s`. |
| `--status` | The status code of a failed attempt (default `503`). |
| `--data` | Push this message instead of running the relay: `@msg.json` reads a file, anything else is the literal body. |
| `--count` | How many messages to push with `--data` (default `100`). |
| `--error` | How a failed attempt fails: `status` (default), `connect` or `timeout`. |
| `--retry-after` | The `Retry-After` hint of a failed attempt, such as `1s`. |
| `--seed` | A random seed, so a run can be repeated exactly. |
| `--json` | Print the summary as JSON. |

## hikyaku dev

```sh
genkit start -- hikyaku dev
```

`hikyaku dev` runs the relay like `hikyaku run`, and also starts the Genkit reflection server.
Run it under `genkit start` to get the Genkit Developer UI. The UI shows every AI route as a
`route/<name>` flow, with a trace of each decision, and can run a flow on a message of your own.
From a clone of the repository, `make genkit-ui` does this for the `ai-triage` example on a fake
model.

| Flag | Meaning |
|---|---|
| `--watch` | Reload when a spec file changes (default `true` for `dev`). |

## hikyaku tail

```sh
hikyaku tail [--source webhooks] [--admin http://localhost:9090]
```

`hikyaku tail` streams the envelopes of a running relay as they arrive. It reads them from the
`/v1/tail` endpoint of the relay's admin API. For each message it prints two lines:

- one when the message arrives: time, source, ID, subject and the start of the body;
- one when it is finished: time, source, ID and its status, which is `delivered`,
  `dead_lettered`, `unrouted`, `filtered` or `failed`.

With `--json`, it prints each event as a JSON line instead.

| Flag | Meaning |
|---|---|
| `--source` | Only the envelopes of this source. |
| `--admin` | The admin API of the relay (default: the spec's `server.admin.listen`, else `http://127.0.0.1:9090`). |
| `--token` | The admin token (default: `HIKYAKU_ADMIN_TOKEN`, else the spec's `server.admin.token`). |
| `--json` | Print each event as JSON. |

## hikyaku dlq

`hikyaku dlq` works on the dead-letter queue of the spec, when it is a file DLQ
(`dead_letter.type: file`). It works offline: it reads the file directly, without a running
relay. Its subcommands are `hikyaku dlq list`, `hikyaku dlq inspect` and `hikyaku dlq replay`.

## hikyaku dlq list

```sh
hikyaku dlq list [--since 1h] [--json]
```

`hikyaku dlq list` lists the dead letters, one line each: ID, time, the destination that failed
(or `route <name>` when routing failed), the reason (`delivery`, `no_route` or `transform`), the
attempts and the last error.

| Flag | Meaning |
|---|---|
| `--since` | Only dead letters newer than this, such as `1h`. |
| `--json` | Print them as NDJSON, one envelope per line. |
| `--path` | The DLQ file (default: the spec's `dead_letter.path`). |

## hikyaku dlq inspect

```sh
hikyaku dlq inspect <id>
```

`hikyaku dlq inspect` prints one dead letter in full, as JSON: the envelope, its `dlq` attribute
(route, destination, reason, attempts, the last error and every hop with its outcome) and, when
an AI router decided, `ai.decision`.

| Flag | Meaning |
|---|---|
| `--path` | The DLQ file (default: the spec's `dead_letter.path`). |

## hikyaku dlq replay

```sh
hikyaku dlq replay [--to billing-dr] [--since 1h] [--id ID] [--dry-run]
```

`hikyaku dlq replay` sends dead letters again. Each one keeps its original ID as its idempotency
key, so a destination that already has the message can drop the duplicate.

- A dead letter goes to the destination that failed, or to `--to` when it is set, through that
  destination's full retry and fallback path.
- A routing failure (reason `no_route` or `transform`) without `--to` runs through its route
  again.
- A message that fails again is dead-lettered again.

Exit `1` when any replay did not go through.

| Flag | Meaning |
|---|---|
| `--to` | Deliver to this destination instead of the original one. |
| `--since` | Only dead letters newer than this, such as `1h`. |
| `--id` | Only this dead letter (repeatable). |
| `--dry-run` | Show what would be replayed, without sending. |
| `--path` | The DLQ file (default: the spec's `dead_letter.path`). |

## hikyaku history

`hikyaku history` reads and edits the [routing history](/docs/history/) of a running relay. It
goes through the relay's admin API, so it works the same with the memory store and the Redis
store. Every subcommand takes `--admin URL` (the admin API) and `--token` (the admin token,
`server.admin.token`). Its subcommands are `hikyaku history show`, `hikyaku history correct`,
`hikyaku history export` and `hikyaku history forget`.

## hikyaku history show

```sh
hikyaku history show --route smart-triage --key c-42
```

`hikyaku history show` prints the stored past calls of a key, exactly as the model will see them.

| Flag | Meaning |
|---|---|
| `--route` | The route. |
| `--key` | The rendered history key (a customer or conversation ID, for example). |
| `--admin` | The admin API of the relay. |
| `--token` | The admin token. |

## hikyaku history correct

```sh
hikyaku history correct <msg_id> --to billing [--note "refund, not a bug"]
```

`hikyaku history correct` records a correction for a past message: where it should have gone.
Later calls for the same key see the correction in their history (the
[feedback loop](/docs/history/#the-feedback-loop)).

| Flag | Meaning |
|---|---|
| `--to` | The correct destination (repeatable). |
| `--note` | Why, kept with the correction. |
| `--admin` | The admin API of the relay. |
| `--token` | The admin token. |

## hikyaku history export

```sh
hikyaku history export [--since 30d] [--route smart-triage] > history.ndjson
hikyaku history export --labeled [--since 30d] [--route smart-triage] > cases.yaml
```

`hikyaku history export` writes the recorded calls as NDJSON, one call record per line. With
`--labeled`, it writes only the corrected calls instead, as a YAML cases file for
`hikyaku ai test`. Each case is labeled with its correct destinations.

| Flag | Meaning |
|---|---|
| `--labeled` | Only calls with a correction, as `ai test` cases. |
| `--since` | Only calls newer than this, such as `30d`. |
| `--route` | Only this route. |
| `--admin` | The admin API of the relay. |
| `--token` | The admin token. |

## hikyaku history forget

```sh
hikyaku history forget --route smart-triage --key c-42
```

`hikyaku history forget` deletes the history of a key, for right-to-be-forgotten requests.

| Flag | Meaning |
|---|---|
| `--route` | The route. |
| `--key` | The rendered history key. |
| `--admin` | The admin API of the relay. |
| `--token` | The admin token. |

## hikyaku version

```sh
hikyaku version [--json]
```

`hikyaku version` prints the version, the commit and the build date, the Genkit version the binary
was built with, and the Go version and platform.

| Flag | Meaning |
|---|---|
| `--json` | Print them as JSON. |

## Shell completion

```sh
hikyaku completion bash|zsh|fish|powershell
```

Prints the completion script for a shell. The Linux packages install the bash, zsh and fish
scripts; [Install](/docs/install/#shell-completion) shows how to load them by hand.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success. |
| `1` | Something that was checked failed. See the list below. |
| `2` | Config or usage error: an invalid spec, a bad flag, an unknown command, an undeclared or unresolved secret. |
| `3` | Infrastructure error: something hikyaku needed could not be reached, such as a secret provider or a store. |

Exit `1` means one of these:

- a lint warning under `--strict`;
- a `doctor` check, including a spec that does not load;
- an `ai test` below `--min-accuracy`;
- a `send` that did not deliver its message;
- a `route` whose message was dead-lettered, failed or unrouted;
- a `chaos --data` run that lost a message;
- a `dlq replay` that did not go through.

## Errors and redaction

Errors go to stderr. Every secret value resolved through mamori is redacted from errors, logs and
output. It never appears in `/debug` either. See [Security](/docs/security/#secrets).
