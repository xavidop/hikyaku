# crm-plugin

This example is an out-of-process plugin written in Go. The `hikyaku-crm` binary adds the `crm` destination type, which posts each lead to a CRM's HTTP API. hikyaku starts the binary, talks gRPC to it, and treats `crm` like a built-in type: `validate` checks its `options:`, and the engine retries, falls back and dead-letters around it. Nothing in hikyaku is rebuilt. To compile your types into hikyaku itself instead, see [custom-destination](../custom-destination).

The steps:

1. **Implement `plugin.Sink`** ([crm.go](crm.go)), exactly as for an in-process plugin. `Deliver` makes one attempt. When it fails, it returns a classified error, one that says how it failed: `plugin.StatusError(code, retryAfter, msg)` for an HTTP answer, `plugin.Errorf(plugin.Permanent, tags, ...)` for a message no retry can fix, or the plain error of a refused connection, which hikyaku classifies itself (as transient with the tag `connect`, or as a timeout).
2. **Read your settings from `options:`.** `spec.Decode(&opts)` decodes the destination's `options:` strictly: an unknown key is an error. The `jsonschema` tags of `CRMOptions` become the JSON Schema the plugin declares, so `hikyaku validate` catches a typo, or a pipeline name that does not match the pattern the plugin allows, before anything runs.
3. **Serve it** ([main.go](main.go)):

   ```go
   grpcplugin.Serve(grpcplugin.Plugins{
   	Sinks: map[string]grpcplugin.SinkDef{
   		"crm": {Factory: newCRM(*region, os.Getenv("CRM_TOKEN")), Options: CRMOptions{}, Description: "..."},
   	},
   })
   ```

4. **List the binary under `plugins:`** ([hikyaku.yaml](hikyaku.yaml)). hikyaku passes the binary the `args` and `env` you list. `sha256` pins the binary you built, so hikyaku refuses to start any other.

```yaml
plugins:
  - name: crm
    command: ./bin/hikyaku-crm
    args: ["--region", "eu"]
    env: { CRM_TOKEN: "{{secrets.CRM_TOKEN}}" }
destinations:
  - name: leads
    type: crm
    options: { pipeline: inbound }
    retry: { attempts: 5, backoff: 200ms, max_backoff: 5s, respect_retry_after: true }
```

Each lead is a JSON object with an `email`. The plugin posts it to `<base_url>/pipelines/<pipeline>/leads` with the bearer token from `CRM_TOKEN`. It sends the message ID as the `Idempotency-Key`, so a retry does not make a second lead. Failures are handled like this:

- A `429` or a `5xx` is retried, after the CRM's `Retry-After` when it sends one.
- Another `4xx` is permanent.
- A message that is not a lead is permanent, with the tag `crm:invalid`. It goes straight to the DLQ (the dead-letter queue, where a message goes when every delivery attempt failed).

## Try it

Run this from this directory, with a hikyaku binary on your `PATH`. You need the full build: `hikyaku-lite` has no out-of-process plugins.

```sh
go build -o bin/hikyaku-crm .
export CRM_TOKEN=dev-token
hikyaku run -f hikyaku.llmfake.yaml
```

In `hikyaku.llmfake.yaml`, the CRM's API is an http source of the same relay, on `127.0.0.1:8082`, which checks the plugin's bearer token. Post a lead, then a message that is not one:

```sh
curl -s localhost:8080/leads -d '{"email":"ada@example.com","name":"Ada"}'   # 200: the CRM has it
cat out/crm.ndjson
curl -s localhost:8080/leads -d '{"name":"Bob"}'                             # 202: dead-lettered
cat out/dlq.ndjson
```

`hikyaku.yaml` is the same relay against a real CRM's API. Pin the binary you built with `sha256:` (`shasum -a 256 bin/hikyaku-crm`), so hikyaku refuses to start any other.

Outside a clone of this repository, drop the `replace` lines of [go.mod](go.mod) and run `go get github.com/xavidop/hikyaku/grpcplugin@latest`.

## Tests

`go test ./...` covers the plugin's error classes and options against a test server. `TestCRMPluginExample`, in `cmd/hikyaku/app`, builds this binary and runs the example through the CLI: `validate` with the options schema and the sha256 pin, then a run where a lead reaches the CRM and a message that is not one is dead-lettered.
