# grpcplugin

Out-of-process plugins for hikyaku. A plugin is a separate binary that hikyaku starts and talks to over gRPC. Use one to add a source or destination type without rebuilding hikyaku, or to write that type in another language.

You write a plugin in Go with `Serve`, or in [your own language](#writing-a-plugin-in-your-own-language) against the protocol. Each binary declares the source and destination types it serves. The spec then uses them like any other type, with their settings under `options:`, exactly as with an in-process Go plugin ([examples/custom-destination](../examples/custom-destination)).

The transport is [hashicorp/go-plugin](https://github.com/hashicorp/go-plugin): gRPC over a Unix socket. Each launch negotiates mutual TLS (AutoMTLS): hikyaku and the plugin each make a certificate and check the other's, so nothing else on the machine can talk to either side. The handshake uses the magic cookie `HIKYAKU_PLUGIN` (an environment variable that tells the binary hikyaku started it) and protocol version 1. The protocol is [`proto/hikyaku/plugin/v1/plugin.proto`](proto/hikyaku/plugin/v1/plugin.proto); its Go code is in [`pluginpb`](pluginpb).

```yaml
secrets:
  ECHO_TOKEN: env:ECHO_TOKEN
plugins:
  - name: echo
    command: ./bin/hikyaku-plugin-echo   # relative to this spec file, or a name on PATH
    args: [--verbose]
    env: { ECHO_TOKEN: "{{secrets.ECHO_TOKEN}}" }
    sha256: 3f0c...e9                     # refuse any other binary
sources:
  - { name: ticks, type: ticker, options: { interval: 1s } }
destinations:
  - name: out
    type: echo
    options: { file: out/echo.ndjson }
    retry: { attempts: 5 }
routes:
  - { name: r, from: [ticks], to: [out] }
```

## Spec keys

The `plugins:` list names the binaries hikyaku starts (`config.PluginBinary`). Each entry takes these keys:

| Key | |
|---|---|
| `name` | Required. The plugin's name in logs (`plugin=<name>`) and errors. Unique. |
| `command` | Required. Either a path or a bare name. A path is anything with a `/`; a relative one is resolved from the spec file's directory. A bare name is looked up on `PATH`. Write `./name` for a binary next to the spec. It must be an executable file. |
| `args` | Arguments passed to the binary. |
| `env` | Environment variables set for the plugin, on top of a minimal environment (below). Values may use `{{secrets.X}}` and `{{env.X}}`. |
| `sha256` | Hex SHA-256 of the binary. When set, hikyaku refuses to start a binary that does not match, at load and on every relaunch. |

The plugin's environment is **minimal**. hikyaku passes only:

- `PATH`, `HOME`, `USER`, `LOGNAME`, `TMPDIR`, `TZ`, `LANG`, `LC_ALL`, `LC_CTYPE`, `LC_MESSAGES`, `SSL_CERT_FILE` and `SSL_CERT_DIR`, when hikyaku has them;
- on Windows, also `SystemRoot`, `WINDIR`, `TEMP`, `TMP`, `USERPROFILE`, `APPDATA`, `LOCALAPPDATA`, `ComSpec` and `PATHEXT`;
- `HIKYAKU_PLUGIN_LOG_LEVEL`: hikyaku's log level, so the plugin does not format lines that would be dropped;
- then the entry's `env:`.

Nothing else of hikyaku's environment gets through. So hikyaku's secrets never reach a plugin unless the spec hands them over. The plugin runs in hikyaku's working directory, so relative paths in `options:` mean the same as they would in process.

Every type a plugin declares can then be a source or destination `type:` in the spec. `hikyaku validate` checks its `options:` block against the JSON Schema the plugin declares, and `hikyaku schema` describes it. The other keys of the entry work as for any type. For destinations, that is `retry`, `fallback`, `circuit_breaker`, groups and the DLQ. For sources, the keys below apply. To settle a message is to tell the plugin the message's final outcome, so the plugin can forget it or send it again.

| Source key | For a plugin source |
|---|---|
| `ack` | `delivered` (default) settles each message in the plugin once the relay is done with it; `accepted` settles it once the relay holds it. Passed to the plugin when the source opens. |
| `max_body_bytes` | Larger messages are logged, dropped and settled as final, so they do not come back forever. Default 10MiB. |
| `reconnect` | How the stream restarts when the plugin process dies: `backoff` (default 500ms, doubling), `max_backoff` (30s), `jitter` (`full`), `max_attempts` (0: forever). |

A type name must be unique. One that clashes with a built-in type, with a type the binary registers in process (a Go plugin), or with another plugin's type is an error, and the error names both.

## Wiring it in (the CLI)

This is the sequence the CLI runs to load a spec's plugins:

```go
raw, _ := config.Load(ctx, files, config.LoadOptions{SkipValidate: true, ...}) // read plugins:
set, err := grpcplugin.LoadAll(ctx, raw.Spec.Plugins, specDir, logger)       // launch, describe, check clashes
defer set.Close()                                                             // after the engine has closed
set.Register()                                                                // plugin.RegisterSource/RegisterSink
x := extensions()                                                             // the in-process Go plugin types
maps.Copy(x.Sources, set.Extensions().Sources)                                // plus the plugins', with schema checks
maps.Copy(x.Sinks, set.Extensions().Sinks)
loaded, err := config.Load(ctx, files, config.LoadOptions{Extensions: x, ...}) // validate with them
```

`Set.Extensions()` gives each type a `config.ExtType`. Its `Check` validates the `options:` block against the plugin's JSON Schema. Its `Schema` carries that schema for `config.SchemaWith`. `TestRelayEndToEnd` runs this sequence against the real engine.

## Semantics

This section says what happens to each call, each message and the plugin process, step by step.

**Sinks.** Each destination (or group member) of a plugin type opens a handle in the plugin (`OpenSink`, with the `options:` block as JSON). The factory fails when the plugin rejects the options. `Deliver` makes one attempt, with hikyaku's deadline as the call's deadline. Retries, timeouts, breakers, fallbacks and the DLQ stay in hikyaku. A sink that batches natively (`plugin.BatchSink` in Go) is told so when it opens, and its proxy implements `plugin.BatchSink` (`DeliverBatch`). Otherwise the engine combines batches itself. A plugin that says it batches and then answers `UNIMPLEMENTED` gets its batches one message at a time; the batch fails as a whole on the first failure. `Health` calls `SinkHealth`. `Close` calls `CloseSink`, which flushes.

**Sources.** `Start` opens a `StartSource` stream and turns each message into an envelope. The host sets `Source` to the spec's source name (routes match on it), and fills in a missing ID, protocol (the type) and receive time. Each message is settled in the plugin with the relay's outcome (`Settle`, once per message): `final: true` when the plugin may forget it (delivered, dead-lettered, unrouted or filtered; with `ack: accepted`, queued), `final: false` when it failed and should be redelivered. `Start` returns nil when ctx ends or the plugin's source runs out, and an error when the plugin's source cannot go on. `Close` (`plugin.Closer`, called once the relay has settled what is in flight) calls `CloseSource`. The plugin treats anything still unsettled as failed.

**Plugin crashes.** A call in flight when the process dies fails as transient with the tag `connect`, and the plugin is marked down. The next call relaunches it, at once the first time. While relaunches fail, or the relaunched process dies before any call works, the wait between relaunches doubles from 250ms up to 30s. Calls made during that wait fail at once, as transient `connect` with `RetryAfter` set to the wait. The handles are re-opened in the new process without the caller noticing. A source's stream restarts the same way, with its `reconnect:` backoff. Messages the dead process sent are not settled; its upstream redelivers them.

**Closing.** `Set.Close` (or `Plugin.Close`) first closes whatever is still open in each plugin, so it can flush. Then it stops the process: gracefully first, and killed two seconds later.

**Logs.** hikyaku reads the plugin's stderr. [hclog](https://github.com/hashicorp/go-hclog) JSON lines (`@level`, `@message`, `@timestamp` as `2006-01-02T15:04:05.000000Z07:00`, other keys as attributes) are logged with their level and attributes, plus `plugin=<name>`. Other lines are logged at debug, or at the level of a `[INFO]`-style prefix. Once a Go plugin serves, what it writes to `os.Stdout` and `os.Stderr` is logged line by line at info, with `stream=stdout|stderr`. Records go through hikyaku's logger, so its redaction applies.

## Errors

A sink failure crosses the wire inside the response, not as a gRPC status. So its class survives: `Error{class, retry_after_ms, tags, message}` becomes a `*plugin.Error` with the same `Class`, `RetryAfter` and `Tags`. `retry_on` and `never_on` then match the plugin's own tags (`5xx`, `503`, `<protocol>:<code>`, and so on). The Go SDK classifies an error without a class the way hikyaku does in process (`plugin.Classify`, `plugin.Tags`): a deadline is a timeout, and a refused connection is transient with `connect`.

When the call itself fails, hikyaku classifies it:

| What happened | Class | Tags |
|---|---|---|
| The plugin process died, or is down and relaunching | Transient | `connect` |
| The handle is unknown in the plugin (re-opened by the next call) | Transient | `connect` |
| hikyaku's deadline passed | Timeout | `timeout` |
| A message larger than the channel (64MiB) | Permanent | |
| A panic in the plugin's code (the plugin keeps running) | Transient | |

## Writing a plugin in Go

Use the Go SDK when you can write your plugin in Go. `Serve` handles the handshake, the transport and the protocol. Your sink and source code stays the same as in process.

```go
func main() {
	grpcplugin.Serve(grpcplugin.Plugins{
		Sinks: map[string]grpcplugin.SinkDef{
			"echo": {Factory: newEcho, Options: EchoOptions{}, Description: "..."},
		},
		Sources: map[string]grpcplugin.SourceDef{
			"ticker": {Factory: newTicker, Options: TickerOptions{}, Description: "..."},
		},
	})
}
```

The factories are ordinary `plugin.Factory[plugin.Sink]` and `plugin.Factory[plugin.Source]`. The sinks and sources are ordinary `plugin.Sink` and `plugin.Source` (plus `plugin.BatchSink` and `plugin.Closer`). The same code runs in process. In a factory:

- `spec.Decode(&opts)` decodes the `options:` block strictly. `spec.Options` holds the block as YAML would decode it (whole numbers are ints).
- hikyaku validates `options:` with a JSON Schema generated from `Options` (a zero value of the options struct). The schema uses the `yaml` names, the `jsonschema` tags (`required`, `enum=...`, `minimum=...`, `pattern=...`) and `jsonschema_description`. Unknown keys are rejected. A nil `Options` accepts any block.
- `spec.Env.Logger` writes hclog JSON lines to stderr. `spec.Env.Relay` is the relay's name. There are no stores across the process boundary: `spec.Env.Store` is nil.
- `spec.Config` is a `*grpcplugin.Remote`. A source settles its messages with `plugin.OnSettle(env, grpcplugin.AckMode(spec), settle)`. `AckMode` reads a spec entry in process too. A final settle runs the envelope's `OnQueued` and `OnAck` hooks (with `Delivered`). A failed settle runs `OnAck` with `Failed`.
- The factory's ctx carries hikyaku's deadline and ends when the open call returns. Start long-lived work in `Start`, or on first use.

`Serve` does three more things. It gives each message an ID if it has none. It recovers panics in plugin code: the call fails, and the stack goes to the plugin's log. And it closes what is still open when hikyaku stops the plugin. See [example/hikyaku-plugin-echo](example/hikyaku-plugin-echo).

A complete plugin is [examples/crm-plugin](../examples/crm-plugin): a `crm` destination in a module of its own, with a relay that runs it. [examples/postgres-plugin](../examples/postgres-plugin) serves a destination and a source (a transactional outbox) of the same name, `postgres`, and maps SQLSTATE codes onto error classes.

## Writing a plugin in your own language

hikyaku ships an SDK for Go only. To build a plugin in another language, implement the protocol yourself. The service is [`proto/hikyaku/plugin/v1/plugin.proto`](proto/hikyaku/plugin/v1/plugin.proto). It is carried over go-plugin's [non-Go plugin protocol](https://github.com/hashicorp/go-plugin/blob/main/docs/guide-plugin-write-non-go.md). Your binary must do five things:

1. Refuse to run unless the environment variable `HIKYAKU_PLUGIN` is `hikyaku.plugin.v1.9f4c2b7e` (`grpcplugin.Handshake`). That is the magic cookie: it tells the binary that hikyaku started it.
2. Serve gRPC on a Unix socket (or TCP on localhost) with TLS. Take hikyaku's client certificate from the environment variable `PLUGIN_CLIENT_CERT`. Make a self-signed server certificate. Require the client's certificate on every connection (this is AutoMTLS). hikyaku's client certificate is ECDSA P-521, which some TLS stacks cannot verify (gRPC's BoringSSL build in Python, for one). If yours cannot, terminate TLS with one that can.
3. Print one line on stdout: `1|1|unix|<socket path>|grpc|<base64 DER server certificate, no padding>`. Then write nothing else to stdout.
4. Serve `grpc.health.v1.Health` (answer `SERVING` for the service `plugin`) and `hikyaku.plugin.v1.Plugin`. `plugin.GRPCController/Shutdown` and `plugin.GRPCStdio` are optional. Without them, hikyaku kills the process on close, and does not sync stdio.
5. Log hclog JSON lines on stderr. Read `HIKYAKU_PLUGIN_LOG_LEVEL` to learn which level hikyaku keeps.

In the service:

- Declare each kind with a role and, ideally, a JSON Schema.
- Answer an unknown handle with `NOT_FOUND`.
- Report sink and source failures in the response's `Error`.
- Give each streamed message an `id`. hikyaku settles it by that ID.
- End a source stream with `OK` when the source runs out, and with `ABORTED` (and the reason) when it cannot go on.
- Answer `DeliverBatch` with `UNIMPLEMENTED` when a sink does not batch, and do not set `batch` when opening that sink.

## Regenerating `pluginpb`

```sh
cd grpcplugin
go generate ./pluginpb
```

This runs `protoc` with `protoc-gen-go` and `protoc-gen-go-grpc`. The committed files were made with protoc 36.2, protoc-gen-go v1.36.12 and protoc-gen-go-grpc 1.6.2. Install the tools with `brew install protobuf` (or a release from github.com/protocolbuffers/protobuf), `go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12` and `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2`, the same versions as the committed files, so a regeneration changes only what you meant to change. Commit the generated files.

## Tests

`go test ./...` builds the example plugin and runs it as a real process. It covers: launch and describe; sha256 match and mismatch; the minimal environment; registration and clashes; round trips; error classes; batches; settling (delivered and accepted); the deadline; a killed process (in flight and idle) relaunched; a refused relaunch and its backoff; a source stream surviving a kill; option validation; log forwarding; closing; a relay end to end through the engine; and no process left running at the end. The rest of the tests run a plugin server in process over an in-memory connection.
