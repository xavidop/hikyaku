---
layout: ../../layouts/DocsLayout.astro
title: Plugins
---

# Plugins

This page explains how to add your own source or destination type to hikyaku. Read it when no
built-in protocol fits: a company API, an internal queue, a file format of your own.

hikyaku's own protocols are plugins too. A source produces envelopes (the message as hikyaku
carries it), and a sink delivers one. Your own types work the same way, in two forms:

- **In process**: you compile your type into a hikyaku binary of your own, a custom build.
- **Out of process**: your type is a separate program. hikyaku starts it and talks to it over
  gRPC, a protocol for calling functions in another process. hikyaku itself is not rebuilt.

## In process: a custom build

A destination is a `plugin.Sink`. Its `Deliver` makes one delivery attempt; `Name`, `Health` and
`Close` complete the interface. Register it, then build your own binary around `app.Main`:

```go
func main() {
	plugin.RegisterSink("chat", newChat, plugin.WithOptions(ChatOptions{}))
	os.Exit(app.Main(os.Args[1:]))
}
```

Its settings go under `options:` in the spec (`type: chat`, `options: { webhook_url: ... }`).
`spec.Decode` decodes them strictly: an unknown key is an error. `hikyaku validate` and
`hikyaku schema` learn the options from `WithOptions`. Retries, breakers, budgets, groups,
hedging, fallbacks, batching and the DLQ come from the engine; your sink only makes one attempt.

A source is a `plugin.Source`, registered with `plugin.RegisterSource`. Its `Start` sends
envelopes on a channel until its context ends. `plugin.OnSettle` hands it the relay's outcome of
each one: final, once the relay is done with it, or failed, so the source can send it again.

```go
plugin.RegisterSource("inbox", newInbox, plugin.WithOptions(InboxOptions{}))
```

[`examples/custom-destination`](https://github.com/xavidop/hikyaku/tree/main/examples/custom-destination)
has both: the `chat` destination, and an `inbox` source. The source turns the files dropped in a
directory into messages, and removes each file once the relay is done with it.

The custom build is an ordinary Go module. Run `go mod init`, then
`go get github.com/xavidop/hikyaku/cmd/hikyaku@vX.Y.Z` and `go build`. Each release tags every
hikyaku module at the same version, so the build gets `app`, `plugin` and the protocols of that
release. To work on hikyaku itself at the same time, build inside a clone instead: copy
`examples/custom-destination` to a directory of its own, keep its `replace` lines, and add it to
`go.work`. That example's README walks through both ways.

## Out of process: a binary of its own

An out-of-process plugin is a program that hikyaku starts and talks gRPC to. Write it in Go with
`grpcplugin.Serve`, or in [your own language](#in-your-own-language) against the protocol.
hikyaku itself is not rebuilt: the same release binary runs any plugin a spec lists.

```yaml
plugins:
  - name: crm
    command: ./bin/hikyaku-crm                 # relative to the spec, or on PATH
    args: ["--region", "eu"]
    env: { CRM_TOKEN: "{{secrets.CRM_TOKEN}}" }
    sha256: "<hex>"                            # refuse any other binary
destinations:
  - { name: leads, type: crm, options: { pipeline: inbound } }
```

- **Validated like built-in types.** hikyaku starts the plugins before it validates the spec.
  Each plugin declares the kinds it serves, and a JSON Schema for their options (a description of
  the keys an object may have and the type of each one). So `validate` checks `options:` like a
  built-in type's keys.
- **The relay's behavior crosses the wire.** Error classes, retry-after hints and tags travel over
  gRPC, so the retry policy works the same. So do deadlines, native batches and source
  acknowledgements (a settle tells the source whether a message is final or not).
- **Supervised.** A plugin that crashes fails the call in flight as a transient error, one worth
  retrying. The next call starts the plugin again, with backoff (a growing wait between
  attempts).
- **Logs and lifetime.** A plugin's stderr goes to hikyaku's logs, with levels. A reload swaps the
  plugins once the old engine has stopped. Every command stops them when it ends.

Out-of-process plugins are in the full build; `hikyaku-lite` leaves them out.

### In Go

A Go plugin is the same `plugin.Sink` or `plugin.Source` as an in-process one, served with
`grpcplugin.Serve`. The options schema is generated from the options struct:

```go
func main() {
	region := flag.String("region", "eu", "The CRM's region.")
	flag.Parse()
	grpcplugin.Serve(grpcplugin.Plugins{
		Sinks: map[string]grpcplugin.SinkDef{
			"crm": {Factory: newCRM(*region, os.Getenv("CRM_TOKEN")), Options: CRMOptions{}, Description: "Posts each message as a lead to a CRM pipeline."},
		},
	})
}
```

The `crm` plugin above is
[`examples/crm-plugin`](https://github.com/xavidop/hikyaku/tree/main/examples/crm-plugin), a
module of its own. Build it with `go build -o bin/hikyaku-crm .`, then list the binary under
`plugins:`. `grpcplugin.Serve` does the handshake for you (the exchange at start-up in which the
plugin proves it speaks hikyaku's protocol). It is in
[`grpcplugin`](https://github.com/xavidop/hikyaku/tree/main/grpcplugin).

[`examples/postgres-plugin`](https://github.com/xavidop/hikyaku/tree/main/examples/postgres-plugin)
is a bigger one. It serves a destination and a source of the same name, `postgres`: a row per
message into a table, and a transactional outbox that hikyaku polls and settles. It shows how a
plugin maps its own error codes onto hikyaku's classes and tags.

### In your own language

hikyaku ships an SDK for Go only. To build a plugin in another language, implement the protocol
yourself: the [`hikyaku.plugin.v1` service in `plugin.proto`](https://github.com/xavidop/hikyaku/blob/main/grpcplugin/proto/hikyaku/plugin/v1/plugin.proto), served
behind [go-plugin's handshake](https://github.com/hashicorp/go-plugin/blob/main/docs/guide-plugin-write-non-go.md):

1. Refuse to run unless the `HIKYAKU_PLUGIN` environment variable is `hikyaku.plugin.v1.9f4c2b7e`.
2. Serve gRPC on a Unix socket (or TCP on localhost) with mutual TLS: each side proves who it is
   with a certificate (go-plugin calls this AutoMTLS). hikyaku's client certificate is in
   `PLUGIN_CLIENT_CERT`. Make a self-signed server certificate and require the client's. That
   client certificate is ECDSA P-521, which some TLS stacks cannot verify (gRPC's BoringSSL build
   in Python, for one).
3. Print `1|1|unix|<socket path>|grpc|<base64 DER server certificate, no padding>` on stdout,
   then keep stdout quiet.
4. Serve `grpc.health.v1.Health` (service `plugin`: `SERVING`) and `hikyaku.plugin.v1.Plugin`.
   `plugin.GRPCController/Shutdown` and `plugin.GRPCStdio` are optional. Without them, hikyaku
   kills the process on close and forwards no stdio.
5. Log hclog JSON lines on stderr, at the level in `HIKYAKU_PLUGIN_LOG_LEVEL`.

In the service, declare each kind with its role and, ideally, its options' JSON Schema. Report a
failure in the response's `Error`, with its class, tags and retry-after, so hikyaku retries it as
it would its own. Give each message a source streams an `id`: hikyaku settles it by that ID. The
details are in grpcplugin's
[README](https://github.com/xavidop/hikyaku/tree/main/grpcplugin#writing-a-plugin-in-your-own-language).

## Examples and reference

All in the [GitHub repository](https://github.com/xavidop/hikyaku):

| Where | What |
|---|---|
| [`examples/custom-destination`](https://github.com/xavidop/hikyaku/tree/main/examples/custom-destination) | In process: a custom build with a `chat` destination and an `inbox` source. |
| [`examples/crm-plugin`](https://github.com/xavidop/hikyaku/tree/main/examples/crm-plugin) | Out of process: a `crm` destination in a binary of its own, and the relay that runs it. |
| [`examples/postgres-plugin`](https://github.com/xavidop/hikyaku/tree/main/examples/postgres-plugin) | Out of process: PostgreSQL as a `postgres` destination (a row per message) and a `postgres` source (a transactional outbox), with settles and error classes. |
| [`grpcplugin/example/hikyaku-plugin-echo`](https://github.com/xavidop/hikyaku/tree/main/grpcplugin/example/hikyaku-plugin-echo) | Out of process: an `echo` destination and a `ticker` source, with batches and settles. |
| [`plugin`](https://github.com/xavidop/hikyaku/tree/main/plugin) | The interfaces (`Sink`, `Source`, `BatchSink`), the registry and the error classes and tags. |
| [`grpcplugin`](https://github.com/xavidop/hikyaku/tree/main/grpcplugin) | `grpcplugin.Serve`, the host that runs plugins, and how a plugin behaves: its rules, errors and tests. |
| [`plugin.proto`](https://github.com/xavidop/hikyaku/blob/main/grpcplugin/proto/hikyaku/plugin/v1/plugin.proto) | The `hikyaku.plugin.v1` protocol between hikyaku and a plugin: implement it to build a plugin [in your own language](#in-your-own-language). |
