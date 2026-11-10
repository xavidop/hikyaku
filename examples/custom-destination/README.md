# custom-destination

This example builds hikyaku with two types of its own: the `chat` destination, which posts each message as a text to a chat webhook, and the `inbox` source, which sends the files dropped in a directory ([below](#a-custom-source)). Follow it to compile your own destinations, or sources, into the binary. The steps:

1. **Implement `plugin.Sink`** ([chat.go](chat.go)). `Deliver` makes one attempt. When it fails, it returns a classified error, one that says how it failed: `plugin.StatusError(code, retryAfter, msg)` for an HTTP answer, `plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)` for a connection that failed, or `plugin.Permanentf(...)` for a failure no retry can fix. The engine does the rest:
   - retries with backoff and jitter, honoring `Retry-After`;
   - the circuit breaker and the retry budget;
   - groups and hedging (when the first member of a group is slow, a second one races it);
   - destination fallbacks and the DLQ (the dead-letter queue, where a message goes when every delivery attempt failed);
   - metrics and traces.
2. **Read your settings from `options:`.** `spec.Decode(&opts)` decodes the destination's `options:` block strictly into your struct. An unknown key is an error, so typos are caught.
3. **Register the type and run the CLI** ([main.go](main.go)):

   ```go
   plugin.RegisterSink("chat", newChat, plugin.WithOptions(ChatOptions{}))
   os.Exit(app.Main(os.Args[1:]))
   ```

   `WithOptions` lets `hikyaku validate` check every `options:` block, and lets `hikyaku schema` describe your keys for editor completion.

The destination then goes in the spec like a built-in one:

```yaml
destinations:
  - name: chat
    type: chat
    options:
      webhook_url: "{{secrets.CHAT_WEBHOOK_URL}}"
      text: "{{ .json.severity }}: {{ .json.summary }}"
    retry: { attempts: 5, backoff: 200ms }
    fallback: [backlog]
```

## A custom source

[inbox.go](inbox.go) adds a source type the same way: `inbox`, which turns the files dropped in a directory into messages, one per file.

1. **Implement `plugin.Source`.** `Start` sends envelopes on its channel until its context ends (then it returns `nil`). It returns an error when it cannot go on. `Health` says whether it can run.
2. **Settle each message.** `plugin.OnSettle(env, "delivered", settle)` hands the source the relay's outcome of each message. The outcome is final once the relay is done with the message (delivered, dead-lettered, unrouted or filtered), and not final when the message failed. The inbox removes a file on a final settle, whatever the source's `ack:` says. A failed one stays, and is sent again with the same ID, its idempotency key, so a destination that already has it can drop the copy.
3. **Register it** next to the destination ([main.go](main.go)). Its options are decoded by `spec.Decode` and checked by `hikyaku validate`, like the destination's:

   ```go
   plugin.RegisterSource("inbox", newInbox, plugin.WithOptions(InboxOptions{}))
   ```

The source then goes in the spec:

```yaml
sources:
  - name: drops
    type: inbox # registered by main.go
    options: { dir: ./inbox, interval: 1s }
routes:
  - { name: notify, from: [alerts, drops], to: [chat] }
```

| Key | |
|---|---|
| `dir` | Required. The directory to watch, created if missing. |
| `pattern` | Glob of the file names to take. Default `*.json`. |
| `interval` | How often the directory is listed, a duration. Default `1s`. |

Each message has the file's content as its body, the file name as its subject and as its `inbox.file` attribute, and the content type `application/json` for a `.json` file. Write a file under another name, then rename it into the directory, so the source never reads half of one. A file the relay has not settled stays in the directory, so a restart sends it again.

With the offline spec running (below), drop an alert in the inbox:

```sh
echo '{"severity":"low","summary":"backup done"}' > inbox/.backup.tmp
mv inbox/.backup.tmp inbox/backup.json
cat out/chat.ndjson   # ... {"text":"low: backup done","username":"hikyaku"}
```

## Try it

Build the binary, validate the offline spec, run it, and post an alert:

```sh
go build -o hikyaku-chat .
./hikyaku-chat validate -f hikyaku.llmfake.yaml
./hikyaku-chat run -f hikyaku.llmfake.yaml &
curl -s localhost:8080/alerts -d '{"severity":"high","summary":"disk full on db-1"}'
cat out/chat.ndjson   # {"text":"high: disk full on db-1","username":"hikyaku"}
```

In `hikyaku.llmfake.yaml`, the chat webhook is an HTTP listen source of the same relay, so everything runs offline.

## Build your own

Outside this repository, your build is an ordinary Go module that requires a hikyaku release. Each release tags every hikyaku module at the same version, so `go get` brings the CLI, `plugin` and the protocols of that release. In a directory with your `chat.go` and `main.go`:

```sh
go mod init example.com/hikyaku-chat
go get github.com/xavidop/hikyaku/cmd/hikyaku@vX.Y.Z
go mod tidy
go build -o hikyaku-chat .
./hikyaku-chat validate -f hikyaku.yaml
```

To work on hikyaku and your type together, build inside a clone instead. Copy this example to a directory of its own (`examples/my-destination`, say). Change the `module` line of its [go.mod](go.mod), and keep the `replace` lines, which point at the modules of the checkout. Add the directory to `go.work` (`go work use ./examples/my-destination`), and run `go build` in it.

To add a type without rebuilding hikyaku, or in another language, write an out-of-process plugin instead: a binary the spec lists under `plugins:`, which hikyaku starts and talks gRPC to. See [grpcplugin](../../grpcplugin/README.md) and its echo example.
