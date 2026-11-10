# hikyaku-plugin-echo

An out-of-process hikyaku plugin, written with the Go SDK in [grpcplugin](../../README.md). hikyaku starts it and talks gRPC to it. The grpcplugin tests run it, and it shows how to write a plugin of your own. It adds two types:

- **`echo`** (destination): appends each message body to a file, one line per message, and replies with the body (status 200, with the message's content type). It batches natively: a batch is one write.
- **`ticker`** (source): emits `{"n":0}`, `{"n":1}`, ... every `interval`. Each message stays pending until hikyaku settles it, that is, tells the plugin the message's final outcome. Once the message is delivered (or dead-lettered, unrouted or filtered), the ticker forgets it. When it failed, the ticker emits it again, with the same ID and body, before any new message.

The plugin's code is ordinary `plugin.Sink` and `plugin.Source` code ([echo.go](echo.go), [ticker.go](ticker.go)). [main.go](main.go) serves it with `grpcplugin.Serve`.

## Try it

Build the plugin, then run hikyaku with a spec that lists it:

```sh
cd grpcplugin
go build -o example/hikyaku-plugin-echo/hikyaku-plugin-echo ./example/hikyaku-plugin-echo
cd example/hikyaku-plugin-echo
shasum -a 256 hikyaku-plugin-echo   # for sha256: below (optional)
```

```yaml
version: 1
name: echo-demo
plugins:
  - name: echo
    command: ./hikyaku-plugin-echo
sources:
  - { name: ticks, type: ticker, options: { interval: 1s, count: 5 } }
destinations:
  - { name: out, type: echo, options: { file: out/echo.ndjson } }
routes:
  - { name: r, from: [ticks], to: [out] }
```

If you run the binary by hand, it only prints that it is a plugin. hikyaku is what starts it.

## Options

Each type's settings go under `options:` in the spec.

`echo`:

| Key | |
|---|---|
| `file` | Required. File each body is appended to (created if missing; its directory must exist). |
| `test_hooks` | Honour the `X-Echo-*` request headers below. Off by default. |

`ticker`:

| Key | |
|---|---|
| `interval` | Time between messages, a Go duration (`250ms`, `1s`). Default `1s`. |
| `count` | Emit this many messages, then end once every one is settled (the source runs out). 0, the default, runs until hikyaku stops. |
| `settle_log` | File each settle is appended to as `<n> <id> <final>`. |

Each ticker message has: the subject set to the source name; the attribute `ordering_key`, also the source name; the attribute `ticker.n`; and the content type `application/json`.

## Test hooks

Test hooks let a test make a delivery fail on purpose, or report what the plugin saw. Turn them on with `test_hooks: true`. The headers below, set on a message, then control its delivery. The grpcplugin tests drive the plugin through them.

| Header | |
|---|---|
| `X-Echo-Fail` | Fail instead of writing, with the class `transient`, `rate_limited`, `permanent` or `timeout`. |
| `X-Echo-Retry-After` | The failure's retry-after hint, a duration. |
| `X-Echo-Tags` | The failure's tags, comma-separated (`5xx,503`). |
| `X-Echo-Sleep` | After writing, sleep this long, or until the delivery's deadline (then fail as a timeout). |
| `X-Echo-Env` | Report this environment variable in `X-Echo-Env-Value` and `X-Echo-Env-Set`. |

With test hooks on, every reply also carries three headers: `X-Echo-Envelope` (the message as the plugin received it, in hikyaku's JSON wire form), `X-Echo-Deadline-Ms` (the time left before the deadline, when there is one) and `X-Echo-Pid` (the plugin's process ID).
