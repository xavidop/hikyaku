# protocols/file

The local stream protocols for hikyaku: the `stdin` and `file` source types, and the `stdout`, `stderr` and `file` destination types. They read and write one message per line (NDJSON, that is, one JSON document per line). Use them for local runs, for piping, and for archives.

```go
import "github.com/xavidop/hikyaku/protocols/file"

file.Register() // registers stdin and file as sources, and stdout, stderr and file as destinations
```

## Sources

Use `stdin` to feed a relay from a pipe, and `file` to read a file on disk. These sources take no `mode`.

- **stdin** reads the process's standard input until EOF (the end of the input).
- **file** reads the file at `path` from its start. Without `follow`, it stops at the end of the file. With `follow: true`, it keeps reading the lines appended to the file, checking every 250ms. When the file at `path` is smaller than what was read, the file was truncated or replaced by a new one. The source then starts over from the top.

Every non-blank line is one message. A line ends at `\n`, and a trailing `\r` is dropped. The longest line is 16MiB.

`format` says what a line is:

| `format` | The line is |
|---|---|
| `body` (default) | the message body |
| `envelope` | a message in wire form, as a `file` destination with `format: envelope` writes it, and as the file DLQ holds it. Its ID, subject, headers, content type, body, attributes and trace come back. Its source becomes this source's name. A line without an `id` is an error. |

With `format: body`, every line becomes this envelope:

| Field | Value |
|---|---|
| `Subject` | `path` (file) or empty (stdin) |
| `ContentType` | `application/json` when the line is valid JSON, else `text/plain; charset=utf-8` |
| `Headers` | none |
| `Attributes` | `line`: the line number, from 1 |

A source reads its next line only once the relay took the last one. So backpressure (the relay stops reading until it catches up) holds it back. There is nothing to acknowledge (no one to tell that a message is done), and no position is saved: a restart reads a file from the top again. `dedupe` can drop the lines a restart reads twice.

When every source of a relay has ended, `hikyaku run` finishes the messages in flight and exits. A source has ended when stdin reached EOF, or when a file without `follow` reached its end. Three things fail the source, and the relay stops: a line over 16MiB, a read error, or a line that is not an envelope with `format: envelope`. The source's health is its last error.

| Key | Source | Meaning |
|---|---|---|
| `path` | file | The file read (required). |
| `format` | both | `body` (default) or `envelope`. |
| `follow` | file | Keep reading the lines appended to the file (default `false`). |

## Destinations

Use `stdout` and `stderr` to print messages, and `file` to append them to a file. Each destination writes one line per message:

- **stdout** and **stderr** write to the process's standard output and standard error.
- **file** appends to the file at `path`. It creates the file (mode `0644`) and its directory (mode `0755`) when they do not exist. It keeps the file open until shutdown.

`format` says what the line is:

| `format` | The line is |
|---|---|
| `envelope` (default) | the message in wire form: `id`, `source`, `protocol`, `subject`, `headers`, `content_type`, the body, `attributes`, `received_at`, `trace_id` and `span_id`. A body that is compact JSON is embedded as `body`, any other UTF-8 body is written as `body_text`, and anything else as `body_base64`, so the body comes back byte for byte. |
| `body` | the body, without its trailing newlines, then one `\n` |

Each line is written whole, in one write, under a lock. So concurrent deliveries never interleave. Lines are not synced to disk one by one.

| Key | Destination | Meaning |
|---|---|---|
| `path` | file | The file appended to (required). |
| `format` | all three | `envelope` (default) or `body`. |

Each delivery is one attempt, because the relay owns retries. A failed attempt is an error with a class and tags. The class says whether hikyaku retries it by default. The tags are what `retry_on` and `never_on` match:

| Failure | Class | Tags |
|---|---|---|
| The write failed (a full disk, a closed file) | transient | `io` |
| The message could not be encoded | permanent | |

The default policy (`retry_on: [connect, timeout, 5xx, 429]`) names neither tag. So each error falls back to its class: a write that failed is retried, and an encoding error is not. List `io` in `never_on` to stop retrying writes.

## Example

```yaml
sources:
  - name: input
    type: stdin
    format: body

  - name: inbox
    type: file
    path: ./in/events.ndjson
    follow: true

destinations:
  - name: console
    type: stdout
    format: body

  - name: archive
    type: file
    path: ./out/archive.ndjson
```

With `stdin` as its only source, a relay works in a pipe and exits at the end of its input:

```sh
cat events.ndjson | hikyaku run -f pipe.yaml
```

A file written with `format: envelope` can be read back by another relay's `file` or `stdin` source with `format: envelope`. IDs and headers come back with it.
