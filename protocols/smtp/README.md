# protocols/smtp

The `smtp` destination sends each message as one email, through an SMTP server. It is the plain way to get an alert ([`notify`](https://xavidop.github.io/hikyaku/docs/resilience/#alerts)) or a report to people.

```yaml
destinations:
  - name: ops-mail
    type: smtp
    url: smtp://smtp.example.com          # STARTTLS, port 587
    username: relay@example.com
    password: "{{secrets.SMTP_PASSWORD}}"
    from: "hikyaku <relay@example.com>"
    to: [ops@example.com]
    subject: "{{ .json.event }} on {{ .json.destination }}"
    body: |
      {{ .json.event }} on {{ .json.destination }}, {{ .json.count }} time(s).
      {{ .json.error }}

notify:
  - to: [ops-mail]
```

It is in `hikyaku` and in `hikyaku-lite`. There is no email source.

## Keys

| Key | Meaning |
|---|---|
| `url` | The server. The scheme picks the transport, see below. Required. |
| `username`, `password` | The account, when the server asks for one. They go together. Use `{{secrets.NAME}}` for the password. |
| `from` | The sender: `relay@example.com` or `Name <relay@example.com>`. Required. |
| `to` | The recipients. Required. Each entry is a per-message template that renders one address, or several separated by commas. An entry that renders empty is skipped. |
| `subject` | Per-message template of the subject. Default `Message from hikyaku (<source>)`. |
| `body` | Per-message template of the email's text. Default: the message body. |
| `headers` | More email headers, such as `Reply-To`. Values may be per-message templates. `From`, `To`, `Cc`, `Bcc`, `Subject`, `Date`, `Message-ID` and the MIME headers are set by the destination. |
| `tls` | `ca_file`, `cert_file` + `key_file`, `server_name`, `insecure_skip_verify`. |

| URL | Transport | Default port |
|---|---|---|
| `smtps://host` | TLS from the first byte | 465 |
| `smtp://host` | Upgrades with STARTTLS. A server that does not offer it is refused, so a password never goes out in clear. | 587 |
| `smtp+plain://host` | No TLS at all. For a mail relay on the same host or network (a sidecar, Mailpit). | 25 |

`timeout`, `retry`, the circuit breaker, `rate_limit`, `fallback` and `batch` work as for every destination. Most mail providers limit how fast you may send: set `rate_limit`.

## The email

- The email is `text/plain` in UTF-8. A message whose content type is `text/html` is sent as `text/html`.
- A JSON message is sent as its JSON text. Use `body`, or a `template` transform, to write something a person reads.
- All recipients are in the `To` header. There is no `Cc` or `Bcc`.
- The `Message-ID` is `<message ID@the domain of from>`, so a retry of the same message has the same one.
- A line break in a subject or a header value becomes a space: a message cannot add headers or recipients of its own.
- With `batch`, one email carries the batch: its messages as NDJSON, one per line.

## Signing in

With `username`, the destination signs in with `PLAIN`, or with `LOGIN` for a server that offers only that (Microsoft 365). It does not send a password over a connection without TLS, unless the server is `localhost`. OAuth sign-in (`XOAUTH2`) is not supported: use an app password or an SMTP relay.

## Delivery

Each delivery opens a connection, sends one email and closes. A success means the server took the email for every recipient. It does not mean the email reached an inbox: a bounce comes back later, to the `from` address, and hikyaku does not see it.

When the server refuses one recipient, nothing is sent to the others, and the delivery fails.

## Errors

| Failure | Class | Tags |
|---|---|---|
| a `4yz` reply (`421`, `450`, `451`, `452`: busy, greylisting, try later) | transient: retry | `smtp:<code>` |
| a `5yz` reply (`550` no such mailbox, `552` too large, `554` refused) | permanent | `smtp:<code>` |
| the server refused the user name or password | permanent | `smtp:auth`, `smtp:535` |
| `smtp://` to a server without STARTTLS | permanent | `smtp:starttls` |
| a TLS handshake that failed (an untrusted certificate) | transient | `connect`, `smtp:tls` |
| a rendered `to` that is not an email address, or no recipient at all | permanent | `smtp:address` |
| no connection | transient | `connect` |
| the attempt's `timeout` ran out (30s when the destination sets none) | timeout | `timeout` |

A code that is transient for you but listed nowhere retries by its class. To retry on one `5yz` code, list it: `retry_on: ["smtp:552"]`.

## Health

The health check opens a TCP connection to the server. It sends nothing.
