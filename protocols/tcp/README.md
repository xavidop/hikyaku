# protocols/tcp

The raw socket protocol for hikyaku: the `tcp` and `udp` source and destination types. Use them for devices, syslog, statsd and other systems that send bytes over a socket with no HTTP in between. Standard library only, except DTLS, which uses [pion/dtls](https://github.com/pion/dtls) (pure Go).

```go
import "github.com/xavidop/hikyaku/protocols/tcp"

tcp.Register() // registers tcp and udp as sources and destinations
```

## Framing

Framing is how a stream of bytes is cut into messages. `frame` and `length_bytes` set it. Sources and destinations use the same rules, so a destination writes exactly what a source with the same framing reads.

| `frame` | One message is | Details |
|---|---|---|
| `newline` (default) | one line | A line ends at `\n` (in a udp datagram, also where the datagram ends). A trailing `\r` is stripped, and empty lines are skipped. |
| `length` | the bytes after a big-endian unsigned length of `length_bytes` bytes | `length_bytes` is 1, 2, 4 (default) or 8, and applies with `frame: length` only. A zero length carries no message. |
| `raw` | tcp: a connection's whole stream, until the client closes (or half-closes) it. udp: one datagram | An empty stream or datagram is no message. A stream the client resets instead of closing is no message either. |

A udp datagram may hold several `newline` or `length` frames. Over [DTLS](#dtls), a record is read the way a datagram is.

A tcp stream that ends inside a frame loses that frame: a last line without its `\n`, or a `length` frame without all its bytes. The loss is logged at warn. A sender cut off in the middle of a write leaves such a partial frame, and it must not pass for a whole message. So a client must end its last line with `\n` (`printf 'msg\n' | nc host 9000`, not `printf 'msg'`).

## Sources

A source receives bytes from clients and turns each frame into a message. Sources only listen: clients connect (tcp) or send datagrams (udp) to `listen`.

| Key | Meaning |
|---|---|
| `listen` | Address, such as `:9000` (required). |
| `frame`, `length_bytes` | Framing (see above). |
| `max_body_bytes` | Longest frame (default 1MiB). A longer frame is logged at warn with the client's `remote_addr`. tcp then closes the connection, because the stream cannot be split into frames again; udp drops the rest of the datagram. |
| `tls` | `cert_file` + `key_file`, and `client_ca_file` to require client certificates (mTLS, mutual TLS). TLS for tcp, [DTLS](#dtls) for udp. Any `tls` key turns it on, so a block without a certificate (`client_ca_file` alone, say) is refused at startup rather than leaving the port in plain text. `enabled: false` turns it off. A client must finish the handshake within 10s. |
| `auth.ip_allow` | IPs and CIDRs (IPv4 or IPv6) allowed to connect (tcp, checked before TLS) or to send (udp, checked before DTLS). An IPv4 client seen through an IPv6 socket (`::ffff:a.b.c.d`) matches IPv4 entries. Others are closed or dropped, and logged at debug. |

`auth.hmac`, `auth.bearer`, `auth.jwt` and `auth.api_key` are refused at startup, because a tcp or udp message carries no headers or signature. `mode` is `listen` (the default).

Each frame becomes one envelope:

| Field | Value |
|---|---|
| `Protocol` | `tcp` or `udp` |
| `Subject`, `Headers` | empty |
| `ContentType` | `application/json` when the frame parses as JSON, `text/plain; charset=utf-8` when it is valid UTF-8, `application/octet-stream` otherwise |
| `Attributes` | tcp: `tcp.remote_addr` (the client's `ip:port`) and `tcp.conn_id` (a ULID per connection). udp: `udp.remote_addr` (the sender's `ip:port`) |
| `ordering_key` attribute | tcp: the connection id. udp: the sender's address. With `ordering` and no `key`, one client's messages keep their order |
| `ID` | a new ULID |

### Acknowledgements, backpressure and loss

Neither protocol acknowledges messages: there is no way to tell the client that a message is done. So there is nothing to ack, nack or resume. `ack` does not apply, and `ack: accepted` is refused.

- **tcp holds clients back.** A connection reads its next frame only once the relay has taken the one before. While the relay's queue is full, the connection is not read, and TCP flow control slows the client down. This is backpressure. Nothing is dropped.
- **udp drops what the relay cannot take.** A sender cannot be held back, so a message that finds the relay's queue full is dropped. Each drop is counted (`Source.Dropped()`). A warning with `dropped_total` is logged at most once every 10s, so a flood does not flood the logs too.
- **Shutdown closes everything.** When the relay stops, the listener and every connection close. A frame read but not yet taken by the relay is kept if the queue has room. Otherwise it is lost and logged at warn. Bytes still in the socket buffers are lost, because tcp gives the client no acknowledgement to wait for.
- **Idle connections close** after 5 minutes without data. Time spent waiting for the relay does not count.
- **A partial frame is dropped** and logged at warn: one that ends a tcp stream (see [Framing](#framing)), or a `length` frame cut off by the end of a udp datagram. A udp datagram with a frame over `max_body_bytes` or a partial frame loses the rest of the datagram too.

## Destinations

A destination writes each message to a server as one frame.

| Key | Meaning |
|---|---|
| `url` | `tcp://host:port`, `tls://host:port` (TLS), `udp://host:port` or `dtls://host:port` ([DTLS](#dtls)) (required). It cannot be a per-message template: the connection is dialed before any message. A path, query or credentials are refused. |
| `frame`, `length_bytes` | Framing (see above). |
| `body` | Per-message template of what is written (default: the message body). |
| `tls` | Client TLS, DTLS for udp: `ca_file`, `cert_file` + `key_file` (mTLS), `server_name` (default: the URL's host), `insecure_skip_verify`. `tls://` (`dtls://` for udp) turns it on, and so does any of these keys on a `tcp://` (`udp://`) URL. `enabled: false` turns it off, and contradicts `tls://` or `dtls://`. |

`headers` are refused: only the body goes on the wire.

**Connections.** A tcp destination with `newline` or `length` framing keeps one persistent connection. It dials on the first delivery and redials on the delivery after a failure. Deliveries take turns on the connection, so frames never interleave. A delivery waits for its turn within its own timeout. The destination reads and discards what the peer sends. So when the peer closes the connection between deliveries, the next delivery dials a new one instead of writing into the closed one. A half-close (the peer ends its side but keeps reading) looks the same. So a server that half-closes every connection gets one connection per message. With `frame: raw`, each message gets a connection of its own: dialed, written, then closed, because the end of the stream ends the message. When the write fails, the connection is reset instead of closed, so the peer does not take the part written as a whole message. A udp destination sends one datagram per message. Over DTLS, it sends one record per message, on one association (see [DTLS](#dtls)).

**What is written.** `newline` appends `\n` unless the body already ends with one. The line ending (`\n` or `\r\n`) is not part of the message, so a body ending in one arrives without it. A body with a newline before its end would arrive as several messages. If it is JSON, it is compacted, because JSON newlines are only whitespace. Anything else fails (`<kind>:newline_in_body`): use `frame: length` or `raw` for multi-line text. `length` writes the prefix, then the body. An empty message fails (`<kind>:empty_body`), because no framing carries one: a source skips empty lines, frames and streams. With `newline`, a body that is only a line ending counts as empty.

**When it counts as delivered.** tcp has no acknowledgement, so a write the kernel accepted counts as delivered. A peer that dies right after it can still lose the message. A retry after a timeout can send it twice. udp cannot tell whether a datagram arrived, and neither can DTLS (see [DTLS](#dtls)). On some systems, a datagram sent to a closed port makes the *next* send fail with `udp:ECONNREFUSED`.

**mTLS over TLS 1.3.** With mTLS, the server checks the client's certificate too. TLS 1.3 completes the client's handshake before the server checks that certificate, so a refusal arrives right after the handshake. When the server asked for a client certificate, the destination waits up to 200ms after the handshake for that verdict. Without the wait, a message written into the doomed connection would count as delivered. The wait happens once per connection, so once per message with `frame: raw`. DTLS 1.3 works the same way, and a DTLS destination waits the same way.

Each delivery is one attempt, because the relay owns retries. A failed attempt is an error with a class and tags. The class says whether hikyaku retries it by default. The tags are what `retry_on` and `never_on` match (`<kind>` is `tcp` or `udp`):

| Failure | Class | Tags |
|---|---|---|
| The dial failed: refused, unreachable, DNS, the TLS or DTLS handshake, or a TLS 1.3 or DTLS 1.3 server that refused the client certificate | transient | `connect`, plus the socket error when there is one: `<kind>:ECONNREFUSED`, `<kind>:EHOSTUNREACH`, `<kind>:ENETUNREACH`, `<kind>:ETIMEDOUT` |
| A write failed on a broken connection (or a closed DTLS association), which is dropped | transient | `connect`, plus `<kind>:ECONNRESET` or `<kind>:EPIPE` when the system says so |
| The attempt's timeout ran out (dial, a handshake the server never answers, waiting for its turn, or a write to a peer that stopped reading) | timeout | `timeout` |
| A message longer than `length_bytes` can carry (255 bytes for 1, 65,535 for 2, ...), a udp datagram longer than the network carries, or a frame longer than one DTLS record carries (16,384 bytes) | permanent | `<kind>:frame_too_large` |
| With `frame: newline`, a body that is not JSON holds a newline | permanent | `<kind>:newline_in_body` |
| The message is empty | permanent | `<kind>:empty_body` |
| The `body` template failed | permanent | `template` |
| The destination is closed | transient | none |

The default policy (`retry_on: [connect, timeout, 5xx, 429]`) retries dial and write failures and timeouts. A connection whose write failed or timed out may end with part of a frame. So it is always dropped, and the retry dials afresh. Frames written before it still arrive, and a hikyaku source drops the partial one at the end (see [Framing](#framing)).

**Health.** A tcp destination is healthy at once while its persistent connection is up. Otherwise it dials one within 2s and keeps it for the next delivery. Health never waits for a delivery: while one is dialing, and always with `frame: raw`, Health dials a connection of its own and closes it. A raw peer sees that as an empty stream, which is no message. A udp destination is healthy when its host resolves. A DTLS destination is checked like a tcp one, with its association.

**Close** waits, within its context, for a delivery in progress on the persistent connection to finish its write. Then it closes the connection: over TLS or DTLS with `close_notify`, unless the context ends first, which cuts the connection. Later deliveries fail.

## DTLS

DTLS is TLS for datagrams: it encrypts udp the way TLS encrypts tcp. A `udp` source or destination with `tls` speaks DTLS 1.2 and 1.3, with the same keys as TLS over tcp. Version 1.3 is preferred.

**Sources.** The source is a DTLS server on its port. Each client's association (its DTLS session) is a peer, and a client must finish its handshake within 10s. Each record a client sends is read the way plain udp reads a datagram: the same framing, `max_body_bytes`, attributes (`udp.remote_addr`, `ordering_key`) and loss rules. A record the full queue cannot take is dropped and counted. Datagrams that are not DTLS are dropped. `auth.ip_allow` is checked before DTLS, so a sender it keeps out gets no handshake. An association that sends nothing for 5 minutes is closed, with a `close_notify`. So is every association when the relay stops. A client that comes back on the same address and port, without having closed its association, cannot handshake until that association idles out.

**Destinations.** The destination keeps one association. It dials and handshakes on the first delivery, and writes each message as one record. A frame longer than one record carries (16,384 bytes, the line ending or length prefix included) fails as `udp:frame_too_large`, before any dial. The destination redials:

- after a failed write, except a record too long for the network, which never left;
- when the server closes the association with a `close_notify`, or its port is closed (the system says so through ICMP);
- before a delivery, when the association has gone unused for 30s. A server or a NAT on the way may have dropped it, and DTLS does not tell the client.

With `ca_file` unset, the server is verified against the system's CAs. The handshake is bounded by the attempt's timeout, or by 10s when the attempt has none.

**When it counts as delivered.** As for plain udp, a record the kernel accepted counts as delivered: DTLS acknowledges nothing after the handshake. A server that loses an association without closing it, such as one that crashes and restarts, drops what is written into it. The destination redials once a write meets the closed port, or once the association has gone unused for 30s.

## Example

```yaml
sources:
  - name: devices
    type: tcp
    listen: ":7000"
    frame: length
    length_bytes: 2
    max_body_bytes: 64KiB
    tls: { cert_file: /etc/hikyaku/tls.crt, key_file: /etc/hikyaku/tls.key, client_ca_file: /etc/hikyaku/devices-ca.crt }

  - name: syslog
    type: udp
    listen: ":5514"
    auth: { ip_allow: [10.0.0.0/8, "fd00::/8"] }

  - name: sensors
    type: udp
    listen: ":5684"
    frame: raw
    tls: { cert_file: /etc/hikyaku/tls.crt, key_file: /etc/hikyaku/tls.key, client_ca_file: /etc/hikyaku/sensors-ca.crt }

destinations:
  - name: legacy-ingest
    type: tcp
    url: tls://ingest.internal:6000
    tls: { ca_file: /etc/hikyaku/internal-ca.crt }
    frame: newline
    retry: { attempts: 5, retry_on: [connect, timeout] }

  - name: metrics
    type: udp
    url: udp://statsd.internal:8125
    frame: raw
    body: "orders.created:1|c"
    required: false

  - name: telemetry
    type: udp
    url: dtls://collector.internal:5684
    tls: { ca_file: /etc/hikyaku/internal-ca.crt, cert_file: /etc/hikyaku/relay.crt, key_file: /etc/hikyaku/relay.key }
    frame: raw
```
