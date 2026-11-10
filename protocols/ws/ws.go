// Package ws is hikyaku's WebSocket protocol: the ws and wss source and
// destination types.
//
// A source listens (hikyaku is the server and clients send to it) or
// subscribes (hikyaku dials a URL and reads what it sends). Every text or
// binary message becomes one envelope. A subscribe source reconnects with
// backoff and jitter and rotates through failover_urls. Both modes ping the
// peer every 30s and drop one that stops answering.
//
// A destination keeps one persistent connection to its URL. It dials on the
// first delivery and redials on the delivery after a failure, and it writes
// each message as one text (default) or binary frame.
package ws

import (
	"context"
	"encoding/json"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/xavidop/hikyaku/plugin"
)

// Register makes the ws and wss source and destination types available.
func Register() {
	for _, kind := range []string{"ws", "wss"} {
		plugin.RegisterSource(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
			return newSource(ctx, spec)
		})
		plugin.RegisterSink(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
			return newSink(ctx, spec)
		})
	}
}

// Content types of received messages.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// textContentType is the content type of a text message: JSON when it
// parses as JSON, else plain text.
func textContentType(b []byte) string {
	if json.Valid(b) {
		return contentJSON
	}
	return contentText
}

// redactURL hides a URL's password, for logs and attributes.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Redacted()
}

// urlPath is the path of a URL, "/" when it has none.
func urlPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Path == "" {
		return "/"
	}
	return u.Path
}

// keepalive pings c every `every` and drops the connection when a pong does
// not come back within timeout. It returns when stop closes or the peer is
// dropped.
//
// A pong is received by the connection's reader. busy (nil for a reader
// that never waits) counts the reader's waits to hand a message over: it is
// odd while the reader waits, so no ping is sent then, and a ping that
// fails while the reader waited is not held against the peer. Backpressure
// must hold a peer back, not disconnect it.
func keepalive(c *websocket.Conn, every, timeout time.Duration, stop <-chan struct{}, busy *atomic.Int64) {
	if every <= 0 {
		return
	}
	waits := func() int64 {
		if busy == nil {
			return 0
		}
		return busy.Load()
	}
	t := time.NewTimer(every)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		before := waits()
		if before%2 == 1 {
			t.Reset(every)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := c.Ping(ctx)
		cancel()
		if err != nil && waits() == before {
			// A dead peer cannot answer a close handshake either.
			_ = c.CloseNow()
			return
		}
		t.Reset(every)
	}
}
