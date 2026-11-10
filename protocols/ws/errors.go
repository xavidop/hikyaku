package ws

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"

	"github.com/xavidop/hikyaku/plugin"
)

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("ws destination is closed")

// closeTag is the error tag of a peer's close status, such as ws:close_1011.
func closeTag(code websocket.StatusCode) string {
	return "ws:close_" + strconv.Itoa(int(code))
}

// normalClose reports whether a close status says the connection simply
// ended (1000 normal closure, 1001 going away, 1005 no status) rather than
// that something went wrong.
func normalClose(code websocket.StatusCode) bool {
	return code == websocket.StatusNormalClosure || code == websocket.StatusGoingAway || code == websocket.StatusNoStatusRcvd
}

// ctxError classifies a failure caused by the attempt's context: a
// deadline is a timeout; a cancellation (shutdown) is transient.
func ctxError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// dialError classifies a failed dial. A handshake the server answered with
// an HTTP status follows HTTP's semantics (a 503 is transient, a 401
// permanent, a 429 rate limited); anything else did not connect.
func dialError(ctx context.Context, endpoint string, err error, resp *http.Response) error {
	if ctx.Err() != nil {
		return ctxError(ctx, fmt.Errorf("dial %s: %w", endpoint, err))
	}
	if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
		retryAfter := plugin.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		return plugin.StatusError(resp.StatusCode, retryAfter, "WebSocket handshake with "+endpoint+" refused")
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("dial %s: %w", endpoint, err))
}

// peerClosed is the failure of a connection the peer closed with a status:
// tagged ws:close_<code> so retry_on can name it. A normal close is also
// tagged connect, so the default policy retries it on a new connection.
func peerClosed(endpoint string, code websocket.StatusCode, err error) error {
	tags := []string{closeTag(code)}
	if normalClose(code) {
		tags = append(tags, plugin.TagConnect)
	}
	return plugin.Wrap(plugin.Transient, tags, fmt.Errorf("%s closed the connection (%d %s): %w", endpoint, int(code), code, err))
}

// writeError classifies a failed write on a live connection. why is the
// reader's error once it stopped (nil when it is still unknown): it carries
// the peer's close status when there was one.
func writeError(ctx context.Context, endpoint string, err, why error) error {
	if ctx.Err() != nil {
		return ctxError(ctx, fmt.Errorf("write to %s: %w", endpoint, err))
	}
	if code := websocket.CloseStatus(why); code >= 0 {
		return peerClosed(endpoint, code, err)
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("write to %s: %w", endpoint, err))
}
