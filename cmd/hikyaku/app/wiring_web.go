package app

import (
	"github.com/xavidop/hikyaku/protocols/sse"
	"github.com/xavidop/hikyaku/protocols/ws"
)

// registerWeb registers the WebSocket (ws, wss) and server-sent events
// (sse) protocols.
func registerWeb() {
	ws.Register()
	sse.Register()
}
