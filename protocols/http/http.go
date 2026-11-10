// Package http is hikyaku's http and https protocol.
//
// Sources listen (hikyaku is the server: TLS and mTLS, HMAC, bearer, JWT,
// API-key and IP-allowlist auth, 503 backpressure, ack modes, and
// respond.from to act as a smart reverse proxy) or poll (a schedule, with
// retries, conditional requests, a jq split and dedupe). The sink sends one
// request per attempt, forwards the message's headers and idempotency key,
// classifies the answer for the retry policy (Retry-After included), reads
// X-Hikyaku-Reroute corrections, and guards templated hosts against SSRF.
package http

import (
	"context"
	"fmt"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// Register makes http and https sources and sinks available.
func Register() {
	for _, kind := range []string{"http", "https"} {
		plugin.RegisterSource(kind, newSource)
		plugin.RegisterSink(kind, func(_ context.Context, s plugin.Spec) (plugin.Sink, error) { return newSink(s) })
	}
}

func newSource(ctx context.Context, s plugin.Spec) (plugin.Source, error) {
	src, ok := s.Config.(*config.Source)
	if !ok || src == nil {
		return nil, fmt.Errorf("source %s: missing config", s.Name)
	}
	mode := src.Mode
	if mode == "" {
		mode = config.DefaultMode(*src)
	}
	switch mode {
	case "listen":
		return newListener(ctx, s, src)
	case "poll":
		return newPoller(s, src)
	}
	return nil, fmt.Errorf("source %s: http sources listen or poll, not %q", s.Name, mode)
}
