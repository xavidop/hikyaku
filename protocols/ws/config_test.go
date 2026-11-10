package ws

import (
	"context"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestSourceDefaults(t *testing.T) {
	_, o, err := parseSource(plugin.Spec{Name: "in", Kind: "ws", Config: &config.Source{Listen: ":8080"}})
	require.NoError(t, err)
	assert.Equal(t, modeListen, o.mode)
	assert.Equal(t, "/", o.path)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Nil(t, o.serverTLS)

	_, o, err = parseSource(plugin.Spec{Name: "in", Kind: "wss", Config: &config.Source{URL: "wss://h/x"}})
	require.NoError(t, err)
	assert.Equal(t, modeSubscribe, o.mode)
	assert.Equal(t, []string{"wss://h/x"}, o.urls)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.reconnect)

	_, o, err = parseSource(plugin.Spec{Name: "in", Kind: "ws", Config: &config.Source{Mode: "subscribe", URL: "ws://h",
		FailoverURLs: []string{"ws://b", "wss://c"},
		Reconnect:    &config.Reconnect{Backoff: config.Duration(time.Minute), MaxBackoff: config.Duration(time.Second), Jitter: "equal", MaxAttempts: 4},
		Headers:      map[string]string{"host": "front.example", "sec-websocket-protocol": "a, b", "X-Key": "1"}}})
	require.NoError(t, err)
	assert.Equal(t, []string{"ws://h", "ws://b", "wss://c"}, o.urls)
	assert.Equal(t, reconnectOptions{backoff: time.Minute, maxBackoff: time.Minute, jitter: "equal", maxAttempts: 4}, o.reconnect,
		"max_backoff is at least backoff")
	assert.Equal(t, "front.example", o.dial.host)
	assert.Equal(t, []string{"a", "b"}, o.dial.subprotocols)
	assert.Equal(t, "1", o.dial.header.Get("X-Key"))
	assert.Empty(t, o.dial.header.Get("Host"))
}

func TestSourceConfigErrors(t *testing.T) {
	cases := map[string]struct {
		kind string
		cfg  config.Source
		want string
	}{
		"bad mode":         {"ws", config.Source{Mode: "poll", URL: "ws://h"}, `ws sources run in listen or subscribe mode, not "poll"`},
		"no listen":        {"ws", config.Source{Mode: "listen"}, "listen is required"},
		"bad listen":       {"ws", config.Source{Listen: "8080"}, "is not host:port"},
		"bad path":         {"ws", config.Source{Listen: ":1", Path: "x"}, "path must start with /"},
		"wss without cert": {"wss", config.Source{Listen: ":1"}, "cert_file and key_file"},
		"negative max":     {"ws", config.Source{Listen: ":1", MaxBodyBytes: -1}, "max_body_bytes"},
		"no url":           {"ws", config.Source{Mode: "subscribe"}, "url is required"},
		"http url":         {"ws", config.Source{URL: "http://h"}, "must start with ws:// or wss://"},
		"no host":          {"ws", config.Source{URL: "ws:///x"}, "has no host"},
		"template url":     {"ws", config.Source{URL: "ws://{{ .json.host }}/x"}, "per-message template"},
		"bad failover":     {"ws", config.Source{URL: "ws://h", FailoverURLs: []string{"tcp://x"}}, "failover_urls[0]"},
		"template header":  {"ws", config.Source{URL: "ws://h", Headers: map[string]string{"X": "{{ .id }}"}}, "headers.X"},
		"bad jitter":       {"ws", config.Source{URL: "ws://h", Reconnect: &config.Reconnect{Jitter: "some"}}, "reconnect.jitter"},
		"negative tries":   {"ws", config.Source{URL: "ws://h", Reconnect: &config.Reconnect{MaxAttempts: -1}}, "reconnect.max_attempts"},
		"bad client tls":   {"ws", config.Source{URL: "wss://h", TLS: &config.TLS{CAFile: "/nope.pem"}}, "tls"},
	}
	for name, c := range cases {
		_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: c.kind, Config: &c.cfg})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "source in: ", name)
		assert.Contains(t, err.Error(), c.want, name)
	}
	_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: "ws"})
	assert.ErrorContains(t, err, "missing config")
}

func TestSinkConfig(t *testing.T) {
	o, err := parseSink(plugin.Spec{Name: "out", Kind: "ws", Config: &config.Endpoint{URL: "wss://u:p@h/x"}})
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageText, o.frame)
	s, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: "ws", Config: &config.Endpoint{URL: "wss://u:p@h/x", Frame: "binary"}})
	require.NoError(t, err)
	assert.Equal(t, websocket.MessageBinary, s.opts.frame)
	assert.Equal(t, "wss://u:xxxxx@h/x", s.endpoint, "the password stays out of results")
	assert.Equal(t, "out", s.Name())

	cases := map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no url":          {config.Endpoint{}, "url is required"},
		"http url":        {config.Endpoint{URL: "https://h"}, "must start with ws:// or wss://"},
		"template url":    {config.Endpoint{URL: "wss://h/{{ .json.room }}"}, "per-message template"},
		"template header": {config.Endpoint{URL: "ws://h", Headers: map[string]string{"X-Id": "{{ .id }}"}}, "headers.X-Id: handshake headers are static"},
		"bad frame":       {config.Endpoint{URL: "ws://h", Frame: "json"}, `frame must be text or binary, not "json"`},
		"bad tls":         {config.Endpoint{URL: "wss://h", TLS: &config.TLS{CertFile: "/nope.pem", KeyFile: "/nope.key"}}, "tls"},
	}
	for name, c := range cases {
		_, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: "ws", Config: &c.ep})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "destination out: ", name)
		assert.Contains(t, err.Error(), c.want, name)
	}
	_, err = newSink(context.Background(), plugin.Spec{Name: "out", Kind: "ws", Config: &config.Source{}})
	assert.ErrorContains(t, err, "missing config")
}
