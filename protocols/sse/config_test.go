package sse

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestSourceDefaults(t *testing.T) {
	_, o, err := parseSource(plugin.Spec{Name: "in", Kind: "sse", Config: &config.Source{Listen: ":8080"}})
	require.NoError(t, err)
	assert.Equal(t, modeListen, o.mode)
	assert.Equal(t, "/", o.path)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)

	_, o, err = parseSource(plugin.Spec{Name: "in", Kind: "sse", Config: &config.Source{URL: "https://h/s",
		FailoverURLs: []string{"http://b/s"}, Headers: map[string]string{"host": "front", "X-Key": "1"},
		Reconnect: &config.Reconnect{MaxBackoff: config.Duration(time.Minute), MaxAttempts: 3}}})
	require.NoError(t, err)
	assert.Equal(t, modeSubscribe, o.mode)
	assert.Equal(t, []string{"https://h/s", "http://b/s"}, o.urls)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: time.Minute, jitter: "full", maxAttempts: 3}, o.reconnect)
	assert.Equal(t, "front", o.host)
	assert.Equal(t, "1", o.header.Get("X-Key"))
}

func TestSourceConfigErrors(t *testing.T) {
	cases := map[string]struct {
		cfg  config.Source
		want string
	}{
		"bad mode":        {config.Source{Mode: "poll", URL: "http://h"}, `sse sources run in listen or subscribe mode, not "poll"`},
		"no listen":       {config.Source{Mode: "listen"}, "listen is required"},
		"bad listen":      {config.Source{Listen: "nope"}, "is not host:port"},
		"bad path":        {config.Source{Listen: ":1", Path: "events"}, "path must start with /"},
		"half tls":        {config.Source{Listen: ":1", TLS: &config.TLS{CertFile: "/c.pem"}}, "cert_file and key_file"},
		"negative max":    {config.Source{Listen: ":1", MaxBodyBytes: -2}, "max_body_bytes"},
		"no url":          {config.Source{Mode: "subscribe"}, "url is required"},
		"ws url":          {config.Source{URL: "ws://h"}, "must start with http:// or https://"},
		"no host":         {config.Source{URL: "http:///x"}, "has no host"},
		"template url":    {config.Source{URL: "https://h/{{ .id }}"}, "per-message template"},
		"bad failover":    {config.Source{URL: "http://h", FailoverURLs: []string{"ftp://x"}}, "failover_urls[0]"},
		"template header": {config.Source{URL: "http://h", Headers: map[string]string{"X": "{{ .id }}"}}, "headers.X"},
		"bad jitter":      {config.Source{URL: "http://h", Reconnect: &config.Reconnect{Jitter: "lots"}}, "reconnect.jitter"},
		"negative tries":  {config.Source{URL: "http://h", Reconnect: &config.Reconnect{MaxAttempts: -1}}, "reconnect.max_attempts"},
		"bad client tls":  {config.Source{URL: "https://h", TLS: &config.TLS{CAFile: "/nope.pem"}}, "tls"},
	}
	for name, c := range cases {
		_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: "sse", Config: &c.cfg})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "source in: ", name)
		assert.Contains(t, err.Error(), c.want, name)
	}
	_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: "sse"})
	assert.ErrorContains(t, err, "missing config")
}

func TestSinkConfig(t *testing.T) {
	o, err := parseSink(plugin.Spec{Name: "dash", Kind: "sse", Config: &config.Endpoint{Listen: ":9000"}})
	require.NoError(t, err)
	assert.Equal(t, "/", o.path)
	assert.Equal(t, 1000, o.replay)
	assert.Nil(t, o.event)
	o, err = parseSink(plugin.Spec{Name: "dash", Kind: "sse", Config: &config.Endpoint{Listen: ":9000", Path: "/live", Replay: 5, Event: "{{ .subject }}"}})
	require.NoError(t, err)
	assert.Equal(t, 5, o.replay)
	assert.NotNil(t, o.event)

	cases := map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no listen":    {config.Endpoint{}, "listen is required: the address the event stream is served on"},
		"bad listen":   {config.Endpoint{Listen: "x"}, "is not host:port"},
		"bad path":     {config.Endpoint{Listen: ":1", Path: "live"}, "path must start with /"},
		"bad replay":   {config.Endpoint{Listen: ":1", Replay: -1}, "replay must not be negative"},
		"bad template": {config.Endpoint{Listen: ":1", Event: "{{ .json.x "}, "event: template"},
		"half tls":     {config.Endpoint{Listen: ":1", TLS: &config.TLS{KeyFile: "/k.pem"}}, "cert_file and key_file"},
	}
	for name, c := range cases {
		_, err := newSink(context.Background(), plugin.Spec{Name: "dash", Kind: "sse", Config: &c.ep})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "destination dash: ", name)
		assert.Contains(t, err.Error(), c.want, name)
	}
	_, err = newSink(context.Background(), plugin.Spec{Name: "dash", Kind: "sse", Config: &config.Source{}})
	assert.ErrorContains(t, err, "missing config")
}
