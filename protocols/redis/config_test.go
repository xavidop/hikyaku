package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func sourceSpec(cfg config.Source) plugin.Spec {
	cfg.Name, cfg.Type = "in", kind
	return plugin.Spec{Name: "in", Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "my relay"}}
}

func sinkSpec(ep config.Endpoint) plugin.Spec {
	ep.Type = kind
	return plugin.Spec{Name: "out", Kind: kind, Config: &ep}
}

func TestParseSourceErrors(t *testing.T) {
	stream := func(f func(*config.Source)) config.Source {
		c := config.Source{URL: "redis://localhost:6379/0", Stream: "orders", Group: "g"}
		f(&c)
		return c
	}
	channel := func(f func(*config.Source)) config.Source {
		c := config.Source{URL: "redis://localhost:6379", Channel: "alerts"}
		f(&c)
		return c
	}
	for name, tc := range map[string]struct {
		cfg  config.Source
		want string
	}{
		"neither":            {config.Source{URL: "redis://h"}, "stream (with group) or channel is required"},
		"both":               {stream(func(c *config.Source) { c.Channel = "x" }), "set stream or channel, not both"},
		"no group":           {stream(func(c *config.Source) { c.Group = "" }), "group is required with stream"},
		"stream template":    {stream(func(c *config.Source) { c.Stream = "{{ .json.x }}" }), "stream is a fixed name"},
		"group template":     {stream(func(c *config.Source) { c.Group = "{{ .id }}" }), "group is a fixed name"},
		"consumer template":  {stream(func(c *config.Source) { c.Consumer = "{{ .id }}" }), "consumer is a fixed name"},
		"channel template":   {channel(func(c *config.Source) { c.Channel = "a.{{ .id }}" }), "channel is a fixed name"},
		"bad start":          {stream(func(c *config.Source) { c.Start = "new" }), `start must be earliest or latest, not "new"`},
		"negative prefetch":  {stream(func(c *config.Source) { c.Prefetch = -1 }), "prefetch must not be negative"},
		"negative ack_wait":  {stream(func(c *config.Source) { c.AckWait = -1 }), "ack_wait must not be negative"},
		"bad ack":            {stream(func(c *config.Source) { c.Ack = "maybe" }), `ack must be delivered or accepted, not "maybe"`},
		"negative max":       {stream(func(c *config.Source) { c.MaxBodyBytes = -1 }), "max_body_bytes must not be negative"},
		"listen mode":        {stream(func(c *config.Source) { c.Mode = "listen" }), `subscribe mode, not "listen"`},
		"no url":             {stream(func(c *config.Source) { c.URL = "" }), "url is required"},
		"bad scheme":         {stream(func(c *config.Source) { c.URL = "http://u:secret@h" }), `url "http://u:xxxxx@h" must start with redis:// or rediss://`},
		"no host":            {stream(func(c *config.Source) { c.URL = "redis://" }), "has no host"},
		"url template":       {stream(func(c *config.Source) { c.URL = "redis://{{ .json.h }}" }), "cannot be a per-message template"},
		"bad db":             {stream(func(c *config.Source) { c.URL = "redis://h/zero" }), "invalid database number"},
		"bad failover":       {stream(func(c *config.Source) { c.FailoverURLs = []string{"tcp://h"} }), `failover_urls[0] "tcp://h" must start with redis://`},
		"bad jitter":         {stream(func(c *config.Source) { c.Reconnect = &config.Reconnect{Jitter: "some"} }), "reconnect.jitter must be full, equal or none"},
		"negative attempts":  {stream(func(c *config.Source) { c.Reconnect = &config.Reconnect{MaxAttempts: -1} }), "reconnect.max_attempts must not be negative"},
		"tls off on rediss":  {stream(func(c *config.Source) { c.URL = "rediss://h"; c.TLS = &config.TLS{Enabled: config.Ptr(false)} }), "is rediss://, but tls.enabled is false"},
		"missing ca file":    {stream(func(c *config.Source) { c.TLS = &config.TLS{CAFile: "/nonexistent/ca.pem"} }), "ca.pem"},
		"group on channel":   {channel(func(c *config.Source) { c.Group = "g" }), "group applies to streams"},
		"start on channel":   {channel(func(c *config.Source) { c.Start = "earliest" }), "start applies to streams"},
		"prefetch channel":   {channel(func(c *config.Source) { c.Prefetch = 5 }), "prefetch applies to streams"},
		"ack_wait channel":   {channel(func(c *config.Source) { c.AckWait = config.Duration(time.Second) }), "ack_wait applies to streams"},
		"accepted channel":   {channel(func(c *config.Source) { c.Ack = "accepted" }), "ack: accepted applies to streams"},
		"consumer channel":   {channel(func(c *config.Source) { c.Consumer = "c" }), "consumer applies to streams"},
		"password in errors": {stream(func(c *config.Source) { c.URL = "redis://u:hunter2@h:notaport" }), "url is not a valid URL"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newSource(context.Background(), sourceSpec(tc.cfg))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "source in: ")
			assert.NotContains(t, err.Error(), "secret")
			assert.NotContains(t, err.Error(), "hunter2")
		})
	}
	_, err := parseSource(plugin.Spec{Name: "in", Kind: kind})
	assert.EqualError(t, err, "source in: missing config")
}

func TestParseStreamSourceDefaults(t *testing.T) {
	o, err := parseSource(sourceSpec(config.Source{URL: "redis://u:pw@localhost:6380/2", Stream: "orders", Group: "g"}))
	require.NoError(t, err)
	host, err := os.Hostname()
	require.NoError(t, err)
	assert.Equal(t, modeStream, o.mode)
	assert.Equal(t, host+"-in", o.consumer, "the host name plus the source name")
	assert.Equal(t, "$", o.start, "latest by default")
	assert.Equal(t, 64, o.prefetch)
	assert.Equal(t, 30*time.Second, o.ackWait)
	assert.Equal(t, ackDelivered, o.ack)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.reconnect)
	require.Len(t, o.servers, 1)
	opt := o.servers[0].opt
	assert.Equal(t, "localhost:6380", opt.Addr)
	assert.Equal(t, 2, opt.DB)
	assert.Equal(t, "u", opt.Username)
	assert.Equal(t, "pw", opt.Password)
	assert.Nil(t, opt.TLSConfig)
	assert.Equal(t, -1, opt.MaxRetries, "one attempt: the loops retry")
	assert.Equal(t, 1, opt.DialerRetries)
	assert.True(t, opt.ContextTimeoutEnabled)
	assert.Equal(t, "hikyaku-my_relay-in", opt.ClientName)
	assert.Equal(t, "redis://u:xxxxx@localhost:6380/2", o.servers[0].shown)
}

func TestParseStreamSourceSettings(t *testing.T) {
	o, err := parseSource(sourceSpec(config.Source{
		URL: "redis://localhost", FailoverURLs: []string{"redis://standby:6379"},
		Username: "relay", Password: "s3cret",
		Stream: "orders", Group: "g", Consumer: "worker-1", Start: "earliest", Prefetch: 8,
		AckWait: config.Duration(time.Minute), Ack: "accepted", MaxBodyBytes: 1024,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(100 * time.Millisecond), Jitter: "none", MaxAttempts: 3},
	}))
	require.NoError(t, err)
	assert.Equal(t, "worker-1", o.consumer)
	assert.Equal(t, "0", o.start)
	assert.Equal(t, 8, o.prefetch)
	assert.Equal(t, time.Minute, o.ackWait)
	assert.Equal(t, ackAccepted, o.ack)
	assert.Equal(t, int64(1024), o.maxBytes)
	assert.Equal(t, reconnectOptions{backoff: time.Second, maxBackoff: time.Second, jitter: "none", maxAttempts: 3}, o.reconnect,
		"max_backoff is at least backoff")
	require.Len(t, o.servers, 2)
	for _, sv := range o.servers {
		assert.Equal(t, "relay", sv.opt.Username, "username overrides the URL's, on every server")
		assert.Equal(t, "s3cret", sv.opt.Password)
	}
	assert.Equal(t, "standby:6379", o.servers[1].opt.Addr)
}

func TestParseChannelSource(t *testing.T) {
	for channel, pattern := range map[string]bool{
		"alerts": false, "events.*": true, "h?llo": true, "user:[ab]": true,
	} {
		o, err := parseSource(sourceSpec(config.Source{URL: "redis://h", Channel: channel, Ack: "delivered"}))
		require.NoError(t, err, channel)
		assert.Equal(t, modeChannel, o.mode)
		assert.Equal(t, channel, o.channel)
		assert.Equal(t, pattern, o.pattern, channel)
	}
}

func TestParseTLS(t *testing.T) {
	certFile, _ := selfSigned(t)
	t.Run("rediss", func(t *testing.T) {
		o, err := parseSource(sourceSpec(config.Source{URL: "rediss://cache.example.com:6380", Channel: "c"}))
		require.NoError(t, err)
		require.NotNil(t, o.servers[0].opt.TLSConfig)
		assert.Equal(t, "cache.example.com", o.servers[0].opt.TLSConfig.ServerName)
	})
	t.Run("tls block on redis://", func(t *testing.T) {
		o, err := parseSource(sourceSpec(config.Source{URL: "redis://cache.example.com", Channel: "c", TLS: &config.TLS{CAFile: certFile}}))
		require.NoError(t, err)
		cfg := o.servers[0].opt.TLSConfig
		require.NotNil(t, cfg)
		assert.NotNil(t, cfg.RootCAs)
		assert.Equal(t, "cache.example.com", cfg.ServerName, "the server name defaults to the URL's host")
	})
	t.Run("server_name kept", func(t *testing.T) {
		s, err := parseSink(sinkSpec(config.Endpoint{URL: "rediss://10.0.0.1", Stream: "s", TLS: &config.TLS{ServerName: "redis.internal"}}))
		require.NoError(t, err)
		assert.Equal(t, "redis.internal", s.server.opt.TLSConfig.ServerName)
	})
}

func TestParseSinkErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no url":            {config.Endpoint{Stream: "s"}, "url is required"},
		"bad scheme":        {config.Endpoint{URL: "amqp://h", Stream: "s"}, "must start with redis:// or rediss://"},
		"url template":      {config.Endpoint{URL: "redis://{{ .json.host }}", Stream: "s"}, "cannot be a per-message template"},
		"neither":           {config.Endpoint{URL: "redis://h"}, "stream (XADD) or channel (PUBLISH) is required"},
		"both":              {config.Endpoint{URL: "redis://h", Stream: "s", Channel: "c"}, "set stream or channel, not both"},
		"stream template":   {config.Endpoint{URL: "redis://h", Stream: "s.{{ .id }}"}, "stream is a fixed key"},
		"negative max_len":  {config.Endpoint{URL: "redis://h", Stream: "s", MaxLen: -1}, "max_len must not be negative"},
		"max_len channel":   {config.Endpoint{URL: "redis://h", Channel: "c", MaxLen: 10}, "max_len applies to streams only"},
		"headers channel":   {config.Endpoint{URL: "redis://h", Channel: "c", Headers: map[string]string{"X": "1"}}, "headers apply to streams"},
		"bad channel":       {config.Endpoint{URL: "redis://h", Channel: "c.{{ .json.x "}, "channel"},
		"bad body":          {config.Endpoint{URL: "redis://h", Stream: "s", Body: "{{ .json.x "}, "body"},
		"bad header":        {config.Endpoint{URL: "redis://h", Stream: "s", Headers: map[string]string{"X-A": "{{ .id "}}, "headers.X-A"},
		"empty header name": {config.Endpoint{URL: "redis://h", Stream: "s", Headers: map[string]string{" ": "v"}}, "header name is empty"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newSink(context.Background(), sinkSpec(tc.ep))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "destination out: ")
		})
	}
	_, err := parseSink(plugin.Spec{Name: "out", Kind: kind})
	assert.EqualError(t, err, "destination out: missing config")
}

func TestRegister(t *testing.T) {
	Register()
	f, ok := plugin.LookupSource(kind)
	require.True(t, ok)
	src, err := f(context.Background(), sourceSpec(config.Source{URL: "redis://h", Stream: "s", Group: "g"}))
	require.NoError(t, err)
	assert.IsType(t, &StreamSource{}, src)
	require.NoError(t, src.(plugin.Closer).Close(context.Background()))
	src, err = f(context.Background(), sourceSpec(config.Source{URL: "redis://h", Channel: "c"}))
	require.NoError(t, err)
	assert.IsType(t, &PubSubSource{}, src)
	assert.Equal(t, "in", src.Name())
	require.NoError(t, src.(plugin.Closer).Close(context.Background()))

	g, ok := plugin.LookupSink(kind)
	require.True(t, ok)
	sink, err := g(context.Background(), sinkSpec(config.Endpoint{URL: "redis://h", Stream: "s"}))
	require.NoError(t, err)
	assert.Implements(t, (*plugin.BatchSink)(nil), sink)
	assert.Equal(t, "out", sink.Name())
	require.NoError(t, sink.Close(context.Background()))
	require.NoError(t, sink.Close(context.Background()), "closing twice is fine")
}

func TestClientName(t *testing.T) {
	assert.Equal(t, "hikyaku-relay-in", clientName("relay", "in"))
	assert.Equal(t, "hikyaku-in", clientName("", "in"))
	assert.Equal(t, "hikyaku-my_relay-a_b_c", clientName("my relay", "a\nb c"))
}
