package amqp

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func sourceSpec(cfg config.Source) plugin.Spec {
	cfg.Name, cfg.Type = "in", kind
	return plugin.Spec{Name: "in", Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "relay"}}
}

func sinkSpec(cfg config.Endpoint) plugin.Spec {
	cfg.Type = kind
	return plugin.Spec{Name: "out", Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "relay"}}
}

func TestRegister(t *testing.T) {
	Register()
	_, ok := plugin.LookupSource(kind)
	assert.True(t, ok)
	_, ok = plugin.LookupSink(kind)
	assert.True(t, ok)

	_, err := plugin.NewSource(context.Background(), sourceSpec(config.Source{URL: "amqp://broker/"}))
	assert.ErrorContains(t, err, "queue is required")
	src, err := plugin.NewSource(context.Background(), sourceSpec(config.Source{URL: "amqp://broker/", Queue: "jobs"}))
	require.NoError(t, err)
	assert.Equal(t, "in", src.Name())
	_, err = plugin.NewSink(context.Background(), sinkSpec(config.Endpoint{URL: "amqp://broker/"}))
	assert.ErrorContains(t, err, "routing_key is required")
	sink, err := plugin.NewSink(context.Background(), sinkSpec(config.Endpoint{URL: "amqp://broker/", RoutingKey: "jobs"}))
	require.NoError(t, err)
	assert.Equal(t, "out", sink.Name())
}

func TestParseSourceDefaults(t *testing.T) {
	o, err := parseSource(sourceSpec(config.Source{
		URL: "amqp://u:p@a:5672/vh", FailoverURLs: []string{"amqps://b/vh"}, Queue: "jobs", Mode: "subscribe",
	}))
	require.NoError(t, err)
	assert.Equal(t, []string{"amqp://u:p@a:5672/vh", "amqps://b/vh"}, o.urls)
	assert.Equal(t, "jobs", o.queue)
	assert.Equal(t, defaultPrefetch, o.prefetch)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, defaultBindingKey, o.bindingKey)
	assert.False(t, o.declare)
	assert.Equal(t, reconnectOptions{backoff: defaultBackoff, maxBackoff: defaultMaxBackoff, jitter: "full"}, o.reconnect)
	assert.Equal(t, "hikyaku-relay-in", o.conn.name)
	assert.Nil(t, o.conn.tls)

	o, err = parseSource(sourceSpec(config.Source{
		URL: "amqps://broker/", Queue: "jobs", Prefetch: 10, MaxBodyBytes: 1024, Ack: "accepted",
		Declare: true, Exchange: "events", RoutingKey: "job.*",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Minute), Jitter: "none", MaxAttempts: 3},
		TLS:       &config.TLS{ServerName: "rabbit.internal"},
	}))
	require.NoError(t, err)
	assert.Equal(t, 10, o.prefetch)
	assert.Equal(t, int64(1024), o.maxBytes)
	assert.Equal(t, "accepted", o.ack)
	assert.Equal(t, "events", o.exchange)
	assert.Equal(t, "job.*", o.bindingKey)
	assert.Equal(t, reconnectOptions{backoff: time.Second, maxBackoff: time.Minute, jitter: "none", maxAttempts: 3}, o.reconnect)
	require.NotNil(t, o.conn.tls)
	assert.Equal(t, "rabbit.internal", o.conn.tls.ServerName)
}

func TestParseSourceErrors(t *testing.T) {
	for _, c := range []struct {
		name string
		cfg  config.Source
		want string
	}{
		{"listen mode", config.Source{Mode: "listen", URL: "amqp://b/", Queue: "q"}, `subscribe mode, not "listen"`},
		{"no queue", config.Source{URL: "amqp://b/"}, "queue is required"},
		{"no url", config.Source{Queue: "q"}, "url is required"},
		{"bad scheme", config.Source{URL: "http://b/", Queue: "q"}, "must start with amqp:// or amqps://"},
		{"template url", config.Source{URL: "amqp://{{ .json.host }}/", Queue: "q"}, "cannot be a per-message template"},
		{"no host", config.Source{URL: "amqp:///vh", Queue: "q"}, "has no host"},
		{"bad port", config.Source{URL: "amqp://b:x/", Queue: "q"}, "not a valid URL"},
		{"bad failover", config.Source{URL: "amqp://b/", FailoverURLs: []string{"nats://c"}, Queue: "q"}, "failover_urls[0]"},
		{"prefetch", config.Source{URL: "amqp://b/", Queue: "q", Prefetch: 70000}, "prefetch must be 1 to 65535"},
		{"negative prefetch", config.Source{URL: "amqp://b/", Queue: "q", Prefetch: -1}, "prefetch must be"},
		{"max body", config.Source{URL: "amqp://b/", Queue: "q", MaxBodyBytes: -1}, "max_body_bytes must not be negative"},
		{"ack", config.Source{URL: "amqp://b/", Queue: "q", Ack: "never"}, "ack must be delivered or accepted"},
		{"key without exchange", config.Source{URL: "amqp://b/", Queue: "q", Declare: true, RoutingKey: "k"}, "routing_key needs exchange"},
		{"exchange without declare", config.Source{URL: "amqp://b/", Queue: "q", Exchange: "e"}, "needs declare: true"},
		{"tls on amqp", config.Source{URL: "amqp://b/", Queue: "q", TLS: &config.TLS{InsecureSkipVerify: true}}, "tls applies to amqps:// URLs"},
		{"tls files", config.Source{URL: "amqps://b/", Queue: "q", TLS: &config.TLS{CAFile: "/no/such/ca.pem"}}, "tls:"},
		{"jitter", config.Source{URL: "amqp://b/", Queue: "q", Reconnect: &config.Reconnect{Jitter: "some"}}, "reconnect.jitter"},
		{"attempts", config.Source{URL: "amqp://b/", Queue: "q", Reconnect: &config.Reconnect{MaxAttempts: -1}}, "reconnect.max_attempts"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseSource(sourceSpec(c.cfg))
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Contains(t, err.Error(), "source in:")
		})
	}
	_, err := parseSource(plugin.Spec{Name: "in", Kind: kind})
	assert.ErrorContains(t, err, "missing config")
}

// A URL that fails to parse is never shown with its password.
func TestURLErrorsHidePasswords(t *testing.T) {
	for _, raw := range []string{"amqp://user:s3cret@b:x/", "amqp://user:s3cret@b/ space", "http://user:s3cret@b/"} {
		_, err := dialURL("url", raw)
		require.Error(t, err, raw)
		assert.NotContains(t, err.Error(), "s3cret", raw)
	}
	assert.Equal(t, "amqp://user:xxxxx@b:5672/vh", redactURL("amqp://user:s3cret@b:5672/vh"))
	assert.Equal(t, "amqp://xxxxx@b:x/", redactURL("amqp://user:s3cret@b:x/"))
	assert.Equal(t, "amqp://b/", redactURL("amqp://b/"))
	assert.Equal(t, "(invalid URL)", redactURL("user:s3cret@b:x\x7f"))
}

func TestParseSink(t *testing.T) {
	o, err := parseSink(sinkSpec(config.Endpoint{URL: "amqp://b/", RoutingKey: "jobs"}))
	require.NoError(t, err)
	assert.True(t, o.persistent, "persistent by default")
	assert.Equal(t, "", o.exchange)
	assert.Equal(t, "hikyaku-relay-out", o.conn.name)

	o, err = parseSink(sinkSpec(config.Endpoint{
		URL: "amqp://b/", Exchange: "events", Declare: true, Mandatory: true, Persistent: config.Ptr(false),
		Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}"}, Body: "{{ .bodyText }}",
	}))
	require.NoError(t, err)
	assert.False(t, o.persistent)
	assert.True(t, o.mandatory)
	assert.True(t, o.declare)
	require.Len(t, o.headers, 1)
	assert.Equal(t, "X-Tenant", o.headers[0].name)
	assert.NotNil(t, o.body)

	for _, c := range []struct {
		name string
		cfg  config.Endpoint
		want string
	}{
		{"no url", config.Endpoint{RoutingKey: "k"}, "url is required"},
		{"no key", config.Endpoint{URL: "amqp://b/"}, "routing_key is required on the default exchange"},
		{"declare default", config.Endpoint{URL: "amqp://b/", RoutingKey: "k", Declare: true}, "declare declares exchange"},
		{"key template", config.Endpoint{URL: "amqp://b/", RoutingKey: "{{ .json.x "}, "template routing_key"},
		{"header template", config.Endpoint{URL: "amqp://b/", RoutingKey: "k", Headers: map[string]string{"X": "{{ end }}"}}, "template headers.X"},
		{"body template", config.Endpoint{URL: "amqp://b/", RoutingKey: "k", Body: "{{"}, "template body"},
		{"tls on amqp", config.Endpoint{URL: "amqp://b/", RoutingKey: "k", TLS: &config.TLS{Enabled: config.Ptr(true)}}, "tls applies"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := parseSink(sinkSpec(c.cfg))
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Contains(t, err.Error(), "destination out:")
		})
	}
	_, err = parseSink(plugin.Spec{Name: "out", Kind: kind})
	assert.ErrorContains(t, err, "missing config")
}

// username and password replace the URL's; the connection is named after
// the relay and the source or destination.
func TestConnConfig(t *testing.T) {
	plain := func(cfg amqp091.Config) *amqp091.PlainAuth {
		require.Len(t, cfg.SASL, 1)
		pa, ok := cfg.SASL[0].(*amqp091.PlainAuth)
		require.True(t, ok)
		return pa
	}
	o := connOptions{name: "hikyaku-relay-in"}
	cfg := o.config("amqp://u:p@b/")
	assert.Nil(t, cfg.SASL, "the URL's credentials")
	assert.Equal(t, "hikyaku-relay-in", cfg.Properties["connection_name"])
	assert.Equal(t, "en_US", cfg.Locale)

	o.username, o.password = "svc", "secret"
	assert.Equal(t, &amqp091.PlainAuth{Username: "svc", Password: "secret"}, plain(o.config("amqp://u:p@b/")))
	o.username = ""
	assert.Equal(t, &amqp091.PlainAuth{Username: "u", Password: "secret"}, plain(o.config("amqp://u:p@b/")))
	o.username, o.password = "svc", ""
	assert.Equal(t, &amqp091.PlainAuth{Username: "svc", Password: "p"}, plain(o.config("amqp://u:p@b/")))

	tlsCfg, err := parseConn(plugin.Spec{Name: "in"}, &config.TLS{ServerName: "x"}, "", "", []string{"amqps://b/"})
	require.NoError(t, err)
	assert.Equal(t, "hikyaku-in", tlsCfg.name)
	a, b := tlsCfg.config("amqps://b/"), tlsCfg.config("amqps://c/")
	require.NotNil(t, a.TLSClientConfig)
	assert.NotSame(t, a.TLSClientConfig, b.TLSClientConfig, "each dial gets its own TLS config")
}
