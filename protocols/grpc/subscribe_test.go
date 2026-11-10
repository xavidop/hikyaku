package grpc

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// fastReconnect retries at once, a hundred times.
var fastReconnect = &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxBackoff: config.Duration(10 * time.Millisecond)}

func newTestSubscriber(t *testing.T, c config.Source, logs *records) *Subscriber {
	t.Helper()
	c.Mode = modeSubscribe
	if c.Reconnect == nil {
		c.Reconnect = fastReconnect
	}
	cfg := sourceConfig(c)
	env := plugin.Env{}
	if logs != nil {
		env.Logger = slog.New(logs)
	}
	s, err := newSubscriber(plugin.Spec{Name: "in", Kind: kind, Config: cfg, Env: env}, cfg)
	require.NoError(t, err)
	return s
}

func TestSubscribeReceivesTheStream(t *testing.T) {
	for _, via := range []string{"descriptor_set", "reflection"} {
		t.Run(via, func(t *testing.T) {
			es := &echoServer{hold: true, data: "d"}
			srv := startEcho(t, es, serverOpts{})
			c := config.Source{URL: "grpc://" + srv.addr, Method: watchMethod, Body: `{"topic":"orders","limit":3}`,
				Headers: map[string]string{"Authorization": "Bearer t0k", "x-client": "relay-1"}}
			if via == "descriptor_set" {
				c.DescriptorSet = echoSet
			}
			s := newTestSubscriber(t, c, nil)
			assert.ErrorIs(t, s.Health(context.Background()), errNotStarted)
			out := make(chan *envelope.Envelope, 8)
			start(t, s, out)
			for i := 1; i <= 3; i++ {
				env := recv(t, out)
				assert.JSONEq(t, `{"topic":"orders","seq":"`+itoa(i)+`","data":"d"}`, env.BodyText())
				assert.Equal(t, contentJSON, env.ContentType)
				assert.Equal(t, watchMethod, env.Subject)
				assert.Equal(t, "grpc", env.Protocol)
				assert.Equal(t, watchMethod, env.Attributes["grpc.method"])
				assert.Equal(t, watchMethod, env.Attributes["ordering_key"])
				assert.Equal(t, "grpc://"+srv.addr, env.Attributes["grpc.url"])
				assert.Equal(t, "test", env.Header("X-Feed"), "the stream's header metadata")
				assert.Len(t, env.ID, 26)
			}
			require.NoError(t, s.Health(context.Background()), "healthy while the stream is up")
			md := es.calls()[len(es.calls())-1]
			assert.Equal(t, []string{"Bearer t0k"}, md.Get("authorization"), "headers are the call's metadata")
			assert.Equal(t, []string{"relay-1"}, md.Get("x-client"))
			assert.Contains(t, md.Get("user-agent")[0], userAgent)
		})
	}
}

func TestSubscribeReconnectsAfterTheServerRestarts(t *testing.T) {
	es := &echoServer{hold: true}
	srv := startEcho(t, es, serverOpts{})
	logs := &records{}
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: watchMethod, Body: `{"limit":2}`}, logs)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	recv(t, out)
	recv(t, out)

	srv.srv.Stop()
	require.Eventually(t, func() bool { return s.Health(context.Background()) != nil }, 3*time.Second, 5*time.Millisecond)
	startEcho(t, es, serverOpts{addr: srv.addr})
	env := recv(t, out)
	assert.JSONEq(t, `{"topic":"","seq":"1","data":""}`, env.BodyText(), "a new stream")
	recv(t, out)
	require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 3*time.Second, 5*time.Millisecond)
	assert.GreaterOrEqual(t, logs.count(slog.LevelWarn, "grpc source reconnecting"), 1)
}

func TestSubscribeEndedStreamsAreCalledAgain(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: watchMethod, Body: `{"limit":1}`}, nil)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	for range 3 {
		assert.JSONEq(t, `{"topic":"","seq":"1","data":""}`, recv(t, out).BodyText())
	}
}

func TestSubscribeRotatesThroughFailoverURLs(t *testing.T) {
	es := &echoServer{hold: true}
	srv := startEcho(t, es, serverOpts{})
	logs := &records{}
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + closedAddr(t), FailoverURLs: []string{"grpc://" + srv.addr},
		Method: watchMethod, Body: `{"limit":1}`, DescriptorSet: echoSet}, logs)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	env := recv(t, out)
	assert.Equal(t, "grpc://"+srv.addr, env.Attributes["grpc.url"], "the failover served it")
	assert.Contains(t, logs.text(), "url=grpc://"+srv.addr, "the reconnect names where it goes next")
}

func TestSubscribeGivesUpAfterMaxAttempts(t *testing.T) {
	logs := &records{}
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + closedAddr(t), Method: watchMethod, DescriptorSet: echoSet,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 3}}, logs)
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "giving up after 3 failed attempts in a row")
	assert.Equal(t, 2, logs.count(slog.LevelWarn, "grpc source reconnecting"), "one warning per retry; none once it gives up")
	assert.Error(t, s.Health(context.Background()))
}

func TestSubscribeDropsOversizedMessages(t *testing.T) {
	es := &echoServer{hold: true, data: strings.Repeat("x", 200)}
	srv := startEcho(t, es, serverOpts{})
	logs := &records{}
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: watchMethod, Body: `{"limit":2}`, MaxBodyBytes: 100}, logs)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	require.Eventually(t, func() bool {
		return logs.count(slog.LevelError, "grpc message dropped: larger than max_body_bytes") == 2
	}, 3*time.Second, 5*time.Millisecond)
	assert.Empty(t, out)
	assert.NoError(t, s.Health(context.Background()), "the stream carries on")
}

func TestSubscribeWarnsWhenAMessageFails(t *testing.T) {
	es := &echoServer{hold: true}
	srv := startEcho(t, es, serverOpts{})
	logs := &records{}
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: watchMethod, Body: `{"limit":1}`}, logs)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	env := recv(t, out)
	env.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	assert.Equal(t, 1, logs.count(slog.LevelWarn, "grpc message failed; a gRPC stream cannot redeliver it"))
}

func TestSubscribeRefusesOtherMethods(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{})
	cfg := sourceConfig(config.Source{Mode: modeSubscribe, URL: "grpc://" + srv.addr, Method: peekMethod, DescriptorSet: echoSet})
	_, err := newSubscriber(plugin.Spec{Name: "in", Kind: kind, Config: cfg}, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "method "+peekMethod+" is unary: a subscribe source calls a server-streaming method")

	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: peekMethod,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 2}}, nil)
	err = s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err, "found through reflection, the method is refused too")
	assert.Contains(t, err.Error(), "is unary")
}

func TestSubscribeNeedsReflectionOrADescriptorSet(t *testing.T) {
	es := &echoServer{}
	srv := startEcho(t, es, serverOpts{noReflection: true})
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: watchMethod,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 1}}, nil)
	err := s.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolving "+watchMethod+" through server reflection")
}

func TestSubscribeTLS(t *testing.T) {
	cert, key := selfSigned(t, "server")
	es := &echoServer{hold: true}
	srv := startEcho(t, es, serverOpts{creds: mustServerTLS(t, cert, key)})
	s := newTestSubscriber(t, config.Source{URL: "grpcs://" + srv.addr, Method: watchMethod, Body: `{"limit":1}`,
		TLS: &config.TLS{CAFile: cert}}, nil)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	assert.Equal(t, "grpcs://"+srv.addr, recv(t, out).Attributes["grpc.url"])

	untrusted := newTestSubscriber(t, config.Source{URL: "grpcs://" + srv.addr, Method: watchMethod,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Millisecond), MaxAttempts: 1}}, nil)
	err := untrusted.Start(context.Background(), make(chan *envelope.Envelope))
	require.Error(t, err, "an unknown certificate fails the call")
}

func mustServerTLS(t *testing.T, cert, key string) credentials.TransportCredentials {
	t.Helper()
	c, err := credentials.NewServerTLSFromFile(cert, key)
	require.NoError(t, err)
	return c
}

func TestSubscribeConfigErrors(t *testing.T) {
	ok := config.Source{URL: "grpc://localhost:50051", Method: watchMethod}
	with := func(f func(*config.Source)) config.Source { c := ok; f(&c); return c }
	cases := []struct {
		name string
		cfg  config.Source
		want string
	}{
		{"no url", with(func(c *config.Source) { c.URL = "" }), "url is required"},
		{"scheme", with(func(c *config.Source) { c.URL = "http://localhost:1" }), `url "http://localhost:1" must start with grpc:// or grpcs://`},
		{"template", with(func(c *config.Source) { c.URL = "grpc://{{ .json.host }}:1" }), "cannot be a per-message template"},
		{"credentials", with(func(c *config.Source) { c.URL = "grpc://u:p@localhost:1" }), `url "grpc://u:xxxxx@localhost:1": credentials go in headers`},
		{"path", with(func(c *config.Source) { c.URL = "grpc://localhost:1/pkg.Svc/M" }), "the method goes in method"},
		{"no host", with(func(c *config.Source) { c.URL = "grpc://:1" }), "has no host"},
		{"failover", with(func(c *config.Source) { c.FailoverURLs = []string{"ws://x"} }), "failover_urls[0]"},
		{"tls on plaintext", with(func(c *config.Source) { c.TLS = &config.TLS{InsecureSkipVerify: true} }), "tls applies to grpcs:// URLs"},
		{"bad ca", with(func(c *config.Source) {
			c.URL = "grpcs://localhost:1"
			c.TLS = &config.TLS{CAFile: "testdata/echo.proto"}
		}), "holds no PEM certificates"},
		{"no method", with(func(c *config.Source) { c.Method = "" }), "method is required"},
		{"bad method", with(func(c *config.Source) { c.Method = "Watch" }), `method "Watch" is not /package.Service/Method`},
		{"body", with(func(c *config.Source) { c.Body = "{nope" }), "body must be the request as JSON"},
		{"header template", with(func(c *config.Source) { c.Headers = map[string]string{"x-id": "{{ .id }}"} }), "the metadata of a subscribe call is static"},
		{"header key", with(func(c *config.Source) { c.Headers = map[string]string{"x id": "1"} }), "a gRPC metadata key holds only"},
		{"reserved header", with(func(c *config.Source) { c.Headers = map[string]string{"grpc-timeout": "1S"} }), "is set by gRPC itself"},
		{"header value", with(func(c *config.Source) { c.Headers = map[string]string{"x-a": "line\nbreak"} }), "not printable ASCII"},
		{"no reflection", with(func(c *config.Source) { c.Reflection = config.Ptr(false) }), "reflection: false needs descriptor_set"},
		{"services", with(func(c *config.Source) { c.Services = []string{"a.B"} }), "services applies to listen sources"},
		{"auth", with(func(c *config.Source) { c.Auth = &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t"}}} }), "auth applies to listen sources"},
		{"jitter", with(func(c *config.Source) { c.Reconnect = &config.Reconnect{Jitter: "some"} }), "reconnect.jitter must be full, equal or none"},
		{"attempts", with(func(c *config.Source) { c.Reconnect = &config.Reconnect{MaxAttempts: -1} }), "reconnect.max_attempts must not be negative"},
		{"ack", with(func(c *config.Source) { c.Ack = "maybe" }), "ack must be delivered or accepted"},
		{"unknown method", with(func(c *config.Source) { c.DescriptorSet = echoSet; c.Method = "/hikyaku.test.echo.v1.Feed/Nope" }), "service hikyaku.test.echo.v1.Feed has no method Nope (it has Watch, Peek)"},
		{"body shape", with(func(c *config.Source) { c.DescriptorSet = echoSet; c.Body = `{"limit":"many"}` }), "body does not fit hikyaku.test.echo.v1.WatchRequest"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.cfg.Mode = modeSubscribe
			cfg := sourceConfig(c.cfg)
			_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg})
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.True(t, strings.HasPrefix(err.Error(), "source in: "), err.Error())
		})
	}
}

func TestSourceModes(t *testing.T) {
	cfg := sourceConfig(config.Source{URL: "grpc://localhost:1", Method: watchMethod})
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg})
	require.NoError(t, err)
	assert.IsType(t, &Subscriber{}, s, "a url without listen subscribes")
	assert.Equal(t, "in", s.Name())

	cfg = sourceConfig(config.Source{Listen: "127.0.0.1:0"})
	s, err = newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg})
	require.NoError(t, err)
	assert.IsType(t, &Listener{}, s)
	assert.Equal(t, "in", s.Name())

	cfg = sourceConfig(config.Source{Mode: "poll", URL: "grpc://localhost:1"})
	_, err = newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: cfg})
	assert.ErrorContains(t, err, `grpc sources run in listen or subscribe mode, not "poll"`)

	_, err = newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind})
	assert.ErrorContains(t, err, "missing config")
}

func TestRegister(t *testing.T) {
	Register()
	_, ok := plugin.LookupSource(kind)
	assert.True(t, ok)
	_, ok = plugin.LookupSink(kind)
	assert.True(t, ok)
	cfg := &config.Endpoint{Type: kind, URL: "grpc://localhost:1", Method: sayMethod, DescriptorSet: echoSet}
	sink, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: kind, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, sink.Close(context.Background()))
	_, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: kind, Config: &config.Endpoint{Type: kind}})
	assert.Error(t, err)
}

func TestSubscribeSendsItsHeadersWithReflection(t *testing.T) {
	es := &echoServer{hold: true}
	srv := startEcho(t, es, serverOpts{token: "t0k"})
	s := newTestSubscriber(t, config.Source{URL: "grpc://" + srv.addr, Method: watchMethod, Body: `{"limit":1}`,
		Headers: map[string]string{"authorization": "Bearer t0k"}}, nil)
	out := make(chan *envelope.Envelope, 8)
	start(t, s, out)
	recv(t, out)
}
