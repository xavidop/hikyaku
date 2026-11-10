package nats

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestRegister(t *testing.T) {
	Register()
	for _, kind := range []string{KindNATS, KindJetStream} {
		_, ok := plugin.LookupSource(kind)
		assert.True(t, ok, kind)
		_, ok = plugin.LookupSink(kind)
		assert.True(t, ok, kind)
	}
	src, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "in", Kind: KindJetStream,
		Config: &config.Source{URL: "nats://127.0.0.1:4222", Stream: "ORDERS"}})
	require.NoError(t, err)
	assert.IsType(t, &StreamSource{}, src)
	assert.Implements(t, (*plugin.Closer)(nil), src)
	sk, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "out", Kind: KindJetStream,
		Config: &config.Endpoint{URL: "nats://127.0.0.1:4222", Subject: "orders.new"}})
	require.NoError(t, err)
	assert.Implements(t, (*plugin.BatchSink)(nil), sk)
	assert.Implements(t, (*plugin.Starter)(nil), sk)
	require.NoError(t, sk.Close(context.Background()))
}

func TestSourceDefaults(t *testing.T) {
	o, err := parseSource(plugin.Spec{Name: "orders.in", Env: plugin.Env{Relay: "edge"}, Config: &config.Source{
		URL: "nats://a:4222, nats://b:4222", FailoverURLs: []string{"tls://c:4443"}, Stream: "ORDERS",
	}}, KindJetStream)
	require.NoError(t, err)
	assert.Equal(t, []string{"nats://a:4222", "nats://b:4222", "tls://c:4443"}, o.conn.servers)
	assert.Equal(t, "hikyaku-edge-orders.in", o.conn.name)
	assert.Equal(t, "orders_in", o.durable, "a dot is not allowed in a consumer name")
	assert.Equal(t, 64, o.prefetch)
	assert.Equal(t, 30*time.Second, o.ackWait)
	assert.Equal(t, jetstream.DeliverNewPolicy, o.deliver)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, ackDelivered, o.ack)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.conn.reconnect)

	o, err = parseSource(plugin.Spec{Name: "in", Config: &config.Source{
		URL: "nats://a:4222", Stream: "S", Subject: "orders.>", Durable: "d", Start: "all", Prefetch: 5,
		AckWait: config.Duration(time.Second), Ack: "accepted", MaxBodyBytes: 10,
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Millisecond), Jitter: "none", MaxAttempts: 3},
	}}, KindJetStream)
	require.NoError(t, err)
	assert.Equal(t, "hikyaku-in", o.conn.name)
	assert.Equal(t, "d", o.durable)
	assert.Equal(t, "orders.>", o.subject)
	assert.Equal(t, jetstream.DeliverAllPolicy, o.deliver)
	assert.Equal(t, 5, o.prefetch)
	assert.Equal(t, time.Second, o.ackWait)
	assert.Equal(t, ackAccepted, o.ack)
	assert.Equal(t, int64(10), o.maxBytes)
	assert.Equal(t, reconnectOptions{backoff: time.Second, maxBackoff: time.Second, jitter: "none", maxAttempts: 3}, o.conn.reconnect)

	o, err = parseSource(plugin.Spec{Name: "in", Config: &config.Source{URL: "nats://a:4222", Stream: "S", Start: "last"}}, KindJetStream)
	require.NoError(t, err)
	assert.Equal(t, jetstream.DeliverLastPolicy, o.deliver)

	o, err = parseSource(plugin.Spec{Name: "in", Config: &config.Source{URL: "wss://a:443", Subject: "a.*.c", Queue: "workers"}}, KindNATS)
	require.NoError(t, err)
	assert.Equal(t, "workers", o.queue)
	assert.Equal(t, "a.*.c", o.subject)
}

func TestSourceConfigErrors(t *testing.T) {
	credsFile := filepath.Join(t.TempDir(), "user.creds")
	require.NoError(t, os.WriteFile(credsFile, []byte("x"), 0o600))
	base := func(kind string) config.Source {
		if kind == KindJetStream {
			return config.Source{URL: "nats://a:4222", Stream: "S"}
		}
		return config.Source{URL: "nats://a:4222", Subject: "orders"}
	}
	cases := []struct {
		name   string
		kind   string
		change func(*config.Source)
		want   string
	}{
		{"listen mode", KindNATS, func(s *config.Source) { s.Mode = "listen" }, "subscribe mode"},
		{"no url", KindNATS, func(s *config.Source) { s.URL = "" }, "url is required"},
		{"bad scheme", KindNATS, func(s *config.Source) { s.URL = "http://a:4222" }, "must start with nats://"},
		{"no host", KindNATS, func(s *config.Source) { s.URL = "nats://" }, "has no host"},
		{"template url", KindNATS, func(s *config.Source) { s.URL = "nats://{{ .json.host }}:4222" }, "per-message template"},
		{"bad failover", KindNATS, func(s *config.Source) { s.FailoverURLs = []string{"mqtt://b"} }, "failover_urls[0]"},
		{"mixed websocket", KindNATS, func(s *config.Source) { s.FailoverURLs = []string{"ws://b:80"} }, "mix ws://"},
		{"no subject", KindNATS, func(s *config.Source) { s.Subject = "" }, "subject is required"},
		{"bad subject", KindNATS, func(s *config.Source) { s.Subject = "orders..new" }, "empty token"},
		{"partial wildcard", KindNATS, func(s *config.Source) { s.Subject = "orders.new*" }, "whole token"},
		{"gt not last", KindNATS, func(s *config.Source) { s.Subject = "orders.>.new" }, "whole token"},
		{"subject space", KindNATS, func(s *config.Source) { s.Subject = "orders new" }, "whitespace"},
		{"queue space", KindNATS, func(s *config.Source) { s.Queue = "a b" }, "queue"},
		{"stream on nats", KindNATS, func(s *config.Source) { s.Stream = "S" }, "stream applies to jetstream"},
		{"durable on nats", KindNATS, func(s *config.Source) { s.Durable = "d" }, "durable applies to jetstream"},
		{"ack_wait on nats", KindNATS, func(s *config.Source) { s.AckWait = config.Duration(time.Second) }, "ack_wait applies"},
		{"start on nats", KindNATS, func(s *config.Source) { s.Start = "all" }, "start applies"},
		{"bad ack", KindNATS, func(s *config.Source) { s.Ack = "maybe" }, "ack must be"},
		{"negative prefetch", KindNATS, func(s *config.Source) { s.Prefetch = -1 }, "prefetch"},
		{"negative max body", KindNATS, func(s *config.Source) { s.MaxBodyBytes = -1 }, "max_body_bytes"},
		{"bad jitter", KindNATS, func(s *config.Source) { s.Reconnect = &config.Reconnect{Jitter: "some"} }, "reconnect.jitter"},
		{"negative attempts", KindNATS, func(s *config.Source) { s.Reconnect = &config.Reconnect{MaxAttempts: -1} }, "max_attempts"},
		{"token and user", KindNATS, func(s *config.Source) { s.Token, s.Username = "t", "u" }, "exclusive"},
		{"creds and token", KindNATS, func(s *config.Source) { s.Token, s.CredsFile = "t", credsFile }, "exclusive"},
		{"password alone", KindNATS, func(s *config.Source) { s.Password = "p" }, "password needs username"},
		{"missing creds", KindNATS, func(s *config.Source) { s.CredsFile = filepath.Join(t.TempDir(), "none.creds") }, "creds_file"},
		{"bad tls", KindNATS, func(s *config.Source) { s.TLS = &config.TLS{CAFile: filepath.Join(t.TempDir(), "none.pem")} }, "tls"},
		{"no stream", KindJetStream, func(s *config.Source) { s.Stream = "" }, "stream is required"},
		{"bad stream", KindJetStream, func(s *config.Source) { s.Stream = "a.b" }, "stream"},
		{"bad durable", KindJetStream, func(s *config.Source) { s.Durable = "a b" }, "durable"},
		{"queue on jetstream", KindJetStream, func(s *config.Source) { s.Queue = "q" }, "queue applies to nats"},
		{"bad filter", KindJetStream, func(s *config.Source) { s.Subject = "a..b" }, "empty token"},
		{"negative ack_wait", KindJetStream, func(s *config.Source) { s.AckWait = -1 }, "ack_wait must be at least 100ms"},
		{"tiny ack_wait", KindJetStream, func(s *config.Source) { s.AckWait = config.Duration(time.Millisecond) }, "ack_wait must be at least"},
		{"kafka start", KindJetStream, func(s *config.Source) { s.Start = "earliest" }, "start must be all, new or last"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := base(tc.kind)
			tc.change(&src)
			_, err := parseSource(plugin.Spec{Name: "in", Config: &src}, tc.kind)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "source in:")
		})
	}
	_, err := parseSource(plugin.Spec{Name: "in"}, KindNATS)
	assert.ErrorContains(t, err, "missing config")
}

func TestSinkConfig(t *testing.T) {
	o, err := parseSink(plugin.Spec{Name: "out", Env: plugin.Env{Relay: "r"}, Config: &config.Endpoint{
		URL: "nats://a:4222,nats://b:4222", Subject: "orders.{{ .json.region }}", Stream: "ORDERS",
		Headers: map[string]string{"X-Tenant": "{{ .json.tenant }}"}, Body: "{{ .json.payload }}",
	}}, KindJetStream)
	require.NoError(t, err)
	assert.Equal(t, []string{"nats://a:4222", "nats://b:4222"}, o.conn.servers)
	assert.Equal(t, "hikyaku-r-out", o.conn.name)
	assert.Equal(t, "ORDERS", o.stream)
	assert.False(t, o.subject.IsStatic())
	assert.Contains(t, o.headers, "X-Tenant")
	assert.NotNil(t, o.body)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.conn.reconnect)

	cases := []struct {
		name   string
		kind   string
		change func(*config.Endpoint)
		want   string
	}{
		{"no url", KindNATS, func(e *config.Endpoint) { e.URL = "" }, "url is required"},
		{"bad scheme", KindNATS, func(e *config.Endpoint) { e.URL = "redis://a" }, "must start with nats://"},
		{"no subject", KindNATS, func(e *config.Endpoint) { e.Subject = "" }, "subject is required"},
		{"wildcard subject", KindNATS, func(e *config.Endpoint) { e.Subject = "orders.*" }, "wildcard"},
		{"bad template", KindNATS, func(e *config.Endpoint) { e.Subject = "orders.{{ .json.x" }, "template"},
		{"bad header name", KindNATS, func(e *config.Endpoint) { e.Headers = map[string]string{"a:b": "x"} }, "header name"},
		{"bad header template", KindNATS, func(e *config.Endpoint) { e.Headers = map[string]string{"X": "{{"} }, "template"},
		{"bad body template", KindNATS, func(e *config.Endpoint) { e.Body = "{{ end }}" }, "template"},
		{"stream on nats", KindNATS, func(e *config.Endpoint) { e.Stream = "S" }, "stream applies to jetstream"},
		{"bad stream", KindJetStream, func(e *config.Endpoint) { e.Stream = "a*" }, "stream"},
		{"token and user", KindJetStream, func(e *config.Endpoint) { e.Token, e.Username = "t", "u" }, "exclusive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep := config.Endpoint{URL: "nats://a:4222", Subject: "orders"}
			tc.change(&ep)
			_, err := parseSink(plugin.Spec{Name: "out", Config: &ep}, tc.kind)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "destination out:")
		})
	}
	_, err = parseSink(plugin.Spec{Name: "out"}, KindNATS)
	assert.ErrorContains(t, err, "missing config")
}

func TestConfigErrorsHideCredentials(t *testing.T) {
	_, err := parseSource(plugin.Spec{Name: "in", Config: &config.Source{
		URL: "http://user:hunter2@a:4222", Subject: "x"}}, KindNATS)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "hunter2")
	_, err = parseSource(plugin.Spec{Name: "in", Config: &config.Source{
		URL: "nats://s3cr3t-token@a:4222", FailoverURLs: []string{"mqtt://s3cr3t-token@b"}, Subject: "x"}}, KindNATS)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "s3cr3t-token")
}

func TestRedactURL(t *testing.T) {
	cases := map[string]string{
		"nats://a:4222":                 "nats://a:4222",
		"nats://user:hunter2@a:4222":    "nats://user:xxxxx@a:4222",
		"nats://s3cr3t@a:4222":          "nats://xxxxx@a:4222",
		"tls://u:p@a:4443/x":            "tls://u:xxxxx@a:4443/x",
		"nats://u:p%zz@a:4222":          "nats://xxxxx@a:4222",
		"u:p%zz@a:4222":                 "xxxxx@a:4222",
		"user:hunter2@a:4222":           "xxxxx@a:4222",
		"":                              "",
		"wss://token@nats.example.com/": "wss://xxxxx@nats.example.com/",
	}
	for in, want := range cases {
		assert.Equal(t, want, redactURL(in), in)
	}
}

func TestCheckSubject(t *testing.T) {
	for _, s := range []string{"a", "a.b.c", "orders.eu-west.new", "_INBOX.x"} {
		assert.NoError(t, checkSubject(s, false), s)
	}
	for _, s := range []string{"a.*", "a.>", "*", ">", "a.*.c.>"} {
		assert.NoError(t, checkSubject(s, true), s)
		assert.Error(t, checkSubject(s, false), s)
	}
	for _, s := range []string{"", ".a", "a.", "a..b", "a b", "a\tb", "a.b*", "a.>.b", "a.>x"} {
		assert.Error(t, checkSubject(s, true), s)
	}
}

func TestHelpers(t *testing.T) {
	assert.Equal(t, "a_b_c_d", durableName("a.b*c>d"))
	assert.Equal(t, "my-source", durableName("my-source"))
	assert.Equal(t, "hikyaku-r-n", connName("r", "n"))
	assert.Equal(t, "hikyaku-n", connName("", "n"))
	assert.True(t, checkHeaderKey("X-Tenant"))
	for _, k := range []string{"", "a b", "a:b", "é"} {
		assert.False(t, checkHeaderKey(k), k)
	}

	h := nats.Header{"content-type": {"text/csv"}, "X-A": {"1"}}
	assert.Equal(t, "text/csv", headerValue(h, "Content-Type"))
	assert.Equal(t, "1", headerValue(h, "x-a"))
	assert.Equal(t, "", headerValue(h, "missing"))
	assert.Equal(t, "text/csv", contentType(h, []byte(`{}`)))
	assert.Equal(t, contentJSON, contentType(nil, []byte(`{"a":1}`)))
	assert.Equal(t, contentText, contentType(nil, []byte("hello")))
	assert.Equal(t, contentBinary, contentType(nil, []byte{0xff, 0xfe, 0x00}))
	assert.Equal(t, "", contentType(nil, nil))
	delHeader(h, "CONTENT-TYPE")
	assert.NotContains(t, h, "content-type")

	for _, k := range []string{"Authorization", "cookie", "Nats-Expected-Stream", "nats-rollup", "Nats-Msg-Id", "Content-Length", "a:b"} {
		assert.True(t, dropHeader(k), k)
	}
	for _, k := range []string{"X-Tenant", "Traceparent", "Content-Type", "Idempotency-Key"} {
		assert.False(t, dropHeader(k), k)
	}

	sc := traceOf(nats.Header{"traceparent": {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}})
	assert.True(t, sc.IsValid())
	assert.Equal(t, "4bf92f3577b34da6a3ce929d0e0e4736", sc.TraceID().String())
	assert.False(t, traceOf(nil).IsValid())
}
