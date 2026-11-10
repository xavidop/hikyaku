package kafka

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestSourceDefaults(t *testing.T) {
	o, err := parseSource(plugin.Spec{Name: "in", Kind: Kind, Env: plugin.Env{Relay: "r"},
		Config: &config.Source{Brokers: []string{"k1:9092", " k2 "}, Topic: "orders", Group: "g"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"k1:9092", "k2"}, o.brokers)
	assert.Equal(t, []string{"orders"}, o.topics)
	assert.Equal(t, "g", o.group)
	assert.Equal(t, "latest", o.start)
	assert.Equal(t, kgo.NewOffset().AtEnd(), o.startOffset())
	assert.Equal(t, ackDelivered, o.ack)
	assert.Equal(t, 64, o.prefetch)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.reconnect)
	assert.Equal(t, "hikyaku-r-in", o.client.clientID)
	assert.Nil(t, o.client.sasl)
	assert.Nil(t, o.client.tls)

	o, err = parseSource(plugin.Spec{Name: "in", Kind: Kind, Config: &config.Source{Mode: "subscribe",
		Brokers: []string{"k1"}, Topics: []string{"b", "a", "b"}, Group: "g", Start: "earliest", Ack: "accepted",
		Prefetch: 8, MaxBodyBytes: 1024, ClientID: "mine",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Minute), MaxBackoff: config.Duration(time.Second), Jitter: "none", MaxAttempts: 3},
		SASL:      &config.SASL{Mechanism: "scram-sha-512", Username: "u", Password: "p"},
		TLS:       &config.TLS{Enabled: config.Ptr(true), ServerName: "kafka.internal"}}})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, o.topics, "sorted, without repeats")
	assert.Equal(t, kgo.NewOffset().AtStart(), o.startOffset())
	assert.Equal(t, ackAccepted, o.ack)
	assert.Equal(t, 8, o.prefetch)
	assert.Equal(t, int64(1024), o.maxBytes)
	assert.Equal(t, reconnectOptions{backoff: time.Minute, maxBackoff: time.Minute, jitter: "none", maxAttempts: 3}, o.reconnect,
		"max_backoff is at least backoff")
	assert.Equal(t, "mine", o.client.clientID)
	require.NotNil(t, o.client.sasl)
	assert.Equal(t, "SCRAM-SHA-512", o.client.sasl.Name())
	require.NotNil(t, o.client.tls)
	assert.Equal(t, "kafka.internal", o.client.tls.ServerName)
}

func TestSourceConfigErrors(t *testing.T) {
	base := func(f func(*config.Source)) config.Source {
		s := config.Source{Brokers: []string{"k:9092"}, Topic: "t", Group: "g"}
		f(&s)
		return s
	}
	cases := map[string]struct {
		cfg  config.Source
		want string
	}{
		"listen mode":      {base(func(s *config.Source) { s.Mode = "listen" }), `subscribe mode, not "listen"`},
		"url":              {base(func(s *config.Source) { s.URL = "kafka://k" }), "take brokers"},
		"no brokers":       {base(func(s *config.Source) { s.Brokers = nil }), "brokers is required"},
		"empty broker":     {base(func(s *config.Source) { s.Brokers = []string{""} }), "brokers[0] is empty"},
		"url broker":       {base(func(s *config.Source) { s.Brokers = []string{"PLAINTEXT://k:9092"} }), "not URLs"},
		"template broker":  {base(func(s *config.Source) { s.Brokers = []string{"{{ .json.b }}"} }), "per-message template"},
		"bad port":         {base(func(s *config.Source) { s.Brokers = []string{"k:http"} }), "bad port"},
		"no topic":         {base(func(s *config.Source) { s.Topic = "" }), "topic (or topics) is required"},
		"topic and topics": {base(func(s *config.Source) { s.Topics = []string{"u"} }), "not both"},
		"bad topic":        {base(func(s *config.Source) { s.Topic = "a b" }), `"a b" has ' '`},
		"dot topic":        {base(func(s *config.Source) { s.Topic = ".." }), "not a valid name"},
		"long topic":       {base(func(s *config.Source) { s.Topic = strings.Repeat("t", 250) }), "longer than 249"},
		"template topic":   {base(func(s *config.Source) { s.Topic = "{{ .x }}" }), "cannot be a per-message template"},
		"no group":         {base(func(s *config.Source) { s.Group = "" }), "group is required"},
		"template group":   {base(func(s *config.Source) { s.Group = "{{ .x }}" }), "group cannot be"},
		"bad start":        {base(func(s *config.Source) { s.Start = "new" }), "start must be earliest or latest"},
		"bad ack":          {base(func(s *config.Source) { s.Ack = "maybe" }), "ack must be delivered or accepted"},
		"negative fetch":   {base(func(s *config.Source) { s.Prefetch = -1 }), "prefetch"},
		"negative max":     {base(func(s *config.Source) { s.MaxBodyBytes = -1 }), "max_body_bytes"},
		"bad jitter":       {base(func(s *config.Source) { s.Reconnect = &config.Reconnect{Jitter: "x"} }), "reconnect.jitter"},
		"negative tries":   {base(func(s *config.Source) { s.Reconnect = &config.Reconnect{MaxAttempts: -1} }), "reconnect.max_attempts"},
		"bad mechanism":    {base(func(s *config.Source) { s.SASL = &config.SASL{Mechanism: "gssapi", Username: "u", Password: "p"} }), "sasl.mechanism"},
		"no password":      {base(func(s *config.Source) { s.SASL = &config.SASL{Mechanism: "plain", Username: "u"} }), "username and password"},
		"bad tls":          {base(func(s *config.Source) { s.TLS = &config.TLS{CAFile: "/nope.pem"} }), "tls"},
	}
	for name, c := range cases {
		_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: Kind, Config: &c.cfg})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "source in: ", name)
		assert.Contains(t, err.Error(), c.want, name)
	}
	_, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: Kind})
	assert.ErrorContains(t, err, "missing config")
}

func TestSinkConfig(t *testing.T) {
	o, err := parseSink(plugin.Spec{Name: "out", Kind: Kind, Config: &config.Endpoint{Brokers: []string{"k:9092"}, Topic: "billing.retry"}})
	require.NoError(t, err)
	assert.True(t, o.topic.IsStatic())
	assert.Nil(t, o.key)
	assert.Nil(t, o.body)
	assert.Empty(t, o.headers)
	assert.Equal(t, acksAll, o.acks)
	assert.Equal(t, kgo.NoCompression(), o.compression)
	assert.Equal(t, "hikyaku-out", o.client.clientID)

	o, err = parseSink(plugin.Spec{Name: "out", Kind: Kind, Config: &config.Endpoint{Brokers: []string{"k:9092"},
		Topic: "{{ .json.kind }}", Key: "{{ .json.id }}", Body: "{{ .bodyText }}", Acks: "none", Compression: "zstd",
		Headers: map[string]string{"b": "2", "a": "{{ .id }}"}}})
	require.NoError(t, err)
	assert.False(t, o.topic.IsStatic())
	assert.NotNil(t, o.key)
	assert.NotNil(t, o.body)
	require.Len(t, o.headers, 2)
	assert.Equal(t, "a", o.headers[0].name, "headers are sorted")
	assert.Equal(t, acksNone, o.acks)
	assert.Equal(t, kgo.ZstdCompression(), o.compression)

	cases := map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"url":             {config.Endpoint{URL: "kafka://k", Brokers: []string{"k"}, Topic: "t"}, "take brokers"},
		"no brokers":      {config.Endpoint{Topic: "t"}, "brokers is required"},
		"no topic":        {config.Endpoint{Brokers: []string{"k"}}, "topic is required"},
		"bad topic":       {config.Endpoint{Brokers: []string{"k"}, Topic: "a/b"}, `"a/b" has '/'`},
		"broken topic":    {config.Endpoint{Brokers: []string{"k"}, Topic: "{{ .json.x "}, "topic:"},
		"broken key":      {config.Endpoint{Brokers: []string{"k"}, Topic: "t", Key: "{{ end }}"}, "key:"},
		"broken body":     {config.Endpoint{Brokers: []string{"k"}, Topic: "t", Body: "{{ .x"}, "body:"},
		"broken header":   {config.Endpoint{Brokers: []string{"k"}, Topic: "t", Headers: map[string]string{"X": "{{"}}, "headers.X"},
		"empty header":    {config.Endpoint{Brokers: []string{"k"}, Topic: "t", Headers: map[string]string{" ": "v"}}, "header name is empty"},
		"bad acks":        {config.Endpoint{Brokers: []string{"k"}, Topic: "t", Acks: "some"}, "acks must be all, leader or none"},
		"bad compression": {config.Endpoint{Brokers: []string{"k"}, Topic: "t", Compression: "brotli"}, "compression must be"},
		"bad sasl":        {config.Endpoint{Brokers: []string{"k"}, Topic: "t", SASL: &config.SASL{Mechanism: "plain"}}, "username and password"},
		"bad tls":         {config.Endpoint{Brokers: []string{"k"}, Topic: "t", TLS: &config.TLS{CertFile: "/nope.pem", KeyFile: "/nope.key"}}, "tls"},
	}
	for name, c := range cases {
		_, err := newSink(context.Background(), plugin.Spec{Name: "out", Kind: Kind, Config: &c.ep})
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "destination out: ", name)
		assert.Contains(t, err.Error(), c.want, name)
	}
	_, err = newSink(context.Background(), plugin.Spec{Name: "out", Kind: Kind, Config: &config.Source{}})
	assert.ErrorContains(t, err, "missing config")
}

func TestProducerOptions(t *testing.T) {
	// The idempotent producer needs acks=all; leader and none turn it off.
	for _, acks := range []string{acksAll, acksLeader, acksNone} {
		o := sinkOptions{acks: acks, compression: kgo.GzipCompression()}
		cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1")}, o.producerOpts()...)...)
		require.NoError(t, err, acks)
		cl.Close()
	}
}

func TestClientID(t *testing.T) {
	assert.Equal(t, "set", clientID("set", "r", "n"))
	assert.Equal(t, "hikyaku-r-n", clientID("", "r", "n"))
	assert.Equal(t, "hikyaku-n", clientID("", "", "n"))
}

func TestDetectContentType(t *testing.T) {
	assert.Equal(t, "", detectContentType(nil))
	assert.Equal(t, contentJSON, detectContentType([]byte(`[1]`)))
	assert.Equal(t, contentText, detectContentType([]byte("héllo")))
	assert.Equal(t, contentBinary, detectContentType([]byte{0xff}))
}
