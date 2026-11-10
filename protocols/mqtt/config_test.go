package mqtt

import (
	"context"
	"testing"
	"time"

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

func TestParseSourceDefaults(t *testing.T) {
	o, err := parseSource(sourceSpec(config.Source{URL: "mqtt://broker", Topic: "a/#"}))
	require.NoError(t, err)
	assert.Equal(t, []string{"a/#"}, o.filters)
	assert.Equal(t, byte(1), o.qos)
	assert.Equal(t, int64(1<<20), o.maxBytes)
	assert.Equal(t, version5, o.conn.version)
	assert.Equal(t, "hikyaku-relay-in", o.conn.clientID)
	assert.False(t, o.conn.cleanStart, "a persistent session by default")
	assert.Equal(t, uint32(24*60*60), o.conn.sessionExpiry)
	assert.Equal(t, uint16(64), o.conn.receiveMaximum)
	assert.True(t, o.conn.manualAck)
	assert.Equal(t, 30*time.Second, o.conn.keepAlive)
	assert.Nil(t, o.conn.tls)
	assert.Equal(t, reconnectOptions{backoff: 500 * time.Millisecond, maxBackoff: 30 * time.Second, jitter: "full"}, o.reconnect)
	require.Len(t, o.servers, 1)
	assert.Equal(t, "broker:1883", o.servers[0].addr)
}

func TestParseSourceSettings(t *testing.T) {
	o, err := parseSource(sourceSpec(config.Source{
		URL: "mqtts://broker", FailoverURLs: []string{"ws://edge:8080/mqtt", "tcp://backup:1884"},
		Topics: []string{"a/+", "b/#"}, Group: "workers", QoS: config.Ptr(2), ClientID: "fixed",
		CleanSession: config.Ptr(true), MaxBodyBytes: 2048, Ack: "accepted", Prefetch: 500,
		Username: "u", Password: "p",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(10 * time.Millisecond),
			Jitter: "equal", MaxAttempts: 3},
	}))
	require.NoError(t, err)
	assert.Equal(t, []string{"$share/workers/a/+", "$share/workers/b/#"}, o.filters)
	assert.Equal(t, byte(2), o.qos)
	assert.Equal(t, "fixed", o.conn.clientID)
	assert.True(t, o.conn.cleanStart)
	assert.Zero(t, o.conn.sessionExpiry, "a clean session ends with the connection")
	assert.Equal(t, int64(2048), o.maxBytes)
	assert.Equal(t, "accepted", o.ack)
	assert.Equal(t, uint16(500), o.conn.receiveMaximum)
	assert.Equal(t, "u", o.conn.username)
	assert.Equal(t, "p", o.conn.password)
	require.NotNil(t, o.conn.tls, "mqtts needs TLS")
	assert.Equal(t, reconnectOptions{backoff: time.Second, maxBackoff: time.Second, jitter: "equal", maxAttempts: 3}, o.reconnect)
	require.Len(t, o.servers, 3)
	assert.Equal(t, transportTLS, o.servers[0].transport)
	assert.Equal(t, "broker:8883", o.servers[0].addr)
	assert.Equal(t, transportWS, o.servers[1].transport)
	assert.Equal(t, "ws://edge:8080/mqtt", o.servers[1].wsURL)
	assert.Equal(t, "backup:1884", o.servers[2].addr)
}

func TestParseSourceErrors(t *testing.T) {
	ok := config.Source{URL: "mqtt://broker", Topic: "t"}
	for name, tc := range map[string]struct {
		mutate func(*config.Source)
		want   string
	}{
		"listen mode":        {func(c *config.Source) { c.Mode = "listen" }, "subscribe mode"},
		"no url":             {func(c *config.Source) { c.URL = "" }, "url is required"},
		"templated url":      {func(c *config.Source) { c.URL = "mqtt://{{ .json.host }}" }, "per-message template"},
		"bad scheme":         {func(c *config.Source) { c.URL = "http://broker" }, "scheme must be"},
		"no host":            {func(c *config.Source) { c.URL = "mqtt://:1883" }, "has no host"},
		"tcp path":           {func(c *config.Source) { c.URL = "mqtt://broker/path" }, "has no path"},
		"bad failover":       {func(c *config.Source) { c.FailoverURLs = []string{"udp://x"} }, "failover_urls[0]"},
		"no topic":           {func(c *config.Source) { c.Topic = "" }, "topic is required"},
		"topic and topics":   {func(c *config.Source) { c.Topics = []string{"u"} }, "not both"},
		"duplicate filter":   {func(c *config.Source) { c.Topic, c.Topics = "", []string{"a", "a"} }, "listed twice"},
		"hash not last":      {func(c *config.Source) { c.Topic = "a/#/b" }, "# must be the last level"},
		"partial hash":       {func(c *config.Source) { c.Topic = "a/b#" }, "# must take a whole level"},
		"partial plus":       {func(c *config.Source) { c.Topic = "a/b+/c" }, "+ must take a whole level"},
		"written share":      {func(c *config.Source) { c.Topic = "$share/g/a" }, "group:"},
		"nul":                {func(c *config.Source) { c.Topic = "a\x00b" }, "NUL"},
		"group on 3.1.1":     {func(c *config.Source) { c.Group, c.ProtocolVersion = "g", "3.1.1" }, "needs MQTT 5"},
		"group with slash":   {func(c *config.Source) { c.Group = "a/b" }, "without /, + or #"},
		"bad version":        {func(c *config.Source) { c.ProtocolVersion = "4" }, "protocol_version"},
		"bad qos":            {func(c *config.Source) { c.QoS = config.Ptr(3) }, "qos must be 0, 1 or 2"},
		"negative max body":  {func(c *config.Source) { c.MaxBodyBytes = -1 }, "max_body_bytes"},
		"bad ack":            {func(c *config.Source) { c.Ack = "sometimes" }, "ack must be"},
		"prefetch too large": {func(c *config.Source) { c.Prefetch = 70000 }, "prefetch"},
		"bad jitter":         {func(c *config.Source) { c.Reconnect = &config.Reconnect{Jitter: "some"} }, "reconnect.jitter"},
		"negative attempts":  {func(c *config.Source) { c.Reconnect = &config.Reconnect{MaxAttempts: -1} }, "max_attempts"},
		"bad ca file":        {func(c *config.Source) { c.TLS = &config.TLS{CAFile: "/does/not/exist"} }, "tls"},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := ok
			tc.mutate(&cfg)
			_, err := parseSource(sourceSpec(cfg))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "source in:")
		})
	}
	_, err := parseSource(plugin.Spec{Name: "in", Kind: kind})
	assert.ErrorContains(t, err, "missing config")
}

func TestParseServer(t *testing.T) {
	for raw, want := range map[string]server{
		"mqtt://b":            {transport: transportTCP, addr: "b:1883", shown: "mqtt://b"},
		"tcp://b:1999":        {transport: transportTCP, addr: "b:1999", shown: "tcp://b:1999"},
		"mqtts://b":           {transport: transportTLS, addr: "b:8883", shown: "mqtts://b"},
		"ssl://b":             {transport: transportTLS, addr: "b:8883", shown: "ssl://b"},
		"TLS://b:9":           {transport: transportTLS, addr: "b:9", shown: "tls://b:9"},
		"mqtt://[::1]":        {transport: transportTCP, addr: "[::1]:1883", shown: "mqtt://[::1]"},
		"ws://b/mqtt":         {transport: transportWS, wsURL: "ws://b/mqtt", shown: "ws://b/mqtt"},
		"wss://b:8443/mqtt":   {transport: transportWSS, wsURL: "wss://b:8443/mqtt", shown: "wss://b:8443/mqtt"},
		"mqtt://u:p@b":        {transport: transportTCP, addr: "b:1883", shown: "mqtt://u:xxxxx@b", user: "u", hasUser: true, password: "p", hasPassword: true},
		"ws://u:p@b/mqtt":     {transport: transportWS, wsURL: "ws://b/mqtt", shown: "ws://u:xxxxx@b/mqtt", user: "u", hasUser: true, password: "p", hasPassword: true},
		"mqtt://only-user@b/": {transport: transportTCP, addr: "b:1883", shown: "mqtt://only-user@b/", user: "only-user", hasUser: true},
	} {
		got, err := parseServer("url", raw, false)
		require.NoError(t, err, raw)
		assert.Equal(t, want, got, raw)
	}

	// A tls block upgrades a plain URL.
	got, err := parseServer("url", "mqtt://b", true)
	require.NoError(t, err)
	assert.Equal(t, transportTLS, got.transport)
	assert.Equal(t, "b:8883", got.addr)
	got, err = parseServer("url", "ws://b/mqtt", true)
	require.NoError(t, err)
	assert.Equal(t, "wss://b/mqtt", got.wsURL)

	_, err = parseServer("url", "mqtt://user:pa%zzword@b", false)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "pa%zzword", "an unparsable URL is not echoed")
}

func TestCredentials(t *testing.T) {
	srv, err := parseServer("url", "mqtt://url-user:url-pass@b", false)
	require.NoError(t, err)
	user, pass, hasUser, hasPass := connOptions{}.credentials(srv)
	assert.Equal(t, []any{"url-user", "url-pass", true, true}, []any{user, pass, hasUser, hasPass})
	user, pass, _, _ = connOptions{username: "field-user", password: "field-pass"}.credentials(srv)
	assert.Equal(t, "field-user", user, "the fields win")
	assert.Equal(t, "field-pass", pass)
	_, _, hasUser, hasPass = connOptions{}.credentials(server{})
	assert.False(t, hasUser)
	assert.False(t, hasPass)
}

func TestParseSink(t *testing.T) {
	o, err := parseSink(sinkSpec(config.Endpoint{URL: "mqtt://broker", Topic: "out/{{ .subject }}",
		Headers: map[string]string{"x-tenant": "{{ .json.tenant }}"}, Body: "{{ .json.payload }}", Retain: true}))
	require.NoError(t, err)
	assert.Equal(t, byte(1), o.qos)
	assert.True(t, o.retain)
	assert.Equal(t, version5, o.conn.version)
	assert.Equal(t, "hikyaku-relay-out", o.conn.clientID)
	assert.True(t, o.conn.cleanStart, "a destination keeps no session")
	assert.Zero(t, o.conn.sessionExpiry)
	assert.False(t, o.conn.manualAck)
	assert.Contains(t, o.headers, "X-Tenant")
	assert.NotNil(t, o.body)

	o, err = parseSink(sinkSpec(config.Endpoint{URL: "wss://broker/mqtt", Topic: "t", QoS: config.Ptr(0),
		ProtocolVersion: "3.1.1", ClientID: "pub"}))
	require.NoError(t, err)
	assert.Equal(t, byte(0), o.qos)
	assert.Equal(t, version311, o.conn.version)
	assert.Equal(t, "pub", o.conn.clientID)
	assert.NotNil(t, o.conn.tls, "wss needs TLS")

	for name, tc := range map[string]struct {
		cfg  config.Endpoint
		want string
	}{
		"no url":         {config.Endpoint{Topic: "t"}, "url is required"},
		"no topic":       {config.Endpoint{URL: "mqtt://b"}, "topic is required"},
		"wildcard topic": {config.Endpoint{URL: "mqtt://b", Topic: "a/+"}, "wildcards"},
		"bad template":   {config.Endpoint{URL: "mqtt://b", Topic: "{{ .x"}, "template"},
		"bad body":       {config.Endpoint{URL: "mqtt://b", Topic: "t", Body: "{{ end }}"}, "template"},
		"bad header":     {config.Endpoint{URL: "mqtt://b", Topic: "t", Headers: map[string]string{"a": "{{"}}, "template"},
		"bad qos":        {config.Endpoint{URL: "mqtt://b", Topic: "t", QoS: config.Ptr(-1)}, "qos"},
		"bad version":    {config.Endpoint{URL: "mqtt://b", Topic: "t", ProtocolVersion: "3"}, "protocol_version"},
		"bad scheme":     {config.Endpoint{URL: "amqp://b", Topic: "t"}, "scheme"},
	} {
		_, err := parseSink(sinkSpec(tc.cfg))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), tc.want, name)
		assert.Contains(t, err.Error(), "destination out:", name)
	}
	_, err = parseSink(plugin.Spec{Name: "out", Kind: kind})
	assert.ErrorContains(t, err, "missing config")
}

func TestClientID(t *testing.T) {
	assert.Equal(t, "mine", clientID("mine", "relay", "src"))
	assert.Equal(t, "hikyaku-relay-src", clientID("", "relay", "src"))
	assert.Equal(t, "hikyaku-src", clientID("", "", "src"))
}

func TestRegister(t *testing.T) {
	Register()
	assert.Contains(t, plugin.SourceKinds(), kind)
	assert.Contains(t, plugin.SinkKinds(), kind)
	src, err := plugin.NewSource(context.Background(), sourceSpec(config.Source{URL: "mqtt://b", Topic: "t"}))
	require.NoError(t, err)
	assert.Equal(t, "in", src.Name())
	sink, err := plugin.NewSink(context.Background(), sinkSpec(config.Endpoint{URL: "mqtt://b", Topic: "t"}))
	require.NoError(t, err)
	assert.Equal(t, "out", sink.Name())
	require.NoError(t, sink.Close(context.Background()))
	_, err = plugin.NewSource(context.Background(), sourceSpec(config.Source{Topic: "t"}))
	assert.Error(t, err)
}

func TestDetectContentType(t *testing.T) {
	assert.Equal(t, contentJSON, detectContentType([]byte(`{"a":1}`)))
	assert.Equal(t, contentJSON, detectContentType([]byte(`42`)))
	assert.Equal(t, contentText, detectContentType([]byte("hello")))
	assert.Equal(t, contentBinary, detectContentType([]byte{0xff, 0x00}))
}

func TestKeepAliveSeconds(t *testing.T) {
	assert.Equal(t, uint16(0), keepAliveSeconds(0))
	assert.Equal(t, uint16(1), keepAliveSeconds(10*time.Millisecond))
	assert.Equal(t, uint16(30), keepAliveSeconds(30*time.Second))
	assert.Equal(t, uint16(65535), keepAliveSeconds(1000*time.Hour))
}
