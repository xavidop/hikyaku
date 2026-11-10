package tcp

import (
	"context"
	"net/netip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func TestParseSourceDefaults(t *testing.T) {
	o, err := parseSource(plugin.Spec{Name: "in", Kind: kindTCP, Config: &config.Source{Listen: ":9000"}})
	require.NoError(t, err)
	assert.Equal(t, ":9000", o.listen)
	assert.Equal(t, framing{mode: frameNewline}, o.frame)
	assert.Equal(t, 1<<20, o.maxBytes, "1MiB")
	assert.Nil(t, o.tls)
	assert.Empty(t, o.allow)

	o, err = parseSource(plugin.Spec{Name: "in", Kind: kindUDP, Config: &config.Source{
		Mode: "listen", Listen: "127.0.0.1:9000", Frame: "length", MaxBodyBytes: 512, Ack: "delivered",
		Auth: &config.Auth{IPAllow: []string{"10.0.0.0/8"}}, TLS: &config.TLS{Enabled: config.Ptr(false)},
	}})
	require.NoError(t, err)
	assert.Equal(t, framing{mode: frameLength, lengthBytes: 4}, o.frame)
	assert.Equal(t, 512, o.maxBytes)
	assert.Len(t, o.allow, 1)

	o, err = parseSource(plugin.Spec{Name: "in", Kind: kindTCP, Config: &config.Source{
		Listen: ":9000", TLS: &config.TLS{Enabled: config.Ptr(false), CertFile: "/nonexistent/c", KeyFile: "/nonexistent/k"},
	}})
	require.NoError(t, err, "enabled: false turns TLS off whatever else the block holds")
	assert.Nil(t, o.tls)

	p := newPKI(t)
	o, err = parseSource(plugin.Spec{Name: "in", Kind: kindUDP, Config: &config.Source{Listen: ":9000", TLS: p.mutualTLS}})
	require.NoError(t, err, "udp takes tls: DTLS")
	require.NotNil(t, o.tls)
	assert.Len(t, o.tls.Certificates, 1)
	assert.NotNil(t, o.tls.ClientCAs, "client_ca_file asks for client certificates")
}

func TestParseSourceErrors(t *testing.T) {
	cases := []struct {
		name string
		kind string
		cfg  *config.Source
		err  string
	}{
		{"missing config", kindTCP, nil, "source in: missing config"},
		{"unknown kind", "sctp", &config.Source{Listen: ":1"}, `source in: unknown kind "sctp" (tcp or udp)`},
		{"subscribe", kindTCP, &config.Source{Mode: "subscribe", Listen: ":1"}, `source in: tcp sources run in listen mode only, not "subscribe"`},
		{"no listen", kindUDP, &config.Source{}, "source in: listen is required (such as :9000)"},
		{"bad listen", kindTCP, &config.Source{Listen: "9000"}, `source in: listen "9000" is not host:port`},
		{"bad frame", kindTCP, &config.Source{Listen: ":1", Frame: "text"}, `source in: frame must be newline, length or raw, not "text"`},
		{"length_bytes without length", kindTCP, &config.Source{Listen: ":1", LengthBytes: 2}, "source in: length_bytes applies with frame: length only"},
		{"bad length_bytes", kindUDP, &config.Source{Listen: ":1", Frame: "length", LengthBytes: 3}, "source in: length_bytes must be 1, 2, 4 or 8, not 3"},
		{"negative max", kindTCP, &config.Source{Listen: ":1", MaxBodyBytes: -1}, "source in: max_body_bytes must not be negative"},
		{"ack accepted", kindTCP, &config.Source{Listen: ":1", Ack: "accepted"}, "source in: ack: accepted does not apply: tcp has no acknowledgements"},
		{"hmac", kindTCP, &config.Source{Listen: ":1", Auth: &config.Auth{HMAC: &config.HMACAuth{Header: "X", Secret: "s"}}}, "source in: auth.hmac does not apply to tcp sources"},
		{"bearer", kindUDP, &config.Source{Listen: ":1", Auth: &config.Auth{Bearer: &config.BearerAuth{Tokens: []string{"t"}}}}, "source in: auth.bearer does not apply to udp sources"},
		{"jwt", kindTCP, &config.Source{Listen: ":1", Auth: &config.Auth{JWT: &config.JWTAuth{JWKSURL: "https://x"}}}, "source in: auth.jwt does not apply to tcp sources"},
		{"api key", kindTCP, &config.Source{Listen: ":1", Auth: &config.Auth{APIKey: &config.APIKeyAuth{Keys: []string{"k"}}}}, "source in: auth.api_key does not apply to tcp sources"},
		{"bad ip_allow", kindTCP, &config.Source{Listen: ":1", Auth: &config.Auth{IPAllow: []string{"10.0.0.0/33"}}}, `source in: auth.ip_allow "10.0.0.0/33" is not an IP or CIDR`},
		{"tls without a certificate", kindTCP, &config.Source{Listen: ":1", TLS: &config.TLS{Enabled: config.Ptr(true)}}, "source in: tls: a listener needs cert_file and key_file"},
		{"client_ca_file without a certificate", kindTCP, &config.Source{Listen: ":1", TLS: &config.TLS{ClientCAFile: "ca.pem"}}, "source in: tls: a listener needs cert_file and key_file"},
		{"an empty tls block", kindTCP, &config.Source{Listen: ":1", TLS: &config.TLS{}}, "source in: tls: a listener needs cert_file and key_file"},
		{"tls with a missing certificate", kindTCP, &config.Source{Listen: ":1", TLS: &config.TLS{CertFile: "/nonexistent/c", KeyFile: "/nonexistent/k"}}, "source in: tls: open /nonexistent/c"},
		// udp takes the same tls keys, for DTLS.
		{"dtls without a certificate", kindUDP, &config.Source{Listen: ":1", TLS: &config.TLS{Enabled: config.Ptr(true)}}, "source in: tls: a listener needs cert_file and key_file"},
		{"dtls client_ca_file without a certificate", kindUDP, &config.Source{Listen: ":1", TLS: &config.TLS{ClientCAFile: "ca.pem"}}, "source in: tls: a listener needs cert_file and key_file"},
		{"an empty tls block on udp", kindUDP, &config.Source{Listen: ":1", TLS: &config.TLS{}}, "source in: tls: a listener needs cert_file and key_file"},
		{"dtls with a missing certificate", kindUDP, &config.Source{Listen: ":1", TLS: &config.TLS{CertFile: "/nonexistent/c", KeyFile: "/nonexistent/k"}}, "source in: tls: open /nonexistent/c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := plugin.Spec{Name: "in", Kind: tc.kind}
			if tc.cfg != nil {
				spec.Config = tc.cfg
			}
			_, err := parseSource(spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
		})
	}
}

func TestParseSink(t *testing.T) {
	o, err := parseSink(plugin.Spec{Name: "out", Kind: kindTCP, Config: &config.Endpoint{URL: "tcp://relay.internal:9000/"}})
	require.NoError(t, err)
	assert.Equal(t, sinkOptions{network: "tcp", addr: "relay.internal:9000", endpoint: "tcp://relay.internal:9000", frame: framing{mode: frameNewline}}, o)

	o, err = parseSink(plugin.Spec{Name: "out", Kind: kindUDP, Config: &config.Endpoint{URL: "udp://[::1]:514", Frame: "raw", Body: "{{ .bodyText }}"}})
	require.NoError(t, err)
	assert.Equal(t, "[::1]:514", o.addr)
	assert.Equal(t, "udp", o.network)
	assert.Equal(t, framing{mode: frameRaw}, o.frame)
	assert.NotNil(t, o.body)
	assert.Nil(t, o.tls)
}

func TestParseSinkTLS(t *testing.T) {
	cases := []struct {
		name       string
		kind       string
		url        string
		tls        *config.TLS
		on         bool
		serverName string
	}{
		{name: "tcp:// is plain", url: "tcp://relay:1"},
		{name: "tcp:// with an empty tls block is plain", url: "tcp://relay:1", tls: &config.TLS{}},
		{name: "tls:// turns TLS on", url: "tls://relay:1", on: true, serverName: "relay"},
		{name: "tls:// keeps server_name", url: "tls://10.0.0.1:1", tls: &config.TLS{ServerName: "relay.internal"}, on: true, serverName: "relay.internal"},
		{name: "tcp:// with enabled", url: "tcp://relay:1", tls: &config.TLS{Enabled: config.Ptr(true)}, on: true, serverName: "relay"},
		{name: "tcp:// with insecure_skip_verify", url: "tcp://relay:1", tls: &config.TLS{InsecureSkipVerify: true}, on: true, serverName: "relay"},
		{name: "enabled: false wins over other fields", url: "tcp://relay:1", tls: &config.TLS{Enabled: config.Ptr(false), ServerName: "x"}},
		// udp follows the same rules, with dtls:// for DTLS.
		{name: "udp:// is plain", kind: kindUDP, url: "udp://relay:1"},
		{name: "udp:// with an empty tls block is plain", kind: kindUDP, url: "udp://relay:1", tls: &config.TLS{}},
		{name: "dtls:// turns DTLS on", kind: kindUDP, url: "dtls://relay:1", on: true, serverName: "relay"},
		{name: "dtls:// keeps server_name", kind: kindUDP, url: "dtls://10.0.0.1:1", tls: &config.TLS{ServerName: "relay.internal"}, on: true, serverName: "relay.internal"},
		{name: "udp:// with enabled", kind: kindUDP, url: "udp://relay:1", tls: &config.TLS{Enabled: config.Ptr(true)}, on: true, serverName: "relay"},
		{name: "udp:// with insecure_skip_verify", kind: kindUDP, url: "udp://relay:1", tls: &config.TLS{InsecureSkipVerify: true}, on: true, serverName: "relay"},
		{name: "enabled: false wins on udp://", kind: kindUDP, url: "udp://relay:1", tls: &config.TLS{Enabled: config.Ptr(false), ServerName: "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			kind := tc.kind
			if kind == "" {
				kind = kindTCP
			}
			o, err := parseSink(plugin.Spec{Name: "out", Kind: kind, Config: &config.Endpoint{URL: tc.url, TLS: tc.tls}})
			require.NoError(t, err)
			if !tc.on {
				assert.Nil(t, o.tls)
				return
			}
			require.NotNil(t, o.tls)
			assert.Equal(t, tc.serverName, o.tls.ServerName)
		})
	}
}

func TestParseSinkErrors(t *testing.T) {
	cases := []struct {
		name string
		kind string
		ep   *config.Endpoint
		err  string
	}{
		{"missing config", kindTCP, nil, "destination out: missing config"},
		{"unknown kind", "sctp", &config.Endpoint{URL: "tcp://h:1"}, `destination out: unknown kind "sctp" (tcp or udp)`},
		{"no url", kindTCP, &config.Endpoint{}, "destination out: url is required: tcp://host:port or tls://host:port"},
		{"template url", kindTCP, &config.Endpoint{URL: "tcp://{{ .json.host }}:1"}, "url cannot be a per-message template"},
		{"http url", kindTCP, &config.Endpoint{URL: "http://h:1"}, `url "http://h:1" must be tcp://host:port or tls://host:port`},
		{"udp url on tcp", kindTCP, &config.Endpoint{URL: "udp://h:1"}, `must be tcp://host:port or tls://host:port`},
		{"tls url on udp", kindUDP, &config.Endpoint{URL: "tls://h:1"}, `url "tls://h:1" must be udp://host:port`},
		{"no port", kindTCP, &config.Endpoint{URL: "tcp://h"}, "with a host and a port"},
		{"no host", kindUDP, &config.Endpoint{URL: "udp://:514"}, "with a host and a port"},
		{"port out of range", kindTCP, &config.Endpoint{URL: "tcp://h:70000"}, "port 70000 is not 1 to 65535"},
		{"path", kindTCP, &config.Endpoint{URL: "tcp://h:1/path"}, "without a path or query"},
		{"query", kindTCP, &config.Endpoint{URL: "tcp://h:1?x=1"}, "without a path or query"},
		{"credentials", kindTCP, &config.Endpoint{URL: "tcp://user:secret@h:1"}, `url "tcp://user:xxxxx@h:1" must not hold credentials`},
		{"bad frame", kindTCP, &config.Endpoint{URL: "tcp://h:1", Frame: "binary"}, `frame must be newline, length or raw, not "binary"`},
		{"headers", kindTCP, &config.Endpoint{URL: "tcp://h:1", Headers: map[string]string{"X": "y"}}, "headers do not apply to tcp destinations"},
		{"bad body template", kindTCP, &config.Endpoint{URL: "tcp://h:1", Body: "{{ .json"}, "destination out: template body"},
		{"dtls url on tcp", kindTCP, &config.Endpoint{URL: "dtls://h:1"}, `url "dtls://h:1" must be tcp://host:port or tls://host:port`},
		{"no url on udp", kindUDP, &config.Endpoint{}, "destination out: url is required: udp://host:port or dtls://host:port"},
		{"tls:// with enabled: false", kindTCP, &config.Endpoint{URL: "tls://h:1", TLS: &config.TLS{Enabled: config.Ptr(false)}}, "tls.enabled: false contradicts the tls:// URL; use tcp:// for a plain connection"},
		{"dtls:// with enabled: false", kindUDP, &config.Endpoint{URL: "dtls://h:1", TLS: &config.TLS{Enabled: config.Ptr(false)}}, "tls.enabled: false contradicts the dtls:// URL; use udp:// for a plain connection"},
		{"missing ca file", kindTCP, &config.Endpoint{URL: "tls://h:1", TLS: &config.TLS{CAFile: "/nonexistent/ca.pem"}}, "tls: open /nonexistent/ca.pem"},
		{"missing ca file on udp", kindUDP, &config.Endpoint{URL: "udp://h:1", TLS: &config.TLS{CAFile: "/nonexistent/ca.pem"}}, "tls: open /nonexistent/ca.pem"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := plugin.Spec{Name: "out", Kind: tc.kind}
			if tc.ep != nil {
				spec.Config = tc.ep
			}
			_, err := parseSink(spec)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.err)
			assert.NotContains(t, err.Error(), "secret", "a URL's password is never shown")
		})
	}
}

func TestAllowList(t *testing.T) {
	l, err := parseAllow([]string{"10.0.0.0/8", " 192.168.1.7 ", "2001:db8::/32", "::ffff:172.16.0.0/108", "fe80::1%en0"})
	require.NoError(t, err)
	for addr, want := range map[string]bool{
		"10.1.2.3":           true,
		"11.0.0.1":           false,
		"192.168.1.7":        true,
		"192.168.1.8":        false,
		"::ffff:10.9.9.9":    true, // an IPv4 client on an IPv6 socket
		"2001:db8::5":        true,
		"2001:db9::5":        false,
		"172.16.3.4":         true,
		"172.32.0.1":         false,
		"fe80::1%eth0":       true, // zones do not matter
		"::1":                false,
		"127.0.0.1":          false,
		"::ffff:192.168.1.7": true,
	} {
		assert.Equal(t, want, l.allows(netip.MustParseAddr(addr)), addr)
	}
	assert.False(t, l.allows(netip.Addr{}), "an unknown address is not let in")

	var anyone allowList
	assert.True(t, anyone.allows(netip.MustParseAddr("203.0.113.9")))

	for _, bad := range []string{"10.0.0.0/33", "not-an-ip", "10.0.0.256", ""} {
		_, err := parseAllow([]string{bad})
		assert.Error(t, err, bad)
	}
}

func TestRegister(t *testing.T) {
	Register()
	for _, kind := range []string{kindTCP, kindUDP} {
		_, ok := plugin.LookupSource(kind)
		assert.True(t, ok, kind)
		_, ok = plugin.LookupSink(kind)
		assert.True(t, ok, kind)
	}
	ctx := context.Background()
	src, err := plugin.NewSource(ctx, plugin.Spec{Name: "in", Kind: kindUDP, Config: &config.Source{Listen: "127.0.0.1:0"}})
	require.NoError(t, err)
	assert.Equal(t, "in", src.Name())
	sink, err := plugin.NewSink(ctx, plugin.Spec{Name: "out", Kind: kindTCP, Config: &config.Endpoint{URL: "tcp://127.0.0.1:9"}})
	require.NoError(t, err)
	assert.Equal(t, "out", sink.Name())
	require.NoError(t, sink.Close(ctx))

	src, err = plugin.NewSource(ctx, plugin.Spec{Name: "in", Kind: kindTCP, Config: &config.Source{}})
	require.Error(t, err)
	assert.True(t, src == nil, "a failed build returns no source at all, not a nil *Source")
	sink, err = plugin.NewSink(ctx, plugin.Spec{Name: "out", Kind: kindTCP, Config: &config.Endpoint{}})
	require.Error(t, err)
	assert.True(t, sink == nil, "a failed build returns no sink at all, not a nil *Sink")
}
