package tcp

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// defaultMaxBodyBytes is the longest frame a source accepts by default.
const defaultMaxBodyBytes = 1 << 20

// sourceOptions is a tcp or udp source's configuration, defaults applied.
type sourceOptions struct {
	listen   string
	frame    framing
	maxBytes int
	tls      *tls.Config // nil without TLS; DTLS for udp
	allow    allowList
}

// sinkOptions is a tcp or udp destination's configuration.
type sinkOptions struct {
	network  string // tcp or udp: what is dialed
	addr     string // host:port
	endpoint string // the URL, scheme://host:port, for results and logs
	frame    framing
	tls      *tls.Config // nil without TLS; DTLS for udp
	body     *transform.Template
}

// parseSource turns the spec of a tcp or udp source into its options.
func parseSource(spec plugin.Spec) (sourceOptions, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return sourceOptions{}, fmt.Errorf("source %s: missing config", spec.Name)
	}
	kind := spec.Kind
	fail := func(format string, args ...any) error {
		return fmt.Errorf("source %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if kind != kindTCP && kind != kindUDP {
		return sourceOptions{}, fail("unknown kind %q (tcp or udp)", kind)
	}
	if src.Mode != "" && src.Mode != "listen" {
		return sourceOptions{}, fail("%s sources run in listen mode only, not %q", kind, src.Mode)
	}
	if src.Listen == "" {
		return sourceOptions{}, fail("listen is required (such as :9000)")
	}
	if _, _, err := net.SplitHostPort(src.Listen); err != nil {
		return sourceOptions{}, fail("listen %q is not host:port (%v)", src.Listen, err)
	}
	if src.Ack != "" && src.Ack != "delivered" {
		return sourceOptions{}, fail("ack: %s does not apply: %s has no acknowledgements", src.Ack, kind)
	}
	o := sourceOptions{listen: src.Listen}
	var err error
	if o.frame, err = parseFraming(src.Frame, src.LengthBytes); err != nil {
		return sourceOptions{}, fail("%v", err)
	}
	switch {
	case src.MaxBodyBytes < 0:
		return sourceOptions{}, fail("max_body_bytes must not be negative")
	case src.MaxBodyBytes == 0:
		o.maxBytes = defaultMaxBodyBytes
	default:
		o.maxBytes = int(src.MaxBodyBytes) // the release builds are 64-bit
	}
	if a := src.Auth; a != nil {
		for _, other := range []struct {
			key string
			set bool
		}{{"hmac", a.HMAC != nil}, {"bearer", a.Bearer != nil}, {"jwt", a.JWT != nil}, {"api_key", a.APIKey != nil}} {
			if other.set {
				return sourceOptions{}, fail("auth.%s does not apply to %s sources: a %s message carries no headers or signature; %s sources take auth.ip_allow (and mTLS, with tls.client_ca_file)", other.key, kind, kind, kind)
			}
		}
		if o.allow, err = parseAllow(a.IPAllow); err != nil {
			return sourceOptions{}, fail("%v", err)
		}
	}
	if !tlsOn(src.TLS) {
		return o, nil
	}
	// Any tls key means TLS (DTLS for udp): client_ca_file alone must not
	// leave the port open in plain text. enabled: false (above) turns it off.
	on := *src.TLS
	on.Enabled = config.Ptr(true)
	if o.tls, _, err = httpmux.ServerTLS(&on); err != nil {
		return sourceOptions{}, fail("%v", err)
	}
	return o, nil
}

// parseSink turns the spec of a tcp or udp destination into its options.
func parseSink(spec plugin.Spec) (sinkOptions, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return sinkOptions{}, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	kind := spec.Kind
	fail := func(format string, args ...any) error {
		return fmt.Errorf("destination %s: %s", spec.Name, fmt.Sprintf(format, args...))
	}
	if kind != kindTCP && kind != kindUDP {
		return sinkOptions{}, fail("unknown kind %q (tcp or udp)", kind)
	}
	u, err := parseURL(kind, ep.URL)
	if err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	o := sinkOptions{network: kind, addr: u.Host, endpoint: u.Scheme + "://" + u.Host}
	if o.frame, err = parseFraming(ep.Frame, ep.LengthBytes); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	if len(ep.Headers) > 0 {
		return sinkOptions{}, fail("headers do not apply to %s destinations: only the body goes on the wire", kind)
	}
	if ep.Body != "" {
		if o.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return sinkOptions{}, fail("%v", err)
		}
	}
	// tls:// (tcp) or dtls:// (udp) turns TLS on, and so does any tls key
	// on a tcp:// or udp:// URL.
	secure := map[string]string{kindTCP: "tls", kindUDP: "dtls"}[kind]
	off := ep.TLS != nil && ep.TLS.Enabled != nil && !*ep.TLS.Enabled
	switch {
	case u.Scheme == secure && off:
		return sinkOptions{}, fail("tls.enabled: false contradicts the %s:// URL; use %s:// for a plain connection", secure, kind)
	case off:
		return o, nil
	}
	if o.tls, err = httpmux.ClientTLS(ep.TLS); err != nil {
		return sinkOptions{}, fail("%v", err)
	}
	if o.tls == nil && u.Scheme == secure {
		o.tls = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if o.tls != nil && o.tls.ServerName == "" {
		o.tls.ServerName = u.Hostname()
	}
	return o, nil
}

// tlsOn reports whether a tls: block asks for TLS: any block, unless it
// says enabled: false.
func tlsOn(t *config.TLS) bool {
	return t != nil && config.Bool(t.Enabled, true)
}

// parseURL checks a destination URL: tcp://host:port or tls://host:port for
// tcp, udp://host:port or dtls://host:port for udp. It cannot be a
// per-message template: the connection is dialed before any message exists.
func parseURL(kind, raw string) (*url.URL, error) {
	schemes := map[string][]string{kindTCP: {"tcp", "tls"}, kindUDP: {"udp", "dtls"}}[kind]
	want := strings.Join(schemes, "://host:port or ") + "://host:port"
	if raw == "" {
		return nil, fmt.Errorf("url is required: %s", want)
	}
	if strings.Contains(raw, "{{") {
		return nil, errors.New("url cannot be a per-message template: the connection is dialed before any message")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("url: %v", err)
	}
	shown := u.Redacted()
	known := false
	for _, s := range schemes {
		known = known || u.Scheme == s
	}
	switch {
	case !known:
		return nil, fmt.Errorf("url %q must be %s", shown, want)
	case u.User != nil:
		return nil, fmt.Errorf("url %q must not hold credentials: %s has none", shown, kind)
	case u.Hostname() == "" || u.Port() == "":
		return nil, fmt.Errorf("url %q must be %s, with a host and a port", shown, want)
	case u.Path != "" && u.Path != "/", u.RawQuery != "", u.Fragment != "":
		return nil, fmt.Errorf("url %q must be %s, without a path or query", shown, want)
	}
	if p, err := strconv.Atoi(u.Port()); err != nil || p < 1 || p > 65535 {
		return nil, fmt.Errorf("url %q: port %s is not 1 to 65535", shown, u.Port())
	}
	return u, nil
}

// allowList holds the networks auth.ip_allow lets in; empty lets anyone in.
type allowList []netip.Prefix

// parseAllow reads auth.ip_allow: IPs and CIDRs, IPv4 or IPv6.
func parseAllow(entries []string) (allowList, error) {
	var l allowList
	for _, e := range entries {
		s := strings.TrimSpace(e)
		if strings.Contains(s, "/") {
			p, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("auth.ip_allow %q is not an IP or CIDR", e)
			}
			if p.Addr().Is4In6() && p.Bits() >= 96 {
				p = netip.PrefixFrom(p.Addr().Unmap(), p.Bits()-96)
			}
			l = append(l, p.Masked())
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("auth.ip_allow %q is not an IP or CIDR", e)
		}
		a = a.Unmap().WithZone("")
		l = append(l, netip.PrefixFrom(a, a.BitLen()))
	}
	return l, nil
}

// allows reports whether a peer's address is let in. IPv4 peers seen
// through an IPv6 socket (::ffff:a.b.c.d) match IPv4 entries.
func (l allowList) allows(a netip.Addr) bool {
	if len(l) == 0 {
		return true
	}
	a = a.Unmap().WithZone("")
	for _, p := range l {
		if p.Contains(a) {
			return true
		}
	}
	return false
}
