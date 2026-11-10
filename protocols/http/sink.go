package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// MaxResponse bounds the response body a sink keeps (for respond.from).
const MaxResponse = 10 << 20

// RerouteHeader is how a destination says a message belonged elsewhere: a
// correction for the feedback loop.
const RerouteHeader = "X-Hikyaku-Reroute"

// dropHeaders are not forwarded from the incoming message: hop-by-hop
// headers, framing, and the caller's own credentials.
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Accept-Encoding": true,
}

// Sink delivers a message as one HTTP request.
type Sink struct {
	name    string
	method  string
	url     *transform.Template
	static  string
	headers map[string]*transform.Template
	body    *transform.Template
	client  *http.Client
	guard   *guard // set for templated URLs
}

func newSink(s plugin.Spec) (*Sink, error) {
	ep, ok := s.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return nil, fmt.Errorf("destination %s: missing config", s.Name)
	}
	if ep.URL == "" {
		return nil, fmt.Errorf("destination %s: url is required", s.Name)
	}
	sk := &Sink{name: s.Name, method: strings.ToUpper(ep.Method), headers: map[string]*transform.Template{}}
	if sk.method == "" {
		sk.method = http.MethodPost
	}
	u, err := transform.CompileTemplate("url", ep.URL)
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", s.Name, err)
	}
	sk.url = u
	if u.IsStatic() {
		if _, err := url.Parse(ep.URL); err != nil {
			return nil, fmt.Errorf("destination %s: url: %w", s.Name, err)
		}
		sk.static = ep.URL
	}
	for k, v := range ep.Headers {
		t, err := transform.CompileTemplate("headers."+k, v)
		if err != nil {
			return nil, fmt.Errorf("destination %s: %w", s.Name, err)
		}
		sk.headers[textproto.CanonicalMIMEHeaderKey(k)] = t
	}
	if ep.Body != "" {
		if sk.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return nil, fmt.Errorf("destination %s: %w", s.Name, err)
		}
	}
	tlsCfg, err := httpmux.ClientTLS(ep.TLS)
	if err != nil {
		return nil, fmt.Errorf("destination %s: %w", s.Name, err)
	}
	if ep.Type == "https" && tlsCfg == nil {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	base := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           base.DialContext,
		TLSClientConfig:       tlsCfg,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	if !u.IsStatic() {
		g, err := newGuard(s.Env.Egress)
		if err != nil {
			return nil, fmt.Errorf("destination %s: %w", s.Name, err)
		}
		sk.guard = g
		tr.DialContext = g.dialer(base)
		tr.Proxy = nil // a proxy would dial for us, around the guard
	}
	sk.client = &http.Client{Transport: tr}
	if sk.guard != nil {
		// A redirect is another request: its host must pass egress.allow
		// too (the dialer already checks the address).
		sk.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return sk.guard.checkHost(req.URL.Hostname())
		}
	}
	return sk, nil
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver sends one request: the message's headers (minus hop-by-hop and
// credentials), the configured headers, the idempotency key, and the body
// (or the body template). A 2xx is a success; anything else is classified
// for the retry policy, with Retry-After passed on.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	target := s.static
	if target == "" {
		u, err := s.url.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		pu, err := url.Parse(u)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Hostname() == "" {
			return nil, plugin.Permanentf("rendered url %q is not an http(s) URL", u)
		}
		if err := s.guard.checkHost(pu.Hostname()); err != nil {
			return nil, err
		}
		target = u
	}
	body := env.Body
	if s.body != nil {
		b, err := s.body.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		body = []byte(b)
	}
	req, err := http.NewRequestWithContext(ctx, s.method, target, bytes.NewReader(body))
	if err != nil {
		return nil, plugin.Permanentf("request: %v", err)
	}
	for k, vs := range env.Headers {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if dropHeaders[ck] {
			continue
		}
		for _, v := range vs {
			req.Header.Add(ck, v)
		}
	}
	if env.ContentType != "" {
		req.Header.Set("Content-Type", env.ContentType)
	}
	for k, t := range s.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "hikyaku")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, classify(err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponse))
	if err != nil {
		return nil, plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("reading the response: %w", err))
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, plugin.StatusError(resp.StatusCode, plugin.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), snippet(data))
	}
	res := &plugin.Result{
		Status:      resp.StatusCode,
		Headers:     resp.Header,
		Body:        data,
		ContentType: resp.Header.Get("Content-Type"),
		Endpoint:    redactURL(target),
	}
	if rr := resp.Header.Get(RerouteHeader); rr != "" {
		for _, d := range strings.Split(rr, ",") {
			if d = strings.TrimSpace(d); d != "" {
				res.Reroute = append(res.Reroute, d)
			}
		}
	}
	return res, nil
}

// classify tags a transport failure: a refused or reset connection is
// connect, a deadline is timeout, the egress guard is permanent.
func classify(err error) error {
	if errors.Is(err, errEgress) {
		return plugin.Wrap(plugin.Permanent, []string{"egress"}, err)
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	var ue *url.Error
	if errors.As(err, &ue) && ue.Timeout() {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
}

func snippet(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}

// redactURL drops credentials and the query from a URL for reports.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "(invalid url)"
	}
	u.User = nil
	u.RawQuery = ""
	return u.String()
}

// Health dials the host of a static URL (no request is sent: a webhook
// endpoint should not see probes).
func (s *Sink) Health(ctx context.Context) error {
	if s.static == "" {
		return nil
	}
	u, err := url.Parse(s.static)
	if err != nil {
		return err
	}
	host := u.Host
	if u.Port() == "" {
		if u.Scheme == "https" {
			host = net.JoinHostPort(u.Hostname(), "443")
		} else {
			host = net.JoinHostPort(u.Hostname(), "80")
		}
	}
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return fmt.Errorf("dial %s: %w", host, err)
	}
	return c.Close()
}

// Close releases idle connections.
func (s *Sink) Close(context.Context) error {
	s.client.CloseIdleConnections()
	return nil
}
