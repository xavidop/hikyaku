package transform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/egress"
	"github.com/xavidop/hikyaku/plugin"
)

const (
	// DefaultEnrichTimeout is enrich.timeout when it is not set.
	DefaultEnrichTimeout = 2 * time.Second
	// enrichMaxAnswer bounds the answer of an enrich call: it goes into
	// every message.
	enrichMaxAnswer = 1 << 20
	// enrichCacheMax bounds the answers a cache holds.
	enrichCacheMax = 10000
)

// Options are what a transforms: list needs from the rest of the spec.
type Options struct {
	// Egress guards the enrich calls whose URL is a per-message template.
	Egress plugin.Egress
}

// EnrichStep calls an HTTP endpoint and puts its JSON answer into the JSON
// body, under a key.
type EnrichStep struct {
	url     *Template
	method  string
	headers map[string]*Template
	into    []string
	timeout time.Duration
	skip    bool // on_error: skip
	client  *http.Client
	guard   *egress.Guard // set for a templated URL
	cache   *enrichCache  // nil without cache
}

func compileEnrich(p string, spec *config.Enrich, o Options) (*EnrichStep, error) {
	if spec.URL == "" {
		return nil, errors.New("url is required")
	}
	if spec.Into == "" {
		return nil, errors.New("into is required")
	}
	u, err := CompileTemplate(p+".enrich.url", spec.URL)
	if err != nil {
		return nil, fmt.Errorf("url: %w", err)
	}
	s := &EnrichStep{
		url: u, method: firstNonEmpty(spec.Method, http.MethodGet), into: strings.Split(spec.Into, "."),
		timeout: spec.Timeout.Std(), skip: spec.OnError == "skip", headers: map[string]*Template{},
	}
	if s.method != http.MethodGet && s.method != http.MethodPost {
		return nil, fmt.Errorf("method %q: use GET or POST", spec.Method)
	}
	if s.timeout <= 0 {
		s.timeout = DefaultEnrichTimeout
	}
	for _, k := range sortedKeys(spec.Headers) {
		t, err := CompileTemplate(p+".enrich.headers."+k, spec.Headers[k])
		if err != nil {
			return nil, fmt.Errorf("headers.%s: %w", k, err)
		}
		s.headers[k] = t
	}
	if spec.Cache > 0 && s.method == http.MethodGet {
		s.cache = &enrichCache{ttl: spec.Cache.Std(), now: time.Now, items: map[string]enrichEntry{}}
	}
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConnsPerHost: 16,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	s.client = &http.Client{Transport: tr}
	if !u.IsStatic() {
		// The host may come from the message: the same guard as an http
		// destination with a templated URL.
		g, err := egress.New(o.Egress)
		if err != nil {
			return nil, err
		}
		s.guard = g
		tr.DialContext = g.Dialer(&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second})
		tr.Proxy = nil // a proxy would dial for us, around the guard
		s.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return g.CheckHost(req.URL.Hostname())
		}
	}
	return s, nil
}

// Apply looks the answer up and merges it. The body must be a JSON object.
// A failed call fails the step, or with on_error: skip leaves the message
// as it is.
func (s *EnrichStep) Apply(env *envelope.Envelope) (bool, error) {
	dec := json.NewDecoder(bytes.NewReader(env.Body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return false, fmt.Errorf("enrich: the body is not JSON: %w", err)
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return false, errors.New("enrich: the body is not a JSON object")
	}
	answer, err := s.lookup(env)
	if err != nil {
		if s.skip {
			return true, nil
		}
		return false, fmt.Errorf("enrich: %w", err)
	}
	if err := setPath(obj, s.into, answer); err != nil {
		return false, fmt.Errorf("enrich: %w", err)
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return false, fmt.Errorf("enrich: %w", err)
	}
	env.SetBody(out)
	return true, nil
}

func (s *EnrichStep) lookup(env *envelope.Envelope) (any, error) {
	target, err := s.url.Render(env)
	if err != nil {
		return nil, fmt.Errorf("url: %w", err)
	}
	headers := make(map[string]string, len(s.headers))
	for k, t := range s.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, fmt.Errorf("headers.%s: %w", k, err)
		}
		headers[k] = v
	}
	if s.cache == nil {
		return s.call(target, headers, env)
	}
	key := cacheKey(target, headers)
	if v, ok := s.cache.get(key); ok {
		return v, nil
	}
	v, err := s.call(target, headers, env)
	if err == nil {
		s.cache.put(key, v)
	}
	return v, err
}

func (s *EnrichStep) call(target string, headers map[string]string, env *envelope.Envelope) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	var body io.Reader
	if s.method == http.MethodPost {
		body = bytes.NewReader(env.Body)
	}
	req, err := http.NewRequestWithContext(ctx, s.method, target, body)
	if err != nil {
		return nil, fmt.Errorf("url %q: %w", target, err)
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return nil, fmt.Errorf("url %q: use http or https", target)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", firstNonEmpty(env.ContentType, "application/json"))
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if s.guard != nil {
		if err := s.guard.CheckHost(req.URL.Hostname()); err != nil {
			return nil, err
		}
	}
	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("%s %s: no answer within %s", s.method, req.URL.Redacted(), s.timeout)
		}
		return nil, fmt.Errorf("%s %s: %w", s.method, req.URL.Redacted(), err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, enrichMaxAnswer+1))
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", s.method, req.URL.Redacted(), err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%s %s: status %d", s.method, req.URL.Redacted(), resp.StatusCode)
	}
	if len(raw) > enrichMaxAnswer {
		return nil, fmt.Errorf("%s %s: the answer is over 1 MiB", s.method, req.URL.Redacted())
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
		return nil, fmt.Errorf("%s %s: the answer is not JSON (content type %q)", s.method, req.URL.Redacted(), mt)
	}
	return v, nil
}

// setPath stores v under a dotted path of keys, making the objects on the
// way. A value on the way that is not an object is an error: enrich never
// replaces part of the message with an object.
func setPath(obj map[string]any, path []string, v any) error {
	for i, k := range path[:len(path)-1] {
		next, ok := obj[k]
		if !ok || next == nil {
			m := map[string]any{}
			obj[k] = m
			obj = m
			continue
		}
		m, ok := next.(map[string]any)
		if !ok {
			return fmt.Errorf("into %s: %s is not an object", strings.Join(path, "."), strings.Join(path[:i+1], "."))
		}
		obj = m
	}
	obj[path[len(path)-1]] = v
	return nil
}

func cacheKey(target string, headers map[string]string) string {
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(target)
	for _, k := range keys {
		b.WriteString("\x00" + k + "\x00" + headers[k])
	}
	return b.String()
}

// enrichCache keeps answers for a while, in this process. Answers are
// shared between messages: nothing changes one after it is stored (Apply
// marshals it into each body).
type enrichCache struct {
	ttl   time.Duration
	now   func() time.Time
	mu    sync.Mutex
	items map[string]enrichEntry
}

type enrichEntry struct {
	v       any
	expires time.Time
}

func (c *enrichCache) get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[key]
	if !ok || !c.now().Before(e.expires) {
		delete(c.items, key)
		return nil, false
	}
	return e.v, true
}

func (c *enrichCache) put(key string, v any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.items) >= enrichCacheMax {
		now := c.now()
		for k, e := range c.items {
			if !now.Before(e.expires) {
				delete(c.items, k)
			}
		}
		if len(c.items) >= enrichCacheMax {
			clear(c.items) // full of live answers: start over
		}
	}
	c.items[key] = enrichEntry{v: v, expires: c.now().Add(c.ttl)}
}
