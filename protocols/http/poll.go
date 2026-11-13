package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Poller is an http poll source: it pulls an endpoint every interval, with
// retries per poll, conditional requests (ETag / Last-Modified), an optional
// jq split into several messages, and dedupe.
type Poller struct {
	name     string
	kind     string
	url      string
	method   string
	body     string
	headers  map[string]string
	interval time.Duration
	timeout  time.Duration
	retry    config.Retry
	split    *transform.JQ
	dedupe   *dedupe
	client   *http.Client
	oauth    *tokenSource // set with oauth2:
	log      *slog.Logger
	rnd      func() float64

	mu       sync.Mutex
	etag     string
	modified string
	lastErr  error
}

func newPoller(s plugin.Spec, src *config.Source) (*Poller, error) {
	p := &Poller{
		name:     s.Name,
		kind:     s.Kind,
		url:      src.URL,
		method:   strings.ToUpper(src.Method),
		body:     src.Body,
		headers:  src.Headers,
		interval: src.Interval.Std(),
		timeout:  src.Timeout.Std(),
		retry:    config.ResolveRetry(src.Retry, &config.Retry{Attempts: 3, Backoff: config.Duration(time.Second)}),
		log:      s.Env.Log(),
		rnd:      rand.Float64,
	}
	if p.method == "" {
		p.method = http.MethodGet
	}
	if p.interval <= 0 {
		p.interval = config.DefaultPollInterval
	}
	if p.timeout <= 0 {
		p.timeout = config.DefaultPollTimeout
	}
	if src.Split != "" {
		q, err := transform.CompileJQ(src.Split)
		if err != nil {
			return nil, fmt.Errorf("source %s: split: %w", s.Name, err)
		}
		p.split = q
	}
	var err error
	if p.dedupe, err = newDedupe(s.Name, src.Dedupe, s.Env); err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Name, err)
	}
	tlsCfg, err := httpmux.ClientTLS(src.TLS)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", s.Name, err)
	}
	if s.Kind == "https" && tlsCfg == nil {
		tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	p.client = &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsCfg}}
	if src.OAuth2 != nil {
		// The token endpoint gets the source's TLS settings, on connections
		// of its own.
		tc := &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsCfg}}
		if p.oauth, err = newTokenSource(src.OAuth2, tc); err != nil {
			return nil, fmt.Errorf("source %s: %w", s.Name, err)
		}
	}
	return p, nil
}

// Name is the source name.
func (p *Poller) Name() string { return p.name }

// Dedupes reports that the poller applies its dedupe: block itself, item
// by item, so the engine leaves it alone.
func (p *Poller) Dedupes() bool { return p.dedupe != nil }

// Health reports the last poll's error.
func (p *Poller) Health(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastErr
}

// Start polls at once, then every interval, until ctx ends. A poll whose
// retries all fail is logged and skipped: the next interval tries again.
func (p *Poller) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		if err := p.pollOnce(ctx, out); err != nil && ctx.Err() == nil {
			p.log.Warn("poll failed; skipping to the next interval", "source", p.name, "url", redactURL(p.url), "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (p *Poller) pollOnce(ctx context.Context, out chan<- *envelope.Envelope) error {
	var lastErr error
	for n := 1; n <= p.retry.Attempts; n++ {
		body, ct, changed, err := p.fetch(ctx)
		if err == nil {
			p.setErr(nil)
			if !changed {
				return nil
			}
			return p.emit(ctx, out, body, ct)
		}
		lastErr = err
		class, ra := plugin.Classify(err)
		if n == p.retry.Attempts || !plugin.Retryable(class, plugin.Tags(err), p.retry.RetryOn, p.retry.NeverOn) {
			break
		}
		wait := backoff.Delay(p.retry.Backoff.Std(), p.retry.MaxBackoff.Std(), p.retry.Jitter, n, p.rnd)
		if ra > 0 && config.Bool(p.retry.RespectRetryAfter, true) {
			wait = min(ra, p.retry.MaxBackoff.Std())
		}
		if !backoff.Sleep(ctx, wait) {
			return ctx.Err()
		}
	}
	p.setErr(lastErr)
	return lastErr
}

// fetch makes one poll. With oauth2, a 401 means the API no longer takes
// the token (revoked, or its keys changed): the poll is made once more with
// a new one.
func (p *Poller) fetch(ctx context.Context) ([]byte, string, bool, error) {
	rctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if p.oauth == nil {
		return p.once(rctx, "")
	}
	for try := 0; ; try++ {
		tok, err := p.oauth.get(rctx)
		if err != nil {
			return nil, "", false, err
		}
		data, ct, changed, err := p.once(rctx, tok)
		var pe *plugin.Error
		if try > 0 || !errors.As(err, &pe) || !slices.Contains(pe.Tags, "401") {
			return data, ct, changed, err
		}
		p.oauth.drop(tok)
	}
}

// once sends one poll request and reads its answer.
func (p *Poller) once(rctx context.Context, token string) ([]byte, string, bool, error) {
	var rb io.Reader
	if p.body != "" {
		rb = strings.NewReader(p.body)
	}
	req, err := http.NewRequestWithContext(rctx, p.method, p.url, rb)
	if err != nil {
		return nil, "", false, plugin.Permanentf("poll request: %v", err)
	}
	for k, v := range p.headers {
		req.Header.Set(k, v)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	p.mu.Lock()
	if p.etag != "" {
		req.Header.Set("If-None-Match", p.etag)
	}
	if p.modified != "" {
		req.Header.Set("If-Modified-Since", p.modified)
	}
	p.mu.Unlock()
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "hikyaku")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, "", false, classify(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNotModified {
		return nil, "", false, nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponse))
	if err != nil {
		return nil, "", false, plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, "", false, plugin.StatusError(resp.StatusCode, plugin.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), snippet(data))
	}
	p.mu.Lock()
	p.etag, p.modified = resp.Header.Get("ETag"), resp.Header.Get("Last-Modified")
	p.mu.Unlock()
	return data, resp.Header.Get("Content-Type"), true, nil
}

// emit sends the response as one message, or one per jq split output.
func (p *Poller) emit(ctx context.Context, out chan<- *envelope.Envelope, body []byte, ct string) error {
	bodies := [][]byte{body}
	if p.split != nil {
		var doc any
		if err := json.Unmarshal(body, &doc); err != nil {
			return plugin.Permanentf("split needs a JSON response: %v", err)
		}
		items, err := p.split.Run(doc)
		if err != nil {
			return err
		}
		bodies = bodies[:0]
		for _, it := range items {
			b, err := json.Marshal(it)
			if err != nil {
				return err
			}
			bodies = append(bodies, b)
		}
		ct = "application/json"
	}
	for _, b := range bodies {
		env := envelope.New(p.name, p.kind, bytes.Clone(b))
		env.ContentType = ct
		env.Subject = redactURL(p.url) // no credentials or query in a message
		env.Attributes["url"] = redactURL(p.url)
		if p.dedupe.seen(ctx, env) {
			continue
		}
		select {
		case out <- env:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *Poller) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastErr = err
}
