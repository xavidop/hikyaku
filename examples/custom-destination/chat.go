package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// ChatOptions are the settings of a chat destination, under options: in
// the spec. The yaml tags name the keys; the jsonschema tags feed
// hikyaku schema and editor completion.
type ChatOptions struct {
	WebhookURL string `yaml:"webhook_url" jsonschema:"required" jsonschema_description:"The chat's incoming-webhook URL."`
	Text       string `yaml:"text,omitempty" jsonschema_description:"Per-message template of the text (default: the message body)."`
	Username   string `yaml:"username,omitempty" jsonschema_description:"Name the message is posted as."`
}

// Chat posts each message as a text to a chat webhook. It makes one
// attempt per Deliver: hikyaku's engine adds the retries, the breaker,
// the budget, fallbacks and the DLQ around it.
type Chat struct {
	name   string
	opts   ChatOptions
	text   *transform.Template
	client *http.Client
}

func newChat(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
	var o ChatOptions
	if err := spec.Decode(&o); err != nil {
		return nil, err
	}
	if o.WebhookURL == "" {
		return nil, errors.New("chat " + spec.Name + ": options.webhook_url is required")
	}
	if o.Text == "" {
		o.Text = "{{ .bodyText }}"
	}
	t, err := transform.CompileTemplate("text", o.Text)
	if err != nil {
		return nil, fmt.Errorf("chat %s: %w", spec.Name, err)
	}
	return &Chat{name: spec.Name, opts: o, text: t, client: &http.Client{}}, nil
}

// Name is the destination name.
func (c *Chat) Name() string { return c.name }

// Deliver posts {"text": ..., "username": ...}.
func (c *Chat) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	text, err := c.text.Render(env)
	if err != nil {
		return nil, plugin.Permanentf("text: %v", err) // the same message would fail every time
	}
	body, _ := json.Marshal(map[string]string{"text": text, "username": c.opts.Username})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.opts.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return nil, plugin.Permanentf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", env.Header("Idempotency-Key")) // set by the engine on every attempt
	resp, err := c.client.Do(req)
	if err != nil {
		// A connection failure: retried by the default policy.
		return nil, plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		// 429 and 5xx are retried, Retry-After honoured; other 4xx go
		// straight to the fallback.
		return nil, plugin.StatusError(resp.StatusCode, plugin.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), string(data))
	}
	return &plugin.Result{Status: resp.StatusCode, Body: data, Endpoint: "chat"}, nil
}

// Health is fine: a webhook URL cannot be probed without posting.
func (c *Chat) Health(context.Context) error { return nil }

// Close releases idle connections.
func (c *Chat) Close(context.Context) error {
	c.client.CloseIdleConnections()
	return nil
}
