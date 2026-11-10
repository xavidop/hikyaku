package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// CRMOptions are the settings of a crm destination, under options: in the
// spec. The yaml tags name the keys; the jsonschema tags become the JSON
// Schema hikyaku validates options: with, before it runs anything.
type CRMOptions struct {
	Pipeline string `yaml:"pipeline" jsonschema:"required,pattern=^[a-z0-9][a-z0-9_-]*$" jsonschema_description:"Pipeline the leads go into."`
	BaseURL  string `yaml:"base_url,omitempty" jsonschema:"pattern=^https?://" jsonschema_description:"Base URL of the CRM's API (default https://api.<region>.crm.example/v1)."`
}

var pipelineName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// maxReply is how much of the CRM's answer is kept as the reply.
const maxReply = 1 << 20

// CRM posts each lead to a pipeline of the CRM.
type CRM struct {
	name   string
	url    string
	region string
	token  string
	client *http.Client
	log    *slog.Logger
}

// newCRM is the crm type's factory, for a region and a token.
func newCRM(region, token string) plugin.Factory[plugin.Sink] {
	return func(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
		var o CRMOptions
		if err := spec.Decode(&o); err != nil {
			return nil, err
		}
		if !pipelineName.MatchString(o.Pipeline) {
			return nil, fmt.Errorf("crm %s: options.pipeline %q: use lowercase letters, digits, _ and -", spec.Name, o.Pipeline)
		}
		if token == "" {
			return nil, fmt.Errorf("crm %s: CRM_TOKEN is not set: pass it in the plugin's env", spec.Name)
		}
		base := o.BaseURL
		if base == "" {
			base = "https://api." + region + ".crm.example/v1"
		}
		if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
			return nil, fmt.Errorf("crm %s: options.base_url %q must start with http:// or https://", spec.Name, base)
		}
		c := &CRM{
			name:   spec.Name,
			url:    strings.TrimSuffix(base, "/") + "/pipelines/" + o.Pipeline + "/leads",
			region: region,
			token:  token,
			client: &http.Client{},
			log:    spec.Env.Log(),
		}
		c.log.Info("crm destination opened", "url", c.url)
		return c, nil
	}
}

// Name is the destination name.
func (c *CRM) Name() string { return c.name }

// Deliver posts one lead. The message ID is the Idempotency-Key, so a retry
// of the same message does not make a second lead.
func (c *CRM) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	if err := checkLead(env.Body); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(env.Body))
	if err != nil {
		return nil, plugin.Wrap(plugin.Permanent, nil, err)
	}
	key := env.Header("Idempotency-Key")
	if key == "" {
		key = env.ID
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("X-CRM-Region", c.region)
	resp, err := c.client.Do(req)
	if err != nil {
		// A refused connection, a DNS failure or the deadline: hikyaku
		// classifies them (transient with the tag connect, or a timeout).
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxReply))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// 429 and 5xx are retried, with the CRM's Retry-After; another 4xx
		// is permanent: straight to the fallback or the dead-letter queue.
		detail := strings.TrimSpace(string(body[:min(len(body), 4096)]))
		return nil, plugin.StatusError(resp.StatusCode, plugin.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), detail)
	}
	return &plugin.Result{Status: resp.StatusCode, Body: body, ContentType: resp.Header.Get("Content-Type"), Endpoint: c.url}, nil
}

// checkLead refuses a message that is not a lead: a JSON object with an
// email. That is permanent, since no retry can fix it.
func checkLead(body []byte) error {
	var lead map[string]any
	if err := json.Unmarshal(body, &lead); err != nil {
		return plugin.Errorf(plugin.Permanent, []string{"crm:invalid"}, "not a lead: the body is not a JSON object")
	}
	if email, _ := lead["email"].(string); !strings.Contains(email, "@") {
		return plugin.Errorf(plugin.Permanent, []string{"crm:invalid"}, "not a lead: it has no email")
	}
	return nil
}

// Health has nothing to check without sending a lead: the CRM's answers
// tell the circuit breaker how it is doing.
func (c *CRM) Health(context.Context) error { return nil }

// Close releases the idle connections.
func (c *CRM) Close(context.Context) error {
	c.client.CloseIdleConnections()
	return nil
}
