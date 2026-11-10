// Package azure is hikyaku's Azure messaging protocols: the servicebus and
// eventhubs source and destination types, built on the Azure SDK for Go.
//
// Every entry connects with its azure: block: a connection string, or a
// fully qualified namespace with Microsoft Entra credentials (client secret
// credentials when tenant_id, client_id and client_secret are set, else
// DefaultAzureCredential). The connection string is never logged.
//
// A servicebus source receives from a queue, or a topic subscription, in
// PeekLock mode, with at most prefetch messages in flight. A message is
// completed once its outcome is final (delivered, dead-lettered, unrouted or
// filtered) and abandoned when it failed, so Service Bus redelivers it and
// dead-letters it after the entity's MaxDeliveryCount. The lock of a message
// still in flight past half its lock duration is renewed. An entity that
// requires sessions is received session by session (several sessions at
// once), renewing each session's lock instead.
//
// An eventhubs source reads every partition of an event hub in a consumer
// group. Each partition's checkpoint, the sequence number up to which every
// event was settled final, lives in a hikyaku store (state_store), so a
// restart resumes after it; an event that failed holds the checkpoint and is
// read again after a restart. Replicas share the partitions through leases
// in the same store, and a replica opens each partition it owns with a
// higher owner level than any reader before it, so Event Hubs disconnects a
// previous reader of the partition.
//
// Messages over max_body_bytes are logged and acknowledged (completed, or
// passed by the checkpoint), because they would otherwise come back forever.
//
// Sources set the attributes servicebus.* or eventhubs.* and ordering_key
// (the session ID, or the partition). Destinations send one message per
// delivery, or native batches for a destination with batch:, with the
// envelope ID as the message ID and the headers as application properties.
package azure

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/textproto"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Register makes the servicebus and eventhubs source and destination types
// available.
func Register() {
	plugin.RegisterSource(kindServiceBus, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSBSource(ctx, spec)
	})
	plugin.RegisterSink(kindServiceBus, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newSBSink(ctx, spec)
	})
	plugin.RegisterSource(kindEventHubs, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newEHSource(ctx, spec)
	})
	plugin.RegisterSink(kindEventHubs, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newEHSink(ctx, spec)
	})
}

// Content types of received messages that do not declare one.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// idempotencyKey is the application property that carries a message's
// idempotency key; a source takes it as the envelope ID.
const idempotencyKey = "Idempotency-Key"

// maxIdempotencyKey bounds an Idempotency-Key taken as the envelope ID, as
// the http source does.
const maxIdempotencyKey = 256

// contentType is the declared content type, or one detected from the body:
// JSON, UTF-8 text, else binary.
func contentType(declared *string, body []byte) string {
	if declared != nil && *declared != "" {
		return *declared
	}
	switch {
	case len(body) > 0 && json.Valid(body):
		return contentJSON
	case utf8.Valid(body):
		return contentText
	}
	return contentBinary
}

// amqpBody is the payload of a message whose body is not a single data
// section: data sections are concatenated, and an amqp-value or
// amqp-sequence body is its text, or else its JSON.
func amqpBody(data [][]byte, value any, sequence [][]any) []byte {
	switch {
	case len(data) > 0:
		return slices.Concat(data...)
	case value != nil:
		switch v := value.(type) {
		case []byte:
			return v
		case string:
			return []byte(v)
		}
		if b, err := json.Marshal(value); err == nil {
			return b
		}
		return []byte(fmt.Sprint(value))
	case len(sequence) > 0:
		if b, err := json.Marshal(sequence); err == nil {
			return b
		}
	}
	return nil
}

// stringify renders an application property value as a header value.
func stringify(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case fmt.Stringer:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	}
	return fmt.Sprint(v)
}

// headersFrom turns application properties into envelope headers, one
// stringified value each, under the property names as they came.
func headersFrom(props map[string]any) map[string][]string {
	h := make(map[string][]string, len(props))
	for k, v := range props {
		h[k] = []string{stringify(v)}
	}
	return h
}

// idempotencyFrom returns the Idempotency-Key property, matched
// case-insensitively, when it is usable as an envelope ID.
func idempotencyFrom(props map[string]any) string {
	for k, v := range props {
		if strings.EqualFold(k, idempotencyKey) {
			if s := stringify(v); s != "" && len(s) <= maxIdempotencyKey {
				return s
			}
		}
	}
	return ""
}

// dropHeaders are not sent as application properties: hop-by-hop and
// framing headers, the caller's credentials, and Content-Type (it is the
// message's content type).
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Accept-Encoding": true,
	"Content-Type": true,
}

// outgoing is what a destination renders from an envelope before it sends:
// the body and the application properties.
type outgoing struct {
	headers map[string]*transform.Template
	body    *transform.Template
}

// compileOutgoing compiles a destination's headers and body templates.
func compileOutgoing(headers map[string]string, body string) (outgoing, error) {
	o := outgoing{headers: map[string]*transform.Template{}}
	for _, k := range slices.Sorted(maps.Keys(headers)) {
		t, err := transform.CompileTemplate("headers."+k, headers[k])
		if err != nil {
			return outgoing{}, err
		}
		o.headers[k] = t
	}
	if body != "" {
		t, err := transform.CompileTemplate("body", body)
		if err != nil {
			return outgoing{}, err
		}
		o.body = t
	}
	return o, nil
}

// render returns the body (the body template's output, or the message
// body) and the application properties: the envelope's headers (several
// values joined with ", ") minus dropHeaders, then the rendered headers
// templates, and Idempotency-Key when no header carries it.
func (o outgoing) render(env *envelope.Envelope) ([]byte, map[string]any, error) {
	body := env.Body
	if o.body != nil {
		b, err := o.body.Render(env)
		if err != nil {
			return nil, nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		body = []byte(b)
	}
	props := make(map[string]any, len(env.Headers)+len(o.headers)+1)
	for k, vs := range env.Headers {
		if len(vs) == 0 || dropHeaders[textproto.CanonicalMIMEHeaderKey(k)] {
			continue
		}
		props[k] = strings.Join(vs, ", ")
	}
	for k, t := range o.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		for p := range props {
			if strings.EqualFold(p, k) {
				delete(props, p)
			}
		}
		props[k] = v
	}
	if !hasProperty(props, idempotencyKey) {
		key := env.ID
		if v, ok := env.Attribute("idempotency_key"); ok {
			if s, ok := v.(string); ok && s != "" {
				key = s
			}
		}
		props[idempotencyKey] = key
	}
	return body, props, nil
}

func hasProperty(props map[string]any, name string) bool {
	for k := range props {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}

// renderKey renders an optional per-message template (a partition key, a
// session ID); "" when there is none.
func renderKey(t *transform.Template, env *envelope.Envelope) (string, error) {
	if t == nil {
		return "", nil
	}
	v, err := t.Render(env)
	if err != nil {
		return "", plugin.Wrap(plugin.Permanent, []string{"template"}, err)
	}
	return v, nil
}

// rfc3339 formats an optional time for an attribute.
func rfc3339(t *time.Time) (string, bool) {
	if t == nil || t.IsZero() {
		return "", false
	}
	return t.UTC().Format(time.RFC3339Nano), true
}
