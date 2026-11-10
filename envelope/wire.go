package envelope

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/trace"
)

// wire is the JSON form of an envelope: one NDJSON line in a file sink, the
// dead-letter queue, `hikyaku tail` and `dlq replay`.
//
// The body is stored so it comes back byte for byte. A compact JSON body is
// embedded as JSON (readable archives), any other UTF-8 body as body_text, and
// anything else as body_base64. Exactly one of the three is set when the body
// is not empty.
type wire struct {
	ID          string              `json:"id"`
	Source      string              `json:"source,omitempty"`
	Protocol    string              `json:"protocol,omitempty"`
	Subject     string              `json:"subject,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	ContentType string              `json:"content_type,omitempty"`
	Body        json.RawMessage     `json:"body,omitempty"`
	BodyText    *string             `json:"body_text,omitempty"`
	BodyBase64  string              `json:"body_base64,omitempty"`
	Attributes  map[string]any      `json:"attributes,omitempty"`
	ReceivedAt  time.Time           `json:"received_at"`
	TraceID     string              `json:"trace_id,omitempty"`
	SpanID      string              `json:"span_id,omitempty"`
}

// MarshalJSON encodes the envelope in its wire form.
func (e *Envelope) MarshalJSON() ([]byte, error) {
	w := wire{
		ID:          e.ID,
		Source:      e.Source,
		Protocol:    e.Protocol,
		Subject:     e.Subject,
		Headers:     e.Headers,
		ContentType: e.ContentType,
		Attributes:  e.AttributesCopy(),
		ReceivedAt:  e.ReceivedAt,
	}
	switch {
	case len(e.Body) == 0:
	case embeddable(e.Body):
		w.Body = json.RawMessage(e.Body)
	case utf8.Valid(e.Body):
		s := string(e.Body)
		w.BodyText = &s
	default:
		w.BodyBase64 = base64.StdEncoding.EncodeToString(e.Body)
	}
	if e.Trace.HasTraceID() {
		w.TraceID = e.Trace.TraceID().String()
	}
	if e.Trace.HasSpanID() {
		w.SpanID = e.Trace.SpanID().String()
	}
	return json.Marshal(w)
}

// UnmarshalJSON decodes the wire form.
func (e *Envelope) UnmarshalJSON(b []byte) error {
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		return err
	}
	if w.ID == "" {
		return fmt.Errorf("envelope: missing id")
	}
	*e = Envelope{
		ID:          w.ID,
		Source:      w.Source,
		Protocol:    w.Protocol,
		Subject:     w.Subject,
		Headers:     w.Headers,
		ContentType: w.ContentType,
		Attributes:  w.Attributes,
		ReceivedAt:  w.ReceivedAt,
	}
	if e.Headers == nil {
		e.Headers = map[string][]string{}
	}
	if e.Attributes == nil {
		e.Attributes = map[string]any{}
	}
	switch {
	case len(w.Body) > 0 && string(w.Body) != "null":
		e.Body = []byte(w.Body)
	case w.BodyText != nil:
		e.Body = []byte(*w.BodyText)
	case w.BodyBase64 != "":
		body, err := base64.StdEncoding.DecodeString(w.BodyBase64)
		if err != nil {
			return fmt.Errorf("envelope %s: body_base64: %w", w.ID, err)
		}
		e.Body = body
	}
	if w.TraceID != "" {
		tid, err := trace.TraceIDFromHex(w.TraceID)
		if err == nil {
			cfg := trace.SpanContextConfig{TraceID: tid}
			if sid, err := trace.SpanIDFromHex(w.SpanID); err == nil {
				cfg.SpanID = sid
			}
			e.Trace = trace.NewSpanContext(cfg)
		}
	}
	return nil
}

// embeddable reports whether b can be embedded as raw JSON and still come
// back byte for byte: valid, already compact, and free of the characters the
// encoder would escape.
func embeddable(b []byte) bool {
	if !json.Valid(b) {
		return false
	}
	if bytes.ContainsAny(b, "<>&") || bytes.Contains(b, []byte(" ")) || bytes.Contains(b, []byte(" ")) {
		return false
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return false
	}
	return bytes.Equal(buf.Bytes(), b)
}
