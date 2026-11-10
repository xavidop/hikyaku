package grpcplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
	"github.com/xavidop/hikyaku/plugin"
)

// Proto strings must be valid UTF-8; a Go string need not be (a binary
// broker header). Invalid bytes become U+FFFD instead of failing the call.
func validUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "�")
}

func headersToPB(h map[string][]string) map[string]*pluginpb.HeaderValues {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]*pluginpb.HeaderValues, len(h))
	for k, vs := range h {
		values := make([]string, len(vs))
		for i, v := range vs {
			values[i] = validUTF8(v)
		}
		out[validUTF8(k)] = &pluginpb.HeaderValues{Values: values}
	}
	return out
}

func headersFromPB(h map[string]*pluginpb.HeaderValues) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		out[k] = slices.Clone(vs.GetValues())
	}
	return out
}

// attributesJSON encodes attributes; a value JSON cannot carry is dropped
// rather than failing the message.
func attributesJSON(attrs map[string]any) string {
	if len(attrs) == 0 {
		return ""
	}
	if b, err := json.Marshal(attrs); err == nil {
		return string(b)
	}
	keep := make(map[string]any, len(attrs))
	for k, v := range attrs {
		if _, err := json.Marshal(v); err == nil {
			keep[k] = v
		}
	}
	b, _ := json.Marshal(keep)
	return string(b)
}

func attributesFromJSON(s string) (map[string]any, error) {
	m := map[string]any{}
	if s == "" {
		return m, nil
	}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return map[string]any{}, fmt.Errorf("attributes_json: %w", err)
	}
	return m, nil
}

var traceContext = propagation.TraceContext{}

func traceToPB(sc trace.SpanContext) *pluginpb.Trace {
	if !sc.IsValid() {
		return nil
	}
	c := propagation.MapCarrier{}
	traceContext.Inject(trace.ContextWithSpanContext(context.Background(), sc), c)
	return &pluginpb.Trace{Traceparent: c.Get("traceparent"), Tracestate: c.Get("tracestate")}
}

func traceFromPB(t *pluginpb.Trace) trace.SpanContext {
	if t.GetTraceparent() == "" {
		return trace.SpanContext{}
	}
	c := propagation.MapCarrier{"traceparent": t.GetTraceparent()}
	if t.GetTracestate() != "" {
		c["tracestate"] = t.GetTracestate()
	}
	return trace.SpanContextFromContext(traceContext.Extract(context.Background(), c))
}

// envelopeToPB converts an envelope for the wire.
func envelopeToPB(e *envelope.Envelope) *pluginpb.Envelope {
	pe := &pluginpb.Envelope{
		Id:             validUTF8(e.ID),
		Source:         validUTF8(e.Source),
		Protocol:       validUTF8(e.Protocol),
		Subject:        validUTF8(e.Subject),
		Headers:        headersToPB(e.Headers),
		ContentType:    validUTF8(e.ContentType),
		Body:           e.Body,
		AttributesJson: attributesJSON(e.AttributesCopy()),
		Trace:          traceToPB(e.Trace),
	}
	if !e.ReceivedAt.IsZero() {
		pe.ReceivedAt = timestamppb.New(e.ReceivedAt)
	}
	return pe
}

// envelopeFromPB converts a wire envelope. The envelope is usable even
// with an error, which only reports attributes that did not parse.
func envelopeFromPB(pe *pluginpb.Envelope) (*envelope.Envelope, error) {
	attrs, err := attributesFromJSON(pe.GetAttributesJson())
	e := &envelope.Envelope{
		ID:          pe.GetId(),
		Source:      pe.GetSource(),
		Protocol:    pe.GetProtocol(),
		Subject:     pe.GetSubject(),
		Headers:     headersFromPB(pe.GetHeaders()),
		ContentType: pe.GetContentType(),
		Body:        pe.GetBody(),
		Attributes:  attrs,
		Trace:       traceFromPB(pe.GetTrace()),
	}
	if ts := pe.GetReceivedAt(); ts != nil {
		e.ReceivedAt = ts.AsTime()
	}
	return e, err
}

func envelopesToPB(envs []*envelope.Envelope) []*pluginpb.Envelope {
	out := make([]*pluginpb.Envelope, len(envs))
	for i, e := range envs {
		out[i] = envelopeToPB(e)
	}
	return out
}

func resultToPB(r *plugin.Result) *pluginpb.Result {
	if r == nil {
		return nil
	}
	return &pluginpb.Result{
		Status:      int32(min(max(r.Status, 0), 1<<31-1)),
		Headers:     headersToPB(r.Headers),
		Body:        r.Body,
		ContentType: validUTF8(r.ContentType),
		Endpoint:    validUTF8(r.Endpoint),
		Reroute:     r.Reroute,
	}
}

func resultFromPB(r *pluginpb.Result) *plugin.Result {
	if r == nil {
		return nil
	}
	out := &plugin.Result{
		Status:      int(r.GetStatus()),
		Body:        r.GetBody(),
		ContentType: r.GetContentType(),
		Endpoint:    r.GetEndpoint(),
		Reroute:     slices.Clone(r.GetReroute()),
	}
	if len(r.GetHeaders()) > 0 {
		out.Headers = headersFromPB(r.GetHeaders())
	}
	return out
}

func classToPB(c plugin.Class) pluginpb.ErrorClass {
	switch c {
	case plugin.RateLimited:
		return pluginpb.ErrorClass_ERROR_CLASS_RATE_LIMITED
	case plugin.Permanent:
		return pluginpb.ErrorClass_ERROR_CLASS_PERMANENT
	case plugin.Timeout:
		return pluginpb.ErrorClass_ERROR_CLASS_TIMEOUT
	}
	return pluginpb.ErrorClass_ERROR_CLASS_TRANSIENT
}

func classFromPB(c pluginpb.ErrorClass) plugin.Class {
	switch c {
	case pluginpb.ErrorClass_ERROR_CLASS_RATE_LIMITED:
		return plugin.RateLimited
	case pluginpb.ErrorClass_ERROR_CLASS_PERMANENT:
		return plugin.Permanent
	case pluginpb.ErrorClass_ERROR_CLASS_TIMEOUT:
		return plugin.Timeout
	}
	return plugin.Transient // the default, and any class this side does not know
}

// errorToPB classifies an error for the wire. An unclassified error is
// classified as the host would (a timeout, a refused connection, else
// transient).
func errorToPB(err error) *pluginpb.Error {
	if err == nil {
		return nil
	}
	class, retryAfter := plugin.Classify(err)
	return &pluginpb.Error{
		Class:        classToPB(class),
		RetryAfterMs: retryAfter.Milliseconds(),
		Tags:         plugin.Tags(err),
		Message:      validUTF8(err.Error()),
	}
}

// errorFromPB turns a wire error back into a *plugin.Error.
func errorFromPB(e *pluginpb.Error) error {
	if e == nil {
		return nil
	}
	class := classFromPB(e.GetClass())
	msg := e.GetMessage()
	if msg == "" {
		msg = class.String()
	}
	return &plugin.Error{
		Class:      class,
		RetryAfter: time.Duration(max(e.GetRetryAfterMs(), 0)) * time.Millisecond,
		Tags:       slices.Clone(e.GetTags()),
		Err:        errors.New(msg),
	}
}

// failure is the plain error of a response that reports one: a failed
// open, health check or close.
func failure(e *pluginpb.Error) error {
	if e == nil {
		return nil
	}
	if e.GetMessage() == "" {
		return errors.New(classFromPB(e.GetClass()).String())
	}
	return errors.New(e.GetMessage())
}
