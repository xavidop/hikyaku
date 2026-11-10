// Package grpc is hikyaku's gRPC protocol: the grpc source, in listen and
// subscribe modes, and the grpc destination.
//
// A listen source is a gRPC server. It always serves
// hikyaku.relay.v1.Relay/Send (package relaypb), whose request carries one
// message's ID, subject, headers, content type and body. With
// descriptor_set it also serves the services of that FileDescriptorSet
// (all, or those named in services) without generated code: a unary call is
// one message, its request as protojson and its subject the full method
// name; every request of a client-streaming call is one message, and the
// call is answered once they are all settled. A server-streaming call's
// request is one message, and a respond.from reply that is a JSON array (or
// NDJSON) streams back one response per value. Every request of a bidi call
// is one message, answered in request order once its outcome is final, with
// at most 64 waiting at a time. With ack: delivered (the default) a call is
// answered once the relay is done with its message: OK with the
// respond.from destination's reply mapped into the method's output type (an
// empty output without respond), or UNAVAILABLE when the relay could not
// keep the message. With ack: accepted it is answered as soon as the relay
// holds the message. A full buffer answers RESOURCE_EXHAUSTED with a
// retry-after trailer instead of blocking. The source also serves
// grpc.health.v1.Health and, unless reflection is false, server reflection
// (v1 and v1alpha).
//
// A subscribe source calls one server-streaming method with the request in
// body and turns every streamed response into a message. It resolves the
// method from descriptor_set or through server reflection, reconnects with
// backoff and jitter, and rotates through failover_urls. A gRPC stream has
// no acknowledgements: a message is delivered at least once only while the
// stream is up, and what the server sent while it was down is not
// redelivered.
//
// A destination makes one call per delivery over one long-lived
// connection, to a method of any kind: the message body (or the body
// template) is the protojson request, and the idempotency key goes in the
// idempotency-key metadata. A streaming call sends the request, closes its
// side and reads until the server ends the stream. The reply is the
// response as protojson, or a JSON array of the responses of a
// server-streaming or bidi method. A client- or bidi-streaming method takes
// a batch in one call, one request per message (plugin.BatchSink). Failures
// carry a grpc:<CODE> tag, such as grpc:UNAVAILABLE, and the broad tags the
// default retry policy matches (5xx, 4xx, 429, timeout, connect).
//
// Attributes: grpc.method on every message; grpc.peer and remote_addr on a
// listen source's messages; grpc.url and ordering_key (the method) on a
// subscribe source's.
package grpc

//go:generate protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative relaypb/relay.proto
//go:generate protoc -I testdata --include_imports --descriptor_set_out=testdata/echo.pb echo.proto
//go:generate protoc -I testdata --go_out=internal/echopb --go_opt=paths=source_relative --go-grpc_out=internal/echopb --go-grpc_opt=paths=source_relative echo.proto

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// kind is the protocol type of the source and the destination.
const kind = "grpc"

// Content types.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// defaultMaxBytes is the largest message a source takes when
// max_body_bytes is not set: gRPC's own default.
const defaultMaxBytes = 4 << 20

// Register makes the grpc source and destination types available.
func Register() {
	plugin.RegisterSource(kind, newSource,
		plugin.WithDescription("gRPC: listen (hikyaku.relay.v1.Relay and descriptor-set services) or subscribe (a server-streaming method)"))
	plugin.RegisterSink(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		s, err := newSink(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	}, plugin.WithDescription("gRPC: one call per message to a unary or streaming method, described by a descriptor set or server reflection"))
}

// newSource builds a listen or subscribe source.
func newSource(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return nil, fmt.Errorf("source %s: missing config", spec.Name)
	}
	mode := src.Mode
	if mode == "" {
		typed := *src
		typed.Type = kind
		mode = config.DefaultMode(typed)
	}
	switch mode {
	case modeListen:
		l, err := newListener(ctx, spec, src)
		if err != nil {
			return nil, err
		}
		return l, nil
	case modeSubscribe:
		s, err := newSubscriber(spec, src)
		if err != nil {
			return nil, err
		}
		return s, nil
	}
	return nil, fmt.Errorf("source %s: grpc sources run in listen or subscribe mode, not %q", spec.Name, mode)
}

// The JSON mapping of messages is grpc-gateway's: lowerCamelCase field
// names, unpopulated fields included, unknown fields ignored on the way in.
var (
	jsonOut = protojson.MarshalOptions{EmitUnpopulated: true}
	jsonIn  = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// toJSON renders a message as compact JSON (protojson adds random spaces
// to keep callers from depending on its exact bytes; compacting removes
// them).
func toJSON(m proto.Message, r resolver) ([]byte, error) {
	o := jsonOut
	o.Resolver = r
	b, err := o.Marshal(m)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := json.Compact(&out, b); err != nil {
		return b, nil
	}
	return out.Bytes(), nil
}

// fromJSON fills m from JSON; an empty or blank body leaves m empty.
func fromJSON(b []byte, m proto.Message, r resolver) error {
	if len(bytes.TrimSpace(b)) == 0 {
		return nil
	}
	o := jsonIn
	o.Resolver = r
	return o.Unmarshal(b, m)
}

// isProtobuf reports a content type that means a binary protobuf body.
func isProtobuf(contentType string) bool {
	switch mediaType(contentType) {
	case "application/protobuf", "application/x-protobuf", "application/vnd.google.protobuf", "application/x-google-protobuf":
		return true
	}
	return false
}

// mediaType is a content type without its parameters, lowercased.
func mediaType(ct string) string {
	ct, _, _ = strings.Cut(ct, ";")
	return strings.ToLower(strings.TrimSpace(ct))
}

// sniff is the content type of a body that came without one.
func sniff(b []byte) string {
	switch {
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	}
	return contentBinary
}
