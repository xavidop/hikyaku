// Package gcp is hikyaku's Google Cloud protocols: the pubsub and gcs source
// and destination types.
//
// A pubsub source subscribes to a subscription with StreamingPull. It keeps
// at most prefetch messages in flight, and the client extends their ack
// deadlines while the relay works on them. A message is acked once the relay
// is done with it (delivered, dead-lettered, unrouted or filtered) and nacked
// when it failed, so Pub/Sub redelivers it under the subscription's retry
// policy and dead-letter topic. The envelope ID is the Idempotency-Key
// attribute, else the message ID. The other attributes become headers, and
// pubsub.message_id, pubsub.publish_time, pubsub.ordering_key,
// pubsub.delivery_attempt and pubsub.subscription describe the message.
//
// A pubsub destination publishes each message to its topic (a per-message
// template), with the headers as attributes plus Idempotency-Key,
// content-type and subject, and an ordering key from key.
//
// A gcs source polls bucket/prefix every interval. Each object becomes one
// message, or one per line (split: lines) or per output of a jq split. Once
// every message of an object is final, the object is deleted (after: delete,
// the default), moved under move_to (after: move), or remembered in
// state_store (after: keep). gcs.bucket, gcs.name, gcs.generation, gcs.size
// and gcs.line describe each message.
//
// A gcs destination writes each message as one object, and a batch as one
// NDJSON object, under key. The write has a does-not-exist precondition, so
// a retried delivery does not write the object twice.
package gcp

import (
	"context"
	"encoding/json"
	"unicode/utf8"

	"github.com/xavidop/hikyaku/plugin"
)

// The protocol kinds.
const (
	kindPubSub = "pubsub"
	kindGCS    = "gcs"
)

// Register makes the pubsub and gcs source and destination types available.
func Register() {
	plugin.RegisterSource(kindPubSub, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newPubSubSource(ctx, spec)
	})
	plugin.RegisterSink(kindPubSub, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newPubSubSink(ctx, spec)
	})
	plugin.RegisterSource(kindGCS, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newGCSSource(ctx, spec)
	})
	plugin.RegisterSink(kindGCS, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newGCSSink(ctx, spec)
	})
}

// Content types of received messages.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
	contentNDJSON = "application/x-ndjson"
)

// detectContentType names what a body looks like: JSON, UTF-8 text or
// binary. An empty body has no content type.
func detectContentType(b []byte) string {
	switch {
	case len(b) == 0:
		return ""
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	}
	return contentBinary
}

// equalFold compares two ASCII strings case-insensitively.
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if lowerASCII(a[i]) != lowerASCII(b[i]) {
			return false
		}
	}
	return true
}

// hasPrefixFold reports whether s starts with prefix, ignoring ASCII case.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && equalFold(s[:len(prefix)], prefix)
}

func lowerASCII(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + 'a' - 'A'
	}
	return c
}

// lookupFold returns the value of a key, matched exactly first and then
// ignoring ASCII case.
func lookupFold(m map[string]string, key string) (string, bool) {
	if v, ok := m[key]; ok {
		return v, true
	}
	for k, v := range m {
		if equalFold(k, key) {
			return v, true
		}
	}
	return "", false
}
