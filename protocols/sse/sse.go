// Package sse is hikyaku's server-sent events protocol: the sse source and
// destination type.
//
// A subscribe source GETs a URL and reads its event stream, one envelope per
// event. It reconnects with backoff (a server's retry: sets the base delay),
// rotates through failover_urls, and resumes with Last-Event-ID. A listen
// source takes an event stream pushed to it as the body of a POST, parsed as
// it streams in.
//
// A destination serves an event stream: every delivery is broadcast to the
// clients connected at the time, and the last events are kept so a client
// that reconnects with Last-Event-ID gets what it missed.
package sse

import (
	"context"
	"encoding/json"
	"mime"
	"net/url"

	"github.com/xavidop/hikyaku/plugin"
)

// Register makes the sse source and destination type available.
func Register() {
	plugin.RegisterSource("sse", func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSource(ctx, spec)
	})
	plugin.RegisterSink("sse", func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newSink(ctx, spec)
	})
}

// contentType is the media type of an event stream.
const contentType = "text/event-stream"

// isEventStream reports whether a Content-Type header names an event
// stream.
func isEventStream(header string) bool {
	mt, _, err := mime.ParseMediaType(header)
	return err == nil && mt == contentType
}

// textContentType is the content type of an event's data: JSON when it
// parses as JSON, else plain text.
func textContentType(b []byte) string {
	if json.Valid(b) {
		return "application/json"
	}
	return "text/plain; charset=utf-8"
}

// redactURL hides a URL's password, for logs and attributes.
func redactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Redacted()
}

// urlPath is the path of a URL, "/" when it has none.
func urlPath(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Path == "" {
		return "/"
	}
	return u.Path
}
