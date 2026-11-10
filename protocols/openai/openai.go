// Package openai is hikyaku's LLM-proxy source (type: openai): an
// OpenAI-compatible endpoint, so any OpenAI SDK or tool can point its base
// URL at the relay.
//
// It serves POST <path>/chat/completions and GET <path>/models (path
// defaults to /v1). Each chat request becomes one envelope: the request
// JSON is the body, the requested model is the subject (and msg.json.model
// for rules), and routes pick the destination, typically model
// destinations (type: model, in the llm module) whose retries and
// fallbacks fail over between models and providers. The caller gets the
// reply of the destination that served the request.
//
// With "stream": true the source answers with server-sent events: a model
// destination writes each chunk as it is generated, and the source ends the
// stream with data: [DONE]. A destination that does not stream (an http
// upstream) is answered as a single chunk, or passed through when it
// already answered with an event stream.
package openai

import (
	"context"

	"github.com/xavidop/hikyaku/plugin"
)

// Register makes the openai source type available.
func Register() {
	plugin.RegisterSource("openai", func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSource(ctx, spec)
	})
}
