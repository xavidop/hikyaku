// Package transform holds the per-message expression languages of a spec:
// Go templates ({{ .json.id }}), CEL (msg.json.type == "x") and jq, and the
// transform steps built from them (filter, jq, template, headers, codec).
package transform

import (
	"net/textproto"

	"github.com/xavidop/hikyaku/envelope"
)

// Data is the view of an envelope that templates and CEL see:
//
//	.json        the parsed JSON body (an empty object when the body is not JSON)
//	.headers     header name (canonical) → first value
//	.bodyText    the body as text
//	.attributes  protocol metadata and decisions (ai.decision, ...)
//	.source .subject .id .protocol .contentType .receivedAt
func Data(env *envelope.Envelope) map[string]any {
	j := env.JSONOrNil()
	if j == nil {
		j = map[string]any{}
	}
	return map[string]any{
		"json":        j,
		"headers":     Headers(env),
		"bodyText":    env.BodyText(),
		"attributes":  env.AttributesCopy(),
		"source":      env.Source,
		"subject":     env.Subject,
		"id":          env.ID,
		"protocol":    env.Protocol,
		"contentType": env.ContentType,
		"receivedAt":  env.ReceivedAt,
	}
}

// Headers flattens an envelope's headers to their first values, under
// canonical names.
func Headers(env *envelope.Envelope) map[string]any {
	out := make(map[string]any, len(env.Headers))
	for k, v := range env.Headers {
		if len(v) > 0 {
			out[textproto.CanonicalMIMEHeaderKey(k)] = v[0]
		}
	}
	return out
}
