package llm

import (
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/store"
)

// FlowInput is the input of a route/<name> flow: a message, as the Dev UI
// and hikyaku ai test send it.
type FlowInput struct {
	ID          string              `json:"id,omitempty"`
	Source      string              `json:"source,omitempty"`
	Subject     string              `json:"subject,omitempty"`
	Headers     map[string][]string `json:"headers,omitempty"`
	ContentType string              `json:"content_type,omitempty"`
	// Body is the message body as text (JSON bodies as their JSON).
	Body       string         `json:"body"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// InputOf turns an envelope into a flow input.
func InputOf(env *envelope.Envelope) FlowInput {
	return FlowInput{
		ID: env.ID, Source: env.Source, Subject: env.Subject, Headers: env.Headers,
		ContentType: env.ContentType, Body: env.BodyText(), Attributes: env.AttributesCopy(),
	}
}

// Envelope rebuilds the message of a flow input.
func (in FlowInput) Envelope() *envelope.Envelope {
	env := envelope.New(in.Source, "flow", []byte(in.Body))
	if in.ID != "" {
		env.ID = in.ID
	}
	env.Subject, env.ContentType = in.Subject, in.ContentType
	for k, v := range in.Headers {
		env.Headers[k] = v
	}
	for k, v := range in.Attributes {
		env.Attributes[k] = v
	}
	return env
}

// FlowOutput is an AI routing decision: what a route/<name> flow returns.
type FlowOutput struct {
	Destinations  []string       `json:"destinations"`
	Engine        string         `json:"engine"`
	Model         string         `json:"model,omitempty"`
	Confidence    float64        `json:"confidence"`
	LowConfidence bool           `json:"low_confidence,omitempty"`
	Sticky        bool           `json:"sticky,omitempty"`
	Cached        bool           `json:"cached,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	Answers       []store.Answer `json:"answers"`
	// FollowUp is the sticky question's probability, when it was asked.
	FollowUp *float64 `json:"follow_up,omitempty"`
	// HistoryTurns is how many past calls the model saw.
	HistoryTurns int `json:"history_turns"`
	// ModelHops lists the models that failed before the one that answered.
	ModelHops []string          `json:"model_hops,omitempty"`
	LatencyMs int64             `json:"latency_ms"`
	Tokens    int               `json:"tokens,omitempty"`
	Record    *store.CallRecord `json:"record,omitempty"`
}

// normalized gives nil lists their empty value: the flow's output schema
// wants arrays.
func (o FlowOutput) normalized() FlowOutput {
	if o.Destinations == nil {
		o.Destinations = []string{}
	}
	if o.Answers == nil {
		o.Answers = []store.Answer{}
	}
	if o.Record != nil {
		r := *o.Record
		if r.DeliveredTo == nil {
			r.DeliveredTo = []string{}
		}
		if r.Decision == nil {
			r.Decision = []store.Answer{}
		}
		o.Record = &r
	}
	return o
}

// Attribute is the ai.decision attribute set on the envelope (design §6.4
// step 6): model, answers, probabilities, reason, which fallback served it.
func (o FlowOutput) Attribute() map[string]any {
	m := map[string]any{
		"engine":       o.Engine,
		"model":        o.Model,
		"destinations": o.Destinations,
		"confidence":   o.Confidence,
		"answers":      o.Answers,
	}
	if o.Reason != "" {
		m["reason"] = o.Reason
	}
	if o.LowConfidence {
		m["low_confidence"] = true
	}
	if o.Sticky {
		m["sticky"] = true
	}
	if o.Cached {
		m["cached"] = true
	}
	if len(o.ModelHops) > 0 {
		m["model_hops"] = o.ModelHops
	}
	if o.FollowUp != nil {
		m["follow_up"] = *o.FollowUp
	}
	if o.HistoryTurns > 0 {
		m["history_turns"] = o.HistoryTurns
	}
	return m
}

func nowUTC() time.Time { return time.Now().UTC() }
