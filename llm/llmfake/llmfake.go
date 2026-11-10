// Package llmfake provides deterministic fake models for tests and for the
// *.llmfake.yaml example specs, so the AI router runs end to end with no
// network and no keys. It is linked only into -tags llmfake builds.
//
// The decision models are served by the real System One plugin through an
// in-process HTTP transport that speaks the System One wire protocol, so
// requests, questions, history state and answers take the same path a real
// server's do. Both kinds of model score each option by the words it
// shares with the message: a message that says "invoice" goes to the
// destination whose description says "invoices".
//
//	llmfake/router              generative: routes by keyword overlap
//	llmfake/chat                generative: "You said: <the last user message>", and tool calls (see chat)
//	llmfake/basic               generative: chat, without tool_choice or media support (like an Ollama model)
//	llmfake/down                generative: always UNAVAILABLE (retried, then the next model)
//	llmfake/bad-json            generative: answers names that are not candidates (bad output)
//	llmfake/denied              generative: UNAUTHENTICATED (not retried)
//	llmfake-decisions/clef      decision: calibrated-looking probabilities by keyword overlap
//	llmfake-decisions/down      decision: HTTP 503
//	llmfake-decisions/garbage   decision: a choice that is no option (bad output)
package llmfake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/core/status"
	systemonex "github.com/firebase/genkit/go/plugins/systemone/exp"
)

// Provider names.
const (
	Provider         = "llmfake"
	DecisionProvider = "llmfake-decisions"
)

// Calls counts the requests every fake model has served (tests).
var Calls atomic.Int64

// Plugins returns the fake generative provider and the fake decision
// server.
func Plugins() []api.Plugin { return []api.Plugin{&Generative{}, Decisions()} }

// Decisions returns a System One plugin whose server is in-process.
func Decisions() *systemonex.SystemOne {
	return &systemonex.SystemOne{
		Provider:   DecisionProvider,
		BaseURL:    "http://llmfake.invalid",
		ModelsPath: "/v1/models",
		HTTPClient: &http.Client{Transport: server{}},
	}
}

// Generative is the fake generative provider.
type Generative struct{}

// Name implements api.Plugin.
func (*Generative) Name() string { return Provider }

// Init defines the fake generative models.
func (g *Generative) Init(context.Context) []api.Action {
	supports := &ai.ModelSupports{Multiturn: true, SystemRole: true, Output: []string{"json", "text"}}
	chat := &ai.ModelSupports{Multiturn: true, SystemRole: true, Media: true, Tools: true, ToolChoice: true, Output: []string{"json", "text"}}
	basic := &ai.ModelSupports{Multiturn: true, SystemRole: true, Tools: true, Output: []string{"json", "text"}}
	return []api.Action{
		ai.NewModelAction[any](Provider+"/router", &ai.ModelOptions{Label: "fake router", Supports: supports}, g.router),
		ai.NewModelAction[any](Provider+"/bad-json", &ai.ModelOptions{Label: "fake bad output", Supports: supports}, g.badJSON),
		ai.NewModelAction[any](Provider+"/chat", &ai.ModelOptions{Label: "fake chat", Supports: chat}, g.chat),
		ai.NewModelAction[any](Provider+"/basic", &ai.ModelOptions{Label: "fake chat without tool choice or media", Supports: basic}, g.chat),
		ai.NewModelAction[any](Provider+"/down", &ai.ModelOptions{Label: "fake outage", Supports: supports}, func(context.Context, *ai.ModelRequest, any, ai.ModelStreamCallback) (*ai.ModelResponse, error) {
			Calls.Add(1)
			return nil, status.Errorf(status.ErrUnavailable, "llmfake: llmfake/down is down")
		}),
		ai.NewModelAction[any](Provider+"/denied", &ai.ModelOptions{Label: "fake refused key", Supports: supports}, func(context.Context, *ai.ModelRequest, any, ai.ModelStreamCallback) (*ai.ModelResponse, error) {
			Calls.Add(1)
			return nil, status.Errorf(status.ErrUnauthenticated, "llmfake: llmfake/denied refused the key")
		}),
	}
}

var candidateRe = regexp.MustCompile(`(?m)^- ([A-Za-z0-9][A-Za-z0-9_.-]*): (.+)$`)

func (g *Generative) router(_ context.Context, req *ai.ModelRequest, _ any, _ ai.ModelStreamCallback) (*ai.ModelResponse, error) {
	Calls.Add(1)
	system, users := split(req)
	current := ""
	if len(users) > 0 {
		current = users[len(users)-1]
	}
	if strings.Contains(system, "follow_up") {
		return respond(req, map[string]any{"follow_up": followUp(current, users[:max(len(users)-1, 0)])})
	}
	var names, descr []string
	for _, m := range candidateRe.FindAllStringSubmatch(system, -1) {
		names, descr = append(names, m[1]), append(descr, m[2])
	}
	if len(names) == 0 {
		// Not a routing prompt (a health check): plain text.
		return &ai.ModelResponse{Request: req, FinishReason: ai.FinishReasonStop, Message: ai.NewModelTextMessage("OK")}, nil
	}
	multi := strings.Contains(system, "one or more")
	msg := tokens(current)
	scores := make([]int, len(names))
	best := 0
	for i := range names {
		scores[i] = overlap(msg, names[i]+" "+descr[i])
		if scores[i] > scores[best] {
			best = i
		}
	}
	var dest, matched []string
	if multi {
		for i, s := range scores {
			if s > 0 {
				dest = append(dest, names[i])
			}
		}
	}
	if len(dest) == 0 {
		dest = []string{names[best]}
	}
	for w := range msg {
		for _, d := range descr {
			if tokens(d)[w] {
				matched = append(matched, w)
				break
			}
		}
	}
	sort.Strings(matched)
	conf := math.Min(0.35+0.2*float64(scores[best]), 0.95)
	return respond(req, map[string]any{
		"destinations": dest,
		"confidence":   round(conf),
		"reason":       "matched: " + strings.Join(matched, ", "),
	})
}

func (g *Generative) badJSON(_ context.Context, req *ai.ModelRequest, _ any, _ ai.ModelStreamCallback) (*ai.ModelResponse, error) {
	Calls.Add(1)
	return respond(req, map[string]any{"destinations": []string{"https://evil.example.com"}, "confidence": 0.99, "reason": "trust me"})
}

func respond(req *ai.ModelRequest, out map[string]any) (*ai.ModelResponse, error) {
	b, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	in := 0
	for _, m := range req.Messages {
		in += len(m.Text())
	}
	return &ai.ModelResponse{
		Request:      req,
		FinishReason: ai.FinishReasonStop,
		Message:      ai.NewModelMessage(ai.NewJSONPart(string(b))),
		Usage:        &ai.GenerationUsage{InputTokens: in/4 + 1, OutputTokens: len(b)/4 + 1, TotalTokens: in/4 + len(b)/4 + 2},
	}, nil
}

// split returns the system text and the user turns (message tags removed).
func split(req *ai.ModelRequest) (string, []string) {
	var system strings.Builder
	var users []string
	for _, m := range req.Messages {
		switch m.Role {
		case ai.RoleSystem:
			system.WriteString(m.Text())
			system.WriteString("\n")
		case ai.RoleUser:
			t := strings.TrimSpace(m.Text())
			t = strings.TrimSuffix(strings.TrimPrefix(t, "<message>"), "</message>")
			users = append(users, strings.TrimSpace(t))
		}
	}
	return system.String(), users
}

// server is the in-process System One server.
type server struct{}

type wireQuestion struct {
	Type         string `json:"type"`
	Instructions any    `json:"instructions"`
	Criteria     any    `json:"criteria"`
}

func (server) RoundTrip(req *http.Request) (*http.Response, error) {
	Calls.Add(1)
	if req.Method == http.MethodGet {
		return reply(req, 200, map[string]any{"data": []map[string]string{{"id": "clef"}, {"id": "down"}, {"id": "garbage"}}}, nil), nil
	}
	raw, _ := io.ReadAll(req.Body)
	var body struct {
		Model     string                  `json:"model"`
		State     json.RawMessage         `json:"state"`
		Questions map[string]wireQuestion `json:"questions"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return reply(req, 400, map[string]any{"error": map[string]any{"message": err.Error()}}, nil), nil
	}
	switch body.Model {
	case "down":
		return reply(req, 503, map[string]any{"error": map[string]any{"message": "llmfake-decisions/down is down"}}, http.Header{"Retry-After": {"0"}}), nil
	}
	current, previous := stateText(body.State)
	msg := tokens(current)
	answers := map[string]any{}
	for id, q := range body.Questions {
		instr := text(q.Instructions)
		switch q.Type {
		case "choice":
			crit, _ := q.Criteria.(map[string]any)
			names := make([]string, 0, len(crit))
			for n := range crit {
				names = append(names, n)
			}
			sort.Strings(names)
			probs := softmax(names, func(n string) int { return overlap(msg, n+" "+text(crit[n])) })
			choice := names[0]
			for _, n := range names {
				if probs[n] > probs[choice] {
					choice = n
				}
			}
			if body.Model == "garbage" {
				choice = "nowhere"
			}
			answers[id] = map[string]any{"type": "choice", "choice": choice, "probabilities": probs, "confidence": probs[choice]}
		case "noul":
			var p float64
			if strings.Contains(instr, "follow-up to a previous call") {
				p = followUp(current, previous)
			} else {
				// The wire keys a yes/no question's criteria by "true" and
				// "false"; with none, the instructions are all there is.
				yes := instr
				if c, ok := q.Criteria.(map[string]any); ok {
					yes = text(c["true"])
				} else if s, ok := q.Criteria.(string); ok {
					yes = s
				}
				p = sigmoid(-1.5 + 1.6*float64(overlap(msg, yes)))
			}
			answers[id] = map[string]any{"type": "noul", "noul": round(p)}
		case "score":
			answers[id] = map[string]any{"type": "score", "score": 1.0, "confidence": 0.5}
		}
	}
	return reply(req, 200, map[string]any{
		"model":   body.Model,
		"answers": answers,
		"usage":   map[string]any{"input_tokens": len(raw)/4 + 1, "output_tokens": 16},
	}, nil), nil
}

func reply(req *http.Request, code int, v any, h http.Header) *http.Response {
	b, _ := json.Marshal(v)
	if h == nil {
		h = http.Header{}
	}
	h.Set("Content-Type", "application/json")
	return &http.Response{StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)), Header: h, Body: io.NopCloser(bytes.NewReader(b)), Request: req, ContentLength: int64(len(b))}
}

// stateText returns the latest user message of a state (a string, a JSON
// object, or {role, content} records) and the earlier user messages.
func stateText(raw json.RawMessage) (string, []string) {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var recs []struct {
		Role    string `json:"role"`
		Content any    `json:"content"`
	}
	if json.Unmarshal(raw, &recs) == nil && len(recs) > 0 {
		var users []string
		for _, r := range recs {
			if r.Role == "user" {
				users = append(users, text(r.Content))
			}
		}
		if len(users) == 0 {
			return "", nil
		}
		return users[len(users)-1], users[:len(users)-1]
	}
	return string(raw), nil
}

// text flattens a JSON value into words.
func text(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = text(e)
		}
		return strings.Join(parts, " ")
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(x)*2)
		for _, k := range keys {
			parts = append(parts, k, text(x[k]))
		}
		return strings.Join(parts, " ")
	}
	return fmt.Sprint(v)
}

var (
	wordRe = regexp.MustCompile(`[a-z0-9]+`)
	stop   = map[string]bool{
		"the": true, "and": true, "for": true, "this": true, "that": true, "with": true, "you": true, "are": true,
		"was": true, "were": true, "has": true, "have": true, "not": true, "but": true, "all": true, "any": true,
		"our": true, "your": true, "who": true, "what": true, "should": true, "receive": true, "message": true,
		"anything": true, "see": true, "into": true, "from": true, "about": true, "none": true, "above": true,
	}
	cues = []string{"again", "still", "follow", "update", "same", "previous", "earlier", "already", "another", "yesterday"}
)

// tokens are the content words of a text, crudely stemmed, so "invoices"
// and "invoice" meet.
func tokens(s string) map[string]bool {
	out := map[string]bool{}
	for _, w := range wordRe.FindAllString(strings.ToLower(s), -1) {
		if len(w) < 3 || stop[w] {
			continue
		}
		out[stem(w)] = true
	}
	return out
}

// stem strips plural and verb endings: invoices, refunds and refunded
// meet invoice and refund.
func stem(w string) string {
	switch {
	case len(w) > 5 && strings.HasSuffix(w, "ing"):
		return w[:len(w)-3]
	case len(w) > 4 && strings.HasSuffix(w, "ed"):
		return w[:len(w)-2]
	case len(w) > 3 && strings.HasSuffix(w, "s") && !strings.HasSuffix(w, "ss"):
		return w[:len(w)-1]
	}
	return w
}

func overlap(msg map[string]bool, criteria string) int {
	n := 0
	for w := range tokens(criteria) {
		if msg[w] {
			n++
		}
	}
	return n
}

func softmax(names []string, score func(string) int) map[string]float64 {
	out := map[string]float64{}
	total := 0.0
	for _, n := range names {
		out[n] = math.Exp(1.4 * float64(score(n)))
		total += out[n]
	}
	for n := range out {
		out[n] = round(out[n] / total)
	}
	return out
}

func sigmoid(x float64) float64 { return 1 / (1 + math.Exp(-x)) }

func round(x float64) float64 { return math.Round(x*1000) / 1000 }

// followUp scores how much the latest message continues an earlier one:
// shared words and follow-up cue words.
func followUp(current string, previous []string) float64 {
	if len(previous) == 0 {
		return 0.05
	}
	cur := tokens(current)
	shared := 0
	for _, p := range previous {
		for w := range tokens(p) {
			if cur[w] {
				shared++
			}
		}
	}
	low := strings.ToLower(current)
	for _, c := range cues {
		if strings.Contains(low, c) {
			shared += 2
		}
	}
	return round(sigmoid(-2.2 + 1.1*float64(shared)))
}

// chat is a tiny deterministic assistant for the LLM-proxy mode: it answers
// "You said: <the last user message>", word by word when streaming, with
// token counts (words). Media in that message is listed after it in
// brackets, by media type. Asked for JSON (response_format), it answers
// {"said": "<the last user message>"}.
//
// With tools, it calls every tool the last user message names (all of
// them: parallel calls), or the first tool when tool_choice is required,
// never when it is none. Each string property of the tool's parameters gets
// the message. The calls carry no ID, as Gemini's and Ollama's do not, and a
// stream sends each one whole. Once the tools answered, it says what each
// one said: "Tools said: <name> <call id> <output JSON>; ...".
func (g *Generative) chat(ctx context.Context, req *ai.ModelRequest, _ any, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
	Calls.Add(1)
	var last string
	var media []string
	in := 0
	for _, m := range req.Messages {
		t := m.Text()
		in += len(strings.Fields(t))
		if m.Role == ai.RoleUser {
			// The message, without the format instructions Genkit adds.
			last, media = "", nil
			for _, p := range m.Content {
				switch {
				case p.IsMedia():
					media = append(media, p.ContentType)
				case p.IsText() && !p.IsOutputInstructions():
					last += p.Text
				}
			}
		}
	}
	if calls := toolCalls(req, last); len(calls) > 0 {
		for _, c := range calls {
			if cb != nil {
				if err := cb(ctx, &ai.ModelResponseChunk{Role: ai.RoleModel, Content: []*ai.Part{c}}); err != nil {
					return nil, err
				}
			}
		}
		return &ai.ModelResponse{
			Request:      req,
			Message:      ai.NewModelMessage(calls...),
			FinishReason: ai.FinishReasonStop,
			Usage:        &ai.GenerationUsage{InputTokens: in, OutputTokens: len(calls), TotalTokens: in + len(calls)},
		}, nil
	}
	answer := "You said: " + last
	if said := toolsSaid(req); said != "" {
		answer = "Tools said: " + said
	} else if req.Output != nil && req.Output.Format == ai.OutputFormatJSON {
		b, err := json.Marshal(map[string]string{"said": last})
		if err != nil {
			return nil, err
		}
		answer = string(b)
	} else if len(media) > 0 {
		answer += " [" + strings.Join(media, " ") + "]"
	}
	words := strings.Fields(answer)
	if cb != nil {
		for i, w := range words {
			if i > 0 {
				w = " " + w
			}
			if err := cb(ctx, &ai.ModelResponseChunk{Role: ai.RoleModel, Content: []*ai.Part{ai.NewTextPart(w)}}); err != nil {
				return nil, err
			}
		}
	}
	return &ai.ModelResponse{
		Request:      req,
		Message:      ai.NewModelTextMessage(answer),
		FinishReason: ai.FinishReasonStop,
		Usage:        &ai.GenerationUsage{InputTokens: in, OutputTokens: len(words), TotalTokens: in + len(words)},
	}, nil
}

// toolCalls are the calls chat makes to the request's tools: to each tool
// the message names, else to the first one when a call is required. A
// conversation that ends with tool answers gets none.
func toolCalls(req *ai.ModelRequest, message string) []*ai.Part {
	if len(req.Tools) == 0 || req.ToolChoice == ai.ToolChoiceNone {
		return nil
	}
	if n := len(req.Messages); n > 0 && req.Messages[n-1].Role == ai.RoleTool {
		return nil
	}
	var calls []*ai.Part
	call := func(t *ai.ToolDefinition) {
		args := map[string]any{}
		props, _ := t.InputSchema["properties"].(map[string]any)
		for name, p := range props {
			if s, _ := p.(map[string]any); s["type"] == "string" {
				args[name] = message
			}
		}
		calls = append(calls, ai.NewToolRequestPart(&ai.ToolRequest{Name: t.Name, Input: args}))
	}
	for _, t := range req.Tools {
		if strings.Contains(message, t.Name) {
			call(t)
		}
	}
	if len(calls) == 0 && req.ToolChoice == ai.ToolChoiceRequired {
		call(req.Tools[0])
	}
	return calls
}

// toolsSaid is what the tools answered, when the conversation ends with
// their answers: "<name> <call id> <output JSON>" for each, joined by "; ".
func toolsSaid(req *ai.ModelRequest) string {
	n := len(req.Messages)
	if n == 0 || req.Messages[n-1].Role != ai.RoleTool {
		return ""
	}
	var said []string
	for _, p := range req.Messages[n-1].Content {
		if p.IsToolResponse() && p.ToolResponse != nil {
			out, _ := json.Marshal(p.ToolResponse.Output)
			said = append(said, fmt.Sprintf("%s %s %s", p.ToolResponse.Name, p.ToolResponse.Ref, out))
		}
	}
	return strings.Join(said, "; ")
}
