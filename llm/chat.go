package llm

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/firebase/genkit/go/ai"
)

// The OpenAI chat completion request, as a model destination reads it, and
// its conversion to Genkit messages and tools.

// maxChoices is the most choices (n) a request may ask for. Each choice is a
// model call of its own, and they run at the same time.
const maxChoices = 8

// toolNameRe is OpenAI's rule for function names.
var toolNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// chatRequest is the part of an OpenAI chat completion request a model
// destination reads.
type chatRequest struct {
	Model               string          `json:"model"`
	Messages            []chatMessage   `json:"messages"`
	Stream              bool            `json:"stream"`
	StreamOptions       *streamOptions  `json:"stream_options"`
	Temperature         *float64        `json:"temperature"`
	TopP                *float64        `json:"top_p"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"`
	Stop                json.RawMessage `json:"stop"`
	N                   *int            `json:"n"`
	Tools               []chatTool      `json:"tools"`
	ToolChoice          json.RawMessage `json:"tool_choice"`
	ParallelToolCalls   *bool           `json:"parallel_tool_calls"`
	Functions           []chatFunction  `json:"functions"`
	FunctionCall        json.RawMessage `json:"function_call"`
	ResponseFormat      *responseFormat `json:"response_format"`
}

type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type responseFormat struct {
	Type       string `json:"type"`
	JSONSchema *struct {
		Schema map[string]any `json:"schema"`
	} `json:"json_schema"`
}

// chatTool is an entry of tools.
type chatTool struct {
	Type     string        `json:"type"`
	Function *chatFunction `json:"function"`
}

// chatFunction is a tool's function, or an entry of the deprecated functions.
type chatFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
	Strict      *bool           `json:"strict"`
}

type chatMessage struct {
	Role         string          `json:"role"`
	Content      json.RawMessage `json:"content"`
	Name         string          `json:"name"`
	ToolCalls    []toolCallIn    `json:"tool_calls"`
	ToolCallID   string          `json:"tool_call_id"`
	FunctionCall *functionCallIn `json:"function_call"`
}

// toolCallIn is a tool call of an earlier assistant message.
type toolCallIn struct {
	ID           string         `json:"id"`
	Type         string         `json:"type"`
	Function     functionCallIn `json:"function"`
	ExtraContent *extraContent  `json:"extra_content"`
}

// functionCallIn is a call's function. Arguments is a JSON string; a JSON
// object is taken too.
type functionCallIn struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// extraContent carries a Gemini thought signature on a tool call, as
// Gemini's own OpenAI-compatible endpoint does: a Gemini model gets it back
// with the call on the next turn.
type extraContent struct {
	Google *googleExtra `json:"google,omitempty"`
}

type googleExtra struct {
	ThoughtSignature string `json:"thought_signature,omitempty"`
}

type contentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text"`
	Refusal  string `json:"refusal"`
	ImageURL *struct {
		URL string `json:"url"`
	} `json:"image_url"`
	File *struct {
		FileData string `json:"file_data"`
		FileID   string `json:"file_id"`
		Filename string `json:"filename"`
	} `json:"file"`
	InputAudio *struct {
		Data   string `json:"data"`
		Format string `json:"format"`
	} `json:"input_audio"`
}

func (r chatRequest) stops() []string {
	if len(r.Stop) == 0 || string(r.Stop) == "null" {
		return nil
	}
	var one string
	if json.Unmarshal(r.Stop, &one) == nil {
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(r.Stop, &many)
	return many
}

// choices is n, 1 when unset.
func (r chatRequest) choices() (int, error) {
	if r.N == nil {
		return 1, nil
	}
	if *r.N < 1 || *r.N > maxChoices {
		return 0, fmt.Errorf("n must be between 1 and %d (each choice is a model call of its own)", maxChoices)
	}
	return *r.N, nil
}

// toolSet is what a request asks of the model's tool use.
type toolSet struct {
	defs   []toolDef
	choice ai.ToolChoice // "" leaves it to the model
	single bool          // at most one tool call per answer
	legacy bool          // the request used functions: answer with function_call
}

// toolDef is a function the model may call. hikyaku never runs it: the
// call goes back to the caller.
type toolDef struct {
	name        string
	description string
	schema      json.RawMessage
	strict      bool
}

// emptySchema is the parameters of a function that declares none.
var emptySchema = json.RawMessage(`{"type":"object","properties":{}}`)

// tool is a Genkit tool the model sees with the request's schema. Each
// generation gets its own: Genkit registers it for that call.
func (d toolDef) tool() ai.ToolRef {
	var schema map[string]any
	_ = json.Unmarshal(d.schema, &schema) // checked by toolSet
	return ai.NewTool(d.name, d.description, func(*ai.ToolContext, any) (any, error) {
		return nil, errors.New("hikyaku returns tool calls to the caller and runs none")
	}, ai.WithInputSchema(schema), ai.WithStrictSchema(d.strict))
}

// toolSet reads tools (or the deprecated functions), tool_choice (or
// function_call) and parallel_tool_calls.
func (r chatRequest) toolSet() (toolSet, error) {
	var ts toolSet
	if len(r.Tools) > 0 && len(r.Functions) > 0 {
		return ts, errors.New("tools and the deprecated functions cannot be used together")
	}
	seen := map[string]bool{}
	add := func(at string, f *chatFunction) error {
		if f == nil {
			return fmt.Errorf("%s: function is required", at)
		}
		if !toolNameRe.MatchString(f.Name) {
			return fmt.Errorf("%s: name %q must be 1 to 64 letters, digits, underscores or dashes", at, f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("%s: function %q is declared twice", at, f.Name)
		}
		seen[f.Name] = true
		schema := emptySchema
		if len(f.Parameters) > 0 && string(f.Parameters) != "null" {
			var m map[string]any
			if err := json.Unmarshal(f.Parameters, &m); err != nil {
				return fmt.Errorf("%s: parameters must be a JSON Schema object", at)
			}
			schema = f.Parameters
		}
		ts.defs = append(ts.defs, toolDef{name: f.Name, description: f.Description, schema: schema, strict: f.Strict != nil && *f.Strict})
		return nil
	}
	for i, t := range r.Tools {
		if t.Type != "function" {
			return ts, fmt.Errorf("tools[%d]: type %q is not supported (function)", i, t.Type)
		}
		if err := add(fmt.Sprintf("tools[%d]", i), t.Function); err != nil {
			return ts, err
		}
	}
	for i := range r.Functions {
		if err := add(fmt.Sprintf("functions[%d]", i), &r.Functions[i]); err != nil {
			return ts, err
		}
	}
	ts.legacy = len(r.Functions) > 0
	ts.single = ts.legacy || (r.ParallelToolCalls != nil && !*r.ParallelToolCalls)

	raw, field := r.ToolChoice, "tool_choice"
	if ts.legacy && len(r.FunctionCall) > 0 {
		raw, field = r.FunctionCall, "function_call"
	}
	choice, name, err := parseToolChoice(raw)
	if err != nil {
		return ts, fmt.Errorf("%s: %w", field, err)
	}
	switch {
	case len(ts.defs) == 0:
		if choice == ai.ToolChoiceRequired {
			return ts, fmt.Errorf("%s asks for a tool call, but the request has no tools", field)
		}
		choice = "" // none or auto, with nothing to call
	case name != "":
		i := slices.IndexFunc(ts.defs, func(d toolDef) bool { return d.name == name })
		if i < 0 {
			return ts, fmt.Errorf("%s names %q, which is not one of the request's functions", field, name)
		}
		// The model is offered that function alone, and must call it.
		ts.defs = []toolDef{ts.defs[i]}
	}
	ts.choice = choice
	return ts, nil
}

// parseToolChoice reads "none", "auto", "required", a tool_choice object
// ({"type":"function","function":{"name":...}}) or a function_call object
// ({"name":...}). A named function comes back as required plus its name.
func parseToolChoice(raw json.RawMessage) (ai.ToolChoice, string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", "", nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		switch s {
		case "auto":
			return ai.ToolChoiceAuto, "", nil
		case "none":
			return ai.ToolChoiceNone, "", nil
		case "required":
			return ai.ToolChoiceRequired, "", nil
		}
		return "", "", fmt.Errorf("%q is not a choice (none, auto, required, or a function)", s)
	}
	var o struct {
		Type     string `json:"type"`
		Name     string `json:"name"`
		Function *struct {
			Name string `json:"name"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		return "", "", errors.New(`must be none, auto, required or {"type":"function","function":{"name":...}}`)
	}
	name := o.Name
	if o.Function != nil {
		name = o.Function.Name
	}
	if (o.Type != "" && o.Type != "function") || name == "" {
		return "", "", errors.New(`must name a function: {"type":"function","function":{"name":...}}`)
	}
	return ai.ToolChoiceRequired, name, nil
}

// genkitMessages converts the conversation. system, when set, is the
// destination's system prompt for requests without one. mergeTools puts the
// answers to one turn's tool calls in a single tool message, which Gemini
// requires; Ollama takes each answer as a message of its own.
func (r chatRequest) genkitMessages(system string, mergeTools bool) ([]*ai.Message, error) {
	if len(r.Messages) == 0 {
		return nil, errors.New("messages is empty")
	}
	c := converter{calls: map[string]string{}, legacy: map[string]string{}}
	var out []*ai.Message
	hasSystem := false
	for i, m := range r.Messages {
		var msg *ai.Message
		var err error
		switch m.Role {
		case "system", "developer":
			msg, err = m.message(ai.RoleSystem)
			hasSystem = true
		case "user":
			msg, err = m.message(ai.RoleUser)
		case "assistant":
			msg, err = c.assistant(i, m)
		case "tool", "function":
			var p *ai.Part
			if p, err = c.toolResponse(m); err == nil {
				if last := len(out) - 1; mergeTools && last >= 0 && out[last].Role == ai.RoleTool {
					out[last].Content = append(out[last].Content, p)
					continue
				}
				msg = ai.NewMessage(ai.RoleTool, nil, p)
			}
		default:
			err = fmt.Errorf("role %q is not supported (system, developer, user, assistant, tool, function)", m.Role)
		}
		if err != nil {
			return nil, fmt.Errorf("messages[%d]: %w", i, err)
		}
		out = append(out, msg)
	}
	if !hasSystem && system != "" {
		out = append([]*ai.Message{ai.NewSystemTextMessage(system)}, out...)
	}
	return out, nil
}

// converter matches tool answers to the calls before them.
type converter struct {
	calls  map[string]string // tool call ID to function name
	legacy map[string]string // function name to the ref of its last function_call
}

// assistant converts an assistant message: its text, then its tool calls.
func (c converter) assistant(i int, m chatMessage) (*ai.Message, error) {
	if len(m.ToolCalls) == 0 && m.FunctionCall == nil {
		return m.message(ai.RoleModel)
	}
	text, err := m.text()
	if err != nil {
		return nil, err
	}
	var parts []*ai.Part
	if text != "" {
		parts = append(parts, ai.NewTextPart(text))
	}
	for j, tc := range m.ToolCalls {
		switch {
		case tc.Type != "" && tc.Type != "function":
			return nil, fmt.Errorf("tool_calls[%d]: type %q is not supported (function)", j, tc.Type)
		case tc.ID == "":
			return nil, fmt.Errorf("tool_calls[%d]: id is required", j)
		case tc.Function.Name == "":
			return nil, fmt.Errorf("tool_calls[%d]: function.name is required", j)
		}
		p := ai.NewToolRequestPart(&ai.ToolRequest{Name: tc.Function.Name, Ref: tc.ID, Input: arguments(tc.Function.Arguments)})
		if x := tc.ExtraContent; x != nil && x.Google != nil && x.Google.ThoughtSignature != "" {
			p.Metadata = map[string]any{"signature": x.Google.ThoughtSignature}
		}
		parts = append(parts, p)
		c.calls[tc.ID] = tc.Function.Name
	}
	if fc := m.FunctionCall; fc != nil {
		if fc.Name == "" {
			return nil, errors.New("function_call.name is required")
		}
		// A function_call has no ID: the function message that answers it
		// is matched by name.
		ref := fmt.Sprintf("call_fn%d", i)
		parts = append(parts, ai.NewToolRequestPart(&ai.ToolRequest{Name: fc.Name, Ref: ref, Input: arguments(fc.Arguments)}))
		c.legacy[fc.Name] = ref
	}
	return ai.NewMessage(ai.RoleModel, nil, parts...), nil
}

// toolResponse converts a tool message (or a deprecated function message)
// into the answer to the call it names.
func (c converter) toolResponse(m chatMessage) (*ai.Part, error) {
	text, err := m.text()
	if err != nil {
		return nil, err
	}
	if m.Role == "function" {
		if m.Name == "" {
			return nil, errors.New("a function message needs name")
		}
		ref, ok := c.legacy[m.Name]
		if !ok {
			return nil, fmt.Errorf("no earlier assistant message calls function %q", m.Name)
		}
		return ai.NewToolResponsePart(&ai.ToolResponse{Name: m.Name, Ref: ref, Output: toolOutput(text)}), nil
	}
	if m.ToolCallID == "" {
		return nil, errors.New("a tool message needs tool_call_id")
	}
	name, ok := c.calls[m.ToolCallID]
	if !ok {
		if m.Name == "" {
			return nil, fmt.Errorf("tool_call_id %q answers no tool call of an earlier assistant message", m.ToolCallID)
		}
		name = m.Name
	}
	return ai.NewToolResponsePart(&ai.ToolResponse{Name: name, Ref: m.ToolCallID, Output: toolOutput(text)}), nil
}

// message converts a message's content into a message of role.
func (m chatMessage) message(role ai.Role) (*ai.Message, error) {
	parts, err := m.parts()
	if err != nil {
		return nil, err
	}
	return ai.NewMessage(role, nil, parts...), nil
}

// text is a content that may only hold text: a string, null, or text (and
// refusal) parts.
func (m chatMessage) text() (string, error) {
	if len(m.Content) == 0 || string(m.Content) == "null" {
		return "", nil
	}
	var s string
	if json.Unmarshal(m.Content, &s) == nil {
		return s, nil
	}
	var ps []contentPart
	if err := json.Unmarshal(m.Content, &ps); err != nil {
		return "", errors.New("content must be a string or an array of parts")
	}
	var b strings.Builder
	for _, p := range ps {
		switch p.Type {
		case "text":
			b.WriteString(p.Text)
		case "refusal":
			b.WriteString(p.Refusal)
		default:
			return "", fmt.Errorf("content part type %q is not supported in this message (text)", p.Type)
		}
	}
	return b.String(), nil
}

func (m chatMessage) parts() ([]*ai.Part, error) {
	if len(m.Content) == 0 || string(m.Content) == "null" {
		return []*ai.Part{ai.NewTextPart("")}, nil
	}
	var text string
	if json.Unmarshal(m.Content, &text) == nil {
		return []*ai.Part{ai.NewTextPart(text)}, nil
	}
	var ps []contentPart
	if err := json.Unmarshal(m.Content, &ps); err != nil {
		return nil, errors.New("content must be a string or an array of parts")
	}
	out := make([]*ai.Part, 0, len(ps))
	for _, p := range ps {
		part, err := p.part()
		if err != nil {
			return nil, err
		}
		out = append(out, part)
	}
	return out, nil
}

func (p contentPart) part() (*ai.Part, error) {
	switch p.Type {
	case "text":
		return ai.NewTextPart(p.Text), nil
	case "refusal":
		return ai.NewTextPart(p.Refusal), nil
	case "image_url":
		if p.ImageURL == nil || p.ImageURL.URL == "" {
			return nil, errors.New("an image_url part needs image_url.url")
		}
		return ai.NewMediaPart(mimeOf(p.ImageURL.URL), p.ImageURL.URL), nil
	case "file":
		return p.file()
	case "input_audio":
		return p.audio()
	}
	return nil, fmt.Errorf("content part type %q is not supported (text, image_url, file, input_audio)", p.Type)
}

// file is a file part: file_data is a data URL, or base64 whose media type
// comes from filename (a PDF when it names none).
func (p contentPart) file() (*ai.Part, error) {
	f := p.File
	if f == nil || (f.FileData == "" && f.FileID == "") {
		return nil, errors.New("a file part needs file.file_data")
	}
	if f.FileData == "" {
		return nil, errors.New("file.file_id is not supported: send the file inline as file.file_data")
	}
	if strings.HasPrefix(f.FileData, "data:") {
		return ai.NewMediaPart(mimeOf(f.FileData), f.FileData), nil
	}
	if _, err := base64.StdEncoding.DecodeString(f.FileData); err != nil {
		return nil, errors.New("file.file_data must be a data URL or base64")
	}
	ct := "application/pdf"
	if t := mime.TypeByExtension(path.Ext(f.Filename)); t != "" {
		ct, _, _ = strings.Cut(t, ";")
	}
	return ai.NewMediaPart(ct, "data:"+ct+";base64,"+f.FileData), nil
}

// audio is an input_audio part: base64 data in wav or mp3.
func (p contentPart) audio() (*ai.Part, error) {
	a := p.InputAudio
	if a == nil || a.Data == "" {
		return nil, errors.New("an input_audio part needs input_audio.data")
	}
	var ct string
	switch a.Format {
	case "wav":
		ct = "audio/wav"
	case "mp3":
		ct = "audio/mp3"
	default:
		return nil, fmt.Errorf("input_audio.format %q is not supported (wav, mp3)", a.Format)
	}
	if _, err := base64.StdEncoding.DecodeString(a.Data); err != nil {
		return nil, errors.New("input_audio.data must be base64")
	}
	return ai.NewMediaPart(ct, "data:"+ct+";base64,"+a.Data), nil
}

// mimeOf is a data URL's media type, else a guess from the extension.
func mimeOf(url string) string {
	if rest, ok := strings.CutPrefix(url, "data:"); ok {
		if i := strings.IndexAny(rest, ";,"); i > 0 {
			return rest[:i]
		}
	}
	switch {
	case strings.HasSuffix(url, ".png"):
		return "image/png"
	case strings.HasSuffix(url, ".gif"):
		return "image/gif"
	case strings.HasSuffix(url, ".webp"):
		return "image/webp"
	}
	return "image/jpeg"
}

// arguments is a call's function.arguments as the tool request's input: the
// JSON value it holds, {} when it is empty, the text itself when it is not
// JSON.
func arguments(raw json.RawMessage) any {
	if len(raw) == 0 || string(raw) == "null" {
		return map[string]any{}
	}
	s := string(raw)
	var str string
	if json.Unmarshal(raw, &str) == nil {
		s = str
	}
	if strings.TrimSpace(s) == "" {
		return map[string]any{}
	}
	if v, ok := decodeJSON(s); ok {
		return v
	}
	return s
}

// toolOutput is a tool message's content as the tool's output: the JSON
// value it holds (an object reaches Gemini as the function response), else
// the text.
func toolOutput(text string) any {
	if v, ok := decodeJSON(text); ok && v != nil {
		return v
	}
	return text
}

// decodeJSON decodes s when it is exactly one JSON value, keeping numbers as
// they are written.
func decodeJSON(s string) (any, bool) {
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	var v any
	if d.Decode(&v) != nil {
		return nil, false
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, false
	}
	return v, true
}

// hasMedia reports whether a message carries media.
func hasMedia(msgs []*ai.Message) bool {
	for _, m := range msgs {
		if slices.ContainsFunc(m.Content, (*ai.Part).IsMedia) {
			return true
		}
	}
	return false
}
