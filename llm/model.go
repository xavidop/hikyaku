package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core/status"
	"github.com/firebase/genkit/go/genkit"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/plugin"
)

// The model destination (type: model) is the LLM-proxy mode's sink. It
// takes an OpenAI chat completion request (the envelope body, as the
// openai source receives it), runs it on a Genkit model, and answers with
// an OpenAI chat completion. Routers pick the model for each request, and
// the destination's retries and fallbacks fail over between models.
//
// The request's tools are offered to the model with their JSON Schemas, and
// the tool calls it makes go back to the caller, who runs them: hikyaku
// runs none. Each of n choices is a generation of its own, and they run at
// the same time.
//
// A request with "stream": true on an envelope that carries a stream (the
// openai source sets one) is answered chunk by chunk. A model that fails
// before its first chunk is retried or falls back like any destination;
// once a chunk reached the caller the failure is permanent.

const contentJSON = "application/json"

type modelSink struct {
	name  string
	model string
	g     *genkit.Genkit
	gen   config.Generation
	log   *slog.Logger
	m     *metrics.Metrics
}

// NewModelSink builds a model destination; it implements router.ModelSinks.
func (f *Factory) NewModelSink(_ context.Context, spec plugin.Spec) (plugin.Sink, error) {
	ep, ok := spec.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return nil, fmt.Errorf("destination %s: missing config", spec.Name)
	}
	if !strings.Contains(ep.Model, "/") {
		return nil, fmt.Errorf("destination %s: model %q must be provider/name", spec.Name, ep.Model)
	}
	s := &modelSink{name: spec.Name, model: ep.Model, g: f.g, log: f.log, m: f.m}
	if ep.Generation != nil {
		s.gen = *ep.Generation
	}
	return s, nil
}

func (s *modelSink) Name() string { return s.name }

// Health checks that the model is defined (its provider plugin loaded)
// without calling it: a call costs tokens.
func (s *modelSink) Health(context.Context) error {
	if genkit.LookupModel(s.g, s.model) == nil {
		return fmt.Errorf("model %s is not available: is its provider in genkit.plugins (or its key set)?", s.model)
	}
	return nil
}

func (s *modelSink) Close(context.Context) error { return nil }

func (s *modelSink) provider() string {
	p, _, _ := strings.Cut(s.model, "/")
	return p
}

func (s *modelSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	var req chatRequest
	if err := json.Unmarshal(env.Body, &req); err != nil {
		return nil, badRequest("the message is not an OpenAI chat completion request: %v", err)
	}
	n, err := req.choices()
	if err != nil {
		return nil, badRequest("%v", err)
	}
	ts, err := req.toolSet()
	if err != nil {
		return nil, badRequest("%v", err)
	}
	merge := s.provider() != "ollama"
	msgs, err := req.genkitMessages(s.gen.System, merge)
	if err != nil {
		return nil, badRequest("%v", err)
	}
	stream := env.Stream()
	streaming := req.Stream && stream != nil
	if streaming && stream.Committed() {
		// An earlier model of the fallback chain streamed part of the
		// answer: no other model may finish it.
		return nil, plugin.Wrap(plugin.Permanent, []string{"model:stream_broken"}, fmt.Errorf("model %s: another model already streamed part of the answer", s.model))
	}
	offer, err := s.offer(ts, msgs)
	if err != nil {
		return nil, err
	}
	r := &run{s: s, req: req, ts: ts, offer: offer, id: "chatcmpl-" + env.ID, created: time.Now().Unix()}
	if streaming {
		r.stream = stream
	}
	resps := make([]*ai.ModelResponse, n)
	err = each(ctx, n, func(ctx context.Context, i int) error {
		m := msgs
		if i > 0 {
			// Each generation gets messages of its own; they converted once
			// already.
			m, _ = req.genkitMessages(s.gen.System, merge)
		}
		var err error
		resps[i], err = r.generate(ctx, i, m)
		return err
	})
	if err != nil {
		return nil, r.fail(err)
	}
	done := completionOf(r.id, r.created, s.model, resps, ts)
	if s.m != nil {
		s.m.ModelTokens.WithLabelValues(s.name, "input").Add(float64(done.Usage.PromptTokens))
		s.m.ModelTokens.WithLabelValues(s.name, "output").Add(float64(done.Usage.CompletionTokens))
	}
	if streaming && req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		b, _ := json.Marshal(completionChunk{ID: r.id, Object: "chat.completion.chunk", Created: r.created, Model: s.model, Choices: []chunkChoice{}, Usage: &done.Usage})
		if err := stream.Write(b); err != nil {
			return nil, r.broken(err)
		}
	}
	body, err := json.Marshal(done)
	if err != nil {
		return nil, err
	}
	return &plugin.Result{Status: 200, Body: body, ContentType: contentJSON, Headers: map[string][]string{"X-Hikyaku-Model": {s.model}}}, nil
}

// run is one chat request on a model destination.
type run struct {
	s       *modelSink
	req     chatRequest
	ts      toolSet
	offer   toolOffer
	id      string
	created int64
	stream  envelope.Stream // nil when the answer is not streamed
	wrote   atomic.Bool     // a chunk reached the caller
}

// generate runs choice i and, when the answer is streamed, writes its
// chunks.
func (r *run) generate(ctx context.Context, i int, msgs []*ai.Message) (*ai.ModelResponse, error) {
	opts := r.s.options(r.req)
	// The model's tool calls come back unanswered: they are the answer, and
	// hikyaku runs no tool.
	opts = append(opts, ai.WithMessages(msgs...), ai.WithReturnToolRequests(true))
	if len(r.offer.defs) > 0 {
		tools := make([]ai.ToolRef, len(r.offer.defs))
		for k, d := range r.offer.defs {
			tools[k] = d.tool()
		}
		opts = append(opts, ai.WithTools(tools...), nameCalls())
		if r.offer.choice != "" {
			opts = append(opts, ai.WithToolChoice(r.offer.choice))
		}
	}
	var cs *choiceStream
	if r.stream != nil {
		cs = &choiceStream{s: r.stream, id: r.id, created: r.created, model: r.s.model, index: i, wrote: &r.wrote}
		opts = append(opts, ai.WithStreaming(cs.chunk))
	}
	resp, err := genkit.Generate(ctx, r.s.g, opts...)
	if err != nil {
		return nil, err
	}
	if cs != nil {
		if err := cs.finish(choiceOf(i, resp, r.ts)); err != nil {
			return nil, r.broken(err)
		}
	}
	return resp, nil
}

// fail classifies a failed generation. Once a chunk reached the caller it
// is permanent: no other model may take over half an answer.
func (r *run) fail(err error) error {
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err // a broken stream, classified already
	}
	if r.wrote.Load() || (r.stream != nil && r.stream.Committed()) {
		return r.broken(err)
	}
	return classifyModel(r.s.model, err)
}

func (r *run) broken(err error) error {
	return plugin.Wrap(plugin.Permanent, []string{"model:stream_broken"}, fmt.Errorf("model %s failed mid-stream: %w", r.s.model, err))
}

// each runs fn for 0 to n-1 at the same time. The first failure cancels the
// others and is returned.
func each(ctx context.Context, n int, fn func(ctx context.Context, i int) error) error {
	if n == 1 {
		return fn(ctx, 0)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for i := range n {
		wg.Go(func() {
			if err := fn(ctx, i); err != nil {
				once.Do(func() {
					first = err
					cancel()
				})
			}
		})
	}
	wg.Wait()
	return first
}

// options are a generation's model, config and output format.
func (s *modelSink) options(req chatRequest) []ai.GenerateOption {
	opts := []ai.GenerateOption{ai.WithModelName(s.model)}
	if cfg := s.config(req); cfg != nil {
		opts = append(opts, ai.WithConfig(providerConfig(s.model, cfg)))
	}
	if rf := req.ResponseFormat; rf != nil {
		switch rf.Type {
		case "json_object":
			opts = append(opts, ai.WithOutputFormat(ai.OutputFormatJSON))
		case "json_schema":
			if rf.JSONSchema != nil && rf.JSONSchema.Schema != nil {
				opts = append(opts, ai.WithOutputSchema(rf.JSONSchema.Schema))
			}
		}
	}
	return opts
}

// toolOffer is what a generation offers the model: tools, and whether it
// must, may or must not call them.
type toolOffer struct {
	defs   []toolDef
	choice ai.ToolChoice
}

// offer checks the request against what the model declares it takes, so
// the caller gets a plain answer when it cannot. A model that cannot be
// told not to call a tool is offered none when tool_choice is none.
func (s *modelSink) offer(ts toolSet, msgs []*ai.Message) (toolOffer, error) {
	o := toolOffer{defs: ts.defs, choice: ts.choice}
	sup := supportsOf(genkit.LookupModel(s.g, s.model))
	if !sup.media && hasMedia(msgs) {
		return o, badRequest("model %s does not take media (images, files, audio)", s.model)
	}
	if len(o.defs) == 0 {
		return o, nil
	}
	if !sup.tools {
		return o, badRequest("model %s does not support tools", s.model)
	}
	switch {
	case sup.toolChoice || o.choice == "" || o.choice == ai.ToolChoiceAuto:
	case o.choice == ai.ToolChoiceNone:
		return toolOffer{}, nil
	default:
		return o, badRequest("model %s does not support tool_choice, so it cannot be made to call a tool", s.model)
	}
	return o, nil
}

// supports is what a model declares it takes. A model that declares
// nothing is taken to take everything: Genkit checks again.
type supports struct{ tools, toolChoice, media bool }

func supportsOf(m ai.Model) supports {
	all := supports{tools: true, toolChoice: true, media: true}
	ma, ok := m.(*ai.ModelAction)
	if !ok || ma == nil {
		return all
	}
	meta, _ := ma.Desc().Metadata["model"].(map[string]any)
	sup, ok := meta["supports"].(map[string]any)
	if !ok {
		return all
	}
	flag := func(k string) bool {
		v, ok := sup[k].(bool)
		return !ok || v
	}
	return supports{tools: flag("tools"), toolChoice: flag("toolChoice"), media: flag("media")}
}

// config merges the request's generation settings over the destination's.
func (s *modelSink) config(req chatRequest) *ai.GenerationCommonConfig {
	c := &ai.GenerationCommonConfig{}
	set := false
	if t := firstFloat(req.Temperature, s.gen.Temperature); t != nil {
		c.Temperature, set = *t, true
	}
	if p := firstFloat(req.TopP, s.gen.TopP); p != nil {
		c.TopP, set = *p, true
	}
	switch {
	case req.MaxCompletionTokens != nil:
		c.MaxOutputTokens, set = *req.MaxCompletionTokens, true
	case req.MaxTokens != nil:
		c.MaxOutputTokens, set = *req.MaxTokens, true
	case s.gen.MaxTokens > 0:
		c.MaxOutputTokens, set = s.gen.MaxTokens, true
	}
	if stop := req.stops(); len(stop) > 0 {
		c.StopSequences, set = stop, true
	}
	if !set {
		return nil
	}
	return c
}

// providerConfig is the generation config in the shape a provider's model
// validates: Genkit checks a request's config against the plugin's own
// schema, and only some take GenerationCommonConfig's names.
func providerConfig(model string, c *ai.GenerationCommonConfig) any {
	provider, _, _ := strings.Cut(model, "/")
	maxKey, stopKey, topPKey := "", "", "top_p"
	switch provider {
	case "ollama":
		maxKey, stopKey = "num_predict", "stop"
	case "anthropic":
		maxKey, stopKey = "max_tokens", "stop_sequences"
	case "openai":
		maxKey, stopKey = "max_completion_tokens", "stop"
	case "openrouter":
		maxKey, stopKey, topPKey = "maxOutputTokens", "stopSequences", "topP"
	default:
		return c // googleai and others take the common names
	}
	m := map[string]any{}
	if c.Temperature != 0 {
		m["temperature"] = c.Temperature
	}
	if c.TopP != 0 {
		m[topPKey] = c.TopP
	}
	if c.MaxOutputTokens > 0 {
		m[maxKey] = c.MaxOutputTokens
	}
	if len(c.StopSequences) > 0 {
		m[stopKey] = c.StopSequences
	}
	return m
}

func firstFloat(vs ...*float64) *float64 {
	for _, v := range vs {
		if v != nil {
			return v
		}
	}
	return nil
}

// classifyModel maps a model error to the retry policy's classes. A
// provider that refused the request as invalid is tagged model:bad_request
// too, which the openai source answers with 400.
func classifyModel(model string, err error) error {
	reason := reasonOf(err)
	tags := []string{"model:" + reason}
	if invalidRequest(err) {
		tags = append(tags, "model:bad_request")
	}
	err = fmt.Errorf("model %s: %w", model, err)
	switch {
	case reason == "rate_limited":
		return plugin.Wrap(plugin.RateLimited, tags, err)
	case reason == "timeout" || errors.Is(err, context.DeadlineExceeded):
		return plugin.Wrap(plugin.Timeout, append(tags, plugin.TagTimeout), err)
	case retryable(err):
		return plugin.Wrap(plugin.Transient, tags, err)
	}
	return plugin.Wrap(plugin.Permanent, tags, err)
}

// invalidRequest reports a provider's answer that the request is invalid:
// Genkit's INVALID_ARGUMENT (an HTTP 400, or a model that does not take
// what the request holds), or the Ollama plugin's "non-200 status: 400".
func invalidRequest(err error) bool {
	if s, ok := status.Classified(err); ok {
		return s == status.InvalidArgument
	}
	return strings.Contains(err.Error(), "non-200 status: 400")
}

func badRequest(format string, args ...any) error {
	return plugin.Wrap(plugin.Permanent, []string{"model:bad_request"}, fmt.Errorf(format, args...))
}
