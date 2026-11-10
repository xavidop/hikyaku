package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/internal/inbound"
	"github.com/xavidop/hikyaku/plugin"
)

const (
	defaultPath    = "/v1"
	defaultWait    = 5 * time.Minute
	defaultMaxBody = 4 << 20
	contentJSON    = "application/json"
	contentSSE     = "text/event-stream"
)

// Source is an openai source.
type Source struct {
	refused func(source string) // hikyaku_backpressure_total
	name    string
	listen  string
	path    string // without the trailing slash
	maxBody int64
	wait    time.Duration
	models  []string
	auth    *inbound.Authenticator
	tlsOpts httpmux.Options

	out chan<- *envelope.Envelope

	mu     sync.Mutex
	handle *httpmux.Handle
	ready  chan struct{}
}

func newSource(ctx context.Context, spec plugin.Spec) (*Source, error) {
	src, ok := spec.Config.(*config.Source)
	if !ok || src == nil {
		return nil, fmt.Errorf("source %s: missing config", spec.Name)
	}
	if src.Listen == "" {
		return nil, fmt.Errorf("source %s: listen is required (such as :8080)", spec.Name)
	}
	if src.Auth != nil && src.Auth.HMAC != nil {
		return nil, fmt.Errorf("source %s: auth.hmac does not apply to an openai source: OpenAI clients send a bearer key, use auth.bearer", spec.Name)
	}
	s := &Source{
		refused: spec.Env.Refused,
		name:    spec.Name,
		listen:  src.Listen,
		path:    strings.TrimRight(src.Path, "/"),
		maxBody: int64(src.MaxBodyBytes),
		wait:    src.Timeout.Std(),
		models:  src.Models,
		ready:   make(chan struct{}),
	}
	if s.path == "" {
		s.path = defaultPath
	}
	if s.maxBody <= 0 {
		s.maxBody = defaultMaxBody
	}
	if s.wait <= 0 {
		s.wait = defaultWait
	}
	if len(s.models) == 0 {
		s.models = []string{"hikyaku"}
	}
	tlsCfg, key, err := httpmux.ServerTLS(src.TLS)
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	s.tlsOpts = httpmux.Options{TLS: tlsCfg, TLSKey: key}
	if s.auth, err = inbound.New(ctx, src.Auth); err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	return s, nil
}

func (s *Source) Name() string { return s.name }

// Health reports whether the endpoint is being served.
func (s *Source) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle == nil {
		return errors.New("not serving")
	}
	return nil
}

// Addr is the bound address once serving ("" before).
func (s *Source) Addr() string {
	<-s.ready
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.handle == nil {
		return ""
	}
	return s.handle.Addr()
}

// Start serves until ctx ends.
func (s *Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	s.out = out
	h, err := httpmux.Register(s.listen, s.path+"/", s.tlsOpts, http.HandlerFunc(s.serve))
	s.mu.Lock()
	s.handle = h
	s.mu.Unlock()
	close(s.ready)
	if err != nil {
		return err
	}
	<-ctx.Done()
	h.Release()
	s.mu.Lock()
	s.handle = nil
	s.mu.Unlock()
	return nil
}

func (s *Source) serve(w http.ResponseWriter, r *http.Request) {
	switch strings.TrimPrefix(r.URL.Path, s.path) {
	case "/chat/completions":
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			apiError(w, http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use POST")
			return
		}
		s.chat(w, r)
	case "/models":
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			apiError(w, http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed", "use GET")
			return
		}
		if err := s.auth.Check(r, nil); err != nil {
			apiError(w, http.StatusUnauthorized, "invalid_request_error", "invalid_api_key", err.Error())
			return
		}
		s.listModels(w)
	default:
		apiError(w, http.StatusNotFound, "invalid_request_error", "unknown_url", fmt.Sprintf("unknown endpoint %s %s (this relay serves %s/chat/completions and %s/models)", r.Method, r.URL.Path, s.path, s.path))
	}
}

func (s *Source) listModels(w http.ResponseWriter) {
	type model struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	data := make([]model, len(s.models))
	for i, m := range s.models {
		data[i] = model{ID: m, Object: "model", OwnedBy: "hikyaku"}
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

// request is the part of a chat request the source itself reads.
type request struct {
	Model    string            `json:"model"`
	Stream   bool              `json:"stream"`
	Messages []json.RawMessage `json:"messages"`
}

func (s *Source) chat(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, s.maxBody+1))
	if err != nil {
		apiError(w, http.StatusBadRequest, "invalid_request_error", "bad_request", err.Error())
		return
	}
	if int64(len(body)) > s.maxBody {
		apiError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request_too_large", fmt.Sprintf("the request is over %d bytes", s.maxBody))
		return
	}
	if err := s.auth.Check(r, body); err != nil {
		apiError(w, http.StatusUnauthorized, "invalid_request_error", "invalid_api_key", err.Error())
		return
	}
	var req request
	if err := json.Unmarshal(body, &req); err != nil {
		apiError(w, http.StatusBadRequest, "invalid_request_error", "invalid_json", "the body is not a JSON chat completion request: "+err.Error())
		return
	}
	if len(req.Messages) == 0 {
		apiError(w, http.StatusBadRequest, "invalid_request_error", "missing_messages", "messages is required")
		return
	}
	env := envelope.New(s.name, "openai", body)
	if k := r.Header.Get("Idempotency-Key"); k != "" {
		env.ID = k
	}
	env.ContentType = contentJSON
	env.Subject = req.Model
	for k, vs := range r.Header {
		if private(k) {
			continue
		}
		env.Headers[k] = append(env.Headers[k], vs...)
	}
	s.auth.StripCredentials(env.Headers)
	env.SetAttribute("openai.model", req.Model)
	env.SetAttribute("openai.stream", req.Stream)
	env.SetAttribute("openai.endpoint", "chat.completions")
	env.SetAttribute("remote_addr", r.RemoteAddr)

	var st *sseStream
	if req.Stream {
		st = newSSE(w)
		env.SetStream(st)
		// A model still streaming after this handler returned (the caller
		// left, or the wait ran out) gets an error instead of writing to a
		// finished response.
		defer st.close()
	}
	done := make(chan envelope.Outcome, 1)
	env.OnAck(func(_ context.Context, o envelope.Outcome) { done <- o })
	select {
	case s.out <- env:
	default:
		if s.refused != nil {
			s.refused(s.name)
		}
		w.Header().Set("Retry-After", "1")
		apiError(w, http.StatusServiceUnavailable, "server_error", "overloaded", "the relay is busy; retry shortly")
		return
	}
	timer := time.NewTimer(s.wait)
	defer timer.Stop()
	select {
	case o := <-done:
		s.answer(w, env, req, st, o)
	case <-timer.C:
		if st != nil && st.Committed() {
			st.fail("timeout", fmt.Sprintf("no answer within %s", s.wait))
			return
		}
		apiError(w, http.StatusGatewayTimeout, "server_error", "timeout", fmt.Sprintf("no answer within %s", s.wait))
	case <-r.Context().Done():
		// The caller went away; the relay finishes the request anyway.
	}
}

// answer replies with the outcome: the served destination's reply, or an
// OpenAI-style error.
func (s *Source) answer(w http.ResponseWriter, env *envelope.Envelope, req request, st *sseStream, o envelope.Outcome) {
	rep := replyOf(o)
	if o.Status != envelope.Delivered || rep == nil {
		code, typ, errCode, msg := failure(o, req.Model)
		if st != nil && st.Committed() {
			st.fail(errCode, msg)
			return
		}
		if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
			w.Header().Set("Retry-After", "1")
		}
		apiError(w, code, typ, errCode, msg)
		return
	}
	if st == nil {
		ct := rep.ContentType
		if ct == "" {
			ct = contentJSON
		}
		w.Header().Set("Content-Type", ct)
		w.Header().Set("X-Request-Id", env.ID)
		code := rep.Status
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		_, _ = w.Write(rep.Body)
		return
	}
	switch {
	case st.Committed():
		// A model destination streamed it.
	case strings.HasPrefix(rep.ContentType, contentSSE):
		st.raw(rep.Body) // an upstream that streamed: pass its events through
		return
	default:
		if err := st.fromCompletion(rep.Body); err != nil {
			st.fail("bad_upstream", err.Error())
			return
		}
	}
	st.done()
}

// replyOf picks the reply of the destination that served the request: the
// only one, else the first chat completion, else the first by name.
func replyOf(o envelope.Outcome) *envelope.Reply {
	if len(o.Replies) == 1 {
		for _, r := range o.Replies {
			return r
		}
	}
	names := make([]string, 0, len(o.Replies))
	for n := range o.Replies {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if r := o.Replies[n]; r != nil && bytes.Contains(r.Body, []byte(`"chat.completion`)) {
			return r
		}
	}
	for _, n := range names {
		if r := o.Replies[n]; r != nil {
			return r
		}
	}
	return nil
}

// failure maps an outcome that is not a reply to an HTTP status and an
// OpenAI error type and code.
func failure(o envelope.Outcome, model string) (int, string, string, string) {
	switch o.Status {
	case envelope.Unrouted:
		return http.StatusNotFound, "invalid_request_error", "model_not_found", fmt.Sprintf("no route serves model %q", model)
	case envelope.Filtered:
		return http.StatusForbidden, "invalid_request_error", "filtered", "the relay's filters rejected this request"
	case envelope.Delivered:
		return http.StatusBadGateway, "server_error", "no_reply", "the destination that served the request sent no reply"
	}
	msg := "every model failed"
	if o.Err != nil {
		msg = o.Err.Error()
	}
	class, _ := plugin.Classify(o.Err)
	tags := plugin.Tags(o.Err)
	switch {
	case containsTag(tags, plugin.TagNoRoute):
		return http.StatusNotFound, "invalid_request_error", "model_not_found", fmt.Sprintf("no route serves model %q: %s", model, msg)
	case containsTag(tags, plugin.TagSchema):
		return http.StatusBadRequest, "invalid_request_error", "schema", msg
	case containsTag(tags, plugin.TagTransform):
		return http.StatusInternalServerError, "server_error", "transform_failed", msg
	case containsTag(tags, "model:bad_request"):
		return http.StatusBadRequest, "invalid_request_error", "bad_request", msg
	case class == plugin.RateLimited:
		return http.StatusTooManyRequests, "rate_limit_error", "rate_limited", msg
	case class == plugin.Timeout:
		return http.StatusGatewayTimeout, "server_error", "timeout", msg
	case class == plugin.Permanent:
		return http.StatusBadGateway, "server_error", "upstream_error", msg
	}
	return http.StatusServiceUnavailable, "server_error", "unavailable", msg
}

func containsTag(tags []string, t string) bool {
	for _, x := range tags {
		if x == t {
			return true
		}
	}
	return false
}

// private headers are not copied into the envelope: the caller's key is
// for this relay, never for the destinations.
func private(h string) bool {
	switch http.CanonicalHeaderKey(h) {
	case "Authorization", "Cookie", "Api-Key", "X-Api-Key", "Openai-Organization", "Openai-Project", "Content-Length":
		return true
	}
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", contentJSON)
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// apiError writes an OpenAI-style error body.
func apiError(w http.ResponseWriter, code int, typ, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": errCode, "param": nil}})
}
