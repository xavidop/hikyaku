package llm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/redact"
	"github.com/xavidop/hikyaku/metrics"
	"github.com/xavidop/hikyaku/router"
	"github.com/xavidop/hikyaku/store"
	"github.com/xavidop/hikyaku/transform"
)

// stateBuilder turns a message into what the model evaluates (design §6.4
// step 2): the state template, the JSONPath redaction, the header
// allowlist, and the byte cap. Message content is data, never
// instructions.
type stateBuilder struct {
	tmpl     *transform.Template
	json     bool
	redact   []redact.Path
	headers  []string
	maxBytes int
}

func newStateBuilder(rs config.RouterSpec) (*stateBuilder, error) {
	src := rs.State
	if src == "" {
		src = "{{ .bodyText }}"
	}
	t, err := transform.CompileTemplate("state", src)
	if err != nil {
		return nil, err
	}
	b := &stateBuilder{tmpl: t, json: rs.StateJSON, maxBytes: config.DefaultAIMaxBytes}
	if in := rs.Input; in != nil {
		if b.redact, err = redact.CompilePaths(in.Redact); err != nil {
			return nil, err
		}
		b.headers = in.Headers
		if in.MaxBytes > 0 {
			b.maxBytes = int(in.MaxBytes)
		}
	}
	return b, nil
}

// build renders the state. A JSON state is redacted by path; headers on
// the allowlist go in as a "headers" member (JSON) or a header block
// (text). The cap is applied with the history, by BuildTurns.
func (b *stateBuilder) build(env *envelope.Envelope) (string, error) {
	s, err := b.tmpl.Render(env)
	if err != nil {
		return "", err
	}
	hdrs := map[string]string{}
	for _, h := range b.headers {
		if v := env.Header(h); v != "" {
			hdrs[textproto.CanonicalMIMEHeaderKey(h)] = v
		}
	}
	var doc any
	isJSON := (b.json || len(b.redact) > 0) && json.Unmarshal([]byte(s), &doc) == nil
	if isJSON {
		redact.JSON(doc, b.redact)
		if len(hdrs) > 0 {
			if m, ok := doc.(map[string]any); ok {
				m["headers"] = hdrs
			} else {
				doc = map[string]any{"message": doc, "headers": hdrs}
			}
		}
		out, err := json.Marshal(doc)
		if err != nil {
			return "", err
		}
		return string(out), nil
	}
	if len(hdrs) > 0 {
		var sb strings.Builder
		for _, h := range b.headers {
			if v, ok := hdrs[textproto.CanonicalMIMEHeaderKey(h)]; ok {
				fmt.Fprintf(&sb, "%s: %s\n", textproto.CanonicalMIMEHeaderKey(h), v)
			}
		}
		sb.WriteString("\n")
		sb.WriteString(s)
		s = sb.String()
	}
	return s, nil
}

// historyStep is router.history: reading the past calls of a key, and the
// record appended after delivery.
type historyStep struct {
	route     string
	store     store.HistoryStore
	key       *transform.Template
	window    store.Window
	ttl       time.Duration
	keep      int
	record    string // redacted | summary | none
	summary   *transform.Template
	sticky    string // model | always_last | off
	threshold float64
	onError   string // ignore | fallback
	m         *metrics.Metrics
}

func newHistory(route string, h *config.History, deps router.AIDeps) (*historyStep, error) {
	if h == nil {
		return nil, nil
	}
	if deps.History == nil {
		return nil, errors.New("router.history needs a store")
	}
	key, err := transform.CompileTemplate("history.key", h.Key)
	if err != nil {
		return nil, err
	}
	hs := &historyStep{
		route: route, store: deps.History, key: key, window: router.WindowOf(h.Window), ttl: h.TTL.Std(),
		record: h.Record.State, onError: h.OnStoreError, sticky: "off", m: deps.Metrics,
	}
	hs.keep = 2 * hs.window.Entries()
	if hs.record == "" {
		hs.record = "redacted"
	}
	if hs.onError == "" {
		hs.onError = "ignore"
	}
	if hs.record == "summary" {
		if hs.summary, err = transform.CompileTemplate("history.record.summary", h.Record.Summary); err != nil {
			return nil, err
		}
	}
	if st := h.Sticky; st != nil && st.Enabled {
		hs.sticky = st.Mode
		if hs.sticky == "" {
			hs.sticky = "model"
		}
		hs.threshold = st.FollowUpThreshold
		if hs.threshold == 0 {
			hs.threshold = config.DefaultFollowUpThreshold
		}
	}
	return hs, nil
}

func (h *historyStep) appendOptions() store.AppendOptions {
	return store.AppendOptions{TTL: h.ttl, Keep: h.keep}
}

// errHistory is a history read failure under on_store_error: fallback.
var errHistory = errors.New("history store unavailable")

// recent reads the history of a message's key. A failure is counted and,
// with on_store_error: ignore (the default), routing goes on without
// history: history makes routing better, never unavailable.
func (h *historyStep) recent(ctx context.Context, env *envelope.Envelope) (string, []store.CallRecord, error) {
	key, err := h.key.Render(env)
	if err != nil || key == "" {
		return "", nil, nil
	}
	recs, err := h.store.Recent(ctx, h.route, key, h.window)
	if err != nil {
		if h.m != nil {
			h.m.HistoryErrors.WithLabelValues("read").Inc()
			h.m.HistoryReads.WithLabelValues(h.route, "error").Inc()
		}
		if h.onError == "fallback" {
			return key, nil, fmt.Errorf("%w: %v", errHistory, err)
		}
		return key, nil, nil
	}
	if h.m != nil {
		outcome := "hit"
		if len(recs) == 0 {
			outcome = "empty"
		}
		h.m.HistoryReads.WithLabelValues(h.route, outcome).Inc()
	}
	return key, recs, nil
}

// recordState is what the call record stores of the message.
func (h *historyStep) recordState(env *envelope.Envelope, state string) string {
	switch h.record {
	case "none":
		return ""
	case "summary":
		s, err := h.summary.Render(env)
		if err != nil {
			return ""
		}
		return s
	}
	return state
}

// systemPrompt is the system message: the route's instructions, plus the
// line on how to read history when there is some.
func systemPrompt(instructions string, withHistory bool) string {
	s := strings.TrimSpace(instructions)
	if withHistory {
		if s != "" {
			s += "\n\n"
		}
		s += store.HistoryPreamble
	}
	return s
}
