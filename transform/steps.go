package transform

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/textproto"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/redact"
)

// Step changes an envelope in place. keep is false when the message is
// filtered out of the route or destination the step belongs to.
type Step interface {
	Apply(env *envelope.Envelope) (keep bool, err error)
}

// Chain is the transforms of a route or destination, applied in order.
type Chain []Step

// Apply runs every step; it stops at the first one that drops the message
// or fails.
func (c Chain) Apply(env *envelope.Envelope) (bool, error) {
	for _, s := range c {
		keep, err := s.Apply(env)
		if err != nil || !keep {
			return keep, err
		}
	}
	return true, nil
}

// Compile builds the steps of a transforms: list.
func Compile(path string, specs []config.Transform) (Chain, error) {
	var out Chain
	for i, t := range specs {
		p := fmt.Sprintf("%s[%d]", path, i)
		var step Step
		switch {
		case t.Filter != "":
			x, err := CompileCEL(t.Filter)
			if err != nil {
				return nil, fmt.Errorf("%s.filter: %w", p, err)
			}
			step = FilterStep{Expr: x}
		case t.JQ != "":
			q, err := CompileJQ(t.JQ)
			if err != nil {
				return nil, fmt.Errorf("%s.jq: %w", p, err)
			}
			step = JQStep{Q: q, ContentType: t.ContentType}
		case t.Template != "":
			tm, err := CompileTemplate(p, t.Template)
			if err != nil {
				return nil, fmt.Errorf("%s.template: %w", p, err)
			}
			step = TemplateStep{T: tm, ContentType: t.ContentType}
		case t.Headers != nil:
			h, err := compileHeaders(p, t.Headers)
			if err != nil {
				return nil, err
			}
			step = h
		case t.Codec != "":
			c, err := codec(t.Codec)
			if err != nil {
				return nil, fmt.Errorf("%s.codec: %w", p, err)
			}
			step = c
		case len(t.Redact) > 0:
			paths, err := redact.CompilePaths(t.Redact)
			if err != nil {
				return nil, fmt.Errorf("%s.redact: %w", p, err)
			}
			step = RedactStep{Paths: paths}
		case t.Validate != nil:
			s, err := CompileSchema(t.Validate)
			if err != nil {
				return nil, fmt.Errorf("%s.validate: %w", p, err)
			}
			step = s
		case t.ContentType != "":
			step = contentTypeStep(t.ContentType)
		default:
			return nil, fmt.Errorf("%s: empty transform", p)
		}
		out = append(out, step)
	}
	return out, nil
}

// FilterStep drops the message when the expression is false. An expression
// that cannot be evaluated (a key missing from this message) keeps the
// message: a filter never drops a message by accident.
type FilterStep struct{ Expr *Expr }

// Apply evaluates the filter.
func (f FilterStep) Apply(env *envelope.Envelope) (bool, error) {
	ok, err := f.Expr.Bool(env)
	if err != nil {
		env.SetAttribute("transform.filter_error", err.Error())
		return true, nil
	}
	return ok, nil
}

// JQStep replaces the body with the program's first output, encoded as
// JSON. No output (select(...) failed) drops the message.
type JQStep struct {
	Q           *JQ
	ContentType string
}

// Apply runs the program on the JSON body.
func (j JQStep) Apply(env *envelope.Envelope) (bool, error) {
	in, err := env.JSON()
	if err != nil {
		return false, fmt.Errorf("jq needs a JSON body: %w", err)
	}
	out, err := j.Q.Run(in)
	if err != nil {
		return false, err
	}
	if len(out) == 0 {
		return false, nil
	}
	var b []byte
	if s, ok := out[0].(string); ok && j.ContentType != "" && !strings.Contains(j.ContentType, "json") {
		b = []byte(s)
	} else if b, err = json.Marshal(out[0]); err != nil {
		return false, fmt.Errorf("jq output: %w", err)
	}
	env.SetBody(b)
	setContentType(env, firstNonEmpty(j.ContentType, "application/json"))
	return true, nil
}

// TemplateStep renders the new body.
type TemplateStep struct {
	T           *Template
	ContentType string
}

// Apply renders the template.
func (t TemplateStep) Apply(env *envelope.Envelope) (bool, error) {
	out, err := t.T.Render(env)
	if err != nil {
		return false, err
	}
	env.SetBody([]byte(out))
	if t.ContentType != "" {
		setContentType(env, t.ContentType)
	}
	return true, nil
}

// HeadersStep edits headers.
type HeadersStep struct {
	Set, Add map[string]*Template
	Remove   []string
}

func compileHeaders(p string, h *config.HeaderOps) (HeadersStep, error) {
	step := HeadersStep{Set: map[string]*Template{}, Add: map[string]*Template{}, Remove: h.Remove}
	for k, v := range h.Set {
		t, err := CompileTemplate(p+".headers.set."+k, v)
		if err != nil {
			return step, err
		}
		step.Set[k] = t
	}
	for k, v := range h.Add {
		t, err := CompileTemplate(p+".headers.add."+k, v)
		if err != nil {
			return step, err
		}
		step.Add[k] = t
	}
	return step, nil
}

// Apply edits the headers: remove, then set, then add.
func (h HeadersStep) Apply(env *envelope.Envelope) (bool, error) {
	for _, k := range h.Remove {
		env.DelHeader(k)
	}
	for _, k := range sortedKeys(h.Set) {
		v, err := h.Set[k].Render(env)
		if err != nil {
			return false, err
		}
		env.SetHeader(k, v)
	}
	for _, k := range sortedKeys(h.Add) {
		v, err := h.Add[k].Render(env)
		if err != nil {
			return false, err
		}
		if env.Headers == nil {
			env.Headers = map[string][]string{}
		}
		ck := textproto.CanonicalMIMEHeaderKey(k)
		env.Headers[ck] = append(env.Headers[ck], v)
	}
	return true, nil
}

// RedactStep replaces the values its JSONPaths select with
// redact.Redacted. A path that selects nothing in a message leaves it as it
// is. A body that is not JSON is an error: letting it through would deliver
// what the step is there to hide.
type RedactStep struct{ Paths []redact.Path }

// Apply redacts the JSON body.
func (r RedactStep) Apply(env *envelope.Envelope) (bool, error) {
	// A copy of its own: the envelope's parsed view is shared, and numbers
	// are kept as written (a 64-bit ID must not come back as a float).
	dec := json.NewDecoder(bytes.NewReader(env.Body))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return false, fmt.Errorf("redact needs a JSON body: %w", err)
	}
	if redact.JSON(doc, r.Paths) == 0 {
		return true, nil
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return false, fmt.Errorf("redact: %w", err)
	}
	env.SetBody(b)
	return true, nil
}

type contentTypeStep string

func (c contentTypeStep) Apply(env *envelope.Envelope) (bool, error) {
	setContentType(env, string(c))
	return true, nil
}

// CodecStep re-encodes the body.
type CodecStep struct {
	Name string
	fn   func(env *envelope.Envelope) error
}

// Apply runs the codec.
func (c CodecStep) Apply(env *envelope.Envelope) (bool, error) {
	if err := c.fn(env); err != nil {
		return false, fmt.Errorf("codec %s: %w", c.Name, err)
	}
	return true, nil
}

func codec(name string) (CodecStep, error) {
	var fn func(env *envelope.Envelope) error
	switch name {
	case "json":
		fn = func(env *envelope.Envelope) error {
			v, err := env.JSON()
			if err != nil {
				return fmt.Errorf("the body is not JSON: %w", err)
			}
			b, err := json.Marshal(v)
			if err != nil {
				return err
			}
			env.SetBody(b)
			setContentType(env, "application/json")
			return nil
		}
	case "text":
		fn = func(env *envelope.Envelope) error {
			setContentType(env, "text/plain; charset=utf-8")
			return nil
		}
	case "base64_encode":
		fn = func(env *envelope.Envelope) error {
			env.SetBody([]byte(base64.StdEncoding.EncodeToString(env.Body)))
			setContentType(env, "text/plain; charset=utf-8")
			return nil
		}
	case "base64_decode":
		fn = func(env *envelope.Envelope) error {
			b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(env.BodyText()))
			if err != nil {
				return err
			}
			env.SetBody(b)
			setContentType(env, "application/octet-stream")
			return nil
		}
	case "form_to_json":
		fn = func(env *envelope.Envelope) error {
			vals, err := url.ParseQuery(env.BodyText())
			if err != nil {
				return err
			}
			m := map[string]any{}
			for k, v := range vals {
				if len(v) == 1 {
					m[k] = v[0]
				} else {
					m[k] = v
				}
			}
			b, err := json.Marshal(m)
			if err != nil {
				return err
			}
			env.SetBody(b)
			setContentType(env, "application/json")
			return nil
		}
	case "cloudevents":
		fn = func(env *envelope.Envelope) error {
			ce := map[string]any{
				"specversion":     "1.0",
				"id":              env.ID,
				"source":          firstNonEmpty(env.Source, "hikyaku"),
				"type":            firstNonEmpty(env.Subject, "hikyaku.message"),
				"time":            env.ReceivedAt.UTC().Format(time.RFC3339Nano),
				"datacontenttype": firstNonEmpty(env.ContentType, "application/octet-stream"),
			}
			if v, err := env.JSON(); err == nil && v != nil {
				ce["data"] = v
			} else if len(env.Body) > 0 {
				ce["data_base64"] = base64.StdEncoding.EncodeToString(env.Body)
			}
			b, err := json.Marshal(ce)
			if err != nil {
				return err
			}
			env.SetBody(b)
			setContentType(env, "application/cloudevents+json")
			return nil
		}
	default:
		return CodecStep{}, fmt.Errorf("unknown codec %q", name)
	}
	return CodecStep{Name: name, fn: fn}, nil
}

func setContentType(env *envelope.Envelope, ct string) {
	env.ContentType = ct
	env.SetHeader("Content-Type", ct)
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
