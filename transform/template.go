package transform

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/textproto"
	"strings"
	"text/template"
	"time"

	"github.com/xavidop/hikyaku/envelope"
)

// Template is a compiled per-message Go template.
type Template struct {
	raw    string
	tmpl   *template.Template
	static bool
}

// noValue is what text/template prints for a missing map key; a template
// renders it as empty, the way Helm does.
const noValue = "<no value>"

// Funcs are the functions available to every template.
var Funcs = template.FuncMap{
	"json": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
	"header": func(headers map[string]any, name string) any {
		return headers[textproto.CanonicalMIMEHeaderKey(name)]
	},
	"default": func(def, v any) any {
		if v == nil || v == "" {
			return def
		}
		return v
	},
	"lower":     strings.ToLower,
	"upper":     strings.ToUpper,
	"trim":      strings.TrimSpace,
	"replace":   func(old, new, s string) string { return strings.ReplaceAll(s, old, new) },
	"contains":  func(sub, s string) bool { return strings.Contains(s, sub) },
	"hasPrefix": func(prefix, s string) bool { return strings.HasPrefix(s, prefix) },
	"join": func(sep string, v any) string {
		switch xs := v.(type) {
		case []string:
			return strings.Join(xs, sep)
		case []any:
			parts := make([]string, len(xs))
			for i, x := range xs {
				parts[i] = fmt.Sprint(x)
			}
			return strings.Join(parts, sep)
		}
		return fmt.Sprint(v)
	},
	"truncate": func(n int, s string) string {
		if len(s) <= n {
			return s
		}
		return s[:n]
	},
	"sha256": func(s string) string {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	},
	"b64enc": func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) },
	"b64dec": func(s string) (string, error) {
		b, err := base64.StdEncoding.DecodeString(s)
		return string(b), err
	},
	"now": func() string { return time.Now().UTC().Format(time.RFC3339) },
	"str": func(v any) string {
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	},
}

// CompileTemplate parses a template. Text without actions renders as is.
func CompileTemplate(name, text string) (*Template, error) {
	t := &Template{raw: text}
	if !strings.Contains(text, "{{") {
		t.static = true
		return t, nil
	}
	tmpl, err := template.New(name).Funcs(Funcs).Parse(text)
	if err != nil {
		return nil, fmt.Errorf("template %s:%s", name, strings.TrimPrefix(err.Error(), "template: "+name+":"))
	}
	t.tmpl = tmpl
	return t, nil
}

// MustTemplate is CompileTemplate for templates known to be valid.
func MustTemplate(name, text string) *Template {
	t, err := CompileTemplate(name, text)
	if err != nil {
		panic(err)
	}
	return t
}

// IsStatic reports whether the template has no actions.
func (t *Template) IsStatic() bool { return t == nil || t.static }

// String returns the template's source.
func (t *Template) String() string {
	if t == nil {
		return ""
	}
	return t.raw
}

// Render renders the template against an envelope.
func (t *Template) Render(env *envelope.Envelope) (string, error) {
	if t == nil {
		return "", nil
	}
	if t.static {
		return t.raw, nil
	}
	return t.Execute(Data(env))
}

// Execute renders the template against arbitrary data.
func (t *Template) Execute(data any) (string, error) {
	if t == nil {
		return "", nil
	}
	if t.static {
		return t.raw, nil
	}
	var b bytes.Buffer
	if err := t.tmpl.Execute(&b, data); err != nil {
		return "", fmt.Errorf("template %s: %w", t.tmpl.Name(), err)
	}
	return strings.ReplaceAll(b.String(), noValue, ""), nil
}
