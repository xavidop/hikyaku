package config

import (
	"regexp"
	"strings"

	"go.yaml.in/yaml/v3"
)

// interpRe matches the load-time references: {{secrets.NAME}} and
// {{env.NAME}}. Per-message templates ({{ .json.id }}) start with a dot and
// are left for the engine.
var interpRe = regexp.MustCompile(`\{\{\s*(secrets|env)\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// wholeEnvRe matches a value that is one {{env.X}} reference and nothing
// else. Secrets are left out: a secret is always a string, even one that
// reads like null or a number.
var wholeEnvRe = regexp.MustCompile(`^\s*\{\{\s*env\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}\s*$`)

// Placeholder stands in for a secret that was not resolved (validate without
// --check-secrets). It names the secret and contains no value.
func Placeholder(name string) string { return "‹secret:" + name + "›" }

// interp resolves load-time references.
type interp struct {
	// secret returns a declared secret's value (or its placeholder) and
	// whether it is declared.
	secret func(name string) (string, bool)
	env    func(name string) (string, bool)
}

// document rewrites a document's root mapping, except its secrets: block,
// which holds refs rather than values.
func (in interp) document(file string, root *yaml.Node, errs *Errors) {
	if root.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value == "secrets" {
			continue
		}
		in.node(file, root.Content[i+1], errs)
	}
}

// node rewrites every scalar under n in place and reports references to
// undeclared secrets.
func (in interp) node(file string, n *yaml.Node, errs *Errors) {
	switch n.Kind {
	case yaml.SequenceNode, yaml.MappingNode, yaml.DocumentNode:
		for _, c := range n.Content {
			in.node(file, c, errs)
		}
	case yaml.ScalarNode:
		if !strings.Contains(n.Value, "{{") {
			return
		}
		// A value that is one env reference ("{{env.REDIS_DB}}") takes the
		// type of what it resolves to, so the environment can fill numbers
		// and booleans: the quotes YAML needed around the braces do not make
		// it a string.
		if wholeEnvRe.MatchString(n.Value) {
			defer func() { n.Style, n.Tag = 0, "" }()
		}
		n.Value = interpRe.ReplaceAllStringFunc(n.Value, func(m string) string {
			sub := interpRe.FindStringSubmatch(m)
			if sub[1] == "secrets" {
				v, ok := in.secret(sub[2])
				if !ok {
					*errs = append(*errs, &Error{File: file, Line: n.Line, Msg: "secret " + sub[2] + " is used but not declared under secrets:"})
					return m
				}
				return v
			}
			v, _ := in.env(sub[2])
			return v
		})
	}
}

// SecretRefsUsed lists the secret names a text references, in order of
// first use.
func SecretRefsUsed(text string) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range interpRe.FindAllStringSubmatch(text, -1) {
		if m[1] == "secrets" && !seen[m[2]] {
			seen[m[2]] = true
			out = append(out, m[2])
		}
	}
	return out
}
