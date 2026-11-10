package grpcplugin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	invopop "github.com/invopop/jsonschema"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// optionsSchema generates the JSON Schema of an options struct the way
// hikyaku generates its own: yaml field names, required only where the
// jsonschema tag says so, no unknown keys.
func optionsSchema(zero any) (_ string, err error) {
	if zero == nil {
		return "", nil
	}
	defer func() {
		if r := recover(); r != nil { // the reflector panics on types JSON has no schema for
			err = fmt.Errorf("%T: %v", zero, r)
		}
	}()
	r := &invopop.Reflector{
		ExpandedStruct:             true,
		RequiredFromJSONSchemaTags: true,
		FieldNameTag:               "yaml",
		Anonymous:                  true,
	}
	s := r.Reflect(zero)
	s.ID = ""
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// optionsValidator checks options: blocks against a kind's JSON Schema.
type optionsValidator struct {
	schema *jsonschema.Schema
}

// compileOptionsSchema compiles a plugin's schema. A schema that does not
// compile is the plugin's bug, reported when the plugin loads.
func compileOptionsSchema(url string, raw json.RawMessage) (*optionsValidator, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("options schema: %w", err)
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(url, doc); err != nil {
		return nil, fmt.Errorf("options schema: %w", err)
	}
	s, err := c.Compile(url)
	if err != nil {
		return nil, fmt.Errorf("options schema: %w", err)
	}
	return &optionsValidator{schema: s}, nil
}

// check validates one options: block.
func (v *optionsValidator) check(options map[string]any) error {
	if options == nil {
		options = map[string]any{}
	}
	b, err := json.Marshal(options)
	if err != nil {
		return err
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return err
	}
	if err := v.schema.Validate(inst); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			return errors.New(describeValidation(ve))
		}
		return err
	}
	return nil
}

var printer = message.NewPrinter(language.English)

// describeValidation lists the leaf failures of a validation error, each
// with the key path it is about: "count: got string, want integer".
func describeValidation(ve *jsonschema.ValidationError) string {
	var msgs []string
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			msg := e.ErrorKind.LocalizedString(printer)
			if p := keyPath(e.InstanceLocation); p != "" {
				msg = p + ": " + msg
			}
			msgs = append(msgs, msg)
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(ve)
	return strings.Join(msgs, "; ")
}

// keyPath renders an instance location as list[0].host.
func keyPath(tokens []string) string {
	var b strings.Builder
	for _, t := range tokens {
		if _, err := strconv.Atoi(t); err == nil {
			b.WriteString("[" + t + "]")
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('.')
		}
		b.WriteString(t)
	}
	return b.String()
}
