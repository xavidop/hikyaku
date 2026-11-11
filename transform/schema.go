package transform

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// maxViolations bounds the failures a SchemaError lists: the text goes into
// the dead letter and the logs.
const maxViolations = 5

// inlineSchemaURL names a schema written in the spec. It is not a file, so
// a relative $ref in an inline schema cannot read one.
const inlineSchemaURL = "hikyaku:///inline-schema.json"

// SchemaError is a message whose body does not match the JSON Schema of a
// validate step. The engine dead-letters it with reason schema.
type SchemaError struct {
	// Violations are the failures, each with the key path it is about:
	// "items[0].price: got string, want number".
	Violations []string
	// More counts the failures left out of Violations.
	More int
	// To are the destinations of the step's on_invalid: a route delivers
	// the refused message there, and does not dead-letter it.
	To []string
}

func (e *SchemaError) Error() string {
	s := "schema: " + strings.Join(e.Violations, "; ")
	if e.More > 0 {
		s += fmt.Sprintf("; and %d more", e.More)
	}
	return s
}

// ValidateStep checks the JSON body against a JSON Schema. It never changes
// the message, and never drops it: a body that does not match is an error.
type ValidateStep struct {
	schema *jsonschema.Schema
	to     []string // on_invalid.to
}

// CompileSchema builds a validate step. Formats (date-time, email, uri...)
// are checked, not only annotated. A $ref may point inside the schema or,
// from a schema file, at another file; nothing is fetched over the network.
func CompileSchema(spec *config.SchemaCheck) (ValidateStep, error) {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	loc := inlineSchemaURL
	var raw []byte
	var err error
	if spec.File != "" {
		if loc, err = filepath.Abs(spec.File); err != nil {
			return ValidateStep{}, err
		}
		if raw, err = os.ReadFile(loc); err != nil {
			return ValidateStep{}, err
		}
		if raw, err = toJSON(raw); err != nil {
			return ValidateStep{}, fmt.Errorf("%s: %w", spec.File, err)
		}
	} else if raw, err = json.Marshal(spec.Schema); err != nil {
		return ValidateStep{}, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return ValidateStep{}, err
	}
	if err := c.AddResource(loc, doc); err != nil {
		return ValidateStep{}, err
	}
	s, err := c.Compile(loc)
	if err != nil {
		return ValidateStep{}, compileError(err)
	}
	step := ValidateStep{schema: s}
	if spec.OnInvalid != nil {
		step.to = spec.OnInvalid.To
	}
	return step, nil
}

// compileError shortens the compiler's report of a schema that is not a
// valid JSON Schema to its failures, on one line.
func compileError(err error) error {
	var sve *jsonschema.SchemaValidationError
	var ve *jsonschema.ValidationError
	if errors.As(err, &sve) && errors.As(sve.Err, &ve) {
		se := schemaError(ve)
		return fmt.Errorf("not a valid JSON Schema: %s", strings.TrimPrefix(se.Error(), "schema: "))
	}
	return err
}

// toJSON turns a schema file into JSON: a file that is not JSON is read as
// YAML.
func toJSON(raw []byte) ([]byte, error) {
	if json.Valid(raw) {
		return raw, nil
	}
	var v any
	if err := yaml.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

// Apply validates the body.
func (s ValidateStep) Apply(env *envelope.Envelope) (bool, error) {
	if len(env.Body) == 0 {
		return false, &SchemaError{Violations: []string{"the body is empty"}, To: s.to}
	}
	v, err := env.JSON()
	if err != nil {
		return false, &SchemaError{Violations: []string{"the body is not JSON: " + err.Error()}, To: s.to}
	}
	if err := s.schema.Validate(v); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			se := schemaError(ve)
			se.To = s.to
			return false, se
		}
		return false, err
	}
	return true, nil
}

var printer = message.NewPrinter(language.English)

// schemaError lists the leaf failures of a validation error.
func schemaError(ve *jsonschema.ValidationError) *SchemaError {
	out := &SchemaError{}
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			if len(out.Violations) == maxViolations {
				out.More++
				return
			}
			msg := e.ErrorKind.LocalizedString(printer)
			if p := keyPath(e.InstanceLocation); p != "" {
				msg = p + ": " + msg
			}
			out.Violations = append(out.Violations, msg)
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(ve)
	return out
}

// keyPath renders an instance location as items[0].price.
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
