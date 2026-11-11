package transform

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

var orderSchema = map[string]any{
	"type":     "object",
	"required": []any{"id", "items"},
	"properties": map[string]any{
		"id":    map[string]any{"type": "string"},
		"email": map[string]any{"type": "string", "format": "email"},
		"items": map[string]any{
			"type":     "array",
			"minItems": 1,
			"items": map[string]any{
				"type":       "object",
				"required":   []any{"price"},
				"properties": map[string]any{"price": map[string]any{"type": "number", "minimum": 0}},
			},
		},
	},
}

func body(s string) *envelope.Envelope { return &envelope.Envelope{Body: []byte(s)} }

func TestValidateStep(t *testing.T) {
	chain, err := Compile("t", []config.Transform{{Validate: &config.SchemaCheck{Schema: orderSchema}}})
	require.NoError(t, err)

	env := body(`{"id":"o-1","items":[{"price":9.5}]}`)
	keep, err := chain.Apply(env)
	require.NoError(t, err)
	assert.True(t, keep)
	assert.JSONEq(t, `{"id":"o-1","items":[{"price":9.5}]}`, env.BodyText(), "validate never changes the message")

	cases := []struct {
		name, body string
		want       []string
	}{
		{"wrong type deep in a list", `{"id":"o-1","items":[{"price":"free"}]}`, []string{"items[0].price: got string, want number"}},
		{"missing keys", `{}`, []string{"missing properties 'id', 'items'"}},
		{"format is checked", `{"id":"o-1","items":[{"price":1}],"email":"nope"}`, []string{"email: 'nope' is not valid email"}},
		{"not JSON", `id=1`, []string{"the body is not JSON"}},
		{"empty", ``, []string{"the body is empty"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			keep, err := chain.Apply(body(c.body))
			assert.False(t, keep)
			var se *SchemaError
			require.ErrorAs(t, err, &se)
			for _, w := range c.want {
				assert.Contains(t, err.Error(), w)
			}
			assert.Contains(t, err.Error(), "schema: ")
		})
	}
}

func TestValidateStepBoundsItsReport(t *testing.T) {
	s, err := CompileSchema(&config.SchemaCheck{Schema: map[string]any{
		"type": "array", "items": map[string]any{"type": "integer"},
	}})
	require.NoError(t, err)
	_, err = s.Apply(body(`["a","b","c","d","e","f","g","h"]`))
	var se *SchemaError
	require.ErrorAs(t, err, &se)
	assert.Len(t, se.Violations, maxViolations)
	assert.Equal(t, 3, se.More)
	assert.Contains(t, err.Error(), "and 3 more")
}

func TestValidateStepFromFile(t *testing.T) {
	dir := t.TempDir()
	write := func(name, text string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte(text), 0o600))
		return p
	}
	write("money.json", `{"type":"number","minimum":0}`)
	for name, text := range map[string]string{
		"order.json": `{"type":"object","required":["total"],"properties":{"total":{"$ref":"money.json"}}}`,
		"order.yaml": "type: object\nrequired: [total]\nproperties:\n  total: { $ref: money.json }\n",
	} {
		t.Run(name, func(t *testing.T) {
			s, err := CompileSchema(&config.SchemaCheck{File: write(name, text)})
			require.NoError(t, err)
			keep, err := s.Apply(body(`{"total":3}`))
			require.NoError(t, err)
			assert.True(t, keep)
			_, err = s.Apply(body(`{"total":-1}`))
			assert.ErrorContains(t, err, "total: ")
		})
	}
}

func TestCompileSchemaNamesWhatIsWrong(t *testing.T) {
	_, err := CompileSchema(&config.SchemaCheck{Schema: map[string]any{
		"properties": map[string]any{"a": map[string]any{"minimum": "x"}},
	}})
	assert.EqualError(t, err, "not a valid JSON Schema: properties.a.minimum: got string, want number")
}

func TestCompileSchemaErrors(t *testing.T) {
	cases := map[string]*config.SchemaCheck{
		"a schema that is not one":        {Schema: map[string]any{"type": "strng"}},
		"a missing file":                  {File: filepath.Join(t.TempDir(), "nope.json")},
		"an inline $ref to a file":        {Schema: map[string]any{"$ref": "other.json"}},
		"a $ref fetched over the network": {Schema: map[string]any{"$ref": "https://example.com/schema.json"}},
	}
	for name, spec := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Compile("route \"r\".transforms", []config.Transform{{Validate: spec}})
			require.Error(t, err)
			assert.Contains(t, err.Error(), `route "r".transforms[0].validate: `)
			assert.NotContains(t, err.Error(), "\n")
		})
	}
}

func TestRedactStep(t *testing.T) {
	chain, err := Compile("t", []config.Transform{{Redact: []string{"$.card.number", "$..email", "$.items[*].sku"}}})
	require.NoError(t, err)

	env := body(`{"id":9007199254740993,"card":{"number":"4242","brand":"visa"},"owner":{"email":"a@b.c"},"items":[{"sku":"x","qty":2}]}`)
	keep, err := chain.Apply(env)
	require.NoError(t, err)
	assert.True(t, keep)
	assert.JSONEq(t, `{"id":9007199254740993,"card":{"number":"[redacted]","brand":"visa"},"owner":{"email":"[redacted]"},"items":[{"sku":"[redacted]","qty":2}]}`, env.BodyText())
	assert.Contains(t, env.BodyText(), `"id":9007199254740993`, "a number past 2^53 is kept as written")

	same := body(`{ "id": 1 }`)
	_, err = chain.Apply(same)
	require.NoError(t, err)
	assert.Equal(t, `{ "id": 1 }`, same.BodyText(), "nothing selected: the body is left as it is")

	_, err = chain.Apply(body(`card=4242`))
	assert.ErrorContains(t, err, "redact needs a JSON body")

	_, err = Compile(`route "r".transforms`, []config.Transform{{Redact: []string{"card.number"}}})
	assert.ErrorContains(t, err, `route "r".transforms[0].redact: `)
}
