package config

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

// compiled is the generated schema, compiled by a validator independent of
// the generator.
func compiled(t *testing.T) *jsonschema.Schema {
	t.Helper()
	b, err := SchemaJSON(Schema())
	require.NoError(t, err)
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	require.NoError(t, err)
	c := jsonschema.NewCompiler()
	require.NoError(t, c.AddResource("hikyaku.schema.json", doc))
	s, err := c.Compile("hikyaku.schema.json")
	require.NoError(t, err)
	return s
}

func yamlToJSON(t *testing.T, path string) any {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var v any
	require.NoError(t, yaml.Unmarshal(data, &v))
	b, err := json.Marshal(v)
	require.NoError(t, err)
	out, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	require.NoError(t, err)
	return out
}

func TestSchemaAcceptsTheDesignExample(t *testing.T) {
	s := compiled(t)
	assert.NoError(t, s.Validate(yamlToJSON(t, "testdata/full.yaml")))
}

func TestSchemaRejectsUnknownKeys(t *testing.T) {
	s := compiled(t)
	doc := map[string]any{
		"version": json.Number("1"), "name": "x", "sources": []any{}, "destinations": []any{}, "routes": []any{},
		"colour": "blue",
	}
	assert.Error(t, s.Validate(doc))
}

// The committed schema is what the structs generate (make schema-check runs
// the same comparison with the docs page).
func TestSchemaFileUpToDate(t *testing.T) {
	want, err := SchemaJSON(Schema())
	require.NoError(t, err)
	got, err := os.ReadFile("../../schema/hikyaku.schema.json")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got), "schema/hikyaku.schema.json is stale: run make schema")
}
