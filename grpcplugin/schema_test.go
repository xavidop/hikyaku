package grpcplugin

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type serverOptions struct {
	Host string `yaml:"host" jsonschema:"required"`
	Port int    `yaml:"port,omitempty" jsonschema:"minimum=1,maximum=65535"`
}

type testOptions struct {
	Name    string          `yaml:"name" jsonschema:"required" jsonschema_description:"A name."`
	Mode    string          `yaml:"mode,omitempty" jsonschema:"enum=fast,enum=slow"`
	Servers []serverOptions `yaml:"servers,omitempty"`
}

func TestOptionsSchema(t *testing.T) {
	s, err := optionsSchema(nil)
	require.NoError(t, err)
	assert.Empty(t, s, "no options struct, no schema")

	s, err = optionsSchema(testOptions{})
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(s), &doc))
	assert.NotContains(t, doc, "$id", "the host names the schema")
	assert.Equal(t, "https://json-schema.org/draft/2020-12/schema", doc["$schema"])
	assert.Equal(t, []any{"name"}, doc["required"])
	assert.Equal(t, false, doc["additionalProperties"])
	props := doc["properties"].(map[string]any)
	assert.Contains(t, props, "servers", "yaml names")
	assert.Equal(t, "A name.", props["name"].(map[string]any)["description"])
	ptr, err := optionsSchema(&testOptions{})
	require.NoError(t, err)
	assert.Equal(t, s, ptr, "a pointer to the struct works too")
}

func TestOptionsValidator(t *testing.T) {
	s, err := optionsSchema(testOptions{})
	require.NoError(t, err)
	v, err := compileOptionsSchema("https://example.com/plugins/x/kind.json", json.RawMessage(s))
	require.NoError(t, err)

	assert.NoError(t, v.check(map[string]any{"name": "a", "mode": "fast", "servers": []any{map[string]any{"host": "h", "port": 80}}}))
	assert.EqualError(t, v.check(nil), "missing property 'name'")
	assert.EqualError(t, v.check(map[string]any{"name": "a", "mode": "medium"}), `mode: value must be one of 'fast', 'slow'`)
	err = v.check(map[string]any{"name": 1, "servers": []any{map[string]any{"port": 0}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name: got number, want string")
	assert.Contains(t, err.Error(), "servers[0]: missing property 'host'")
	assert.Contains(t, err.Error(), "servers[0].port: minimum: got 0, want 1")
	assert.ErrorContains(t, v.check(map[string]any{"name": "a", "x": 1}), "additional properties 'x' not allowed")
	assert.Error(t, v.check(map[string]any{"name": func() {}}), "options JSON cannot carry")

	_, err = compileOptionsSchema("https://example.com/bad.json", json.RawMessage(`{"type": 3}`))
	assert.ErrorContains(t, err, "options schema")
	_, err = compileOptionsSchema("https://example.com/bad.json", json.RawMessage(`{`))
	assert.ErrorContains(t, err, "options schema")
}

func TestKeyPath(t *testing.T) {
	assert.Equal(t, "", keyPath(nil))
	assert.Equal(t, "a", keyPath([]string{"a"}))
	assert.Equal(t, "list[0].host", keyPath([]string{"list", "0", "host"}))
	assert.Equal(t, "[2]", keyPath([]string{"2"}))
}
