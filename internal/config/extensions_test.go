package config

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chatOptions struct {
	WebhookURL string `yaml:"webhook_url" jsonschema:"required" jsonschema_description:"Where to post."`
	Text       string `yaml:"text,omitempty"`
}

const pluginSpec = `
version: 1
name: demo
sources:
  - { name: in, type: stdin }
  - { name: crm-events, type: x-crm, options: { tenant: acme } }
destinations:
  - name: chat
    type: x-chat
    options: { webhook_url: "https://chat.example.com/hook", text: "{{ .json.title }}" }
    retry: { attempts: 2 }
    fallback: [log]
  - { name: log, type: stdout }
routes:
  - { name: r, from: [in, crm-events], to: [chat] }
`

func ext() Extensions {
	return Extensions{
		Sinks: map[string]ExtType{"x-chat": {Options: chatOptions{}, Description: "Chat webhook", Check: func(o map[string]any) error {
			if _, ok := o["webhook_url"]; !ok {
				return errors.New("webhook_url is required")
			}
			return nil
		}}},
		Sources: map[string]ExtType{"x-crm": {}},
	}
}

func TestValidatePluginTypes(t *testing.T) {
	_, err := LoadBytes(context.Background(), "s.yaml", []byte(pluginSpec), LoadOptions{})
	text := errText(t, err)
	assert.Contains(t, text, `unknown destination type "x-chat"`, "a stock build does not know the type")
	assert.Contains(t, text, `unknown source type "x-crm"`)

	l, err := LoadBytes(context.Background(), "s.yaml", []byte(pluginSpec), LoadOptions{Extensions: ext()})
	require.NoError(t, err)
	d, _ := l.Spec.DestinationByName("chat")
	assert.Equal(t, "https://chat.example.com/hook", d.Options["webhook_url"])
	assert.Equal(t, "{{ .json.title }}", d.Options["text"], "per-message templates in options are left for the plugin")

	bad := strings.Replace(pluginSpec, `webhook_url: "https://chat.example.com/hook", `, "", 1)
	_, err = LoadBytes(context.Background(), "s.yaml", []byte(bad), LoadOptions{Extensions: ext()})
	assert.Contains(t, errText(t, err), `destination "chat".options: webhook_url is required`)
}

func TestOptionsRejectedOnBuiltInTypes(t *testing.T) {
	_, err := LoadBytes(context.Background(), "s.yaml", []byte(strings.Replace(minimal, "type: stdout }", "type: stdout, options: { a: 1 } }", 1)), LoadOptions{})
	assert.Contains(t, errText(t, err), "options: applies to plugin types")
}

func TestSchemaWithPluginOptions(t *testing.T) {
	s := SchemaWith(ext())
	b, err := json.Marshal(s.Definitions["Destination"])
	require.NoError(t, err)
	assert.Contains(t, string(b), `"const":"x-chat"`)
	assert.Contains(t, string(b), `"webhook_url"`)
	src, err := json.Marshal(s.Definitions["Source"])
	require.NoError(t, err)
	assert.Contains(t, string(src), `"const":"x-crm"`)
	plain, err := json.Marshal(Schema().Definitions["Destination"])
	require.NoError(t, err)
	assert.NotContains(t, string(plain), "x-chat")
}

type nestedOptions struct {
	Server chatServer   `yaml:"server"`
	Extra  []chatServer `yaml:"extra,omitempty"`
}

type chatServer struct {
	Host string `yaml:"host"`
}

func TestSchemaWithNestedOptionsRefs(t *testing.T) {
	s := SchemaWith(Extensions{Sinks: map[string]ExtType{"x-nested": {Options: nestedOptions{}}}})
	require.Contains(t, s.Definitions, "x-nested.chatServer", "nested types move to the top, under the kind")
	b, err := json.Marshal(s.Definitions["Destination"])
	require.NoError(t, err)
	assert.Contains(t, string(b), `"$ref":"#/$defs/x-nested.chatServer"`, "refs follow the moved definitions")
	assert.NotContains(t, string(b), `"#/$defs/chatServer"`)
}

func TestSchemaWithRawSchema(t *testing.T) {
	raw := `{
	  "$schema": "https://json-schema.org/draft/2020-12/schema",
	  "$id": "https://example.com/echo.json",
	  "type": "object",
	  "properties": {
	    "file": {"type": "string", "description": "Where to write."},
	    "tls": {"$ref": "#/$defs/tls"},
	    "peers": {"type": "array", "items": {"$ref": "#/$defs/tls"}}
	  },
	  "required": ["file"],
	  "additionalProperties": false,
	  "$defs": {"tls": {"type": "object", "properties": {"ca": {"type": "string"}}}}
	}`
	x := Extensions{
		Sinks:   map[string]ExtType{"x-echo": {Schema: json.RawMessage(raw), Description: "Echo"}},
		Sources: map[string]ExtType{"x-bad": {Schema: json.RawMessage(`{"type": 3`), Description: "Broken"}},
	}
	s := SchemaWith(x)
	b, err := json.Marshal(s.Definitions["Destination"])
	require.NoError(t, err)
	text := string(b)
	assert.Contains(t, text, `"const":"x-echo"`)
	assert.Contains(t, text, `"file":{"type":"string","description":"Where to write."}`)
	assert.Contains(t, text, `"required":["file"]`)
	assert.Contains(t, text, `"$ref":"#/$defs/x-echo.tls"`)
	assert.NotContains(t, text, "example.com", "the plugin's $id is dropped, like a reflected one")
	assert.NotContains(t, text, `"$schema"`)
	assert.Contains(t, text, `"description":"Echo"`, "the type's description fills in")
	require.Contains(t, s.Definitions, "x-echo.tls")
	member, err := json.Marshal(s.Definitions["Member"])
	require.NoError(t, err)
	assert.Contains(t, string(member), `"$ref":"#/$defs/x-echo.tls"`, "group members get the same options")

	src, err := json.Marshal(s.Definitions["Source"])
	require.NoError(t, err)
	assert.Contains(t, string(src), `"const":"x-bad"`)
	assert.Contains(t, string(src), `"options":{"type":"object","description":"Broken"}`, "an unparsable schema is any object")

	both := SchemaWith(Extensions{Sinks: map[string]ExtType{"x-chat": {Options: chatOptions{}, Schema: json.RawMessage(raw)}}})
	b, err = json.Marshal(both.Definitions["Destination"])
	require.NoError(t, err)
	assert.Contains(t, string(b), `"webhook_url"`, "the Go struct wins over a raw schema")
	assert.NotContains(t, string(b), `"Where to write."`)
}
