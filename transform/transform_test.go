package transform

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

func msg(body string) *envelope.Envelope {
	e := envelope.New("webhooks", "http", []byte(body))
	e.Subject = "/ingest"
	e.Headers["X-Event-Type"] = []string{"invoice.paid"}
	return e
}

func TestTemplateRender(t *testing.T) {
	e := msg(`{"customer_id":"c-42","ticket":{"id":7},"tags":["a","b"]}`)
	for src, want := range map[string]string{
		"{{ .json.customer_id }}":                       "c-42",
		"{{ .json.ticket.id }}":                         "7",
		"{{ .json.missing }}":                           "",
		"{{ header .headers \"x-event-type\" }}":        "invoice.paid",
		"{{ .source }}:{{ .subject }}":                  "webhooks:/ingest",
		"{{ .json | json }}":                            `{"customer_id":"c-42","tags":["a","b"],"ticket":{"id":7}}`,
		"{{ join \",\" .json.tags }}":                   "a,b",
		"{{ default \"none\" .json.missing }}":          "none",
		"{{ .json.customer_id | upper }}":               "C-42",
		"static text":                                   "static text",
		"{{ truncate 3 .bodyText }}":                    `{"c`,
		"{{ sha256 \"x\" | truncate 8 }}":               "2d711642",
		"{{ b64enc \"hi\" }}{{ b64dec \"aGk=\" }}":      "aGk=hi",
		"{{ str .json.ticket.id }}{{ str .json.nope }}": "7",
	} {
		tm, err := CompileTemplate("t", src)
		require.NoError(t, err, src)
		got, err := tm.Render(e)
		require.NoError(t, err, src)
		assert.Equal(t, want, got, src)
	}
	assert.True(t, MustTemplate("t", "plain").IsStatic())
	assert.Equal(t, "{{ .id }}", MustTemplate("t", "{{ .id }}").String())
}

func TestTemplateNonJSONBody(t *testing.T) {
	tm := MustTemplate("key", "{{ .json.customer_id }}")
	got, err := tm.Render(msg("not json"))
	require.NoError(t, err)
	assert.Equal(t, "", got, "a non-JSON body renders .json keys as empty")
}

func TestTemplateCompileError(t *testing.T) {
	_, err := CompileTemplate("state", "{{ .json.x ")
	assert.ErrorContains(t, err, "template state")
	var nilT *Template
	s, err := nilT.Render(msg("{}"))
	assert.NoError(t, err)
	assert.Empty(t, s)
}

func TestCEL(t *testing.T) {
	e := msg(`{"type":"invoice.paid","amount":120,"customer":{"tier":"gold"}}`)
	for src, want := range map[string]bool{
		`msg.json.type.startsWith("invoice.")`:                 true,
		`msg.json.amount > 100`:                                true,
		`msg.json.customer.tier == "silver"`:                   false,
		`msg.headers["X-Event-Type"] == "invoice.paid"`:        true,
		`msg.source == "webhooks" && msg.subject == "/ingest"`: true,
		`has(msg.json.missing)`:                                false,
	} {
		x, err := CompileCEL(src)
		require.NoError(t, err, src)
		got, err := x.Bool(e)
		require.NoError(t, err, src)
		assert.Equal(t, want, got, src)
	}

	_, err := CompileCEL(`msg.json.type +`)
	assert.Error(t, err)
	_, err = CompileCEL(`"a string"`)
	assert.ErrorContains(t, err, "want a bool")

	x, _ := CompileCEL(`msg.json.nope.deeper == 1`)
	_, err = x.Bool(e)
	assert.Error(t, err, "a missing key is an evaluation error")
	assert.Equal(t, `msg.json.nope.deeper == 1`, x.String())
}

func TestJQ(t *testing.T) {
	q, err := CompileJQ(`{id: .id, kind: .type}`)
	require.NoError(t, err)
	var in any
	require.NoError(t, json.Unmarshal([]byte(`{"id":1,"type":"x","noise":true}`), &in))
	out, err := q.Run(in)
	require.NoError(t, err)
	assert.Equal(t, []any{map[string]any{"id": 1.0, "kind": "x"}}, out)

	many, _ := CompileJQ(`.[]`)
	out, err = many.Run([]any{1.0, 2.0})
	require.NoError(t, err)
	assert.Len(t, out, 2)

	_, err = CompileJQ(`{`)
	assert.Error(t, err)
	bad, _ := CompileJQ(`error("boom")`)
	_, err = bad.Run(in)
	assert.ErrorContains(t, err, "boom")
	assert.Equal(t, `.[]`, many.String())
}

func chain(t *testing.T, specs ...config.Transform) Chain {
	t.Helper()
	c, err := Compile("transforms", specs)
	require.NoError(t, err)
	return c
}

func TestChainFilterJQTemplateHeaders(t *testing.T) {
	c := chain(t,
		config.Transform{Filter: `msg.json.type != "ping"`},
		config.Transform{JQ: `{id: .id, type: .type}`},
		config.Transform{Headers: &config.HeaderOps{Set: map[string]string{"X-Type": "{{ .json.type }}"}, Add: map[string]string{"X-Seen": "hikyaku"}, Remove: []string{"X-Event-Type"}}},
	)
	e := msg(`{"id":3,"type":"bug","secret":"s"}`)
	keep, err := c.Apply(e)
	require.NoError(t, err)
	assert.True(t, keep)
	assert.JSONEq(t, `{"id":3,"type":"bug"}`, e.BodyText())
	assert.Equal(t, "application/json", e.ContentType)
	assert.Equal(t, "bug", e.Header("X-Type"))
	assert.Equal(t, "hikyaku", e.Header("X-Seen"))
	assert.Empty(t, e.Header("X-Event-Type"))

	keep, err = c.Apply(msg(`{"type":"ping"}`))
	require.NoError(t, err)
	assert.False(t, keep, "the filter drops pings")
}

func TestFilterErrorKeepsMessage(t *testing.T) {
	c := chain(t, config.Transform{Filter: `msg.json.nested.flag == true`})
	e := msg(`{}`)
	keep, err := c.Apply(e)
	require.NoError(t, err)
	assert.True(t, keep, "a filter that cannot be evaluated lets the message through")
	_, ok := e.Attribute("transform.filter_error")
	assert.True(t, ok)
}

func TestJQSelectDrops(t *testing.T) {
	c := chain(t, config.Transform{JQ: `select(.priority > 3)`})
	keep, err := c.Apply(msg(`{"priority":1}`))
	require.NoError(t, err)
	assert.False(t, keep)
	_, err = c.Apply(msg(`not json`))
	assert.Error(t, err)
}

func TestTemplateStepAndContentType(t *testing.T) {
	c := chain(t,
		config.Transform{Template: `{"text": {{ .json.message | json }}}`, ContentType: "application/json"},
		config.Transform{ContentType: "application/vnd.slack+json"},
	)
	e := msg(`{"message":"hi \"there\""}`)
	_, err := c.Apply(e)
	require.NoError(t, err)
	assert.JSONEq(t, `{"text":"hi \"there\""}`, e.BodyText())
	assert.Equal(t, "application/vnd.slack+json", e.Header("Content-Type"))
}

func TestCodecs(t *testing.T) {
	e := msg(`{ "a" : 1 }`)
	_, err := chain(t, config.Transform{Codec: "json"}).Apply(e)
	require.NoError(t, err)
	assert.Equal(t, `{"a":1}`, e.BodyText())

	_, err = chain(t, config.Transform{Codec: "base64_encode"}, config.Transform{Codec: "base64_decode"}).Apply(e)
	require.NoError(t, err)
	assert.Equal(t, `{"a":1}`, e.BodyText())

	f := msg("name=hikyaku&tag=a&tag=b")
	_, err = chain(t, config.Transform{Codec: "form_to_json"}).Apply(f)
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"hikyaku","tag":["a","b"]}`, f.BodyText())

	ce := msg(`{"x":1}`)
	ce.ContentType = "application/json"
	_, err = chain(t, config.Transform{Codec: "cloudevents"}).Apply(ce)
	require.NoError(t, err)
	var got map[string]any
	require.NoError(t, json.Unmarshal(ce.Body, &got))
	assert.Equal(t, "1.0", got["specversion"])
	assert.Equal(t, "webhooks", got["source"])
	assert.Equal(t, map[string]any{"x": 1.0}, got["data"])

	txt := msg("plain")
	_, err = chain(t, config.Transform{Codec: "text"}).Apply(txt)
	require.NoError(t, err)
	assert.Equal(t, "text/plain; charset=utf-8", txt.ContentType)

	_, err = chain(t, config.Transform{Codec: "json"}).Apply(msg("nope"))
	assert.ErrorContains(t, err, "codec json")
	_, err = Compile("t", []config.Transform{{Codec: "yaml"}})
	assert.ErrorContains(t, err, "unknown codec")
}

func TestCompileErrorsNamePath(t *testing.T) {
	_, err := Compile("route \"a\".transforms", []config.Transform{{}, {}})
	assert.ErrorContains(t, err, "empty transform")
	_, err = Compile("x", []config.Transform{{Filter: "msg.(("}})
	assert.ErrorContains(t, err, "x[0].filter")
	_, err = Compile("x", []config.Transform{{JQ: "{"}})
	assert.ErrorContains(t, err, "x[0].jq")
	_, err = Compile("x", []config.Transform{{Template: "{{"}})
	assert.ErrorContains(t, err, "x[0].template")
	_, err = Compile("x", []config.Transform{{Headers: &config.HeaderOps{Set: map[string]string{"A": "{{"}}}})
	assert.Error(t, err)
}
