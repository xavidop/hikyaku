package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xavidop/hikyaku/envelope"
)

func TestEncodeEntry(t *testing.T) {
	env := envelope.New("in", "http", nil)
	env.ID = "01J0ABC"
	env.ContentType = "application/json"
	env.Subject = "/hooks/stripe"
	env.Headers = map[string][]string{
		"X-Tenant":      {"acme"},
		"Accept":        {"a", "b"},
		"Authorization": {"Bearer secret"},
		"Cookie":        {"session=1"},
		"Content-Type":  {"application/json"},
		"X-Empty":       {},
		"x-override":    {"old"},
	}
	vals := encodeEntry(env, []byte(`{"a":1}`), map[string]string{"X-Override": "new"})
	assert.Equal(t, []any{
		"id", "01J0ABC",
		"body", []byte(`{"a":1}`),
		"content_type", "application/json",
		"subject", "/hooks/stripe",
		"h:Accept", "a, b",
		"h:X-Override", "new",
		"h:X-Tenant", "acme",
	}, vals, "credentials and Content-Type stay out; configured headers win, case-insensitively")

	bare := envelope.New("in", "http", []byte("x"))
	bare.ID = "1"
	assert.Equal(t, []any{"id", "1", "body", []byte("x")}, encodeEntry(bare, bare.Body, nil),
		"empty content type and subject are left out")
}

func TestDecodeEntry(t *testing.T) {
	t.Run("hikyaku format", func(t *testing.T) {
		e := decodeEntry("orders", "1-0", map[string]any{
			"id": "01J0ABC", "body": `{"a":1}`, "content_type": "application/cloudevents+json",
			"subject": "/hooks", "h:X-Tenant": "acme", "h:": "ignored", "other": "ignored",
		})
		assert.False(t, e.foreign)
		assert.Equal(t, "01J0ABC", e.id)
		assert.Equal(t, "/hooks", e.subject)
		assert.Equal(t, "application/cloudevents+json", e.contentType)
		assert.Equal(t, map[string][]string{"X-Tenant": {"acme"}}, e.headers)
		assert.Equal(t, []byte(`{"a":1}`), e.body)
	})
	t.Run("defaults", func(t *testing.T) {
		e := decodeEntry("orders", "1-0", map[string]any{"body": "plain"})
		assert.Equal(t, "orders-1-0", e.id, "no id field: <stream>-<entry ID>")
		assert.Equal(t, "orders", e.subject, "no subject field: the stream key")
		assert.Equal(t, contentText, e.contentType, "no content_type field: sniffed")
		assert.Empty(t, e.headers)
		assert.Equal(t, contentJSON, decodeEntry("s", "1-0", map[string]any{"body": `[1]`}).contentType)
		assert.Equal(t, contentBinary, decodeEntry("s", "1-0", map[string]any{"body": "\xff\xfe"}).contentType)
	})
	t.Run("foreign producer", func(t *testing.T) {
		e := decodeEntry("orders", "1700000000000-3", map[string]any{"id": "42", "user": "ada", "note": "<b>&"})
		assert.True(t, e.foreign)
		assert.Equal(t, "orders-1700000000000-3", e.id, "an id field of foreign data is not the message ID")
		assert.Equal(t, "orders", e.subject)
		assert.Equal(t, contentJSON, e.contentType)
		assert.JSONEq(t, `{"id":"42","note":"<b>&","user":"ada"}`, string(e.body))
		assert.Equal(t, `{"id":"42","note":"<b>&","user":"ada"}`, string(e.body), "compact, sorted, not HTML-escaped")
	})
}

func TestText(t *testing.T) {
	assert.Equal(t, "", text(nil))
	assert.Equal(t, "s", text("s"))
	assert.Equal(t, "b", text([]byte("b")))
	assert.Equal(t, "42", text(int64(42)))
}

func TestIDLess(t *testing.T) {
	assert.True(t, idLess("1-0", "1-1"))
	assert.True(t, idLess("9-5", "10-0"), "numeric, not lexical")
	assert.False(t, idLess("10-0", "9-5"))
	assert.False(t, idLess("3-3", "3-3"))
}
