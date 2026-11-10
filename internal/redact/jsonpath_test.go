package redact

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, s string) any {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal([]byte(s), &v))
	return v
}

func TestJSONPathRedact(t *testing.T) {
	doc := parse(t, `{"card":{"number":"4111","exp":"12/30"},"email":"a@b.c","items":[{"sku":"x","email":"i@b.c"},{"sku":"y"}],"odd-key":1,"profile":{"contact":{"email":"deep@b.c"}}}`)
	paths, err := CompilePaths([]string{"$.card.number", "$.email", "$.items[*].sku", "$['odd-key']", "$..email"})
	require.NoError(t, err)
	n := JSON(doc, paths)

	m := doc.(map[string]any)
	assert.Equal(t, Redacted, m["card"].(map[string]any)["number"])
	assert.Equal(t, "12/30", m["card"].(map[string]any)["exp"], "untouched siblings stay")
	assert.Equal(t, Redacted, m["email"])
	items := m["items"].([]any)
	assert.Equal(t, Redacted, items[0].(map[string]any)["sku"])
	assert.Equal(t, Redacted, items[1].(map[string]any)["sku"])
	assert.Equal(t, Redacted, items[0].(map[string]any)["email"], "$..email reaches into arrays")
	assert.Equal(t, Redacted, m["odd-key"])
	assert.Equal(t, Redacted, m["profile"].(map[string]any)["contact"].(map[string]any)["email"])
	assert.Equal(t, 8, n, "card.number, email, 2 skus, odd-key and the 3 emails of $..email")
}

func TestJSONPathIndexAndMissing(t *testing.T) {
	doc := parse(t, `{"a":[1,2,3]}`)
	p, err := CompilePath("$.a[1]")
	require.NoError(t, err)
	assert.Equal(t, 1, JSON(doc, []Path{p}))
	assert.Equal(t, []any{1.0, Redacted, 3.0}, doc.(map[string]any)["a"])
	assert.Equal(t, "$.a[1]", p.String())

	missing, _ := CompilePath("$.nope.deeper")
	out, _ := CompilePath("$.a[9]")
	assert.Equal(t, 0, JSON(doc, []Path{missing, out}))
	assert.Equal(t, 0, JSON("a string", []Path{missing}))
}

func TestCompilePathErrors(t *testing.T) {
	for _, bad := range []string{"card.number", "$", "$.", "$.a[", "$.a[x]", "$.a[-1]", "$!"} {
		_, err := CompilePath(bad)
		assert.Error(t, err, bad)
	}
	_, err := CompilePaths([]string{"$.ok", "bad"})
	assert.Error(t, err)
}
