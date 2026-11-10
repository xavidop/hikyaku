package azure

import (
	"errors"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

func ptr[T any](v T) *T { return &v }

func TestContentType(t *testing.T) {
	assert.Equal(t, "text/csv", contentType(ptr("text/csv"), []byte("{}")))
	assert.Equal(t, contentJSON, contentType(nil, []byte(`{"a":1}`)))
	assert.Equal(t, contentJSON, contentType(ptr(""), []byte(`[1]`)))
	assert.Equal(t, contentText, contentType(nil, []byte("hello")))
	assert.Equal(t, contentText, contentType(nil, nil))
	assert.Equal(t, contentBinary, contentType(nil, []byte{0xff, 0xfe, 0x00}))
}

func TestAMQPBody(t *testing.T) {
	assert.Equal(t, []byte("abcd"), amqpBody([][]byte{[]byte("ab"), []byte("cd")}, nil, nil))
	assert.Equal(t, []byte("text"), amqpBody(nil, "text", nil))
	assert.Equal(t, []byte("raw"), amqpBody(nil, []byte("raw"), nil))
	assert.JSONEq(t, `{"n":1}`, string(amqpBody(nil, map[string]any{"n": 1}, nil)))
	assert.JSONEq(t, `[[1,"a"]]`, string(amqpBody(nil, nil, [][]any{{1, "a"}})))
	assert.Nil(t, amqpBody(nil, nil, nil))
}

func TestStringify(t *testing.T) {
	at := time.Date(2026, 11, 9, 10, 0, 0, 0, time.FixedZone("x", 3600))
	assert.Equal(t, "", stringify(nil))
	assert.Equal(t, "s", stringify("s"))
	assert.Equal(t, "b", stringify([]byte("b")))
	assert.Equal(t, "2026-11-09T09:00:00Z", stringify(at))
	assert.Equal(t, "true", stringify(true))
	assert.Equal(t, "42", stringify(int64(42)))
	assert.Equal(t, "1.5", stringify(1.5))
	uuid := amqp.UUID{0x12, 0x34}
	assert.Equal(t, uuid.String(), stringify(uuid))
}

func TestHeadersAndIdempotency(t *testing.T) {
	props := map[string]any{"orderId": int32(7), "idempotency-key": "k-1", "flag": true}
	h := headersFrom(props)
	assert.Equal(t, map[string][]string{"orderId": {"7"}, "idempotency-key": {"k-1"}, "flag": {"true"}}, h,
		"property names keep their case; templates canonicalize them")
	assert.Equal(t, "k-1", idempotencyFrom(props))
	assert.Empty(t, idempotencyFrom(map[string]any{"Idempotency-Key": string(make([]byte, maxIdempotencyKey+1))}))
	assert.Empty(t, idempotencyFrom(nil))
}

func TestOutgoingRender(t *testing.T) {
	out, err := compileOutgoing(map[string]string{"X-Team": "core", "x-route": "{{ .json.route }}"}, "")
	require.NoError(t, err)
	env := envelope.New("in", "http", []byte(`{"route":"eu"}`))
	env.Headers = map[string][]string{
		"Authorization": {"Bearer secret"}, "Content-Type": {"application/json"}, "Accept": {"a", "b"},
		"X-Route": {"us"}, "Cookie": {"c"}, "Idempotency-Key": {"key-1"}, "Empty": {},
	}
	body, props, err := out.render(env)
	require.NoError(t, err)
	assert.Equal(t, env.Body, body)
	assert.Equal(t, map[string]any{"Accept": "a, b", "X-Team": "core", "x-route": "eu", "Idempotency-Key": "key-1"}, props,
		"credentials and framing headers are dropped; a configured header replaces any spelling of it")

	// Without the header, the idempotency key is added: the attribute, else the ID.
	env = envelope.New("in", "http", []byte("x"))
	env.ID = "env-1"
	_, props, err = outgoing{}.render(env)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"Idempotency-Key": "env-1"}, props)
	env.SetAttribute("idempotency_key", "caller-key")
	_, props, err = outgoing{}.render(env)
	require.NoError(t, err)
	assert.Equal(t, "caller-key", props["Idempotency-Key"])

	out, err = compileOutgoing(nil, `{"wrapped":{{ .bodyText }}}`)
	require.NoError(t, err)
	body, _, err = out.render(envelope.New("in", "http", []byte(`1`)))
	require.NoError(t, err)
	assert.JSONEq(t, `{"wrapped":1}`, string(body))

	out, err = compileOutgoing(map[string]string{"A": `{{ index .json "missing" "deeper" }}`}, "")
	require.NoError(t, err)
	_, _, err = out.render(envelope.New("in", "http", []byte(`{}`)))
	var pe *plugin.Error
	require.True(t, errors.As(err, &pe))
	assert.Equal(t, plugin.Permanent, pe.Class)
	assert.Equal(t, []string{"template"}, pe.Tags)

	out, err = compileOutgoing(nil, `{{ index .json "missing" "deeper" }}`)
	require.NoError(t, err)
	_, _, err = out.render(envelope.New("in", "http", []byte(`{}`)))
	assert.Error(t, err)

	key, err := renderKey(transform.MustTemplate("key", "{{ .json.k }}"), envelope.New("in", "http", []byte(`{"k":"v"}`)))
	require.NoError(t, err)
	assert.Equal(t, "v", key)
	key, err = renderKey(nil, envelope.New("in", "http", nil))
	require.NoError(t, err)
	assert.Empty(t, key)
	_, err = renderKey(transform.MustTemplate("key", `{{ index .json "a" "b" }}`), envelope.New("in", "http", []byte(`{}`)))
	assert.Error(t, err)
}

func TestRFC3339(t *testing.T) {
	_, ok := rfc3339(nil)
	assert.False(t, ok)
	_, ok = rfc3339(&time.Time{})
	assert.False(t, ok)
	v, ok := rfc3339(ptr(time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)))
	assert.True(t, ok)
	assert.Equal(t, "2026-01-02T03:04:05.000000006Z", v)
}
