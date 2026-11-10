package redact

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedact(t *testing.T) {
	r := New()
	require.True(t, r.Add("API_TOKEN", "s3cret-value"))
	require.True(t, r.Add("PASS", `p"w:d`))
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain", "Authorization: Bearer s3cret-value", "Authorization: Bearer ‹secret:API_TOKEN›"},
		{"twice", "s3cret-value s3cret-value", "‹secret:API_TOKEN› ‹secret:API_TOKEN›"},
		{"base64", "token=czNjcmV0LXZhbHVl", "token=‹secret:API_TOKEN›"},
		{"url encoded", `pw=p%22w%3Ad`, "pw=‹secret:PASS›"},
		{"json escaped", `{"pw":"p\"w:d"}`, `{"pw":"‹secret:PASS›"}`},
		{"nothing", "hello world", "hello world"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, r.Redact(tt.in))
			assert.Equal(t, tt.want, string(r.RedactBytes([]byte(tt.in))))
		})
	}
	assert.Equal(t, []string{"API_TOKEN", "PASS"}, r.Names())
}

func TestRedactLongestFirst(t *testing.T) {
	r := New()
	r.Add("SHORT", "abc123")
	r.Add("LONG", "abc123def")
	assert.Equal(t, "‹secret:LONG› ‹secret:SHORT›", r.Redact("abc123def abc123"))
}

func TestRedactShortValuesAreIgnored(t *testing.T) {
	r := New()
	assert.False(t, r.Add("PIN", "1"))
	assert.False(t, r.Add("PIN", "12"))
	assert.Equal(t, "call 1 or 12", r.Redact("call 1 or 12"))
	assert.Empty(t, r.Names())
}

func TestAddEnv(t *testing.T) {
	r := New()
	tests := []struct {
		name string
		want bool
	}{
		{"LIVEKIT_API_KEY", true},
		{"LIVEKIT_API_SECRET", true},
		{"API_TOKEN", true},
		{"DB_PASSWORD_PROD", true},
		{"db_password", true},
		{"HOME", false},
		{"KEYBOARD", false},
		{"TOKENIZER", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, r.AddEnv(tt.name, "value-"+tt.name))
		})
	}
	assert.Equal(t, "value-HOME", r.Redact("value-HOME")) // HOME was never added
	assert.Equal(t, "‹secret:API_TOKEN›", r.Redact("value-API_TOKEN"))
}

func TestWriter(t *testing.T) {
	r := New()
	r.Add("T", "topsecret")
	var buf bytes.Buffer
	w := r.Writer(&buf)
	_, err := w.Write([]byte("a tops"))
	require.NoError(t, err)
	_, err = w.Write([]byte("ecret b\nsecond topsecret\ntail topsecret"))
	require.NoError(t, err)
	assert.Equal(t, "a ‹secret:T› b\nsecond ‹secret:T›\n", buf.String())
	require.NoError(t, w.Close())
	assert.Equal(t, "a ‹secret:T› b\nsecond ‹secret:T›\ntail ‹secret:T›", buf.String())
}

func TestHandler(t *testing.T) {
	r := New()
	r.Add("T", "topsecret")
	var buf bytes.Buffer
	logger := slog.New(r.Handler(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			return slog.Attr{}
		}
		return a
	}})))
	logger = logger.With("auth", "Bearer topsecret")
	logger.Info("sent topsecret", "err", errors.New("boom topsecret"), slog.Group("req", slog.String("h", "topsecret")), "n", 3)
	out := buf.String()
	assert.NotContains(t, out, "topsecret")
	assert.Contains(t, out, `msg="sent ‹secret:T›"`)
	assert.Contains(t, out, `auth="Bearer ‹secret:T›"`)
	assert.Contains(t, out, `err="boom ‹secret:T›"`)
	assert.Contains(t, out, `req.h=‹secret:T›`)
	assert.Contains(t, out, `n=3`)
}
