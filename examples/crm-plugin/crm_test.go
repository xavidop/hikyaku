package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// open builds a crm destination with options.
func open(t *testing.T, token string, options map[string]any) (plugin.Sink, error) {
	t.Helper()
	return newCRM("eu", token)(context.Background(), plugin.Spec{Name: "leads", Kind: "crm", Options: options})
}

func lead(body string) *envelope.Envelope {
	return envelope.New("forms", "http", []byte(body))
}

func TestDeliverPostsTheLead(t *testing.T) {
	var got *http.Request
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, body = r, must(io.ReadAll(r.Body))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"lead":"ld_1"}`)
	}))
	defer srv.Close()

	s, err := open(t, "tok", map[string]any{"pipeline": "inbound", "base_url": srv.URL + "/v1/"})
	require.NoError(t, err)
	env := lead(`{"email":"ada@example.com","name":"Ada"}`)
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, got.Method)
	assert.Equal(t, "/v1/pipelines/inbound/leads", got.URL.Path)
	assert.Equal(t, "Bearer tok", got.Header.Get("Authorization"))
	assert.Equal(t, env.ID, got.Header.Get("Idempotency-Key"), "the message ID, the same on every attempt")
	assert.Equal(t, "eu", got.Header.Get("X-CRM-Region"))
	assert.JSONEq(t, `{"email":"ada@example.com","name":"Ada"}`, string(body))
	assert.Equal(t, http.StatusCreated, res.Status)
	assert.Equal(t, `{"lead":"ld_1"}`, string(res.Body))
	assert.Equal(t, "application/json", res.ContentType)
	assert.Equal(t, srv.URL+"/v1/pipelines/inbound/leads", res.Endpoint)

	env = lead(`{"email":"bob@example.com"}`)
	env.Headers["Idempotency-Key"] = []string{"form-42"}
	_, err = s.Deliver(context.Background(), env)
	require.NoError(t, err)
	assert.Equal(t, "form-42", got.Header.Get("Idempotency-Key"), "the caller's key wins")
	require.NoError(t, s.Health(context.Background()))
	require.NoError(t, s.Close(context.Background()))
}

func TestNotALeadIsPermanent(t *testing.T) {
	s, err := open(t, "tok", map[string]any{"pipeline": "inbound", "base_url": "http://127.0.0.1:1"})
	require.NoError(t, err)
	for body, want := range map[string]string{
		`{"name":"Bob"}`:      "it has no email",
		`{"email":"nobody"}`:  "it has no email",
		`["ada@example.com"]`: "not a JSON object",
		`not json`:            "not a JSON object",
	} {
		_, err := s.Deliver(context.Background(), lead(body))
		require.Error(t, err, body)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, body)
		assert.Contains(t, plugin.Tags(err), "crm:invalid", body)
		assert.ErrorContains(t, err, want, body)
	}
}

func TestStatusClasses(t *testing.T) {
	code, retryAfter := 0, ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(code)
		_, _ = io.WriteString(w, "  slow down  ")
	}))
	defer srv.Close()
	s, err := open(t, "tok", map[string]any{"pipeline": "inbound", "base_url": srv.URL})
	require.NoError(t, err)

	for _, c := range []struct {
		code       int
		retryAfter string
		class      plugin.Class
		wait       time.Duration
		tag        string
	}{
		{http.StatusTooManyRequests, "2", plugin.RateLimited, 2 * time.Second, "429"},
		{http.StatusServiceUnavailable, "", plugin.Transient, 0, "5xx"},
		{http.StatusBadRequest, "", plugin.Permanent, 0, "4xx"},
		{http.StatusUnauthorized, "", plugin.Permanent, 0, "401"},
	} {
		code, retryAfter = c.code, c.retryAfter
		_, err := s.Deliver(context.Background(), lead(`{"email":"ada@example.com"}`))
		require.Error(t, err, c.code)
		class, wait := plugin.Classify(err)
		assert.Equal(t, c.class, class, c.code)
		assert.Equal(t, c.wait, wait, c.code)
		assert.Contains(t, plugin.Tags(err), c.tag, c.code)
		assert.ErrorContains(t, err, "slow down", "the CRM's answer is in the error")
	}
}

func TestUnreachableCRMIsTransient(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	s, err := open(t, "tok", map[string]any{"pipeline": "inbound", "base_url": url})
	require.NoError(t, err)
	_, err = s.Deliver(context.Background(), lead(`{"email":"ada@example.com"}`))
	require.Error(t, err)
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
}

func TestOptions(t *testing.T) {
	_, err := open(t, "", map[string]any{"pipeline": "inbound"})
	assert.ErrorContains(t, err, "CRM_TOKEN is not set")
	_, err = open(t, "tok", map[string]any{"pipeline": "Inbound!"})
	assert.ErrorContains(t, err, "options.pipeline")
	_, err = open(t, "tok", map[string]any{"pipeline": "inbound", "base_url": "ftp://crm"})
	assert.ErrorContains(t, err, "must start with http:// or https://")
	_, err = open(t, "tok", map[string]any{"pipline": "inbound"})
	assert.ErrorContains(t, err, "pipline", "options are decoded strictly")

	s, err := open(t, "tok", map[string]any{"pipeline": "inbound"})
	require.NoError(t, err)
	assert.Equal(t, "https://api.eu.crm.example/v1/pipelines/inbound/leads", s.(*CRM).url, "the region picks the default API host")
	assert.Equal(t, "leads", s.Name())
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
