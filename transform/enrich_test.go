package transform

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// private lets a templated URL reach the test server on loopback.
var private = Options{Egress: plugin.Egress{AllowPrivate: true}}

func enrich(t *testing.T, e config.Enrich, o Options) *EnrichStep {
	t.Helper()
	chain, err := CompileWith("t", []config.Transform{{Enrich: &e}}, o)
	require.NoError(t, err)
	return chain[0].(*EnrichStep)
}

// crm answers /customers/<id> with that customer, and counts the calls.
func crm(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/customers/42":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"tier":"gold","credit":12345678901234567890,"auth":"`+r.Header.Get("Authorization")+`"}`)
		case "/echo":
			b, _ := io.ReadAll(r.Body)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"method":"`+r.Method+`","got":`+string(b)+`}`)
		case "/text":
			_, _ = io.WriteString(w, "hello")
		case "/slow":
			time.Sleep(300 * time.Millisecond)
			_, _ = io.WriteString(w, `{}`)
		default:
			http.Error(w, "no such customer", http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestEnrichStep(t *testing.T) {
	srv, _ := crm(t)
	s := enrich(t, config.Enrich{
		URL:     srv.URL + "/customers/{{ .json.customer_id }}",
		Headers: map[string]string{"Authorization": "Bearer {{ .json.region }}-key"},
		Into:    "meta.customer",
	}, private)

	env := body(`{"customer_id":42,"region":"eu","total":19.90,"meta":{"seen":true}}`)
	keep, err := s.Apply(env)
	require.NoError(t, err)
	assert.True(t, keep)
	assert.JSONEq(t, `{"customer_id":42,"region":"eu","total":19.90,
		"meta":{"seen":true,"customer":{"tier":"gold","credit":12345678901234567890,"auth":"Bearer eu-key"}}}`, env.BodyText())
	assert.Contains(t, env.BodyText(), "12345678901234567890", "a big number is not rounded")
	assert.Contains(t, env.BodyText(), "19.90", "the message's own numbers keep their text")

	// into makes the objects on the way, and replaces an old answer.
	top := enrich(t, config.Enrich{URL: srv.URL + "/customers/42", Into: "a.b.c"}, Options{})
	env = body(`{"a":{"b":null}}`)
	_, err = top.Apply(env)
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":{"b":{"c":{"tier":"gold","credit":12345678901234567890,"auth":""}}}}`, env.BodyText())
}

func TestEnrichStepFailures(t *testing.T) {
	srv, _ := crm(t)
	for name, tc := range map[string]struct {
		spec config.Enrich
		body string
		want string
	}{
		"status":         {config.Enrich{URL: srv.URL + "/customers/7", Into: "c"}, `{}`, "status 404"},
		"not json":       {config.Enrich{URL: srv.URL + "/text", Into: "c"}, `{}`, "the answer is not JSON"},
		"timeout":        {config.Enrich{URL: srv.URL + "/slow", Into: "c", Timeout: config.Duration(20 * time.Millisecond)}, `{}`, "no answer within 20ms"},
		"connect":        {config.Enrich{URL: "http://127.0.0.1:1/x", Into: "c"}, `{}`, "connect"},
		"scheme":         {config.Enrich{URL: "file:///etc/passwd", Into: "c"}, `{}`, "use http or https"},
		"body not json":  {config.Enrich{URL: srv.URL + "/customers/42", Into: "c"}, `nope`, "the body is not JSON"},
		"body an array":  {config.Enrich{URL: srv.URL + "/customers/42", Into: "c"}, `[1]`, "the body is not a JSON object"},
		"into a string":  {config.Enrich{URL: srv.URL + "/customers/42", Into: "name.c"}, `{"name":"x"}`, "into name.c: name is not an object"},
		"url has no key": {config.Enrich{URL: srv.URL + "/customers/{{ .json.a.b.c }}", Into: "c"}, `{"a":1}`, "url:"},
	} {
		t.Run(name, func(t *testing.T) {
			env := body(tc.body)
			_, err := enrich(t, tc.spec, private).Apply(env)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), "enrich: ")
			assert.Equal(t, tc.body, env.BodyText(), "a failed step leaves the body alone")
		})
	}
}

func TestEnrichStepOnErrorSkip(t *testing.T) {
	srv, _ := crm(t)
	s := enrich(t, config.Enrich{URL: srv.URL + "/customers/{{ .json.id }}", Into: "customer", OnError: "skip"}, private)
	env := body(`{"id":7}`)
	keep, err := s.Apply(env)
	require.NoError(t, err)
	assert.True(t, keep, "the message goes on")
	assert.Equal(t, `{"id":7}`, env.BodyText())

	// skip is for the call. A body enrich cannot write into still fails.
	_, err = s.Apply(body(`[]`))
	assert.ErrorContains(t, err, "not a JSON object")
}

func TestEnrichStepPost(t *testing.T) {
	srv, _ := crm(t)
	s := enrich(t, config.Enrich{URL: srv.URL + "/echo", Method: "POST", Into: "score"}, Options{})
	env := body(`{"text":"hi"}`)
	_, err := s.Apply(env)
	require.NoError(t, err)
	assert.JSONEq(t, `{"text":"hi","score":{"method":"POST","got":{"text":"hi"}}}`, env.BodyText())
}

func TestEnrichStepCache(t *testing.T) {
	srv, calls := crm(t)
	s := enrich(t, config.Enrich{URL: srv.URL + "/customers/{{ .json.id }}", Into: "customer", Cache: config.Duration(time.Minute), OnError: "skip"}, private)
	now := time.Now()
	s.cache.now = func() time.Time { return now }

	for i := 0; i < 3; i++ {
		env := body(`{"id":42}`)
		_, err := s.Apply(env)
		require.NoError(t, err)
		assert.Contains(t, env.BodyText(), `"tier":"gold"`)
	}
	assert.EqualValues(t, 1, calls.Load(), "one call, then the cache answers")

	// A failed call is not kept: the next message asks again.
	_, _ = s.Apply(body(`{"id":7}`))
	_, _ = s.Apply(body(`{"id":7}`))
	assert.EqualValues(t, 3, calls.Load())

	now = now.Add(time.Minute)
	_, err := s.Apply(body(`{"id":42}`))
	require.NoError(t, err)
	assert.EqualValues(t, 4, calls.Load(), "the answer expired")

	// Without cache, every message calls.
	plain := enrich(t, config.Enrich{URL: srv.URL + "/customers/42", Into: "customer"}, Options{})
	assert.Nil(t, plain.cache)
}

func TestEnrichStepCacheIsBounded(t *testing.T) {
	now := time.Now()
	c := &enrichCache{ttl: time.Minute, now: func() time.Time { return now }, items: map[string]enrichEntry{}}
	for i := 0; i < enrichCacheMax; i++ {
		c.put(string(rune(i)), i)
	}
	now = now.Add(2 * time.Minute)
	c.put("fresh", 1)
	assert.Len(t, c.items, 1, "the expired answers went")
	_, ok := c.get("fresh")
	assert.True(t, ok)
}

// A URL built from the message cannot reach a private address unless the
// spec's egress allows it. A fixed URL is the operator's own choice.
func TestEnrichStepEgressGuard(t *testing.T) {
	srv, calls := crm(t)
	guarded := enrich(t, config.Enrich{URL: "{{ .json.lookup }}/customers/42", Into: "c"}, Options{})
	_, err := guarded.Apply(body(`{"lookup":"` + srv.URL + `"}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "private address")
	assert.EqualValues(t, 0, calls.Load())

	listed := enrich(t, config.Enrich{URL: "{{ .json.lookup }}/customers/42", Into: "c"}, Options{Egress: plugin.Egress{Allow: []string{"crm.example.com"}}})
	_, err = listed.Apply(body(`{"lookup":"` + srv.URL + `"}`))
	assert.ErrorContains(t, err, "not in egress.allow")

	fixed := enrich(t, config.Enrich{URL: srv.URL + "/customers/42", Into: "c"}, Options{})
	_, err = fixed.Apply(body(`{}`))
	require.NoError(t, err)
	assert.EqualValues(t, 1, calls.Load())
}

func TestCompileEnrichErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		spec config.Enrich
		want string
	}{
		"no url":     {config.Enrich{Into: "c"}, "t[0].enrich: url is required"},
		"no into":    {config.Enrich{URL: "http://x.test"}, "t[0].enrich: into is required"},
		"bad url":    {config.Enrich{URL: "http://x.test/{{ .json.", Into: "c"}, "t[0].enrich: url:"},
		"bad header": {config.Enrich{URL: "http://x.test", Into: "c", Headers: map[string]string{"X": "{{ nope }}"}}, "t[0].enrich: headers.X:"},
		"bad method": {config.Enrich{URL: "http://x.test", Into: "c", Method: "DELETE"}, `method "DELETE": use GET or POST`},
		"bad egress": {config.Enrich{URL: "http://{{ .json.h }}", Into: "c"}, "egress.allow"},
	} {
		t.Run(name, func(t *testing.T) {
			o := Options{}
			if name == "bad egress" {
				o.Egress.Allow = []string{"10.0.0.0/99"}
			}
			_, err := CompileWith("t", []config.Transform{{Enrich: &tc.spec}}, o)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// Options.Enrich answers the call: no request is made.
func TestEnrichStepAnsweredByOptions(t *testing.T) {
	var calls []EnrichCall
	o := Options{Enrich: func(c EnrichCall) (any, error) {
		calls = append(calls, c)
		if strings.HasSuffix(c.URL, "/missing") {
			return nil, errors.New("status 404")
		}
		return map[string]any{"tier": "gold"}, nil
	}}
	s := enrich(t, config.Enrich{
		URL: "https://crm.invalid/customers/{{ .json.id }}", Into: "customer", Headers: map[string]string{"X-Tenant": "acme"},
	}, o)
	env := body(`{"id":42}`)
	_, err := s.Apply(env)
	require.NoError(t, err)
	assert.JSONEq(t, `{"id":42,"customer":{"tier":"gold"}}`, env.BodyText())
	require.Len(t, calls, 1)
	assert.Equal(t, EnrichCall{Method: "GET", URL: "https://crm.invalid/customers/42", Headers: map[string]string{"X-Tenant": "acme"}}, calls[0])

	_, err = s.Apply(body(`{"id":"missing"}`))
	require.ErrorContains(t, err, "status 404")

	post := enrich(t, config.Enrich{URL: "https://score.invalid", Method: "POST", Into: "score"}, o)
	_, err = post.Apply(body(`{"text":"hi"}`))
	require.NoError(t, err)
	assert.JSONEq(t, `{"text":"hi"}`, string(calls[2].Body))
}
