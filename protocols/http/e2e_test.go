package http

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/inbound"
)

type recorder struct {
	mu   sync.Mutex
	keys []string
	n    atomic.Int32
	fail int32
}

func (r *recorder) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.ReadAll(req.Body)
		r.mu.Lock()
		r.keys = append(r.keys, req.Header.Get("Idempotency-Key"))
		r.mu.Unlock()
		if r.n.Add(1) <= r.fail {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(202)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// A signed webhook fans out to billing and support; billing is down and its
// fallback takes the message; the caller gets 200 once both have it, and
// every attempt carries the same idempotency key.
func TestEndToEndWebhookFanout(t *testing.T) {
	billing := &recorder{fail: 100}
	dr := &recorder{}
	support := &recorder{fail: 1}
	bSrv, drSrv, sSrv := billing.server(t), dr.server(t), support.server(t)
	addr := freePort(t)
	spec := fmt.Sprintf(`
version: 1
name: e2e
secrets: { HOOK: env:HOOK }
sources:
  - name: webhooks
    type: http
    listen: "%s"
    path: /ingest
    auth: { hmac: { header: X-Signature-256, secret: "{{secrets.HOOK}}" } }
destinations:
  - { name: billing, type: http, url: "%s", retry: { attempts: 2, backoff: 1ms }, fallback: [billing-dr] }
  - { name: billing-dr, type: http, url: "%s" }
  - { name: support, type: http, url: "%s", retry: { attempts: 3, backoff: 1ms } }
routes:
  - { name: fanout, from: [webhooks], to: [billing, support] }
server: { admin: { disabled: true } }
`, addr, bSrv.URL, drSrv.URL, sSrv.URL)
	l, err := config.LoadBytes(context.Background(), "e2e.yaml", []byte(spec), config.LoadOptions{
		ResolveSecrets: func(context.Context, map[string]string) (map[string]string, error) {
			return map[string]string{"HOOK": "s3cret"}, nil
		},
	})
	require.NoError(t, err)
	e, err := engine.New(context.Background(), l.Spec, engine.Options{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- e.Run(ctx) }()

	body := `{"type":"invoice.paid","id":"evt_1"}`
	url := "http://" + addr + "/ingest"
	var resp *http.Response
	require.Eventually(t, func() bool {
		req, _ := http.NewRequest("POST", url, nil)
		req.Body = io.NopCloser(strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Signature-256", "sha256="+inbound.Sign("s3cret", []byte(body)))
		req.Header.Set("Idempotency-Key", "evt_1")
		var err error
		resp, err = http.DefaultClient.Do(req)
		return err == nil
	}, 3*time.Second, 20*time.Millisecond)
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode, "answered once every required destination has it")

	assert.Equal(t, int32(2), billing.n.Load(), "billing retried, then failed over")
	assert.Equal(t, int32(1), dr.n.Load())
	assert.Equal(t, int32(2), support.n.Load(), "support's 503 was retried")
	for _, r := range []*recorder{billing, dr, support} {
		r.mu.Lock()
		for _, k := range r.keys {
			assert.Equal(t, "evt_1", k)
		}
		r.mu.Unlock()
	}

	unsigned, err := http.Post(url, "application/json", strings.NewReader(body))
	require.NoError(t, err)
	_ = unsigned.Body.Close()
	assert.Equal(t, 401, unsigned.StatusCode)

	cancel()
	require.NoError(t, <-done)
}
