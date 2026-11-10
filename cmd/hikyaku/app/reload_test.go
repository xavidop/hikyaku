package app

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// A reload of a relay with a disk buffer while a delivery is in flight for
// longer than the buffer's lock timeout: the old engine finishes it and
// hands the buffer over, the relay keeps running, and nothing is lost.
func TestReloadHandsTheDiskBufferOver(t *testing.T) {
	var delivered atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if strings.Contains(string(b), "slow") {
			time.Sleep(2500 * time.Millisecond)
		}
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer dest.Close()

	dir := t.TempDir()
	src, adm := freeAddr(t), freeAddr(t)
	spec := filepath.Join(dir, "hikyaku.yaml")
	require.NoError(t, os.WriteFile(spec, []byte(fmt.Sprintf(`
version: 1
name: reload
buffer: { type: disk, path: %q }
sources: [{ name: in, type: http, listen: %q, ack: accepted }]
destinations: [{ name: out, type: http, url: %q }]
routes: [{ name: r, from: [in], to: [out] }]
server: { admin: { listen: %q } }
`, filepath.Join(dir, "buffer.db"), src, dest.URL, adm)), 0o600))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out syncBuffer // read while the relay writes it
	code := make(chan int, 1)
	go func() {
		code <- Main([]string{"run", "-f", spec, "--color", "never"}, WithContext(ctx), WithStreams(strings.NewReader(""), &out, &out))
	}()
	post := func(url, body string) int {
		t.Helper()
		status := 0
		require.Eventually(t, func() bool {
			resp, err := http.Post(url, "application/json", strings.NewReader(body))
			if err != nil {
				return false
			}
			_ = resp.Body.Close()
			status = resp.StatusCode
			return true
		}, 15*time.Second, 50*time.Millisecond, "POST %s", url)
		return status
	}
	require.Equal(t, http.StatusAccepted, post("http://"+src+"/", `{"m":"slow"}`))
	time.Sleep(300 * time.Millisecond) // the slow delivery is in flight

	require.Equal(t, http.StatusOK, post("http://"+adm+"/reload", ``))
	require.Equal(t, http.StatusAccepted, post("http://"+src+"/", `{"m":"after"}`))
	require.Eventually(t, func() bool { return delivered.Load() == 2 }, 20*time.Second, 50*time.Millisecond, out.String())
	select {
	case c := <-code:
		t.Fatalf("the relay stopped after the reload (exit %d): %s", c, out.String())
	default:
	}
	cancel()
	select {
	case c := <-code:
		assert.Equal(t, 0, c, out.String())
	case <-time.After(30 * time.Second):
		t.Fatal("the relay did not stop")
	}
	assert.Contains(t, out.String(), "disk buffer handed over")
}

// A reload that moves server.admin: the POST /reload that asked for it
// gets its 200 from the old address, which then refuses; the new address
// answers, and a later reload there applies a new token in place. The
// relay delivers throughout.
func TestReloadMovesTheAdminAPI(t *testing.T) {
	var delivered atomic.Int32
	dest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer dest.Close()

	dir := t.TempDir()
	src, a, b := freeAddr(t), freeAddr(t), freeAddr(t)
	spec := filepath.Join(dir, "hikyaku.yaml")
	write := func(admin, token string) {
		require.NoError(t, os.WriteFile(spec, []byte(fmt.Sprintf(`
version: 1
name: admin-move
sources: [{ name: in, type: http, listen: %q, ack: accepted }]
destinations: [{ name: out, type: http, url: %q }]
routes: [{ name: r, from: [in], to: [out] }]
server: { admin: { listen: %q, token: %q } }
`, src, dest.URL, admin, token)), 0o600))
	}
	write(a, "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out syncBuffer
	code := make(chan int, 1)
	go func() {
		code <- Main([]string{"run", "-f", spec, "--color", "never"}, WithContext(ctx), WithStreams(strings.NewReader(""), &out, &out))
	}()
	call := func(method, url, token, body string) (int, string, error) {
		req, err := http.NewRequest(method, url, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, "", err
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), nil
	}
	answers := func(method, url, token, body string) (int, string) {
		t.Helper()
		var status int
		var got string
		require.Eventually(t, func() bool {
			s, b, err := call(method, url, token, body)
			status, got = s, b
			return err == nil
		}, 15*time.Second, 50*time.Millisecond, "%s %s: %s", method, url, out.String())
		return status, got
	}
	status, _ := answers("GET", "http://"+a+"/healthz", "", "")
	require.Equal(t, http.StatusOK, status)

	write(b, "")
	status, body := answers("POST", "http://"+a+"/reload", "", "")
	require.Equal(t, http.StatusOK, status, "the old address answers the reload that moves it: %s", body)
	assert.Contains(t, body, "reloaded")
	require.Eventually(t, func() bool {
		_, _, err := call("GET", "http://"+a+"/healthz", "", "")
		return err != nil
	}, 10*time.Second, 50*time.Millisecond, "the old address refuses")
	status, _ = answers("GET", "http://"+b+"/healthz", "", "")
	assert.Equal(t, http.StatusOK, status, "the new address answers")
	status, _ = answers("POST", "http://"+src+"/", "", `{"m":1}`)
	assert.Equal(t, http.StatusAccepted, status)
	require.Eventually(t, func() bool { return delivered.Load() == 1 }, 10*time.Second, 50*time.Millisecond, out.String())

	write(b, "t0ken")
	status, body = answers("POST", "http://"+b+"/reload", "", "")
	require.Equal(t, http.StatusOK, status, body)
	status, _ = answers("GET", "http://"+b+"/debug/config", "", "")
	assert.Equal(t, http.StatusUnauthorized, status, "the new token applies")
	status, _ = answers("GET", "http://"+b+"/debug/config", "t0ken", "")
	assert.Equal(t, http.StatusOK, status)

	cancel()
	select {
	case c := <-code:
		assert.Equal(t, 0, c, out.String())
	case <-time.After(30 * time.Second):
		t.Fatal("the relay did not stop")
	}
	assert.Contains(t, out.String(), "admin API moved")
}

// syncBuffer is a relay's output, read while it runs.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
