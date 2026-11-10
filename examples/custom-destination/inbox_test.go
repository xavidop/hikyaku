package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/cmd/hikyaku/app"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

func newTestInbox(t *testing.T, options map[string]any) (plugin.Source, string) {
	t.Helper()
	dir := t.TempDir()
	opts := map[string]any{"dir": dir, "interval": "10ms"}
	for k, v := range options {
		opts[k] = v
	}
	s, err := newInbox(context.Background(), plugin.Spec{Name: "drops", Kind: "inbox", Options: opts})
	require.NoError(t, err)
	return s, dir
}

// run starts a source; the returned stop ends it and returns what Start returned.
func run(t *testing.T, s plugin.Source) (<-chan *envelope.Envelope, func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *envelope.Envelope)
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	stopped := false
	var err error
	stop := func() error {
		if !stopped {
			stopped = true
			cancel()
			err = <-done
		}
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return out, stop
}

func next(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
		return nil
	}
}

func drop(t *testing.T, dir, name, body string) {
	t.Helper()
	tmp := filepath.Join(dir, "."+name+".tmp")
	require.NoError(t, os.WriteFile(tmp, []byte(body), 0o600))
	require.NoError(t, os.Rename(tmp, filepath.Join(dir, name)), "renamed in, so the source never reads half a file")
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestInboxOptions(t *testing.T) {
	_, err := newInbox(context.Background(), plugin.Spec{Name: "drops", Kind: "inbox"})
	assert.EqualError(t, err, "inbox drops: options.dir is required")
	_, err = newInbox(context.Background(), plugin.Spec{Name: "drops", Kind: "inbox", Options: map[string]any{"dir": "x", "intervl": "1s"}})
	assert.ErrorContains(t, err, "inbox drops: options:")
	_, err = newInbox(context.Background(), plugin.Spec{Name: "drops", Kind: "inbox", Options: map[string]any{"dir": "x", "interval": "soon"}})
	assert.EqualError(t, err, `inbox drops: options.interval "soon" is not a positive duration`)
	_, err = newInbox(context.Background(), plugin.Spec{Name: "drops", Kind: "inbox", Options: map[string]any{"dir": "x", "pattern": "["}})
	assert.ErrorContains(t, err, "inbox drops: options.pattern")
}

func TestInboxSendsEachFileUntilTheRelayIsDoneWithIt(t *testing.T) {
	s, dir := newTestInbox(t, nil)
	assert.Equal(t, "drops", s.Name())
	drop(t, dir, "a.json", `{"severity":"high","summary":"disk full"}`)
	drop(t, dir, "b.json", `{"severity":"low","summary":"cert expires soon"}`)
	drop(t, dir, "notes.txt", "not a match")
	out, stop := run(t, s)

	a := next(t, out)
	assert.Equal(t, "a.json", a.Subject)
	assert.Equal(t, "drops", a.Source)
	assert.Equal(t, "inbox", a.Protocol)
	assert.Equal(t, "application/json", a.ContentType)
	assert.JSONEq(t, `{"severity":"high","summary":"disk full"}`, string(a.Body))
	file, _ := a.Attribute("inbox.file")
	assert.Equal(t, "a.json", file)
	b := next(t, out)
	assert.Equal(t, "b.json", b.Subject)

	// Delivered: the file is removed.
	a.Ack(context.Background(), envelope.Outcome{Status: envelope.Delivered})
	assert.False(t, exists(filepath.Join(dir, "a.json")))

	// Failed: the file stays, and comes again with the same ID.
	b.Ack(context.Background(), envelope.Outcome{Status: envelope.Failed})
	again := next(t, out)
	assert.Equal(t, "b.json", again.Subject)
	assert.Equal(t, b.ID, again.ID, "the idempotency key survives a retry")
	again.Ack(context.Background(), envelope.Outcome{Status: envelope.DeadLettered})
	assert.False(t, exists(filepath.Join(dir, "b.json")), "dead-lettered is done too")

	// A file dropped later is picked up; the one that does not match stays.
	drop(t, dir, "c.json", `{}`)
	c := next(t, out)
	assert.Equal(t, "c.json", c.Subject)
	assert.NotEqual(t, b.ID, c.ID)
	assert.True(t, exists(filepath.Join(dir, "notes.txt")))
	require.NoError(t, stop(), "Start returns nil when ctx ends")
}

func TestInboxHealth(t *testing.T) {
	s, dir := newTestInbox(t, nil)
	require.NoError(t, s.Health(context.Background()))
	require.NoError(t, os.RemoveAll(dir))
	assert.Error(t, s.Health(context.Background()))
	require.NoError(t, os.WriteFile(dir, []byte("x"), 0o600))
	assert.ErrorContains(t, s.Health(context.Background()), "is not a directory")
}

func TestInboxOptionsAreValidated(t *testing.T) {
	code, out, _ := cli(t, nil, "schema")
	require.Equal(t, 0, code)
	assert.Contains(t, out, `"const": "inbox"`)

	bad := strings.Replace(read(t, "hikyaku.yaml"), "interval: 1s", "intervl: 1s", 1)
	code, _, errOut := cli(t, nil, "validate", "-f", writeTemp(t, bad))
	assert.Equal(t, 2, code)
	assert.Contains(t, errOut, "intervl", "a typo in the source's options is caught by validate")
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// A file dropped in the inbox reaches the chat webhook through a running
// relay, and is removed once the chat has it.
func TestInboxToChatThroughTheRelay(t *testing.T) {
	posted := make(chan map[string]string, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var got map[string]string
		_ = json.Unmarshal(b, &got)
		posted <- got
		w.WriteHeader(200)
	}))
	defer srv.Close()
	inbox := filepath.Join(t.TempDir(), "inbox")
	spec := writeTemp(t, fmt.Sprintf(`
version: 1
name: inbox-to-chat
sources:
  - { name: drops, type: inbox, options: { dir: %q, interval: 20ms } }
destinations:
  - { name: chat, type: chat, options: { webhook_url: %q, text: "{{ .json.severity }}: {{ .json.summary }}" } }
routes:
  - { name: notify, from: [drops], to: [chat] }
server:
  admin: { listen: %q }
`, inbox, srv.URL, freeAddr(t)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	code := make(chan int, 1)
	go func() {
		code <- app.Main([]string{"run", "-f", spec, "--color", "never"}, app.WithContext(ctx),
			app.WithStreams(strings.NewReader(""), io.Discard, io.Discard))
	}()
	require.Eventually(t, func() bool { return exists(inbox) }, 10*time.Second, 10*time.Millisecond, "the source creates its directory")
	drop(t, inbox, "alert.json", `{"severity":"high","summary":"disk full on db-1"}`)
	select {
	case got := <-posted:
		assert.Equal(t, "high: disk full on db-1", got["text"])
	case <-time.After(10 * time.Second):
		t.Fatal("the chat got nothing")
	}
	require.Eventually(t, func() bool { return !exists(filepath.Join(inbox, "alert.json")) }, 10*time.Second, 10*time.Millisecond,
		"the file is removed once the chat has it")
	cancel()
	select {
	case c := <-code:
		assert.Equal(t, 0, c)
	case <-time.After(30 * time.Second):
		t.Fatal("the relay did not stop")
	}
}
