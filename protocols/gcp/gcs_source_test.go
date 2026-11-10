package gcp

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

func newTestGCSSource(t *testing.T, srv *fakestorage.Server, cfg config.Source, env plugin.Env) *GCSSource {
	t.Helper()
	cfg.Name, cfg.Type = "in", kindGCS
	if cfg.Bucket == "" {
		cfg.Bucket = testBucket
	}
	cfg.GCP = gcsGCP(srv)
	if cfg.Interval == 0 {
		cfg.Interval = config.Duration(20 * time.Millisecond)
	}
	s, err := newGCSSource(context.Background(), plugin.Spec{Name: "in", Kind: kindGCS, Config: &cfg, Env: env})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func generation(t *testing.T, srv *fakestorage.Server, name string) int64 {
	t.Helper()
	o, err := srv.GetObject(testBucket, name)
	require.NoError(t, err)
	return o.Generation
}

func exists(srv *fakestorage.Server, name string) bool {
	_, err := srv.GetObject(testBucket, name)
	return err == nil
}

func TestGCSSourceDeletesAnObjectOnceItsMessageIsFinal(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "in/a.json", "application/json", `{"a":1}`, map[string]string{"origin": "test"})
	putObject(srv, "other/b.json", "application/json", `{}`, nil)
	gen := generation(t, srv, "in/a.json")
	s := newTestGCSSource(t, srv, config.Source{Prefix: "in/"}, plugin.Env{})
	out, _ := run(t, s)

	env := recv(t, out)
	assert.Equal(t, fmt.Sprintf("%d-0", gen), env.ID)
	assert.Equal(t, kindGCS, env.Protocol)
	assert.Equal(t, "in/a.json", env.Subject)
	assert.Equal(t, "application/json", env.ContentType)
	assert.JSONEq(t, `{"a":1}`, string(env.Body))
	assert.Equal(t, []string{"test"}, env.Headers["origin"], "metadata becomes headers")
	assert.Equal(t, testBucket, env.Attributes["gcs.bucket"])
	assert.Equal(t, "in/a.json", env.Attributes["gcs.name"])
	assert.Equal(t, gen, env.Attributes["gcs.generation"])
	assert.Equal(t, int64(7), env.Attributes["gcs.size"])
	assert.Equal(t, "in/a.json", env.Attributes["ordering_key"])
	assert.NotContains(t, env.Attributes, "gcs.line")

	quiet(t, out, 150*time.Millisecond) // polls go on, but the object is in flight
	assert.True(t, exists(srv, "in/a.json"), "not deleted before the message is final")
	settle(env, envelope.Delivered)
	require.Eventually(t, func() bool { return !exists(srv, "in/a.json") }, wait, tick)
	assert.True(t, exists(srv, "other/b.json"), "outside the prefix")
	quiet(t, out, 100*time.Millisecond)
}

func TestGCSSourceLeavesAFailedObjectForTheNextPoll(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "a.txt", "", "hello", nil)
	s := newTestGCSSource(t, srv, config.Source{}, plugin.Env{})
	out, _ := run(t, s)

	first := recv(t, out)
	assert.Equal(t, contentText, first.ContentType, "no content type: detected")
	settle(first, envelope.Failed)
	again := recv(t, out)
	assert.Equal(t, first.ID, again.ID, "the same object, read again")
	assert.True(t, exists(srv, "a.txt"))
	settle(again, envelope.Filtered)
	require.Eventually(t, func() bool { return !exists(srv, "a.txt") }, wait, tick)
}

func TestGCSSourceMovesObjects(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "a.json", "application/json", `{"a":1}`, map[string]string{"k": "v"})
	s := newTestGCSSource(t, srv, config.Source{After: afterMove, MoveTo: "done/"}, plugin.Env{})
	out, _ := run(t, s)

	settle(recv(t, out), envelope.Delivered)
	require.Eventually(t, func() bool { return !exists(srv, "a.json") && exists(srv, "done/a.json") }, wait, tick)
	moved, err := srv.GetObject(testBucket, "done/a.json")
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, string(moved.Content))
	assert.Equal(t, "v", moved.Metadata["k"])
	quiet(t, out, 150*time.Millisecond) // what is under move_to is not read again
}

func TestGCSSourceKeepsAndRemembersObjects(t *testing.T) {
	srv := newGCSServer(t)
	st := store.NewMemory("state")
	putObject(srv, "a.json", "application/json", `{"v":1}`, nil)
	gen := generation(t, srv, "a.json")
	s := newTestGCSSource(t, srv, config.Source{After: afterKeep}, testEnv(st))
	out, stop := run(t, s)

	settle(recv(t, out), envelope.Delivered)
	key := "gcs:" + testBucket + ":a.json:" + strconv.FormatInt(gen, 10)
	require.Eventually(t, func() bool { _, ok, _ := st.Get(context.Background(), key); return ok }, wait, tick)
	assert.True(t, exists(srv, "a.json"), "kept")
	quiet(t, out, 150*time.Millisecond)
	stop()

	// A restart reads the store: the object is not read again, but a new
	// version of it is.
	again := newTestGCSSource(t, srv, config.Source{After: afterKeep}, testEnv(st))
	out2, _ := run(t, again)
	quiet(t, out2, 150*time.Millisecond)
	putObject(srv, "a.json", "application/json", `{"v":2}`, nil)
	env := recv(t, out2)
	assert.JSONEq(t, `{"v":2}`, string(env.Body))
	assert.NotEqual(t, fmt.Sprintf("%d-0", gen), env.ID)
	settle(env, envelope.Delivered)
}

func TestGCSSourceSplitsLines(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "in.ndjson", "application/x-ndjson", "{\"n\":1}\n\n  \nplain\r\n{\"n\":3}", nil)
	gen := generation(t, srv, "in.ndjson")
	s := newTestGCSSource(t, srv, config.Source{Split: splitLines}, plugin.Env{})
	out, _ := run(t, s)

	want := []struct {
		line int
		body string
		ct   string
	}{{1, `{"n":1}`, contentJSON}, {4, "plain", contentText}, {5, `{"n":3}`, contentJSON}}
	var envs []*envelope.Envelope
	for _, w := range want {
		env := recv(t, out)
		assert.Equal(t, fmt.Sprintf("%d-%d", gen, w.line), env.ID)
		assert.Equal(t, w.line, env.Attributes["gcs.line"])
		assert.Equal(t, w.body, string(env.Body))
		assert.Equal(t, w.ct, env.ContentType)
		envs = append(envs, env)
	}
	settle(envs[0], envelope.Delivered)
	settle(envs[1], envelope.Delivered)
	quiet(t, out, 100*time.Millisecond)
	assert.True(t, exists(srv, "in.ndjson"), "deleted only once every line is final")
	settle(envs[2], envelope.Delivered)
	require.Eventually(t, func() bool { return !exists(srv, "in.ndjson") }, wait, tick)
}

func TestGCSSourceSplitsWithJQ(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "a.json", "application/json", `{"items":[{"id":1},{"id":2}]}`, nil)
	s := newTestGCSSource(t, srv, config.Source{Split: ".items[]"}, plugin.Env{})
	out, _ := run(t, s)

	for i, body := range []string{`{"id":1}`, `{"id":2}`} {
		env := recv(t, out)
		assert.JSONEq(t, body, string(env.Body))
		assert.Equal(t, i+1, env.Attributes["gcs.line"])
		assert.Equal(t, contentJSON, env.ContentType)
		settle(env, envelope.Delivered)
	}
	require.Eventually(t, func() bool { return !exists(srv, "a.json") }, wait, tick)

	putObject(srv, "bad.json", "text/plain", "not json", nil)
	env := recv(t, out)
	assert.Equal(t, "not json", string(env.Body), "an object the split cannot take is sent whole")
	assert.Contains(t, env.Attributes["gcs.split_error"], "JSON")
	settle(env, envelope.DeadLettered)
	require.Eventually(t, func() bool { return !exists(srv, "bad.json") }, wait, tick)
}

func TestGCSSourceDropsOversizeMessages(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "a-big.txt", "", "far too large", nil)
	putObject(srv, "b-ok.txt", "", "ok", nil)
	s := newTestGCSSource(t, srv, config.Source{MaxBodyBytes: 4}, plugin.Env{})
	out, _ := run(t, s)

	env := recv(t, out)
	assert.Equal(t, "ok", string(env.Body))
	settle(env, envelope.Delivered)
	require.Eventually(t, func() bool { return !exists(srv, "a-big.txt") && !exists(srv, "b-ok.txt") }, wait, tick,
		"an oversize object counts as handled: it would never fit")

	lines := newTestGCSSource(t, srv, config.Source{MaxBodyBytes: 4, Split: splitLines}, plugin.Env{})
	putObject(srv, "c.txt", "", "ok\nway too long\nfin\n", nil)
	out2, _ := run(t, lines)
	for _, want := range []string{"ok", "fin"} {
		env := recv(t, out2)
		assert.Equal(t, want, string(env.Body))
		settle(env, envelope.Delivered)
	}
	require.Eventually(t, func() bool { return !exists(srv, "c.txt") }, wait, tick)
}

func TestGCSSourceSkipsFolderPlaceholders(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "dir/", "", "", nil)
	s := newTestGCSSource(t, srv, config.Source{}, plugin.Env{})
	out, _ := run(t, s)
	quiet(t, out, 100*time.Millisecond)
	assert.True(t, exists(srv, "dir/"))
}

func TestGCSSourceSettlesAfterShutdown(t *testing.T) {
	srv := newGCSServer(t)
	putObject(srv, "a.txt", "", "x", nil)
	s := newTestGCSSource(t, srv, config.Source{}, plugin.Env{})
	out, stop := run(t, s)
	env := recv(t, out)
	stop()
	settle(env, envelope.Delivered) // the engine finishes in-flight messages after Start returned
	require.Eventually(t, func() bool { return !exists(srv, "a.txt") }, wait, tick)
	require.NoError(t, s.Close(context.Background()))
}

func TestGCSSourceHealthReportsPollErrors(t *testing.T) {
	srv := newGCSServer(t)
	s := newTestGCSSource(t, srv, config.Source{Bucket: "missing"}, plugin.Env{})
	assert.NoError(t, s.Health(context.Background()), "healthy until a poll fails")
	_, _ = run(t, s)
	require.Eventually(t, func() bool { return s.Health(context.Background()) != nil }, wait, tick)
	assert.Contains(t, plugin.Tags(s.Health(context.Background())), "gcs:404")
}

func TestGCSSourceRegistered(t *testing.T) {
	Register()
	srv := newGCSServer(t)
	cfg := config.Source{Name: "in", Type: kindGCS, Bucket: testBucket, GCP: gcsGCP(srv)}
	src, err := plugin.NewSource(context.Background(), plugin.Spec{Name: "in", Kind: kindGCS, Config: &cfg})
	require.NoError(t, err)
	require.Implements(t, (*plugin.Closer)(nil), src)
	assert.NoError(t, src.(plugin.Closer).Close(context.Background()))
}
