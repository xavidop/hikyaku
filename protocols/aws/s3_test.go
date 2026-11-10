package aws

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const bucket = "archive"

// fakeS3 is gofakes3 in process, with a client of its own for setting up
// and checking objects.
type fakeS3 struct {
	url    string
	client *s3.Client
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	hermeticAWS(t)
	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket(bucket))
	srv := httptest.NewServer(gofakes3.New(backend).Server())
	t.Cleanup(srv.Close)
	cfg, err := loadAWS(context.Background(), testAWS(srv.URL), "", 1, "test")
	require.NoError(t, err)
	return &fakeS3{url: srv.URL, client: newS3Client(cfg, true)}
}

func (f *fakeS3) put(t *testing.T, key, body, contentType string, meta map[string]string) string {
	t.Helper()
	in := &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: strings.NewReader(body), Metadata: meta}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	_, err := f.client.PutObject(context.Background(), in)
	require.NoError(t, err)
	sum := md5.Sum([]byte(body))
	return hex.EncodeToString(sum[:])
}

type storedObject struct {
	body        string
	contentType string
	meta        map[string]string
}

func (f *fakeS3) get(t *testing.T, key string) (storedObject, bool) {
	t.Helper()
	out, err := f.client.GetObject(context.Background(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return storedObject{}, false
	}
	defer func() { _ = out.Body.Close() }()
	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	return storedObject{body: string(b), contentType: aws.ToString(out.ContentType), meta: out.Metadata}, true
}

func (f *fakeS3) keys(t *testing.T) []string {
	t.Helper()
	out, err := f.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	sort.Strings(keys)
	return keys
}

func (f *fakeS3) aws() *config.AWS { return testAWS(f.url) }

func TestS3SinkDeliver(t *testing.T) {
	f := newFakeS3(t)
	s, err := newS3Sink(context.Background(), sinkSpec(kindS3, &config.Endpoint{Bucket: bucket, AWS: f.aws(),
		Headers: map[string]string{"tenant": "{{ .json.tenant }}"}}))
	require.NoError(t, err)
	env := newEnv(`{"tenant":"acme"}`)
	env.ReceivedAt = time.Date(2026, 11, 9, 23, 30, 0, 0, time.UTC)
	env.SetHeader("X-Trace", "t-1")
	env.SetHeader("Authorization", "Bearer secret")
	env.SetHeader("X-Accent", "café")
	res, err := s.Deliver(context.Background(), env)
	require.NoError(t, err)
	key := "relay/2026/11/09/" + env.ID + ".json"
	assert.Equal(t, []string{key}, res.Headers["s3-key"])
	assert.Equal(t, "s3://archive/"+key, res.Endpoint)
	assert.NotEmpty(t, res.Headers["s3-etag"])
	obj, ok := f.get(t, key)
	require.True(t, ok)
	assert.Equal(t, `{"tenant":"acme"}`, obj.body)
	assert.Equal(t, contentJSON, obj.contentType, "the message's content type")
	assert.Equal(t, map[string]string{"tenant": "acme", "x-trace": "t-1", "idempotency-key": env.ID}, obj.meta,
		"credentials and values S3 cannot hold stay out")
	require.NoError(t, s.Health(context.Background()))
	require.NoError(t, s.Close(context.Background()))
	assert.Equal(t, "out", s.Name())
}

func TestS3SinkKeyTemplateAndContentType(t *testing.T) {
	f := newFakeS3(t)
	s, err := newS3Sink(context.Background(), sinkSpec(kindS3, &config.Endpoint{Bucket: bucket, AWS: f.aws(),
		Key: "/orders/{{ .json.kind }}/{{ .id }}.json", ContentType: "application/vnd.order+json", Body: `{"wrapped":{{ .bodyText }}}`}))
	require.NoError(t, err)
	env := newEnv(`{"kind":"placed"}`)
	_, err = s.Deliver(context.Background(), env)
	require.NoError(t, err)
	obj, ok := f.get(t, "orders/placed/"+env.ID+".json")
	require.True(t, ok, "a leading slash is dropped")
	assert.Equal(t, "application/vnd.order+json", obj.contentType)
	assert.Equal(t, `{"wrapped":{"kind":"placed"}}`, obj.body)

	s, err = newS3Sink(context.Background(), sinkSpec(kindS3, &config.Endpoint{Bucket: bucket, AWS: f.aws(), Key: "{{ .json.missing }}"}))
	require.NoError(t, err)
	_, err = s.Deliver(context.Background(), newEnv(`{}`))
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.ErrorContains(t, err, "empty key")
}

func TestS3SinkBatchIsOneNDJSONObject(t *testing.T) {
	f := newFakeS3(t)
	s, err := newS3Sink(context.Background(), sinkSpec(kindS3, &config.Endpoint{Bucket: bucket, AWS: f.aws()}))
	require.NoError(t, err)
	envs := []*envelope.Envelope{newEnv("{\n  \"a\": 1\n}"), newEnv("plain text\n"), newEnv(`[1, 2]`)}
	envs[0].ReceivedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	res, err := s.DeliverBatch(context.Background(), envs)
	require.NoError(t, err)
	key := "relay/2026/01/02/" + envs[0].ID + ".ndjson"
	assert.Equal(t, []string{key}, res.Headers["s3-key"], "keyed by the first message")
	obj, ok := f.get(t, key)
	require.True(t, ok)
	assert.Equal(t, "{\"a\":1}\nplain text\n[1,2]\n", obj.body)
	assert.Equal(t, contentNDJSON, obj.contentType)

	res, err = s.DeliverBatch(context.Background(), nil)
	require.NoError(t, err)
	assert.Empty(t, res.Headers)
}

func TestS3SinkMissingBucket(t *testing.T) {
	f := newFakeS3(t)
	s, err := newS3Sink(context.Background(), sinkSpec(kindS3, &config.Endpoint{Bucket: "missing", AWS: f.aws()}))
	require.NoError(t, err)
	_, err = s.Deliver(context.Background(), newEnv("x"))
	class, tags := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
	assert.Contains(t, tags, "s3:NoSuchBucket")
	assert.Contains(t, tags, "404")

	err = s.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
	class, _ = errorTags(err)
	assert.Equal(t, plugin.Permanent, class)
}

// runningS3 is an s3 source polling the fake.
type runningS3 struct {
	src    *s3Source
	out    chan *envelope.Envelope
	cancel context.CancelFunc
	done   chan error
	logs   *logBuffer
}

func runS3(t *testing.T, f *fakeS3, src *config.Source, st store.Store) *runningS3 {
	t.Helper()
	src.Bucket = bucket
	src.AWS = f.aws()
	if src.Interval == 0 {
		src.Interval = config.Duration(50 * time.Millisecond)
	}
	spec := sourceSpec(kindS3, src)
	if st != nil {
		spec = withStore(spec, st)
	}
	logs := &logBuffer{}
	spec.Env.Logger = logs.logger()
	s, err := newS3Source(context.Background(), spec)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	r := &runningS3{src: s, out: make(chan *envelope.Envelope, 16), cancel: cancel, done: make(chan error, 1), logs: logs}
	go func() { r.done <- s.Start(ctx, r.out) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-r.done:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("s3 source did not stop")
		}
	})
	return r
}

func TestS3SourceReadsAndDeletes(t *testing.T) {
	f := newFakeS3(t)
	etagA := f.put(t, "a.json", `{"n":1}`, "application/json", map[string]string{"origin": "billing"})
	f.put(t, "dir/", "", "", nil)
	etagB := f.put(t, "b.txt", "hello", "", nil)
	r := runS3(t, f, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, etagA, env.ID, "the object's ETag")
	assert.Equal(t, "a.json", env.Subject)
	assert.Equal(t, `{"n":1}`, string(env.Body))
	assert.Equal(t, "application/json", env.ContentType)
	assert.Equal(t, map[string][]string{"origin": {"billing"}}, env.Headers, "x-amz-meta-* headers")
	assert.Equal(t, bucket, env.Attributes["s3.bucket"])
	assert.Equal(t, "a.json", env.Attributes["s3.key"])
	assert.Equal(t, etagA, env.Attributes["s3.etag"])
	assert.Equal(t, int64(7), env.Attributes["s3.size"])
	assert.Equal(t, "a.json", env.Attributes["ordering_key"])
	assert.NotContains(t, env.Attributes, "s3.line")
	assert.Equal(t, kindS3, env.Protocol)

	envB := recv(t, r.out, 2*time.Second)
	assert.Equal(t, etagB, envB.ID)
	assert.Equal(t, contentText, envB.ContentType, "a generic octet-stream type is detected instead")

	// In flight, an object is not read again.
	noMessage(t, r.out, 200*time.Millisecond)
	ack(env, envelope.Delivered)
	ack(envB, envelope.Filtered)
	require.Eventually(t, func() bool { return len(f.keys(t)) == 1 }, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, []string{"dir/"}, f.keys(t), "directory markers are skipped")
	require.NoError(t, r.src.Health(context.Background()))
	assert.Equal(t, "in", r.src.Name())
}

func TestS3SourceLeavesFailedObjectsForTheNextPoll(t *testing.T) {
	f := newFakeS3(t)
	etag := f.put(t, "a.json", `{"n":1}`, "", nil)
	r := runS3(t, f, &config.Source{}, nil)

	env := recv(t, r.out, 2*time.Second)
	ack(env, envelope.Failed)
	again := recv(t, r.out, 2*time.Second)
	assert.Equal(t, etag, again.ID, "the same ID: destinations drop what they already have")
	assert.Equal(t, []string{"a.json"}, f.keys(t))
	ack(again, envelope.DeadLettered)
	require.Eventually(t, func() bool { return len(f.keys(t)) == 0 }, 2*time.Second, 10*time.Millisecond)
}

func TestS3SourceMoves(t *testing.T) {
	f := newFakeS3(t)
	f.put(t, "in/a b+c.json", `{"n":1}`, "application/json", map[string]string{"origin": "billing"})
	r := runS3(t, f, &config.Source{After: "move", MoveTo: "done/"}, nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "in/a b+c.json", env.Subject)
	ack(env, envelope.Delivered)
	require.Eventually(t, func() bool { return len(f.keys(t)) == 1 && f.keys(t)[0] == "done/in/a b+c.json" },
		2*time.Second, 10*time.Millisecond, "copied to move_to + key, then deleted")
	obj, ok := f.get(t, "done/in/a b+c.json")
	require.True(t, ok)
	assert.Equal(t, `{"n":1}`, obj.body)
	assert.Equal(t, "billing", obj.meta["origin"])
	noMessage(t, r.out, 300*time.Millisecond) // moved objects are not read again
}

func TestS3SourceKeeps(t *testing.T) {
	f := newFakeS3(t)
	st := store.NewMemory("m")
	etag := f.put(t, "a.json", `{"n":1}`, "", nil)
	r := runS3(t, f, &config.Source{After: "keep"}, st)
	ack(recv(t, r.out, 2*time.Second), envelope.Delivered)
	require.Eventually(t, func() bool {
		_, ok, err := st.Get(context.Background(), "s3:archive:a.json:"+etag)
		return err == nil && ok
	}, 2*time.Second, 10*time.Millisecond, "recorded in the store")
	noMessage(t, r.out, 300*time.Millisecond)
	assert.Equal(t, []string{"a.json"}, f.keys(t), "kept")

	// A new version is read again.
	etag2 := f.put(t, "a.json", `{"n":2}`, "", nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, etag2, env.ID)
	ack(env, envelope.Delivered)

	// So is nothing after a restart: the store remembers.
	r.cancel()
	require.NoError(t, <-r.done)
	r.done <- nil
	r2 := runS3(t, f, &config.Source{After: "keep"}, st)
	noMessage(t, r2.out, 300*time.Millisecond)
}

func TestS3SourceSplitsLines(t *testing.T) {
	f := newFakeS3(t)
	etag := f.put(t, "batch.ndjson", "line one\n\n{\"a\":1}\r\n   \nlast", "application/x-ndjson", nil)
	r := runS3(t, f, &config.Source{Split: "lines"}, nil)
	want := []struct {
		body, ct string
		line     int
	}{{"line one", contentText, 1}, {`{"a":1}`, contentJSON, 3}, {"last", contentText, 5}}
	var envs []*envelope.Envelope
	for _, w := range want {
		env := recv(t, r.out, 2*time.Second)
		assert.Equal(t, w.body, string(env.Body))
		assert.Equal(t, w.ct, env.ContentType)
		assert.Equal(t, w.line, env.Attributes["s3.line"])
		assert.Equal(t, etag+"-"+strconv.Itoa(w.line), env.ID)
		assert.Equal(t, "batch.ndjson", env.Attributes["ordering_key"])
		envs = append(envs, env)
	}
	ack(envs[0], envelope.Delivered)
	ack(envs[1], envelope.Delivered)
	noMessage(t, r.out, 200*time.Millisecond)
	assert.Equal(t, []string{"batch.ndjson"}, f.keys(t), "deleted once every line is final")
	ack(envs[2], envelope.Delivered)
	require.Eventually(t, func() bool { return len(f.keys(t)) == 0 }, 2*time.Second, 10*time.Millisecond)
}

func TestS3SourceJQSplit(t *testing.T) {
	f := newFakeS3(t)
	etag := f.put(t, "a.json", `{"records":[{"id":1},{"id":2}]}`, "", nil)
	r := runS3(t, f, &config.Source{Split: ".records[]"}, nil)
	for i, want := range []string{`{"id":1}`, `{"id":2}`} {
		env := recv(t, r.out, 2*time.Second)
		assert.Equal(t, want, string(env.Body))
		assert.Equal(t, etag+"-"+strconv.Itoa(i+1), env.ID)
		assert.Equal(t, i+1, env.Attributes["s3.item"])
		assert.Equal(t, contentJSON, env.ContentType)
		ack(env, envelope.Delivered)
	}
	require.Eventually(t, func() bool { return len(f.keys(t)) == 0 }, 2*time.Second, 10*time.Millisecond)

	etag = f.put(t, "b.txt", "not json", "text/plain", nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "not json", string(env.Body), "an object that cannot be split passes whole")
	assert.Equal(t, etag, env.ID)
	assert.Contains(t, r.logs.String(), "cannot split an object")
	ack(env, envelope.Delivered)
}

func TestS3SourceOversizeAndEmptyObjects(t *testing.T) {
	f := newFakeS3(t)
	f.put(t, "big.txt", "0123456789", "", nil)
	f.put(t, "empty.txt", "", "", nil)
	f.put(t, "ok.txt", "small", "", nil)
	r := runS3(t, f, &config.Source{MaxBodyBytes: 5, Split: "lines"}, nil)
	env := recv(t, r.out, 2*time.Second)
	assert.Equal(t, "small", string(env.Body))
	ack(env, envelope.Delivered)
	require.Eventually(t, func() bool { return len(f.keys(t)) == 0 }, 2*time.Second, 10*time.Millisecond,
		"an oversize object is handled as after says; an empty one has nothing to wait for")
	assert.Contains(t, r.logs.String(), "over max_body_bytes")
}

func TestS3SourceShutdownMidObjectKeepsIt(t *testing.T) {
	f := newFakeS3(t)
	f.put(t, "a.txt", "1\n2\n3", "", nil)
	s, err := newS3Source(context.Background(), sourceSpec(kindS3, &config.Source{Bucket: bucket, AWS: f.aws(), Split: "lines"}))
	require.NoError(t, err)
	out := make(chan *envelope.Envelope) // the second line waits for a reader that never comes
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, out) }()
	env := recv(t, out, 2*time.Second)
	cancel()
	require.NoError(t, <-done)
	ack(env, envelope.Delivered)
	assert.Equal(t, []string{"a.txt"}, f.keys(t), "lines never handed over keep the object")
	s.mu.Lock()
	assert.Empty(t, s.inflight, "and it can be read again")
	s.mu.Unlock()
}

func TestS3SourceHealth(t *testing.T) {
	f := newFakeS3(t)
	spec := sourceSpec(kindS3, &config.Source{Bucket: "missing", AWS: f.aws(), Interval: config.Duration(20 * time.Millisecond),
		Retry: &config.Retry{Attempts: 1}})
	s, err := newS3Source(context.Background(), spec)
	require.NoError(t, err)
	err = s.Health(context.Background())
	require.Error(t, err, "before polling: HeadBucket")
	class, _ := errorTags(err)
	assert.Equal(t, plugin.Permanent, class)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, make(chan *envelope.Envelope)) }()
	require.Eventually(t, func() bool {
		err := s.Health(context.Background())
		return err != nil && strings.Contains(err.Error(), "list s3://missing")
	}, 2*time.Second, 10*time.Millisecond, "then the last poll's error")
	assert.ErrorContains(t, s.Start(ctx, nil), "already started")
	cancel()
	require.NoError(t, <-done)
}

func TestS3SourceRetriesListing(t *testing.T) {
	f := newFakeS3(t)
	flaky := &flakyS3{s3API: f.client, failures: 2}
	spec := sourceSpec(kindS3, &config.Source{Bucket: bucket, AWS: f.aws(), Retry: &config.Retry{Attempts: 3, Backoff: config.Duration(time.Millisecond)}})
	_, opts, err := parseS3Source(spec)
	require.NoError(t, err)
	s := buildS3Source(spec, opts, flaky, nil)
	f.put(t, "a.txt", "x", "", nil)
	out := make(chan *envelope.Envelope, 1)
	require.NoError(t, s.poll(context.Background(), out))
	assert.Equal(t, "x", string((<-out).Body))
	assert.Equal(t, 3, flaky.calls)

	flaky.failures, flaky.calls = 5, 0
	err = s.poll(context.Background(), out)
	assert.ErrorContains(t, err, "InternalError")
	assert.Equal(t, 3, flaky.calls, "retry.attempts per poll")
}

// flakyS3 fails its first ListObjectsV2 calls.
type flakyS3 struct {
	s3API
	failures int
	calls    int
}

func (f *flakyS3) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	f.calls++
	if f.failures > 0 {
		f.failures--
		return nil, apiError("InternalError", 500, 0)
	}
	return f.s3API.ListObjectsV2(ctx, in, opts...)
}

func TestCopySource(t *testing.T) {
	assert.Equal(t, "b/in/a%20b%2Bc~.json", copySource("b", "in/a b+c~.json"))
	assert.Equal(t, "b/%C3%A9t%C3%A9", copySource("b", "été"))
}
