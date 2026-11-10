package aws

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/backoff"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const (
	// defaultReadTimeout bounds one object download.
	defaultReadTimeout = 5 * time.Minute
	ackDelivered       = "delivered"
)

// s3Source is an s3 poll source.
type s3Source struct {
	name        string
	kind        string
	opts        s3SourceOptions
	client      s3API
	kv          store.KV // after: keep
	log         *slog.Logger
	rnd         func() float64
	callTimeout time.Duration
	readTimeout time.Duration

	started atomic.Bool

	mu       sync.Mutex
	polled   bool
	lastErr  error
	inflight map[string]*s3Object // by key: objects whose messages are not all final
	kept     map[string]bool      // keep marks known to be recorded
}

// s3Object is an object whose messages are in the relay.
type s3Object struct {
	key     string
	etag    string
	pending int  // messages not settled yet
	failed  bool // a message was not final, or never reached the relay
}

func newS3Source(ctx context.Context, spec plugin.Spec) (*s3Source, error) {
	src, opts, err := parseS3Source(spec)
	if err != nil {
		return nil, err
	}
	var kv store.KV
	if opts.after == afterKeep {
		if spec.Env.Store == nil {
			return nil, fmt.Errorf("source %s: after: keep needs a store to remember the objects read", spec.Name)
		}
		st, err := spec.Env.Store(opts.stateStore)
		if err != nil {
			return nil, fmt.Errorf("source %s: state_store: %w", spec.Name, err)
		}
		kv = st
	}
	cfg, err := loadAWS(ctx, src.AWS, "", sourceAttempts, roleSessionName(spec))
	if err != nil {
		return nil, fmt.Errorf("source %s: %w", spec.Name, err)
	}
	return buildS3Source(spec, opts, newS3Client(cfg, src.AWS != nil && src.AWS.Endpoint != ""), kv), nil
}

// buildS3Source assembles a source around its client and store.
func buildS3Source(spec plugin.Spec, opts s3SourceOptions, client s3API, kv store.KV) *s3Source {
	s := &s3Source{
		name: spec.Name, kind: spec.Kind, opts: opts, client: client, kv: kv, log: spec.Env.Log(),
		rnd: rand.Float64, callTimeout: defaultCallTimeout, readTimeout: defaultReadTimeout,
		inflight: map[string]*s3Object{}, kept: map[string]bool{},
	}
	if opts.timeout > 0 {
		s.callTimeout = opts.timeout
		s.readTimeout = max(opts.timeout, defaultReadTimeout) // a download may take longer than a call
	}
	return s
}

// Name is the source name.
func (s *s3Source) Name() string { return s.name }

// Health is the last poll's error once the source polled; before, it checks
// the bucket with HeadBucket.
func (s *s3Source) Health(ctx context.Context) error {
	s.mu.Lock()
	polled, err := s.polled, s.lastErr
	s.mu.Unlock()
	if polled {
		return err
	}
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.opts.bucket)}); err != nil {
		return fmt.Errorf("s3 bucket %s: %w", s.opts.bucket, classify(kindS3, err))
	}
	return nil
}

func (s *s3Source) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.polled, s.lastErr = true, err
}

// Start polls at once, then every interval, until ctx ends. A listing whose
// retries all fail is logged and skipped: the next interval tries again.
func (s *s3Source) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("already started")
	}
	s.log.Info("s3 source polling", "source", s.name, "bucket", s.opts.bucket, "prefix", s.opts.prefix,
		"interval", s.opts.interval.String(), "after", s.opts.after)
	t := time.NewTicker(s.opts.interval)
	defer t.Stop()
	for {
		if err := s.poll(ctx, out); err != nil && ctx.Err() == nil {
			s.log.Warn("s3 poll failed; skipping to the next interval", "source", s.name, "bucket", s.opts.bucket, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// poll lists the bucket, page by page, and reads the objects not seen yet.
func (s *s3Source) poll(ctx context.Context, out chan<- *envelope.Envelope) error {
	var token *string
	for {
		page, err := s.list(ctx, token)
		if err != nil {
			if ctx.Err() == nil {
				s.setErr(err)
			}
			return err
		}
		s.setErr(nil)
		for _, obj := range page.Contents {
			if ctx.Err() != nil {
				return nil
			}
			s.object(ctx, out, obj)
		}
		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			return nil
		}
		token = page.NextContinuationToken
	}
}

// list is one ListObjectsV2 page, with the source's retry policy.
func (s *s3Source) list(ctx context.Context, token *string) (*s3.ListObjectsV2Output, error) {
	in := &s3.ListObjectsV2Input{Bucket: aws.String(s.opts.bucket), ContinuationToken: token}
	if s.opts.prefix != "" {
		in.Prefix = aws.String(s.opts.prefix)
	}
	r := s.opts.retry
	var last error
	for n := 1; n <= max(r.Attempts, 1); n++ {
		cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
		out, err := s.client.ListObjectsV2(cctx, in)
		cancel()
		if err == nil {
			return out, nil
		}
		last = classify(kindS3, err)
		if ctx.Err() != nil {
			return nil, last
		}
		class, ra := plugin.Classify(last)
		if n >= r.Attempts || !plugin.Retryable(class, plugin.Tags(last), r.RetryOn, r.NeverOn) {
			break
		}
		wait := backoff.Delay(r.Backoff.Std(), r.MaxBackoff.Std(), r.Jitter, n, s.rnd)
		if ra > 0 && config.Bool(r.RespectRetryAfter, true) {
			wait = min(ra, r.MaxBackoff.Std())
		}
		if !backoff.Sleep(ctx, wait) {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("list s3://%s/%s: %w", s.opts.bucket, s.opts.prefix, last)
}

// object reads one listed object into messages and hands them over, unless
// it is a directory marker, already moved, still in flight, or (keep)
// already read.
func (s *s3Source) object(ctx context.Context, out chan<- *envelope.Envelope, obj types.Object) {
	key := aws.ToString(obj.Key)
	if key == "" || strings.HasSuffix(key, "/") {
		return // a "directory"
	}
	if s.opts.after == afterMove && strings.HasPrefix(key, s.opts.moveTo) {
		return
	}
	if !s.claim(key) {
		return // its messages are still in the relay
	}
	etag := strings.Trim(aws.ToString(obj.ETag), `"`)
	if s.opts.after == afterKeep {
		seen, err := s.recorded(ctx, key, etag)
		if err != nil {
			s.log.Warn("cannot tell whether an object was read already; skipping it this poll", "source", s.name,
				"bucket", s.opts.bucket, "key", key, "err", err)
		}
		if err != nil || seen {
			s.unclaim(key)
			return
		}
	}
	size := aws.ToInt64(obj.Size)
	if size > s.opts.maxBytes {
		s.oversize(ctx, key, etag, size)
		return
	}
	body, meta, ct, readETag, err := s.read(ctx, key)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("cannot read an object; trying again next poll", "source", s.name, "bucket", s.opts.bucket,
				"key", key, "err", err)
		}
		s.unclaim(key)
		return
	}
	if readETag != "" {
		etag = readETag
	}
	// What was read wins over the listing: the object may have changed
	// in between.
	if int64(len(body)) > s.opts.maxBytes {
		s.oversize(ctx, key, etag, int64(len(body)))
		return
	}
	s.emit(ctx, out, &s3Object{key: key, etag: etag}, s.messages(key, etag, int64(len(body)), body, meta, ct))
}

// oversize skips an object over max_body_bytes and handles it as after
// says, since it would be read again every poll otherwise.
func (s *s3Source) oversize(ctx context.Context, key, etag string, size int64) {
	s.log.Warn("object over max_body_bytes; skipping it", "source", s.name, "bucket", s.opts.bucket, "key", key,
		"bytes", size, "max_body_bytes", s.opts.maxBytes, "after", s.opts.after)
	s.after(ctx, &s3Object{key: key, etag: etag})
	s.unclaim(key)
}

// read downloads an object, up to one byte over max_body_bytes.
func (s *s3Source) read(ctx context.Context, key string) (body []byte, meta map[string]string, contentType, etag string, err error) {
	cctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()
	out, err := s.client.GetObject(cctx, &s3.GetObjectInput{Bucket: aws.String(s.opts.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, nil, "", "", classify(kindS3, err)
	}
	defer func() { _ = out.Body.Close() }()
	body, err = io.ReadAll(io.LimitReader(out.Body, s.opts.maxBytes+1))
	if err != nil {
		return nil, nil, "", "", classify(kindS3, err)
	}
	return body, out.Metadata, aws.ToString(out.ContentType), strings.Trim(aws.ToString(out.ETag), `"`), nil
}

// messages turns an object into envelopes: one per non-empty line with
// split: lines, one per output of a jq split, else the whole object.
func (s *s3Source) messages(key, etag string, size int64, body []byte, meta map[string]string, ct string) []*envelope.Envelope {
	base := etag
	if base == "" {
		sum := sha256.Sum256(body)
		base = hex.EncodeToString(sum[:16])
	}
	newEnv := func(b []byte, id string) *envelope.Envelope {
		env := envelope.New(s.name, s.kind, b)
		env.ID = id
		env.Subject = key
		for k, v := range meta {
			env.Headers[k] = []string{v}
		}
		env.Attributes["s3.bucket"] = s.opts.bucket
		env.Attributes["s3.key"] = key
		env.Attributes["s3.etag"] = etag
		env.Attributes["s3.size"] = size
		env.Attributes["ordering_key"] = key
		return env
	}
	switch {
	case s.opts.lines:
		var envs []*envelope.Envelope
		for i, line := range bytes.Split(body, []byte("\n")) {
			line = bytes.TrimRight(line, "\r")
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			env := newEnv(bytes.Clone(line), base+"-"+strconv.Itoa(i+1))
			env.ContentType = detectContentType(line)
			env.Attributes["s3.line"] = i + 1
			envs = append(envs, env)
		}
		return envs
	case s.opts.split != nil:
		items, err := s.splitJSON(body)
		if err == nil {
			envs := make([]*envelope.Envelope, 0, len(items))
			for i, it := range items {
				env := newEnv(it, base+"-"+strconv.Itoa(i+1))
				env.ContentType = contentJSON
				env.Attributes["s3.item"] = i + 1
				envs = append(envs, env)
			}
			return envs
		}
		s.log.Warn("cannot split an object; passing it on whole", "source", s.name, "bucket", s.opts.bucket,
			"key", key, "split", s.opts.split.String(), "err", err)
	}
	env := newEnv(body, base)
	env.ContentType = ct
	if genericContentType(ct) {
		env.ContentType = detectContentType(body)
	}
	return []*envelope.Envelope{env}
}

// genericContentType reports a content type that says nothing: none, or
// the octet-stream S3 gives objects uploaded without one.
func genericContentType(ct string) bool {
	switch strings.ToLower(strings.TrimSpace(ct)) {
	case "", "binary/octet-stream", contentBinary:
		return true
	}
	return false
}

// splitJSON runs the jq split over an object's JSON.
func (s *s3Source) splitJSON(body []byte) ([][]byte, error) {
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("split needs a JSON object: %w", err)
	}
	items, err := s.opts.split.Run(doc)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(items))
	for i, it := range items {
		if out[i], err = json.Marshal(it); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// emit hands an object's messages over, in order, each settling into the
// object's tally. An object without messages is done at once. When ctx ends
// first, the messages not handed over count as failed: the object stays
// for the next run.
func (s *s3Source) emit(ctx context.Context, out chan<- *envelope.Envelope, obj *s3Object, envs []*envelope.Envelope) {
	obj.pending = len(envs)
	s.mu.Lock()
	s.inflight[obj.key] = obj
	s.mu.Unlock()
	if len(envs) == 0 {
		s.after(ctx, obj)
		s.unclaim(obj.key)
		return
	}
	for i, env := range envs {
		// A poll source settles once a message is done (ack: delivered).
		plugin.OnSettle(env, ackDelivered, func(ctx context.Context, final bool) { s.settle(ctx, obj, 1, final) })
		select {
		case out <- env:
		case <-ctx.Done():
			s.settle(ctx, obj, len(envs)-i, false)
			return
		}
	}
}

// settle counts n messages of an object as settled. Once none is pending,
// a fully final object is deleted, moved or recorded (after), and the
// object is free to be read again.
func (s *s3Source) settle(ctx context.Context, obj *s3Object, n int, final bool) {
	s.mu.Lock()
	obj.pending -= n
	if !final {
		obj.failed = true
	}
	done, failed := obj.pending <= 0, obj.failed
	s.mu.Unlock()
	if !done {
		return
	}
	if failed {
		s.log.Debug("object has failed messages; reading it again next poll", "source", s.name, "key", obj.key)
	} else {
		s.after(context.WithoutCancel(ctx), obj)
	}
	s.unclaim(obj.key)
}

// after deletes, moves or records an object whose messages are all final.
// A failure is logged: the object is read again next poll, and its
// messages keep their IDs.
func (s *s3Source) after(ctx context.Context, obj *s3Object) {
	cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	var err error
	switch s.opts.after {
	case afterDelete:
		err = s.delete(cctx, obj.key)
	case afterMove:
		dst := s.opts.moveTo + obj.key
		_, err = s.client.CopyObject(cctx, &s3.CopyObjectInput{
			Bucket: aws.String(s.opts.bucket), Key: aws.String(dst), CopySource: aws.String(copySource(s.opts.bucket, obj.key)),
		})
		if err == nil {
			err = s.delete(cctx, obj.key)
		} else {
			err = classify(kindS3, err)
		}
	case afterKeep:
		mark := s.keepMark(obj.key, obj.etag)
		if _, err = s.kv.SetNX(cctx, mark, []byte(time.Now().UTC().Format(time.RFC3339)), 0); err == nil {
			s.mu.Lock()
			s.kept[mark] = true
			s.mu.Unlock()
		}
	}
	if err != nil {
		s.log.Warn("cannot "+s.opts.after+" a read object; it is read again next poll", "source", s.name,
			"bucket", s.opts.bucket, "key", obj.key, "err", err)
	}
}

func (s *s3Source) delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.opts.bucket), Key: aws.String(key)})
	return classify(kindS3, err)
}

// keepMark is the store key that records an object version as read.
func (s *s3Source) keepMark(key, etag string) string {
	return "s3:" + s.opts.bucket + ":" + key + ":" + etag
}

// recorded reports whether after: keep recorded this version of an object.
func (s *s3Source) recorded(ctx context.Context, key, etag string) (bool, error) {
	mark := s.keepMark(key, etag)
	s.mu.Lock()
	known := s.kept[mark]
	s.mu.Unlock()
	if known {
		return true, nil
	}
	cctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()
	_, ok, err := s.kv.Get(cctx, mark)
	if err != nil {
		return false, err
	}
	if ok {
		s.mu.Lock()
		s.kept[mark] = true
		s.mu.Unlock()
	}
	return ok, nil
}

// claim marks an object as being read; false when it already is.
func (s *s3Source) claim(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, busy := s.inflight[key]; busy {
		return false
	}
	s.inflight[key] = &s3Object{key: key}
	return true
}

func (s *s3Source) unclaim(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, key)
}

// copySource is a CopyObject source, bucket/key with the key URL-encoded
// (every byte but the unreserved ones and /).
func copySource(bucket, key string) string {
	var b strings.Builder
	b.WriteString(bucket)
	b.WriteByte('/')
	for i := 0; i < len(key); i++ {
		c := key[i]
		if c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || strings.IndexByte("-_.~/", c) >= 0 {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}
