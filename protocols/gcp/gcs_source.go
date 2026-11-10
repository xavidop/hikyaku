package gcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const (
	// listPage is how many objects one list call returns.
	listPage = 1000
	// keptCache bounds the in-process memory of objects recorded in the
	// state store (after: keep), which spares a store read per object per
	// poll.
	keptCache = 100_000
)

// GCSSource is a gcs poll source.
type GCSSource struct {
	name   string
	kind   string
	o      gcsSourceOptions
	client *storage.Client
	bkt    *storage.BucketHandle
	kv     store.KV   // after: keep
	kept   *store.LRU // after: keep, objects known to be recorded
	log    *slog.Logger

	mu       sync.Mutex
	inflight map[string]bool // objects whose messages are in flight, by name
	lastErr  error
}

func newGCSSource(ctx context.Context, spec plugin.Spec) (*GCSSource, error) {
	o, err := parseGCSSource(spec)
	if err != nil {
		return nil, err
	}
	s := &GCSSource{name: spec.Name, kind: spec.Kind, o: o, log: spec.Env.Log(), inflight: map[string]bool{}}
	if o.after == afterKeep {
		if spec.Env.Store == nil {
			return nil, fmt.Errorf("source %s: after: keep needs a store (state_store) to remember the objects already read", spec.Name)
		}
		st, err := spec.Env.Store(o.stateStore)
		if err != nil {
			return nil, fmt.Errorf("source %s: state_store: %w", spec.Name, err)
		}
		s.kv, s.kept = st, store.NewLRU(keptCache)
	}
	if s.client, err = newStorageClient(ctx, o.client); err != nil {
		return nil, fmt.Errorf("source %s: storage client: %w", spec.Name, err)
	}
	s.bkt = s.client.Bucket(o.bucket)
	return s, nil
}

// Name is the source name.
func (s *GCSSource) Name() string { return s.name }

// Health reports the last poll's error.
func (s *GCSSource) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastErr
}

// Start polls at once, then every interval, until ctx ends. A failed poll
// is logged and skipped: the next interval tries again.
func (s *GCSSource) Start(ctx context.Context, out chan<- *envelope.Envelope) error {
	t := time.NewTicker(s.o.interval)
	defer t.Stop()
	for {
		err := s.poll(ctx, out)
		if ctx.Err() != nil {
			return nil
		}
		s.mu.Lock()
		s.lastErr = err
		s.mu.Unlock()
		if err != nil {
			s.log.Warn("gcs poll failed; trying again next interval", "source", s.name, "bucket", s.o.bucket,
				"prefix", s.o.prefix, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// poll lists bucket/prefix a page at a time and reads each object found. An
// object that fails is logged and left for the next poll; the first such
// error is returned once the listing is done.
func (s *GCSSource) poll(ctx context.Context, out chan<- *envelope.Envelope) error {
	q := &storage.Query{Prefix: s.o.prefix}
	if err := q.SetAttrSelection([]string{"Name", "Generation", "Size", "ContentType", "Metadata"}); err != nil {
		return err
	}
	var first error
	token := ""
	for {
		var page []*storage.ObjectAttrs
		lctx, cancel := context.WithTimeout(ctx, s.o.timeout)
		next, err := iterator.NewPager(s.bkt.Objects(lctx, q), listPage, token).NextPage(&page)
		cancel()
		if err != nil {
			return gcsError("list gs://"+s.o.bucket+"/"+s.o.prefix, err)
		}
		for _, a := range page {
			if err := s.object(ctx, out, a); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				s.log.Warn("gcs object left for the next poll", "source", s.name, "object", s.uri(a.Name), "err", err)
				if first == nil {
					first = err
				}
			}
		}
		if next == "" {
			return first
		}
		token = next
	}
}

// object reads one object and hands its messages to the relay, unless it
// is a folder placeholder, already moved, in flight or (after: keep)
// already recorded.
func (s *GCSSource) object(ctx context.Context, out chan<- *envelope.Envelope, a *storage.ObjectAttrs) error {
	if strings.HasSuffix(a.Name, "/") && a.Size == 0 {
		return nil
	}
	if s.o.after == afterMove && strings.HasPrefix(a.Name, s.o.moveTo) {
		return nil
	}
	if !s.claim(a.Name) {
		return nil
	}
	if s.kv != nil {
		recorded, err := s.recorded(ctx, a)
		if err != nil || recorded {
			s.unclaim(a.Name)
			return err
		}
	}
	run := &objectRun{src: s, a: a}
	if a.Size > s.o.maxBytes && s.o.split != splitLines {
		s.oversize(a, 0, a.Size)
		run.end(ctx, true)
		return nil
	}
	r, err := s.open(ctx, a)
	if err != nil {
		s.unclaim(a.Name)
		if isNotFound(err) {
			return nil // deleted since the listing
		}
		return gcsError("read "+s.uri(a.Name), err)
	}
	defer func() { _ = r.Close() }()
	if s.o.split == splitLines {
		err = s.lines(ctx, out, run, a, r)
	} else {
		err = s.whole(ctx, out, run, a, r)
	}
	run.end(ctx, err == nil)
	return err
}

// open reads the listed generation of an object. Opening, and each read,
// fail after timeout without progress.
func (s *GCSSource) open(ctx context.Context, a *storage.ObjectAttrs) (io.ReadCloser, error) {
	rctx, cancel := context.WithCancel(ctx)
	t := time.AfterFunc(s.o.timeout, cancel)
	r, err := s.bkt.Object(a.Name).Generation(a.Generation).NewReader(rctx)
	t.Stop()
	if err != nil {
		cancel()
		return nil, err
	}
	return &stallReader{r: r, timeout: s.o.timeout, cancel: cancel}, nil
}

// lines sends each non-empty line as a message.
func (s *GCSSource) lines(ctx context.Context, out chan<- *envelope.Envelope, run *objectRun, a *storage.ObjectAttrs, r io.Reader) error {
	br := bufio.NewReaderSize(r, 64<<10)
	for n := 1; ; n++ {
		line, over, err := readLine(br, s.o.maxBytes)
		if err != nil && !errors.Is(err, io.EOF) {
			return gcsError("read "+s.uri(a.Name), err)
		}
		switch {
		case over:
			s.oversize(a, n, -1)
		case len(bytes.TrimSpace(line)) > 0:
			env := s.envelope(a, line, n)
			env.ContentType = detectContentType(line)
			if err := s.emit(ctx, out, run, env); err != nil {
				return err
			}
		}
		if err != nil {
			return nil
		}
	}
}

// whole sends the object as one message, or one per output of the jq
// split. An object the split cannot take (not JSON, or the program fails)
// is sent whole with gcs.split_error, so a route or the dead-letter queue
// deals with it instead of every poll retrying it.
func (s *GCSSource) whole(ctx context.Context, out chan<- *envelope.Envelope, run *objectRun, a *storage.ObjectAttrs, r io.Reader) error {
	body, err := io.ReadAll(io.LimitReader(r, s.o.maxBytes+1))
	if err != nil {
		return gcsError("read "+s.uri(a.Name), err)
	}
	if int64(len(body)) > s.o.maxBytes {
		s.oversize(a, 0, int64(len(body)))
		return nil
	}
	if s.o.jq != nil {
		items, err := s.split(body)
		if err == nil {
			for i, it := range items {
				env := s.envelope(a, it, i+1)
				env.ContentType = contentJSON
				if err := s.emit(ctx, out, run, env); err != nil {
					return err
				}
			}
			return nil
		}
		s.log.Warn("gcs object could not be split; sent whole", "source", s.name, "object", s.uri(a.Name), "err", err)
		env := s.wholeEnvelope(a, body)
		env.Attributes["gcs.split_error"] = err.Error()
		return s.emit(ctx, out, run, env)
	}
	return s.emit(ctx, out, run, s.wholeEnvelope(a, body))
}

// split runs the jq split on the object's JSON.
func (s *GCSSource) split(body []byte) ([][]byte, error) {
	var doc any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("split needs a JSON object: %w", err)
	}
	items, err := s.o.jq.Run(doc)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(items))
	for _, it := range items {
		b, err := json.Marshal(it)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// oversize logs a message over max_body_bytes. It is dropped and counts as
// handled: delivering it can never succeed, and every poll would retry it.
func (s *GCSSource) oversize(a *storage.ObjectAttrs, line int, size int64) {
	args := []any{"source", s.name, "object", s.uri(a.Name), "max_body_bytes", s.o.maxBytes}
	if line > 0 {
		args = append(args, "line", line)
	}
	if size >= 0 {
		args = append(args, "bytes", size)
	}
	s.log.Error("gcs message over max_body_bytes; dropped and counted as handled", args...)
}

// emit hands a message to the relay; its settlement counts toward the
// object's.
func (s *GCSSource) emit(ctx context.Context, out chan<- *envelope.Envelope, run *objectRun, env *envelope.Envelope) error {
	run.add()
	plugin.OnSettle(env, s.o.ack, run.settle)
	select {
	case out <- env:
		return nil
	case <-ctx.Done():
		run.settle(ctx, false)
		return ctx.Err()
	}
}

// envelope is a message of an object; line is its line (or split item)
// number, 0 for the whole object.
func (s *GCSSource) envelope(a *storage.ObjectAttrs, body []byte, line int) *envelope.Envelope {
	env := envelope.New(s.name, s.kind, body)
	env.ID = strconv.FormatInt(a.Generation, 10) + "-" + strconv.Itoa(line)
	env.Subject = a.Name
	for k, v := range a.Metadata {
		env.Headers[k] = []string{v}
	}
	env.Attributes["gcs.bucket"] = s.o.bucket
	env.Attributes["gcs.name"] = a.Name
	env.Attributes["gcs.generation"] = a.Generation
	env.Attributes["gcs.size"] = a.Size
	if line > 0 {
		env.Attributes["gcs.line"] = line
	}
	env.Attributes["ordering_key"] = a.Name
	return env
}

// wholeEnvelope is the message of a whole object, with the object's content
// type (else what the body looks like).
func (s *GCSSource) wholeEnvelope(a *storage.ObjectAttrs, body []byte) *envelope.Envelope {
	env := s.envelope(a, body, 0)
	env.ContentType = a.ContentType
	if env.ContentType == "" {
		env.ContentType = detectContentType(body)
	}
	return env
}

// claim marks an object in flight; false when it already is.
func (s *GCSSource) claim(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight[name] {
		return false
	}
	s.inflight[name] = true
	return true
}

func (s *GCSSource) unclaim(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inflight, name)
}

// keptKey is the state store key of an object read with after: keep.
func (s *GCSSource) keptKey(a *storage.ObjectAttrs) string {
	return "gcs:" + s.o.bucket + ":" + a.Name + ":" + strconv.FormatInt(a.Generation, 10)
}

// recorded reports whether the object's generation was already read
// (after: keep).
func (s *GCSSource) recorded(ctx context.Context, a *storage.ObjectAttrs) (bool, error) {
	key := s.keptKey(a)
	if _, ok, _ := s.kept.Get(ctx, key); ok {
		return true, nil
	}
	kctx, cancel := context.WithTimeout(ctx, s.o.timeout)
	defer cancel()
	_, ok, err := s.kv.Get(kctx, key)
	if err != nil {
		return false, fmt.Errorf("state_store: %w", err)
	}
	if ok {
		_, _ = s.kept.SetNX(ctx, key, nil, 0)
	}
	return ok, nil
}

// finish runs once every message of an object is settled: when they are
// all final, the object is deleted, moved or recorded; otherwise it is left
// for the next poll. The object stays claimed until then.
func (s *GCSSource) finish(ctx context.Context, a *storage.ObjectAttrs, final bool) {
	defer s.unclaim(a.Name)
	if !final {
		s.log.Debug("gcs object left for the next poll: a message failed", "source", s.name, "object", s.uri(a.Name))
		return
	}
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.o.timeout)
	defer cancel()
	if err := s.after(actx, a); err != nil {
		// A kept object is remembered in process anyway: only a restart
		// reads it again.
		again := "next poll"
		if s.o.after == afterKeep {
			again = "after a restart"
		}
		s.log.Warn("gcs object handled, but after: "+s.o.after+" failed; it is read again "+again,
			"source", s.name, "object", s.uri(a.Name), "err", err)
	}
}

// after applies after: to a handled object. Delete and move touch only the
// generation that was read: a newer one is left for the next poll.
func (s *GCSSource) after(ctx context.Context, a *storage.ObjectAttrs) error {
	switch s.o.after {
	case afterKeep:
		key := s.keptKey(a)
		_, _ = s.kept.SetNX(ctx, key, nil, 0)
		if _, err := s.kv.SetNX(ctx, key, []byte(time.Now().UTC().Format(time.RFC3339)), 0); err != nil {
			return fmt.Errorf("state_store: %w", err)
		}
		return nil
	case afterMove:
		src := s.bkt.Object(a.Name).Generation(a.Generation)
		// Copying one generation to the same name is idempotent, so the
		// copy may be retried although it has no precondition.
		dst := s.bkt.Object(s.o.moveTo + a.Name).Retryer(storage.WithPolicy(storage.RetryAlways))
		if _, err := dst.CopierFrom(src).Run(ctx); err != nil {
			if isNotFound(err) {
				return nil // that generation is gone: nothing left to move
			}
			return gcsError("copy to "+s.uri(s.o.moveTo+a.Name), err)
		}
	}
	err := s.bkt.Object(a.Name).If(storage.Conditions{GenerationMatch: a.Generation}).Delete(ctx)
	switch {
	case err == nil, isNotFound(err):
		return nil
	case isPrecondition(err):
		s.log.Info("gcs object replaced since it was read; the new version is read next poll",
			"source", s.name, "object", s.uri(a.Name))
		return nil
	}
	return gcsError("delete "+s.uri(a.Name), err)
}

func (s *GCSSource) uri(name string) string { return "gs://" + s.o.bucket + "/" + name }

// Close closes the client once the relay is done with the messages in
// flight (their settlement deletes, moves or records objects).
func (s *GCSSource) Close(context.Context) error { return s.client.Close() }

// objectRun follows the messages of one object. Once the object was read
// to its end and every message is settled, it finishes the object.
type objectRun struct {
	src *GCSSource
	a   *storage.ObjectAttrs

	mu       sync.Mutex
	pending  int
	read     bool // reading ended
	failed   bool // a message was not final, or reading stopped early
	finished bool
}

func (r *objectRun) add() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending++
}

// settle counts one settled message.
func (r *objectRun) settle(ctx context.Context, final bool) {
	r.mu.Lock()
	r.pending--
	if !final {
		r.failed = true
	}
	done, ok := r.done()
	r.mu.Unlock()
	if done {
		r.src.finish(ctx, r.a, ok)
	}
}

// end says reading is over: complete when the object was read to its end.
func (r *objectRun) end(ctx context.Context, complete bool) {
	r.mu.Lock()
	r.read = true
	if !complete {
		r.failed = true
	}
	done, ok := r.done()
	r.mu.Unlock()
	if done {
		r.src.finish(ctx, r.a, ok)
	}
}

// done reports, once, that the run is over and whether every message was
// final. Called with the lock held.
func (r *objectRun) done() (bool, bool) {
	if !r.read || r.pending > 0 || r.finished {
		return false, false
	}
	r.finished = true
	return true, !r.failed
}
