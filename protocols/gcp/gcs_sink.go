package gcp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"log/slog"
	"net/http"
	"net/textproto"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
)

// Result headers of a gcs delivery.
const (
	headerGeneration = "gcs-generation"
	headerExisting   = "gcs-existing"
)

var crc32c = crc32.MakeTable(crc32.Castagnoli)

// GCSSink is a gcs destination: one object per message, one NDJSON object
// per batch.
type GCSSink struct {
	name   string
	relay  string
	o      gcsSinkOptions
	client *storage.Client
	bkt    *storage.BucketHandle
	log    *slog.Logger

	mu     sync.Mutex
	closed bool
}

func newGCSSink(ctx context.Context, spec plugin.Spec) (*GCSSink, error) {
	o, err := parseGCSSink(spec)
	if err != nil {
		return nil, err
	}
	client, err := newStorageClient(ctx, o.client)
	if err != nil {
		return nil, fmt.Errorf("destination %s: storage client: %w", spec.Name, err)
	}
	// One attempt per delivery: the relay owns retries.
	client.SetRetry(storage.WithPolicy(storage.RetryNever))
	relay := spec.Env.Relay
	if relay == "" {
		relay = "hikyaku"
	}
	return &GCSSink{name: spec.Name, relay: relay, o: o, client: client, bkt: client.Bucket(o.bucket), log: spec.Env.Log()}, nil
}

// Name is the destination name.
func (s *GCSSink) Name() string { return s.name }

// Deliver writes the message as one object: key (default
// <relay>/<yyyy>/<mm>/<dd>/<id>.json), the content_type (default the
// message's), and the headers as metadata.
func (s *GCSSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	key, err := s.key(env, env.ID, ".json")
	if err != nil {
		return nil, err
	}
	body, err := s.body(env)
	if err != nil {
		return nil, err
	}
	ct := s.o.contentType
	if ct == "" {
		ct = env.ContentType
	}
	meta, err := s.metadata(env, true)
	if err != nil {
		return nil, err
	}
	return s.write(ctx, key, body, ct, meta)
}

// DeliverBatch writes the messages as one NDJSON object, one body per
// line. Its default key ends in <first id>-<hash of the ids>.ndjson; a key
// template renders against the first message, as does the headers: block,
// the only metadata of a batch.
func (s *GCSSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	if len(envs) == 0 {
		return &plugin.Result{}, nil
	}
	key, err := s.key(envs[0], batchID(envs), ".ndjson")
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for _, env := range envs {
		body, err := s.body(env)
		if err != nil {
			return nil, err
		}
		writeLine(&buf, body)
	}
	ct := s.o.contentType
	if ct == "" {
		ct = contentNDJSON
	}
	meta, err := s.metadata(envs[0], false)
	if err != nil {
		return nil, err
	}
	return s.write(ctx, key, buf.Bytes(), ct, meta)
}

// write creates the object unless it exists. An existing object is this
// delivery's earlier attempt (the key is unique per message), so it is a
// success.
func (s *GCSSink) write(ctx context.Context, key string, body []byte, ct string, meta map[string]string) (*plugin.Result, error) {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return nil, plugin.Wrap(plugin.Transient, nil, errClosed)
	}
	uri := "gs://" + s.o.bucket + "/" + key
	w := s.bkt.Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	// One request and no buffer: a retry is the relay's.
	w.ChunkSize = 0
	w.ContentType = ct
	w.Metadata = meta
	w.CRC32C = crc32.Checksum(body, crc32c)
	w.SendCRC32C = true
	_, werr := w.Write(body)
	err := w.Close()
	if werr != nil && err == nil {
		err = werr
	}
	if err != nil {
		if isPrecondition(err) {
			s.log.Debug("gcs object already written; taken as an earlier attempt", "destination", s.name, "object", uri)
			return &plugin.Result{Endpoint: uri, Headers: map[string][]string{headerExisting: {"true"}}}, nil
		}
		return nil, gcsError("write "+uri, err)
	}
	res := &plugin.Result{Endpoint: uri}
	if a := w.Attrs(); a != nil {
		res.Headers = map[string][]string{headerGeneration: {strconv.FormatInt(a.Generation, 10)}}
	}
	return res, nil
}

// key renders the key template, or builds the default key from the
// message's receive date, which a retry keeps.
func (s *GCSSink) key(env *envelope.Envelope, id, ext string) (string, error) {
	if s.o.key != nil {
		k, err := s.o.key.Render(env)
		if err != nil {
			return "", plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		if strings.TrimSpace(k) == "" {
			return "", plugin.Errorf(plugin.Permanent, []string{tagTemplate}, "key rendered empty")
		}
		return k, nil
	}
	t := env.ReceivedAt
	if t.IsZero() {
		t = time.Now()
	}
	t = t.UTC()
	return fmt.Sprintf("%s/%04d/%02d/%02d/%s%s", s.relay, t.Year(), int(t.Month()), t.Day(), id, ext), nil
}

// body is the message body, or the body template rendered.
func (s *GCSSink) body(env *envelope.Envelope) ([]byte, error) {
	if s.o.body == nil {
		return env.Body, nil
	}
	b, err := s.o.body.Render(env)
	if err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
	}
	return []byte(b), nil
}

// metadata is the message's headers (forward), but the dropped ones and
// Content-Type, then the headers: block.
func (s *GCSSink) metadata(env *envelope.Envelope, forward bool) (map[string]string, error) {
	meta := map[string]string{}
	if forward {
		for k, vs := range env.Headers {
			ck := textproto.CanonicalMIMEHeaderKey(k)
			if len(vs) == 0 || dropHeaders[ck] || ck == "Content-Type" {
				continue
			}
			meta[k] = strings.Join(vs, ", ")
		}
	}
	for k, t := range s.o.headers {
		v, err := t.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{tagTemplate}, err)
		}
		meta[k] = v
	}
	if len(meta) == 0 {
		return nil, nil
	}
	return meta, nil
}

// batchID names a batch: its first message's ID and a hash of every ID, so
// a retry of the same batch writes the same key and another batch does not.
func batchID(envs []*envelope.Envelope) string {
	h := sha256.New()
	for _, env := range envs {
		h.Write([]byte(env.ID))
		h.Write([]byte{'\n'})
	}
	return envs[0].ID + "-" + hex.EncodeToString(h.Sum(nil))[:12]
}

// writeLine writes a body as one NDJSON line: JSON compacted, anything else
// as it is, without its trailing newlines.
func writeLine(buf *bytes.Buffer, body []byte) {
	body = bytes.TrimRight(body, "\r\n")
	if !json.Valid(body) || json.Compact(buf, body) != nil {
		buf.Write(body)
	}
	buf.WriteByte('\n')
}

// Health checks that the bucket exists. Credentials that may write objects
// but not read the bucket's metadata (403) count as healthy.
func (s *GCSSink) Health(ctx context.Context) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return errClosed
	}
	_, err := s.bkt.Attrs(ctx)
	var ge *googleapi.Error
	switch {
	case err == nil:
		return nil
	case errors.Is(err, storage.ErrBucketNotExist):
		return fmt.Errorf("bucket %s does not exist", s.o.bucket)
	case errors.As(err, &ge) && ge.Code == http.StatusForbidden:
		return nil
	}
	return gcsError("bucket "+s.o.bucket, err)
}

// Close closes the client. Every write is synchronous, so nothing is
// pending.
func (s *GCSSink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.client.Close()
}
