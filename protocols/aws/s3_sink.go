package aws

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Result headers.
const (
	headerS3Key       = "s3-key"
	headerS3ETag      = "s3-etag"
	headerS3VersionID = "s3-version-id"
)

// s3Sink is an s3 destination: one object per message (PutObject), or one
// NDJSON object per batch.
type s3Sink struct {
	name        string
	relay       string
	bucket      string
	key         *transform.Template // nil: <relay>/<yyyy>/<mm>/<dd>/<id>.json
	contentType string
	body        *transform.Template
	meta        *metadataBuilder
	client      s3API
}

func newS3Sink(ctx context.Context, spec plugin.Spec) (*s3Sink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	s, err := parseS3Sink(spec)
	if err != nil {
		return nil, err
	}
	cfg, err := loadAWS(ctx, ep.AWS, "", sinkAttempts, roleSessionName(spec))
	if err != nil {
		return nil, fail("%v", err)
	}
	s.client = newS3Client(cfg, ep.AWS != nil && ep.AWS.Endpoint != "")
	return s, nil
}

// parseS3Sink builds an s3 destination without its client.
func parseS3Sink(spec plugin.Spec) (*s3Sink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	if ep.Bucket == "" {
		return nil, fail("bucket is required: the bucket written to")
	}
	if err := static("bucket", ep.Bucket); err != nil {
		return nil, fail("%v", err)
	}
	if strings.Contains(ep.Bucket, "/") {
		return nil, fail("bucket %q is a bucket name, without a key (use key)", ep.Bucket)
	}
	s := &s3Sink{name: spec.Name, relay: relayName(spec), bucket: ep.Bucket, contentType: ep.ContentType}
	if s.key, err = optionalTemplate("key", ep.Key); err != nil {
		return nil, fail("%v", err)
	}
	if s.body, err = optionalTemplate("body", ep.Body); err != nil {
		return nil, fail("%v", err)
	}
	m := &metadataBuilder{dest: spec.Name, log: spec.Env.Log()}
	if m.headers, err = compileHeaders(ep.Headers); err != nil {
		return nil, fail("%v", err)
	}
	s.meta = m
	return s, nil
}

// Name is the destination name.
func (s *s3Sink) Name() string { return s.name }

// keyFor is the object key of an envelope: the key template, or
// <relay>/<yyyy>/<mm>/<dd>/<id><ext> by the time the message was received.
func (s *s3Sink) keyFor(env *envelope.Envelope, ext string) (string, error) {
	if s.key != nil {
		k, err := s.key.Render(env)
		if err != nil {
			return "", templateError(err)
		}
		if k = strings.TrimPrefix(strings.TrimSpace(k), "/"); k == "" {
			return "", plugin.Permanentf("key template rendered an empty key")
		}
		return k, nil
	}
	t := env.ReceivedAt
	if t.IsZero() {
		t = time.Now()
	}
	return s.relay + "/" + t.UTC().Format("2006/01/02") + "/" + env.ID + ext, nil
}

// put writes one object.
func (s *s3Sink) put(ctx context.Context, key, contentType string, body []byte, meta map[string]string) (*plugin.Result, error) {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		Metadata:      meta,
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		return nil, classify(kindS3, err)
	}
	h := map[string][]string{headerS3Key: {key}}
	if out.ETag != nil {
		h[headerS3ETag] = []string{strings.Trim(*out.ETag, `"`)}
	}
	if out.VersionId != nil {
		h[headerS3VersionID] = []string{*out.VersionId}
	}
	return &plugin.Result{Endpoint: "s3://" + s.bucket + "/" + key, Headers: h}, nil
}

// Deliver writes the message as one object: its body, content_type (else
// the message's), and its headers as metadata.
func (s *s3Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	key, err := s.keyFor(env, ".json")
	if err != nil {
		return nil, err
	}
	body, err := bodyOf(s.body, env)
	if err != nil {
		return nil, err
	}
	meta, err := s.meta.build(env, true)
	if err != nil {
		return nil, err
	}
	ct := s.contentType
	if ct == "" {
		ct = env.ContentType
	}
	return s.put(ctx, key, ct, body, meta)
}

// DeliverBatch writes a batch as one NDJSON object, keyed by its first
// message: one body per line, JSON bodies compacted onto theirs.
func (s *s3Sink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	if len(envs) == 0 {
		return &plugin.Result{}, nil
	}
	key, err := s.keyFor(envs[0], ".ndjson")
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for _, env := range envs {
		body, err := bodyOf(s.body, env)
		if err != nil {
			return nil, err
		}
		if json.Valid(body) {
			_ = json.Compact(&buf, body) // valid JSON always compacts
		} else {
			buf.Write(bytes.TrimRight(body, "\r\n"))
		}
		buf.WriteByte('\n')
	}
	meta, err := s.meta.build(envs[0], false)
	if err != nil {
		return nil, err
	}
	ct := s.contentType
	if ct == "" {
		ct = contentNDJSON
	}
	return s.put(ctx, key, ct, buf.Bytes(), meta)
}

// Health checks the bucket with HeadBucket: a cheap call that needs the
// bucket to exist and the credentials to work.
func (s *s3Sink) Health(ctx context.Context) error {
	if _, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)}); err != nil {
		return fmt.Errorf("s3 bucket %s: %w", s.bucket, classify(kindS3, err))
	}
	return nil
}

// Close has nothing to flush: every delivery is one call.
func (s *s3Sink) Close(context.Context) error { return nil }
