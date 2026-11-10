package aws

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/textproto"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Limits of SQS and SNS messages.
const (
	maxMessageAttributes = 10
	maxAttributeName     = 256
	maxFIFOID            = 128
	maxS3Metadata        = 2 << 10 // user metadata bytes S3 accepts
	attrTypeString       = "String"
)

// dropHeaders are not passed on as message attributes or object metadata:
// hop-by-hop headers, framing, and the caller's own credentials (as the
// http destination drops them). Content-Type travels as content-type.
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Set-Cookie": true,
	"Accept-Encoding": true, "Content-Type": true, "Content-Transfer-Encoding": true,
}

// attribute is one message attribute (or metadata entry), a string.
type attribute struct {
	name  string
	value string
}

// outMessage is an SQS or SNS message ready to send.
type outMessage struct {
	body  string
	attrs []attribute
	// size is what counts toward a batch's 256KiB: the body and the
	// attributes' names, types and values.
	size int
}

// messageBuilder turns envelopes into SQS or SNS messages: the body (text,
// or base64 with content-transfer-encoding: base64), and at most 10
// attributes: content-transfer-encoding, content-type, Idempotency-Key, the
// headers block, then the message's headers in name order.
type messageBuilder struct {
	service string // sqs or sns
	dest    string
	body    *transform.Template
	headers []headerTemplate
	log     *slog.Logger

	limitOnce   sync.Once
	invalidOnce sync.Once
}

// templateError is the failure of a per-message template: the message
// cannot be built, so retrying will not help.
func templateError(err error) error {
	return plugin.Wrap(plugin.Permanent, []string{"template"}, err)
}

// bodyOf is what a destination sends: the body template's rendering, or
// the message body.
func bodyOf(t *transform.Template, env *envelope.Envelope) ([]byte, error) {
	if t == nil {
		return env.Body, nil
	}
	s, err := t.Render(env)
	if err != nil {
		return nil, templateError(err)
	}
	return []byte(s), nil
}

func (b *messageBuilder) build(env *envelope.Envelope) (outMessage, error) {
	raw, err := bodyOf(b.body, env)
	if err != nil {
		return outMessage{}, err
	}
	if len(raw) == 0 {
		return outMessage{}, plugin.Wrap(plugin.Permanent, []string{plugin.Tag4xx},
			fmt.Errorf("%s cannot carry an empty message body", b.service))
	}
	var m outMessage
	var attrs []attribute
	if textSafe(raw) {
		m.body = string(raw)
	} else {
		m.body = base64.StdEncoding.EncodeToString(raw)
		attrs = append(attrs, attribute{attrTransferEncoder, encodingBase64})
	}
	configured := make([]attribute, 0, len(b.headers))
	for _, h := range b.headers {
		v, err := h.tmpl.Render(env)
		if err != nil {
			return outMessage{}, templateError(err)
		}
		if v != "" && !strings.EqualFold(h.name, attrTransferEncoder) {
			configured = append(configured, attribute{h.name, v})
		}
	}
	take := func(name, fallback string) attribute {
		for i, a := range configured {
			if strings.EqualFold(a.name, name) {
				configured = append(configured[:i], configured[i+1:]...)
				return attribute{name, a.value}
			}
		}
		return attribute{name, fallback}
	}
	if ct := take(attrContentType, env.ContentType); ct.value != "" {
		attrs = append(attrs, ct)
	}
	attrs = append(attrs, take(attrIdempotencyKey, idempotencyKey(env)))
	attrs = append(attrs, configured...)
	attrs = append(attrs, headerAttributes(env)...)
	m.attrs = b.limit(attrs)
	m.size = len(m.body)
	for _, a := range m.attrs {
		m.size += len(a.name) + len(attrTypeString) + len(a.value)
	}
	return m, nil
}

// headerAttributes are a message's headers in name order, first values,
// without the ones that do not travel.
func headerAttributes(env *envelope.Envelope) []attribute {
	names := make([]string, 0, len(env.Headers))
	for k, v := range env.Headers {
		if len(v) > 0 && !dropHeaders[textproto.CanonicalMIMEHeaderKey(k)] {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	out := make([]attribute, len(names))
	for i, k := range names {
		out[i] = attribute{k, env.Headers[k][0]}
	}
	return out
}

// limit keeps the first attribute of each name (case-insensitively), drops
// the ones SQS or SNS would refuse, and keeps 10. Dropping is logged once
// per destination and reason.
func (b *messageBuilder) limit(attrs []attribute) []attribute {
	out := make([]attribute, 0, min(len(attrs), maxMessageAttributes))
	var invalid, over []string
	seen := map[string]bool{}
	for _, a := range attrs {
		key := strings.ToLower(a.name)
		switch {
		case seen[key]:
			continue
		case !validAttrName(a.name) || a.value == "" || !textSafe([]byte(a.value)):
			invalid = append(invalid, a.name)
			continue
		case len(out) == maxMessageAttributes:
			over = append(over, a.name)
			continue
		}
		seen[key] = true
		out = append(out, a)
	}
	if len(invalid) > 0 {
		b.invalidOnce.Do(func() {
			b.log.Warn(b.service+" destination drops headers it cannot send as message attributes (bad name or value); logged once",
				"destination", b.dest, "headers", invalid)
		})
	}
	if len(over) > 0 {
		b.limitOnce.Do(func() {
			b.log.Warn(b.service+" destination drops headers over the 10 message attributes allowed; logged once",
				"destination", b.dest, "headers", over)
		})
	}
	return out
}

// validAttrName reports whether SQS and SNS accept a message attribute
// name: up to 256 of A-Z a-z 0-9 _ - ., not starting with AWS. or Amazon.,
// and without a leading, trailing or doubled period.
func validAttrName(n string) bool {
	if n == "" || len(n) > maxAttributeName || n[0] == '.' || n[len(n)-1] == '.' || strings.Contains(n, "..") {
		return false
	}
	lower := strings.ToLower(n)
	if strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.") {
		return false
	}
	for i := 0; i < len(n); i++ {
		if c := n[i]; !alnum(c) && c != '_' && c != '-' && c != '.' {
			return false
		}
	}
	return true
}

func alnum(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' }

// textSafe reports whether SQS and SNS can carry b as text: valid UTF-8
// holding only the characters they accept.
func textSafe(b []byte) bool {
	for i := 0; i < len(b); {
		r, n := utf8.DecodeRune(b[i:])
		if r == utf8.RuneError && n <= 1 || !xmlChar(r) {
			return false
		}
		i += n
	}
	return true
}

// xmlChar reports the characters SQS and SNS accept: #x9, #xA, #xD, #x20
// to #xD7FF, #xE000 to #xFFFD and #x10000 to #x10FFFF.
func xmlChar(r rune) bool {
	return r == 0x9 || r == 0xA || r == 0xD || r >= 0x20 && r <= 0xD7FF ||
		r >= 0xE000 && r <= 0xFFFD || r >= 0x10000 && r <= 0x10FFFF
}

// idempotencyKey is the message's idempotency key, as the engine set it on
// this delivery (the Idempotency-Key header), else the envelope ID.
func idempotencyKey(env *envelope.Envelope) string {
	if k := env.Header(attrIdempotencyKey); k != "" {
		return k
	}
	if v, ok := env.Attribute("idempotency_key"); ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return env.ID
}

// fifoID makes a value fit a FIFO message group or deduplication ID (1 to
// 128 printable ASCII characters): as it is when it fits, else its SHA-256.
func fifoID(s string) string {
	ok := s != "" && len(s) <= maxFIFOID
	for i := 0; ok && i < len(s); i++ {
		ok = s[i] > 0x20 && s[i] < 0x7f
	}
	if ok {
		return s
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// groupID is a message's group: the group_id template's rendering, else
// the envelope source (else the relay's name).
func groupID(t *transform.Template, env *envelope.Envelope, fallback string) (string, error) {
	g := ""
	if t != nil {
		v, err := t.Render(env)
		if err != nil {
			return "", templateError(err)
		}
		g = strings.TrimSpace(v)
	}
	if g == "" {
		g = env.Source
	}
	if g == "" {
		g = fallback
	}
	return fifoID(g), nil
}

func sqsAttributes(attrs []attribute) map[string]sqstypes.MessageAttributeValue {
	out := make(map[string]sqstypes.MessageAttributeValue, len(attrs))
	for _, a := range attrs {
		v := a.value
		out[a.name] = sqstypes.MessageAttributeValue{DataType: strPtr(attrTypeString), StringValue: &v}
	}
	return out
}

func snsAttributes(attrs []attribute) map[string]snstypes.MessageAttributeValue {
	out := make(map[string]snstypes.MessageAttributeValue, len(attrs))
	for _, a := range attrs {
		v := a.value
		out[a.name] = snstypes.MessageAttributeValue{DataType: strPtr(attrTypeString), StringValue: &v}
	}
	return out
}

func strPtr(s string) *string { return &s }

// chunk splits messages into batch calls of at most maxN entries and
// maxBytes. A message larger than maxBytes goes alone, and the service
// decides.
func chunk[T any](items []T, size func(T) int, maxN, maxBytes int) [][]T {
	var out [][]T
	var cur []T
	curBytes := 0
	for _, it := range items {
		n := size(it)
		if len(cur) > 0 && (len(cur) == maxN || curBytes+n > maxBytes) {
			out = append(out, cur)
			cur, curBytes = nil, 0
		}
		cur = append(cur, it)
		curBytes += n
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// metadataBuilder turns envelope headers into S3 user metadata (x-amz-meta-*):
// Idempotency-Key, the headers block, then the message's headers in name
// order, within the 2KiB S3 accepts. Dropping is logged once per reason.
type metadataBuilder struct {
	dest    string
	headers []headerTemplate
	log     *slog.Logger

	limitOnce   sync.Once
	invalidOnce sync.Once
}

func (b *metadataBuilder) build(env *envelope.Envelope, withMessageHeaders bool) (map[string]string, error) {
	attrs := []attribute{{attrIdempotencyKey, idempotencyKey(env)}}
	var configured []attribute
	for _, h := range b.headers {
		v, err := h.tmpl.Render(env)
		if err != nil {
			return nil, templateError(err)
		}
		if v != "" {
			configured = append(configured, attribute{h.name, v})
		}
	}
	attrs = append(configured, attrs...) // a configured Idempotency-Key wins
	if withMessageHeaders {
		attrs = append(attrs, headerAttributes(env)...)
	}
	out := map[string]string{}
	seen := map[string]bool{}
	used := 0
	var invalid, over []string
	for _, a := range attrs {
		key := strings.ToLower(a.name)
		switch {
		case seen[key]:
			continue
		case !validMetadata(a.name, a.value):
			invalid = append(invalid, a.name)
			continue
		case used+len(a.name)+len(a.value) > maxS3Metadata:
			over = append(over, a.name)
			continue
		}
		seen[key] = true
		used += len(a.name) + len(a.value)
		out[key] = a.value
	}
	if len(invalid) > 0 {
		b.invalidOnce.Do(func() {
			b.log.Warn("s3 destination drops headers it cannot send as object metadata (bad name, or a value that is not printable ASCII); logged once",
				"destination", b.dest, "headers", invalid)
		})
	}
	if len(over) > 0 {
		b.limitOnce.Do(func() {
			b.log.Warn("s3 destination drops headers over the 2KiB of object metadata allowed; logged once",
				"destination", b.dest, "headers", over)
		})
	}
	return out, nil
}

// validMetadata reports whether S3 takes a metadata entry: a header-name
// token and a printable ASCII value.
func validMetadata(name, value string) bool {
	if name == "" || value == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if c := name[i]; !alnum(c) && strings.IndexByte("!#$%&'*+-.^_`|~", c) < 0 {
			return false
		}
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}
