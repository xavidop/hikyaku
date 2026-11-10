package redis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"net/textproto"
	"slices"
	"strconv"
	"strings"

	"github.com/xavidop/hikyaku/envelope"
)

// The fields of a stream entry a destination writes and a source reads.
const (
	fieldID          = "id"
	fieldBody        = "body"
	fieldContentType = "content_type"
	fieldSubject     = "subject"
	// headerPrefix starts one field per header: h:<Header-Name>.
	headerPrefix = "h:"
)

// dropHeaders are not written to a stream entry: hop-by-hop headers,
// framing, the caller's own credentials, and Content-Type, which the
// content_type field carries.
var dropHeaders = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	"Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true, "Host": true,
	"Content-Length": true, "Authorization": true, "Cookie": true, "Accept-Encoding": true,
	"Content-Type": true,
}

// encodeEntry returns the fields of the stream entry for env, as XADD's
// field-value pairs: id, body, content_type and subject (each when set),
// then h:<Header> for each header, sorted by name, its values joined with
// ", ". extra holds headers that replace the message's own of the same
// name (matched case-insensitively).
func encodeEntry(env *envelope.Envelope, body []byte, extra map[string]string) []any {
	vals := []any{fieldID, env.ID, fieldBody, body}
	if env.ContentType != "" {
		vals = append(vals, fieldContentType, env.ContentType)
	}
	if env.Subject != "" {
		vals = append(vals, fieldSubject, env.Subject)
	}
	headers := make(map[string]string, len(env.Headers)+len(extra))
	for k, vs := range env.Headers {
		if len(vs) == 0 || k == "" || dropHeaders[textproto.CanonicalMIMEHeaderKey(k)] {
			continue
		}
		headers[k] = strings.Join(vs, ", ")
	}
	for k, v := range extra {
		for h := range headers {
			if strings.EqualFold(h, k) {
				delete(headers, h)
			}
		}
		headers[k] = v
	}
	for _, k := range slices.Sorted(maps.Keys(headers)) {
		vals = append(vals, headerPrefix+k, headers[k])
	}
	return vals
}

// entry is a stream entry turned back into the parts of an envelope.
type entry struct {
	id          string
	subject     string
	contentType string
	headers     map[string][]string
	body        []byte
	// foreign: the entry has no body field, so it was not written in
	// hikyaku's format; its body is the JSON object of all its fields.
	foreign bool
}

// decodeEntry reads a stream entry. An entry with a body field is in
// hikyaku's format: the ID is its id field, the subject its subject field
// (else the stream key), the content type its content_type field (else
// sniffed from the body), and every h:<Header> field one header. Any other
// entry, written by another producer, becomes a JSON object of all its
// fields (string values) with the ID <stream>-<entry ID>: its fields are
// data, and an id among them names something else.
func decodeEntry(stream, entryID string, values map[string]any) entry {
	e := entry{id: stream + "-" + entryID, subject: stream, headers: map[string][]string{}}
	raw, ok := values[fieldBody]
	if !ok {
		fields := make(map[string]string, len(values))
		for k, v := range values {
			fields[k] = text(v)
		}
		e.body, e.contentType, e.foreign = jsonObject(fields), contentJSON, true
		return e
	}
	e.body = []byte(text(raw))
	if v := text(values[fieldID]); v != "" {
		e.id = v
	}
	if v := text(values[fieldSubject]); v != "" {
		e.subject = v
	}
	if e.contentType = text(values[fieldContentType]); e.contentType == "" {
		e.contentType = sniff(e.body)
	}
	for k, v := range values {
		if name, ok := strings.CutPrefix(k, headerPrefix); ok && name != "" {
			e.headers[name] = []string{text(v)}
		}
	}
	return e
}

// jsonObject encodes fields as one compact JSON object, keys sorted,
// without HTML escaping.
func jsonObject(fields map[string]string) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(fields); err != nil {
		return []byte("{}") // a map of strings always encodes
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// text is a stream field value as a string. go-redis returns strings; any
// other type is printed.
func text(v any) string {
	switch v := v.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

// idLess orders stream entry IDs (<ms>-<seq>) numerically.
func idLess(a, b string) bool {
	am, as := splitID(a)
	bm, bs := splitID(b)
	if am != bm {
		return am < bm
	}
	return as < bs
}

func splitID(id string) (ms, seq uint64) {
	m, s, _ := strings.Cut(id, "-")
	ms, _ = strconv.ParseUint(m, 10, 64)
	seq, _ = strconv.ParseUint(s, 10, 64)
	return ms, seq
}
