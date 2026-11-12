package http

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// errTooLarge is a decoded body over the source's max_body_bytes.
var errTooLarge = errors.New("the decoded body is over max_body_bytes")

var gzipWriters = sync.Pool{New: func() any { return gzip.NewWriter(io.Discard) }}

// gzipBody compresses a request body (destination compression: gzip).
func gzipBody(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(len(body)/4 + 64)
	zw := gzipWriters.Get().(*gzip.Writer)
	defer gzipWriters.Put(zw)
	zw.Reset(&buf)
	if _, err := zw.Write(body); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeBody undoes a request's Content-Encoding. It reports whether it
// decoded anything: identity (or no encoding) is returned as it came. The
// decoded body may hold at most limit bytes, so a small request cannot
// expand into a huge message.
func decodeBody(encoding string, body []byte, limit int64) ([]byte, bool, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return body, false, nil
	case "gzip", "x-gzip":
	default:
		return nil, false, fmt.Errorf("content encoding %q is not supported (gzip is)", encoding)
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("the body is not gzip: %w", err)
	}
	defer func() { _ = zr.Close() }()
	out, err := io.ReadAll(io.LimitReader(zr, limit+1))
	if err != nil {
		return nil, false, fmt.Errorf("the body is not gzip: %w", err)
	}
	if int64(len(out)) > limit {
		return nil, false, errTooLarge
	}
	return out, true, nil
}
