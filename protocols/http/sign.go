package http

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"net/http"
	"strconv"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
)

// DefaultSignatureHeader carries the signature when sign.header is unset.
const DefaultSignatureHeader = "X-Signature-256"

// signer adds an HMAC signature of the body to a request (destination
// sign:). It is the sending side of a source's auth.hmac: the same header,
// prefix, hash and encoding verify there.
type signer struct {
	secret   []byte
	header   string
	tsHeader string
	prefix   string
	newHash  func() hash.Hash
	encode   func([]byte) string
	now      func() time.Time
}

func newSigner(s *config.Sign) (*signer, error) {
	if s.Secret == "" {
		return nil, fmt.Errorf("sign.secret is required")
	}
	out := &signer{secret: []byte(s.Secret), header: s.Header, tsHeader: s.TimestampHeader, prefix: s.Prefix, now: time.Now}
	if out.header == "" {
		out.header = DefaultSignatureHeader
	}
	switch s.Algo {
	case "", "sha256":
		out.newHash = sha256.New
	case "sha512":
		out.newHash = sha512.New
	default:
		return nil, fmt.Errorf("sign.algo %q: use sha256 or sha512", s.Algo)
	}
	switch s.Encoding {
	case "", "hex":
		out.encode = hex.EncodeToString
	case "base64":
		out.encode = base64.StdEncoding.EncodeToString
	default:
		return nil, fmt.Errorf("sign.encoding %q: use hex or base64", s.Encoding)
	}
	return out, nil
}

// sign sets the signature header for body. With a timestamp header, the
// send time goes out in Unix seconds and the signature covers
// "<timestamp>.<body>", so a captured request cannot be sent again later.
func (s *signer) sign(req *http.Request, body []byte) {
	mac := hmac.New(s.newHash, s.secret)
	if s.tsHeader != "" {
		ts := strconv.FormatInt(s.now().Unix(), 10)
		req.Header.Set(s.tsHeader, ts)
		mac.Write([]byte(ts + "."))
	}
	mac.Write(body)
	req.Header.Set(s.header, s.prefix+s.encode(mac.Sum(nil)))
}
