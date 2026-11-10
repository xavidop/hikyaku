package grpc

import (
	"encoding/base64"
	"fmt"
	"net/textproto"
	"strings"

	"google.golang.org/grpc/metadata"
)

// reservedKey reports a metadata key gRPC sets itself or HTTP/2 forbids.
func reservedKey(k string) bool {
	if strings.HasPrefix(k, ":") || strings.HasPrefix(k, "grpc-") {
		return true
	}
	switch k {
	case "content-type", "user-agent", "te", "connection", "keep-alive", "proxy-connection",
		"transfer-encoding", "upgrade", "host":
		return true
	}
	return false
}

// validKey reports a key gRPC accepts: 0-9, a-z, -, _ and . only.
func validKey(k string) bool {
	if k == "" {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

// validValue reports a value gRPC sends under key: any bytes under a binary
// (-bin) key, else printable ASCII.
func validValue(k, v string) bool {
	if strings.HasSuffix(k, "-bin") {
		return true
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 0x20 || v[i] > 0x7e {
			return false
		}
	}
	return true
}

// headerKey checks a configured header name and returns its metadata key.
func headerKey(name string) (string, error) {
	k := strings.ToLower(strings.TrimSpace(name))
	switch {
	case !validKey(k):
		return "", fmt.Errorf("headers.%s: a gRPC metadata key holds only lowercase letters, digits and the characters - _ . (no others)", name)
	case reservedKey(k):
		return "", fmt.Errorf("headers.%s: %s is set by gRPC itself", name, k)
	}
	return k, nil
}

// metadataValue turns a header value into what gRPC sends: a -bin value is
// base64 in a header and raw bytes in metadata.
func metadataValue(k, v string) (string, error) {
	if strings.HasSuffix(k, "-bin") {
		b, err := decodeBin(v)
		if err != nil {
			return "", fmt.Errorf("the value of %s is not base64: %v", k, err)
		}
		return string(b), nil
	}
	if !validValue(k, v) {
		return "", fmt.Errorf("the value of %s is not printable ASCII", k)
	}
	return v, nil
}

func decodeBin(v string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(v); err == nil {
		return b, nil
	}
	return base64.RawStdEncoding.DecodeString(v)
}

// headersOf turns metadata into message headers under canonical names.
// Pseudo-headers, content-type and gRPC's own keys are left out, and binary
// (-bin) values are base64-encoded, as on the wire.
func headersOf(md metadata.MD) map[string][]string {
	out := make(map[string][]string, len(md))
	for k, vs := range md {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, ":") || strings.HasPrefix(k, "grpc-") || k == "content-type" {
			continue
		}
		ck := textproto.CanonicalMIMEHeaderKey(k)
		bin := strings.HasSuffix(k, "-bin")
		for _, v := range vs {
			if bin {
				v = base64.StdEncoding.EncodeToString([]byte(v))
			}
			out[ck] = append(out[ck], v)
		}
	}
	return out
}

// dropForward are headers that are not passed on as metadata: credentials,
// framing, and the idempotency key, which is set on its own.
var dropForward = map[string]bool{
	"authorization": true, "proxy-authorization": true, "proxy-authenticate": true, "cookie": true,
	"set-cookie": true, "content-length": true, "accept-encoding": true, "trailer": true,
	"idempotency-key": true,
}

// forwarded is the metadata made of headers (a message's, or a reply's):
// those gRPC accepts, without credentials, framing or gRPC's own keys.
func forwarded(h map[string][]string) metadata.MD {
	md := metadata.MD{}
	for name, vs := range h {
		k := strings.ToLower(name)
		if !validKey(k) || reservedKey(k) || dropForward[k] {
			continue
		}
		for _, v := range vs {
			if mv, err := metadataValue(k, v); err == nil {
				md.Append(k, mv)
			}
		}
	}
	return md
}

// staticMetadata turns a subscribe source's headers into the metadata of
// its call. They are static: there is no message to render a template for.
func staticMetadata(headers map[string]string) (metadata.MD, error) {
	md := metadata.MD{}
	for name, v := range headers {
		if strings.Contains(v, "{{") {
			return nil, fmt.Errorf("headers.%s: the metadata of a subscribe call is static; a per-message template has no message to render", name)
		}
		k, err := headerKey(name)
		if err != nil {
			return nil, err
		}
		mv, err := metadataValue(k, v)
		if err != nil {
			return nil, fmt.Errorf("headers.%s: %v", name, err)
		}
		md.Append(k, mv)
	}
	return md, nil
}

// first is the first value of a metadata key, "" when it has none.
func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}
