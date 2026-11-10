package grpc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestHeadersOf(t *testing.T) {
	md := metadata.MD{
		":authority":           {"x"},
		"content-type":         {"application/grpc"},
		"grpc-accept-encoding": {"gzip"},
		"user-agent":           {"grpc-go/1.85"},
		"x-tenant":             {"acme", "beta"},
		"trace-bin":            {"\x01\x02"},
	}
	h := headersOf(md)
	assert.Equal(t, map[string][]string{
		"User-Agent": {"grpc-go/1.85"},
		"X-Tenant":   {"acme", "beta"},
		"Trace-Bin":  {"AQI="},
	}, h)
}

func TestForwarded(t *testing.T) {
	md := forwarded(map[string][]string{
		"X-Tenant":        {"acme"},
		"Authorization":   {"Bearer caller"},
		"Cookie":          {"c=1"},
		"Idempotency-Key": {"k"},
		"Content-Type":    {"application/json"},
		"Grpc-Timeout":    {"1S"},
		"Connection":      {"close"},
		"Bad Key":         {"x"},
		"X-Bin":           {"AQI="},
		"Y-Bin":           {"%%%"},
		"X-Utf8":          {"café"},
	})
	assert.Equal(t, metadata.MD{"x-tenant": {"acme"}, "x-bin": {"\x01\x02"}}, md)
}

func TestHeaderKey(t *testing.T) {
	k, err := headerKey(" X-Request-ID ")
	require.NoError(t, err)
	assert.Equal(t, "x-request-id", k)
	for _, bad := range []string{"", "x y", "x:y", "é", "grpc-status", "Content-Type", "TE", ":path", "Host", "user-agent"} {
		_, err := headerKey(bad)
		assert.Error(t, err, bad)
	}
}

func TestStaticMetadata(t *testing.T) {
	md, err := staticMetadata(map[string]string{"Authorization": "Bearer t", "x-trace-bin": "AQI="})
	require.NoError(t, err)
	assert.Equal(t, metadata.MD{"authorization": {"Bearer t"}, "x-trace-bin": {"\x01\x02"}}, md)
	_, err = staticMetadata(map[string]string{"x-trace-bin": "%%%"})
	assert.ErrorContains(t, err, "headers.x-trace-bin: the value of x-trace-bin is not base64")
}

func TestFirst(t *testing.T) {
	assert.Equal(t, "a", first(metadata.Pairs("k", "a", "k", "b"), "k"))
	assert.Equal(t, "", first(nil, "k"))
}
