package amqp

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

// Envelope headers that carry AMQP properties rather than table entries.
const (
	headerContentType     = "Content-Type"
	headerContentEncoding = "Content-Encoding"
	headerCorrelationID   = "Correlation-Id"
	headerReplyTo         = "Reply-To"
	headerIdempotencyKey  = "Idempotency-Key"
)

// maxShortstr is the longest AMQP short string (message ID, routing key,
// content type, ...).
const maxShortstr = 255

// headersFromTable turns an AMQP headers table into envelope headers, under
// the table's own names. Each value becomes text: numbers and booleans as
// written, timestamps in RFC 3339, byte arrays as text (base64 unless they
// are UTF-8), nested tables as JSON. An array becomes one header value per
// element; a void value is left out.
func headersFromTable(t amqp091.Table) map[string][]string {
	out := make(map[string][]string, len(t))
	for k, v := range t {
		switch x := v.(type) {
		case nil:
		case []any:
			vals := make([]string, 0, len(x))
			for _, e := range x {
				if e != nil {
					vals = append(vals, fieldText(e))
				}
			}
			if len(vals) > 0 {
				out[k] = vals
			}
		default:
			out[k] = []string{fieldText(v)}
		}
	}
	return out
}

// fieldText renders one table value as header text.
func fieldText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return bytesText(x)
	case bool:
		return strconv.FormatBool(x)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case amqp091.Decimal:
		return decimalText(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339)
	case amqp091.Table, []any:
		if b, err := json.Marshal(jsonValue(x)); err == nil {
			return string(b)
		}
	}
	return fmt.Sprint(v)
}

// jsonValue converts a table value to what encoding/json renders as the
// same text fieldText would.
func jsonValue(v any) any {
	switch x := v.(type) {
	case amqp091.Table:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = jsonValue(e)
		}
		return m
	case []any:
		a := make([]any, len(x))
		for i, e := range x {
			a[i] = jsonValue(e)
		}
		return a
	case []byte:
		return bytesText(x)
	case amqp091.Decimal:
		return json.Number(decimalText(x))
	case time.Time:
		return x.UTC().Format(time.RFC3339)
	case float32:
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return fieldText(x)
		}
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return fieldText(x)
		}
	}
	return v
}

// bytesText is a byte array as text: as is when it is UTF-8, else base64.
func bytesText(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return base64.StdEncoding.EncodeToString(b)
}

// decimalText renders an AMQP decimal: Value scaled down by Scale digits.
func decimalText(d amqp091.Decimal) string {
	return new(big.Rat).SetFrac(big.NewInt(int64(d.Value)), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(d.Scale)), nil)).FloatString(int(d.Scale))
}

// tableString returns a string table value by name, matched
// case-insensitively.
func tableString(t amqp091.Table, name string) (string, bool) {
	for k, v := range t {
		if !strings.EqualFold(k, name) {
			continue
		}
		switch x := v.(type) {
		case string:
			return x, true
		case []byte:
			return string(x), utf8.Valid(x)
		}
	}
	return "", false
}

// tableInt returns an integer table value by name, matched
// case-insensitively, such as a quorum queue's x-delivery-count.
func tableInt(t amqp091.Table, name string) (int64, bool) {
	for k, v := range t {
		if !strings.EqualFold(k, name) {
			continue
		}
		switch x := v.(type) {
		case int8:
			return int64(x), true
		case int16:
			return int64(x), true
		case int32:
			return int64(x), true
		case int64:
			return x, true
		case int:
			return int64(x), true
		case uint8:
			return int64(x), true
		case uint16:
			return int64(x), true
		case uint32:
			return int64(x), true
		}
	}
	return 0, false
}

// dropHeaders are envelope headers a destination does not copy into the
// table (keys in lower case): hop-by-hop and framing headers and the
// caller's credentials (as an http destination drops them), the headers that
// become properties, and the broker's own bookkeeping of the message it came
// from (dead-lettering history and delivery counts), which would be wrong
// on a new message.
var dropHeaders = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-authenticate": true, "proxy-authorization": true,
	"te": true, "trailer": true, "transfer-encoding": true, "upgrade": true, "host": true,
	"content-length": true, "authorization": true, "cookie": true, "accept-encoding": true,
	"content-type": true, "content-encoding": true, "correlation-id": true, "reply-to": true,
	"x-death": true, "x-delivery-count": true,
}

// dropHeader reports whether an envelope header stays out of the table.
func dropHeader(name string) bool {
	l := strings.ToLower(name)
	return dropHeaders[l] || strings.HasPrefix(l, "x-first-death-") || strings.HasPrefix(l, "x-last-death-")
}

// tableFromHeaders turns envelope headers into an AMQP headers table: one
// value as a string, several as an array of strings.
func tableFromHeaders(h map[string][]string) amqp091.Table {
	t := make(amqp091.Table, len(h))
	for k, vs := range h {
		if dropHeader(k) {
			continue
		}
		switch len(vs) {
		case 0:
		case 1:
			t[k] = vs[0]
		default:
			arr := make([]any, len(vs))
			for i, v := range vs {
				arr[i] = v
			}
			t[k] = arr
		}
	}
	return t
}

// setFold sets a table value, replacing every spelling of its name.
func setFold(t amqp091.Table, name string, v any) {
	for k := range t {
		if strings.EqualFold(k, name) {
			delete(t, k)
		}
	}
	t[name] = v
}

// hasFold reports whether a table has a value by name, matched
// case-insensitively.
func hasFold(t amqp091.Table, name string) bool {
	for k := range t {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}
