package amqp

import (
	"math"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

func TestHeadersFromTable(t *testing.T) {
	ts := time.Date(2026, 11, 9, 18, 0, 0, 0, time.UTC)
	h := headersFromTable(amqp091.Table{
		"x-string":  "v",
		"x-bytes":   []byte("text"),
		"x-binary":  []byte{0xff, 0x00},
		"x-bool":    true,
		"x-int8":    int8(-8),
		"x-uint8":   uint8(8),
		"x-int16":   int16(-16),
		"x-uint16":  uint16(16),
		"x-int32":   int32(-32),
		"x-uint32":  uint32(32),
		"x-int64":   int64(1) << 40,
		"x-float32": float32(1.5),
		"x-float64": 2.25,
		"x-decimal": amqp091.Decimal{Scale: 2, Value: 12345},
		"x-time":    ts,
		"x-table":   amqp091.Table{"a": int32(1), "b": amqp091.Table{"c": "d"}, "e": []byte("f"), "t": ts, "n": math.NaN()},
		"x-array":   []any{"a", int32(2), amqp091.Table{"k": "v"}, nil},
		"x-empty":   []any{},
		"x-void":    nil,
	})
	assert.Equal(t, map[string][]string{
		"x-string":  {"v"},
		"x-bytes":   {"text"},
		"x-binary":  {"/wA="},
		"x-bool":    {"true"},
		"x-int8":    {"-8"},
		"x-uint8":   {"8"},
		"x-int16":   {"-16"},
		"x-uint16":  {"16"},
		"x-int32":   {"-32"},
		"x-uint32":  {"32"},
		"x-int64":   {"1099511627776"},
		"x-float32": {"1.5"},
		"x-float64": {"2.25"},
		"x-decimal": {"123.45"},
		"x-time":    {"2026-11-09T18:00:00Z"},
		"x-table":   {`{"a":1,"b":{"c":"d"},"e":"f","n":"NaN","t":"2026-11-09T18:00:00Z"}`},
		"x-array":   {"a", "2", `{"k":"v"}`},
	}, h)
	assert.Equal(t, "-0.5", decimalText(amqp091.Decimal{Scale: 1, Value: -5}))
	assert.Equal(t, "7", decimalText(amqp091.Decimal{Value: 7}))
}

func TestTableFromHeaders(t *testing.T) {
	tbl := tableFromHeaders(map[string][]string{
		"X-One":               {"1"},
		"X-Many":              {"a", "b"},
		"X-None":              {},
		"Authorization":       {"Bearer secret"},
		"Cookie":              {"c"},
		"Content-Type":        {"application/json"},
		"Correlation-Id":      {"c-1"},
		"x-death":             {"[]"},
		"X-Delivery-Count":    {"3"},
		"x-first-death-queue": {"q"},
		"x-last-death-reason": {"rejected"},
	})
	assert.Equal(t, amqp091.Table{"X-One": "1", "X-Many": []any{"a", "b"}}, tbl)
	assert.NoError(t, tbl.Validate())
}

func TestTableLookups(t *testing.T) {
	tbl := amqp091.Table{"idempotency-key": "k-1", "X-Delivery-Count": int64(2), "raw": []byte{0xff}, "n": "x"}
	v, ok := tableString(tbl, "Idempotency-Key")
	assert.True(t, ok)
	assert.Equal(t, "k-1", v)
	_, ok = tableString(tbl, "raw")
	assert.False(t, ok, "not UTF-8")
	_, ok = tableString(tbl, "missing")
	assert.False(t, ok)
	n, ok := tableInt(tbl, "x-delivery-count")
	assert.True(t, ok)
	assert.Equal(t, int64(2), n)
	_, ok = tableInt(tbl, "n")
	assert.False(t, ok)

	setFold(tbl, "IDEMPOTENCY-KEY", "k-2")
	assert.Equal(t, "k-2", tbl["IDEMPOTENCY-KEY"])
	_, old := tbl["idempotency-key"]
	assert.False(t, old)
	assert.True(t, hasFold(tbl, "idempotency-key"))
	assert.False(t, hasFold(tbl, "nope"))
}

func TestDetectContentType(t *testing.T) {
	assert.Equal(t, contentJSON, detectContentType([]byte(`{"a":1}`)))
	assert.Equal(t, contentText, detectContentType([]byte("hello")))
	assert.Equal(t, contentBinary, detectContentType([]byte{0xff, 0xfe}))
}
