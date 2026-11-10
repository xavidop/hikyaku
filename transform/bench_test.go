package transform

import (
	"testing"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
)

// The per-message cost of each transform step (make bench).

func benchEnv() *envelope.Envelope {
	e := envelope.New("in", "http", []byte(`{"id":"evt_1","type":"invoice.paid","customer":{"id":"c42","tier":"gold"},"amount":4200,"items":[{"sku":"a"},{"sku":"b"}]}`))
	e.ContentType = "application/json"
	return e
}

func BenchmarkCELFilter(b *testing.B) {
	x, err := CompileCEL(`msg.json.type.startsWith("invoice.") && msg.json.amount > 1000`)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()
	b.ReportAllocs()
	for b.Loop() {
		if ok, err := x.Bool(env); err != nil || !ok {
			b.Fatal(ok, err)
		}
	}
}

func BenchmarkJQReshape(b *testing.B) {
	q, err := CompileJQ(`{id, kind: .type, customer: .customer.id, skus: [.items[].sku]}`)
	if err != nil {
		b.Fatal(err)
	}
	in, err := benchEnv().JSON()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := q.Run(in); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTemplateRender(b *testing.B) {
	t, err := CompileTemplate("body", `{"id":"{{ .json.id }}","customer":"{{ .json.customer.id }}","tier":"{{ .json.customer.tier }}"}`)
	if err != nil {
		b.Fatal(err)
	}
	env := benchEnv()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := t.Render(env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkChainFilterJQHeaders(b *testing.B) {
	c, err := Compile("bench", []config.Transform{
		{Filter: `msg.json.type == "invoice.paid"`},
		{JQ: `{id, customer: .customer.id}`},
		{Headers: &config.HeaderOps{Set: map[string]string{"X-Customer": "{{ .json.customer }}"}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		env := benchEnv()
		if keep, err := c.Apply(env); err != nil || !keep {
			b.Fatal(keep, err)
		}
	}
}
