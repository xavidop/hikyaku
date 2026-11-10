package envelope

import (
	"encoding/json"
	"testing"
)

func BenchmarkEnvelopeWireJSON(b *testing.B) {
	e := New("in", "http", []byte(`{"id":"evt_1","type":"invoice.paid","amount":4200}`))
	e.ContentType = "application/json"
	e.SetHeader("X-Trace", "t-1")
	e.SetAttribute("kafka.partition", 3)
	b.ReportAllocs()
	for b.Loop() {
		raw, err := json.Marshal(e)
		if err != nil {
			b.Fatal(err)
		}
		var back Envelope
		if err := json.Unmarshal(raw, &back); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnvelopeClone(b *testing.B) {
	e := New("in", "http", []byte(`{"id":"evt_1","type":"invoice.paid","amount":4200}`))
	e.SetHeader("X-Trace", "t-1")
	b.ReportAllocs()
	for b.Loop() {
		_ = e.Clone()
	}
}
