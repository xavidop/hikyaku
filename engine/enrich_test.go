package engine

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A route's enrich step runs before the router, so the answer can route
// the message. The spec's egress applies to its templated URL.
func TestEnrichOnARoute(t *testing.T) {
	crm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/customers/42" {
			http.Error(w, "unknown", http.StatusNotFound)
			return
		}
		_, _ = io.WriteString(w, `{"tier":"gold"}`)
	}))
	defer crm.Close()
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: vip, type: fault }
  - { name: standard, type: fault }
dead_letter: { type: fault }
egress: { allow_private: true }
routes:
  - name: orders
    from: [in]
    transforms:
      - enrich: { url: "CRM/customers/{{ .json.customer_id }}", into: customer, cache: 1m }
    router:
      type: rules
      rules:
        - { when: 'msg.json.customer.tier == "gold"', to: [vip] }
      default: [standard]
`
	vip, standard, dlq := &scripted{name: "vip"}, &scripted{name: "standard"}, &scripted{name: "dead_letter"}
	setSinks(t, vip, standard, dlq)
	e := build(t, strings.ReplaceAll(s, "CRM", crm.URL), Options{})

	rep := e.Inject(context.Background(), msg("in", `{"id":"o-1","customer_id":42}`))
	assert.Equal(t, "delivered", rep.Status)
	require.Equal(t, 1, vip.count())
	assert.JSONEq(t, `{"id":"o-1","customer_id":42,"customer":{"tier":"gold"}}`, vip.call(0).BodyText())
	assert.Equal(t, 0, standard.count())

	// A failed lookup is a failed route transform: a dead letter that
	// dlq replay sends through the route again.
	rep = e.Inject(context.Background(), msg("in", `{"id":"o-2","customer_id":7}`))
	assert.Equal(t, "dead_lettered", rep.Status)
	assert.Contains(t, rep.Routes[0].Error, "enrich: GET")
	assert.Contains(t, rep.Routes[0].Error, "status 404")
	require.Equal(t, 1, dlq.count())
	info, _ := InfoOf(dlq.call(0))
	assert.Equal(t, ReasonTransform, info.Reason)
	assert.Equal(t, "orders", info.Route)
	assert.JSONEq(t, `{"id":"o-2","customer_id":7}`, dlq.call(0).BodyText())
}

func TestEnrichRespectsEgress(t *testing.T) {
	const s = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations: [{ name: out, type: fault }]
dead_letter: { type: fault }
routes:
  - name: orders
    from: [in]
    transforms:
      - enrich: { url: "{{ .json.lookup }}", into: answer }
    to: [out]
`
	out, dlq := &scripted{name: "out"}, &scripted{name: "dead_letter"}
	setSinks(t, out, dlq)
	e := build(t, s, Options{})
	rep := e.Inject(context.Background(), msg("in", `{"lookup":"http://127.0.0.1:9/latest/meta-data"}`))
	assert.Equal(t, "dead_lettered", rep.Status)
	assert.Contains(t, rep.Routes[0].Error, "private address")
	assert.Equal(t, 0, out.count())
}
