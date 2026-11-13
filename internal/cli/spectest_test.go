package cli_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/cli"
)

// testSpec has what a test file can check: a filter, a route transform, a
// destination transform, a fallback, a shadow, an AI route, and a secret
// nothing can resolve here.
const testSpec = `
version: 1
name: cli
secrets: { CRM_KEY: "env:NOT_SET_ANYWHERE" }
sources:
  - { name: in, type: stdin }
  - { name: chat, type: stdin }
destinations:
  - name: billing
    type: http
    url: https://billing.example.com/hook
    headers: { Authorization: "Bearer {{secrets.CRM_KEY}}" }
    retry: { attempts: 3, backoff: 1h }
    fallback: [spool]
    transforms:
      - jq: '{id: .id, cents: (.amount * 100)}'
      - headers: { set: { X-Kind: payment } }
  - { name: support, type: kafka, brokers: ["kafka:9092"], topic: support }
  - { name: spool, type: file, path: ./spool.ndjson }
  - { name: audit, type: http, url: https://audit.example.com, shadow: true }
dead_letter: { type: file, path: ./dlq.ndjson }
routes:
  - name: payments
    from: [in]
    filter: 'msg.json.type != "ping"'
    router:
      type: rules
      rules:
        - { when: 'msg.json.type == "refund"', to: [support] }
      default: [billing, audit]
  - name: triage
    from: [chat]
    router:
      type: ai
      model: fake/model
      candidates:
        - { name: billing, description: payments }
        - { name: support, description: bugs }
`

func writeTests(t *testing.T, dir, name, body string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600))
}

func TestSpecTestsPass(t *testing.T) {
	dir := writeSpec(t, testSpec)
	writeTests(t, dir, "order.json", `{"id":"f1","type":"payment","amount":3}`+"\n")
	writeTests(t, dir, cli.DefaultTestFile, `
tests:
  - name: a payment goes to billing in cents
    source: in
    body: { id: p1, type: payment, amount: 12.5 }
    headers: { X-Tenant: acme }
    expect:
      status: delivered
      to: [billing]
      not_to: [support]
      shadowed: [audit]
      failed: []
      routes: { payments: { router: rules, filtered: false, to: [billing, audit] } }
      delivered:
        billing:
          via: billing
          body: { id: p1, cents: 1250 }
          content_type: application/json
          headers: { X-Kind: payment, X-Tenant: acme }
        audit:
          contains: { type: payment }
          attributes: { shadow: true }
  - name: the body can come from a file
    source: in
    file: order.json
    expect: { delivered: { billing: { contains: '"cents":300' } } }
  - name: billing down falls back to the spool
    source: in
    body: { id: p2, type: payment, amount: 1 }
    down: [billing]
    expect:
      status: delivered
      failed: []
      delivered: { billing: { via: spool, contains: { id: p2 } } }
  - name: nothing left dead-letters
    source: in
    body: { id: p3, type: payment, amount: 1 }
    down: [billing, spool]
    expect: { status: dead_lettered, failed: [billing] }
  - name: no dead-letter queue either is a failure
    source: in
    body: { id: p4, type: payment, amount: 1 }
    down: [billing, spool, dead_letter]
    expect: { status: failed }
  - name: a refund goes to support
    source: in
    body: { id: r1, type: refund }
    expect: { to: [support], shadowed: [], delivered: { support: { body: { id: r1, type: refund } } } }
  - name: a ping is filtered out
    source: in
    body: { type: ping }
    expect: { status: filtered, to: [], routes: { payments: { filtered: true } } }
  - name: text is sent as it is
    source: in
    body: "not json"
    expect:
      # The route's filter reads msg.json, which a text body does not have.
      filter_errors: true
      delivered: { audit: { body: "not json", content_type: "text/plain; charset=utf-8" } }
  - name: a test answers for the model
    source: chat
    body: { text: "where is my invoice" }
    decide: { triage: [support] }
    expect: { to: [support], routes: { triage: { router: test } } }
`)
	code, out, errOut := run(t, dir, "test")
	require.Equal(t, 0, code, out+errOut)
	assert.Equal(t, 9, strings.Count(out, "✓ ")-1, out)
	assert.Contains(t, out, "9 test(s) passed")
	// Nothing was written: the spool and the dead-letter queue are stand-ins.
	assert.NoFileExists(t, filepath.Join(dir, "spool.ndjson"))
	assert.NoFileExists(t, filepath.Join(dir, "dlq.ndjson"))
}

func TestSpecTestsFailWithReasons(t *testing.T) {
	dir := writeSpec(t, testSpec)
	writeTests(t, dir, cli.DefaultTestFile, `
tests:
  - name: wrong on purpose
    source: in
    body: { id: p1, type: payment, amount: 2 }
    expect:
      status: filtered
      to: [support]
      not_to: [billing]
      shadowed: []
      failed: [billing]
      routes:
        payments: { router: ai, filtered: true, to: [support] }
        triage: { router: ai }
      delivered:
        billing:
          via: spool
          body: { id: p1, cents: 2 }
          contains: { cents: 200, missing: 1 }
          content_type: text/plain
          headers: { X-Kind: refund }
          attributes: { shadow: true }
        support: { body: {} }
  - name: right
    source: in
    body: { id: p2, type: payment, amount: 2 }
    expect: { to: [billing] }
`)
	code, out, _ := run(t, dir, "test")
	assert.Equal(t, 1, code)
	for _, want := range []string{
		"✗ wrong on purpose",
		"status: want filtered, got delivered",
		"to: want [support], got [billing]",
		"not_to: billing was picked",
		"shadowed: want [], got [audit]",
		"failed: want [billing], got []",
		"routes.payments.filtered: want true, got false",
		"routes.payments.to: want [support], got [audit billing]",
		"routes.payments.router: want ai, got rules",
		"routes.triage: the route did not run",
		"delivered.billing.via: want spool, got billing",
		`delivered.billing.body: want {"cents":2,"id":"p1"}, got {"cents":200,"id":"p1"}`,
		"delivered.billing.contains: missing differs",
		"delivered.billing.content_type: want text/plain, got application/json",
		`delivered.billing.headers.X-Kind: want "refund", got "payment"`,
		"delivered.billing.attributes.shadow: want true, got null",
		"delivered.support: no route sent the message there",
		"    route payments: rules → billing, audit",
		"✓ right",
		"1 of 2 test(s) failed",
	} {
		assert.Contains(t, out, want)
	}

	// --run picks tests by name, and --json prints the results.
	code, out, _ = run(t, dir, "test", "--run", "^right$", "--json")
	require.Equal(t, 0, code, out)
	var rep cli.SpecReport
	require.NoError(t, json.Unmarshal([]byte(out), &rep))
	require.Len(t, rep.Results, 1)
	assert.Equal(t, 1, rep.Tests)
	assert.True(t, rep.Results[0].Passed)
	assert.Equal(t, "delivered", rep.Results[0].Report.Status)

	code, out, _ = run(t, dir, "test", "--run", "nothing like this")
	assert.Equal(t, 1, code, "no test ran: that is not a pass")
	assert.Contains(t, out, "no test matches")
}

// An AI route without decide calls its model, which needs the secrets.
func TestSpecTestsCallTheModelWhenNotAnswered(t *testing.T) {
	dir := writeSpec(t, spec) // the spec of the other tests: no secret
	writeTests(t, dir, "a.test.yaml", `
tests:
  - name: the model picks billing for an invoice
    source: in
    body: { text: "my invoice is wrong" }
    expect: { to: [billing], routes: { triage: { router: ai } } }
`)
	writeTests(t, dir, "b.test.yml", `
decide: { triage: [support] }
tests:
  - name: the file answers for the model
    source: in
    body: { text: "my invoice is wrong" }
    expect: { to: [support] }
`)
	code, out, errOut := run(t, dir, "test", ".")
	require.Equal(t, 0, code, out+errOut)
	assert.Contains(t, out, "2 test(s) passed")

	// With a secret that cannot be read, a test that calls the model stops
	// at the spec; one that answers for it does not need the secret.
	dir = writeSpec(t, testSpec)
	writeTests(t, dir, cli.DefaultTestFile, `
tests:
  - name: the model decides
    source: chat
    body: { text: hello }
    expect: { status: delivered }
`)
	code, _, errOut = run(t, dir, "test")
	assert.Equal(t, 2, code)
	assert.Contains(t, errOut, "CRM_KEY")
}

func TestSpecTestsBadFiles(t *testing.T) {
	dir := writeSpec(t, testSpec)
	cases := map[string]string{
		"unknown key":   "tests:\n  - { name: a, source: in, expects: {} }\n",
		"no tests":      "tests: []\n",
		"no name":       "tests:\n  - { source: in }\n",
		"no source":     "tests:\n  - { name: a }\n",
		"twice":         "tests:\n  - { name: a, source: in }\n  - { name: a, source: in }\n",
		"body and file": "tests:\n  - { name: a, source: in, body: x, file: y }\n",
	}
	for name, body := range cases {
		writeTests(t, dir, cli.DefaultTestFile, body)
		code, _, errOut := run(t, dir, "test")
		assert.Equal(t, 2, code, name)
		assert.Contains(t, errOut, cli.DefaultTestFile, name)
	}
	code, _, errOut := run(t, dir, "test", "missing.yaml")
	assert.Equal(t, 2, code)
	assert.Contains(t, errOut, "missing.yaml")

	// What a test names must exist: these fail the test, they do not stop
	// the run.
	writeTests(t, dir, cli.DefaultTestFile, `
tests:
  - { name: source, source: nope }
  - { name: down, source: in, down: [nope] }
  - { name: route, source: in, decide: { nope: [billing] } }
  - { name: destination, source: in, decide: { triage: [nope] } }
  - { name: file, source: in, file: nope.json }
`)
	code, out, _ := run(t, dir, "test")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, `source "nope" is not declared`)
	assert.Contains(t, out, `down: "nope" is not a destination`)
	assert.Contains(t, out, `decide: route "nope" is not declared`)
	assert.Contains(t, out, `decide.triage: destination "nope" is not declared`)
	assert.Contains(t, out, "nope.json")
	assert.Contains(t, out, "5 of 5 test(s) failed")
}

// enrichSpec calls a server nothing can reach, and has a filter on each
// level that reads a key a message may not have.
const enrichSpec = `
version: 1
name: cli
sources: [{ name: in, type: stdin }]
destinations:
  - name: crm
    type: http
    url: https://crm.example.com/hook
    transforms: [{ filter: 'msg.json.customer.tier != "blocked"' }]
  - { name: scores, type: http, url: https://scores.example.com/hook }
routes:
  - name: orders
    from: [in]
    filter: 'msg.json.kind == "order"'
    transforms:
      - enrich:
          url: "https://customers.invalid/v1/customers/{{ .json.customer_id }}"
          into: customer
    to: [crm]
  - name: optional
    from: [in]
    transforms:
      - enrich: { url: "https://scores.invalid/score", method: POST, into: score, on_error: skip }
    to: [scores]
`

func TestSpecTestsAnswerEnrichCalls(t *testing.T) {
	dir := writeSpec(t, enrichSpec)
	writeTests(t, dir, cli.DefaultTestFile, `
enrich:
  - { url: "https://customers.invalid/v1/customers/*", answer: { tier: free } }
  - { url: "https://scores.invalid/score", answer: 7 }
tests:
  - name: the file's answers
    source: in
    body: { kind: order, customer_id: c1 }
    expect:
      status: delivered
      routes: { orders: { to: [crm] } }
      delivered:
        crm: { contains: { customer: { tier: free } } }
        scores: { contains: { score: 7 } }
  - name: a test's answer comes first
    source: in
    body: { kind: order, customer_id: c2 }
    enrich:
      - { url: "https://customers.invalid/v1/customers/c2", answer: { tier: gold, seats: 3 } }
    expect:
      status: delivered
      delivered: { crm: { contains: { customer: { tier: gold, seats: 3 } } } }
  - name: a failed call
    source: in
    body: { kind: order, customer_id: c3 }
    enrich:
      - { url: "*/customers/c3", error: status 503 }
    expect:
      status: dead_lettered
      routes:
        orders: { error: "status 503" }
`)
	code, out, errb := run(t, dir, "test")
	require.Equal(t, 0, code, out+errb)
	assert.Contains(t, out, "3 test(s) passed")
}

func TestSpecTestsEnrichWithoutAnswer(t *testing.T) {
	dir := writeSpec(t, enrichSpec)
	writeTests(t, dir, cli.DefaultTestFile, `
enrich:
  - { url: "https://customers.invalid/v1/customers/*", answer: { tier: free } }
tests:
  - name: the score call has no answer
    source: in
    body: { kind: order, customer_id: c1 }
    expect: { status: delivered, filter_errors: true }
`)
	code, out, _ := run(t, dir, "test")
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, "enrich: POST https://scores.invalid/score has no answer in the test file")

	writeTests(t, dir, cli.DefaultTestFile, `
tests:
  - name: bad
    source: in
    body: {}
    enrich: [{ url: "https://x.invalid", answer: 1, error: nope }]
    expect: {}
`)
	code, out, _ = run(t, dir, "test")
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, "set answer or error, not both")
}

func TestSpecTestsFailOnAFilterThatCannotBeEvaluated(t *testing.T) {
	dir := writeSpec(t, enrichSpec)
	body := `
enrich:
  - { url: "https://customers.invalid/*", answer: { plan: team } }
  - { url: "https://scores.invalid/score", answer: 7 }
tests:
  - name: no kind
    source: in
    body: { customer_id: c1 }
    expect: { status: delivered%s }
`
	writeTests(t, dir, cli.DefaultTestFile, strings.ReplaceAll(body, "%s", ""))
	code, out, _ := run(t, dir, "test")
	assert.Equal(t, 1, code, out)
	assert.Contains(t, out, "route orders: a filter could not be evaluated, so the message was kept")
	assert.Contains(t, out, "msg.json.kind")
	assert.Contains(t, out, "filter_errors: true allows it")
	// The customer has no tier: the destination's filter fails too.
	assert.Contains(t, out, "destination crm: a filter could not be evaluated")

	writeTests(t, dir, cli.DefaultTestFile, strings.ReplaceAll(body, "%s", ", filter_errors: true"))
	code, out, _ = run(t, dir, "test")
	assert.Equal(t, 0, code, out)
}
