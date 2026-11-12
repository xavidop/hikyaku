package cli_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/cli"
)

const graphSpec = `
version: 1
name: shop
sources:
  - { name: in, type: stdin }
  - { name: other, type: stdin }
destinations:
  - { name: billing, type: fault, fallback: [spool] }
  - { name: support, type: fault }
  - { name: spool, type: fault }
  - { name: audit, type: fault, shadow: true, sample: 0.05 }
  - { name: quarantine, type: fault, required: false }
  - name: crm
    type: group
    strategy: failover
    members:
      - { name: crm-a, type: fault }
      - { name: crm-b, type: fault }
dead_letter: { type: file, path: ./dlq.ndjson, fallback: [stderr] }
routes:
  - name: triage
    from: [in, other]
    transforms:
      - validate:
          schema: { type: object }
          on_invalid: { to: [quarantine] }
    router:
      type: ai
      model: fake/model
      candidates:
        - { name: billing, description: payments }
        - { name: support, description: "bugs \"and\" <questions>" }
      fallback: [{ type: static, to: [support, crm] }]
  - { name: copy, from: [in], to: [crm, audit] }
`

func TestGraphMermaid(t *testing.T) {
	dir := writeSpec(t, graphSpec)
	code, out, errOut := run(t, dir, "graph")
	require.Equal(t, 0, code, errOut)

	assert.Contains(t, out, "flowchart LR\n")
	for _, want := range []string{
		`(["in<br/>stdin"])`,                    // a source
		`{{"triage<br/>ai → static"}}`,          // a route with its router chain
		`["billing<br/>fault"]`,                 // a destination
		`["crm<br/>failover group of 2 fault"]`, // a group
		`-.->|"fallback"|`,                      // billing → spool
		`-.->|"shadow 5%"|`,                     // copy → audit
		`-.->|"invalid"|`,                       // triage → quarantine
		`-->|"static fallback"|`,                // triage → crm, by the second router
	} {
		assert.Contains(t, out, want)
	}
	assert.NotContains(t, out, "dead_letter", "the dead-letter queue is drawn only when asked for")
}

func TestGraphJSONAndDeadLetter(t *testing.T) {
	dir := writeSpec(t, graphSpec)
	code, out, errOut := run(t, dir, "graph", "--format", "json", "--dead-letter")
	require.Equal(t, 0, code, errOut)
	var g cli.Graph
	require.NoError(t, json.Unmarshal([]byte(out), &g))
	assert.Equal(t, "shop", g.Name)

	id := map[string]string{}
	for _, n := range g.Nodes {
		id[n.Kind+"/"+n.Name] = n.ID
	}
	require.Len(t, g.Nodes, 2+2+6+2, "sources, routes, destinations, the dead-letter queue and its last resort")
	has := func(from, to, kind string) bool {
		for _, e := range g.Edges {
			if e.From == id[from] && e.To == id[to] && e.Kind == kind {
				return true
			}
		}
		return false
	}
	assert.True(t, has("source/in", "route/triage", "route"))
	assert.True(t, has("source/other", "route/triage", "route"))
	assert.True(t, has("source/in", "route/copy", "route"))
	assert.False(t, has("source/other", "route/copy", "route"))
	assert.True(t, has("route/triage", "destination/billing", "to"))
	assert.True(t, has("route/triage", "destination/support", "to"))
	assert.True(t, has("route/triage", "destination/crm", "to"))
	assert.True(t, has("route/triage", "destination/quarantine", "invalid"))
	assert.True(t, has("route/copy", "destination/audit", "shadow"))
	assert.True(t, has("destination/billing", "destination/spool", "fallback"))
	// The dead-letter queue takes what a chain could not deliver: from the
	// end of a chain, for a required destination.
	assert.True(t, has("destination/spool", "dead_letter/dead_letter", "dead_letter"))
	assert.True(t, has("destination/support", "dead_letter/dead_letter", "dead_letter"))
	assert.False(t, has("destination/billing", "dead_letter/dead_letter", "dead_letter"), "billing has a fallback")
	assert.False(t, has("destination/audit", "dead_letter/dead_letter", "dead_letter"), "a shadow is never dead-lettered")
	assert.False(t, has("destination/quarantine", "dead_letter/dead_letter", "dead_letter"), "best effort")
	assert.True(t, has("dead_letter/dead_letter", "dead_letter/stderr", "fallback"))
}

func TestGraphDot(t *testing.T) {
	dir := writeSpec(t, graphSpec)
	code, out, errOut := run(t, dir, "graph", "--format", "dot")
	require.Equal(t, 0, code, errOut)
	assert.Contains(t, out, `digraph "shop" {`)
	assert.Contains(t, out, `[shape=hexagon, label="triage\nai → static"]`)
	assert.Contains(t, out, `[label="fallback", style=dashed]`)

	code, _, errOut = run(t, dir, "graph", "--format", "png")
	assert.Equal(t, 2, code)
	assert.Contains(t, errOut, "mermaid, dot or json")
}
