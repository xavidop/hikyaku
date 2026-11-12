package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/plugin"
)

// standinSpec names types no test binary can build (kafka) or should reach
// (a real URL, a file): Options.Sinks must keep every one of them unbuilt.
const standinSpec = `
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - name: crm
    type: http
    url: https://crm.example.com/hook
    retry: { attempts: 3, backoff: 1h }
    fallback: [queue]
  - { name: queue, type: kafka, brokers: ["kafka:9092"], topic: orders }
  - { name: later, type: http, url: https://later.example.com, deliver_after: 1h }
  - name: slow
    type: http
    url: https://slow.example.com
    rate_limit: { per_second: 0.001, burst: 1 }
dead_letter: { type: file, path: /nonexistent/dir/dlq.ndjson }
routes: [{ name: orders, from: [in], to: [crm, later, slow] }]
`

func TestSinksAndNoWait(t *testing.T) {
	sinks := map[string]*scripted{}
	e := build(t, standinSpec, Options{
		NoWait: true,
		Sinks: func(dest string, spec plugin.Spec) plugin.Sink {
			s := &scripted{name: spec.Name}
			if dest == "crm" {
				s.final = plugin.Wrap(plugin.Transient, []string{"connect"}, errors.New("down"))
			}
			sinks[dest] = s
			return s
		},
	})
	require.Contains(t, sinks, DeadLetterName, "the dead-letter hop is a stand-in too")

	start := time.Now()
	rep := e.Inject(context.Background(), msg("in", `{"id":1}`))
	assert.Equal(t, "delivered", rep.Status)
	assert.Less(t, time.Since(start), 5*time.Second, "no backoff, no hold, no rate-limit wait")

	assert.Equal(t, 3, sinks["crm"].count(), "every attempt is made, without the 1h backoff")
	assert.Equal(t, 1, sinks["queue"].count(), "the fallback took it")
	assert.Equal(t, 1, sinks["later"].count(), "deliver_after did not hold it")

	// The rate limit lets one through every 1000 seconds: a second message does not wait.
	rep = e.Inject(context.Background(), msg("in", `{"id":2}`))
	assert.Equal(t, "delivered", rep.Status)
	assert.Equal(t, 2, sinks["slow"].count())
	assert.Zero(t, sinks[DeadLetterName].count())
}
