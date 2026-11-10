//go:build integration

package mqtt

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// mosquittoImage is the broker the integration suite runs against.
const mosquittoImage = "eclipse-mosquitto:2"

// mosquittoConf lets anonymous clients in on 1883.
const mosquittoConf = `listener 1883
allow_anonymous true
persistence false
`

// startMosquitto runs a broker container and returns its mqtt:// URL.
func startMosquitto(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mosquittoImage,
			ExposedPorts: []string{"1883/tcp"},
			Files: []testcontainers.ContainerFile{{
				Reader:            strings.NewReader(mosquittoConf),
				ContainerFilePath: "/mosquitto/config/mosquitto.conf",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForAll(
				wait.ForLog(" running"), // "mosquitto version 2.x running"
				wait.ForListeningPort("1883/tcp"),
			).WithDeadline(2 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	url, err := ctr.PortEndpoint(ctx, "1883/tcp", "mqtt")
	require.NoError(t, err)
	return url
}

// mosquittoSource is a source of url's it/# with fast reconnects.
func mosquittoSource(t *testing.T, url, version, clientID string) *Source {
	t.Helper()
	cfg := config.Source{
		Name: "in", Type: kind, URL: url, Topic: "it/#", ProtocolVersion: version, ClientID: clientID,
		Reconnect: &config.Reconnect{Backoff: config.Duration(20 * time.Millisecond), MaxBackoff: config.Duration(200 * time.Millisecond)},
	}
	s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg, Env: plugin.Env{Relay: "it"}})
	require.NoError(t, err)
	return s
}

// waitHealthy waits for a source to be connected and subscribed.
func waitHealthy(t *testing.T, s *Source) {
	t.Helper()
	require.Eventually(t, func() bool { return s.Health(context.Background()) == nil }, 30*time.Second, 10*time.Millisecond)
}

func TestMosquittoRoundTrip(t *testing.T) {
	url := startMosquitto(t)
	for _, v := range versions {
		t.Run("v"+v, func(t *testing.T) {
			clientID := "it-source-" + strings.ReplaceAll(v, ".", "")
			src := mosquittoSource(t, url, v, clientID)
			out, stop := run(t, src)
			waitHealthy(t, src)

			sink := newNamedSink(t, "it-sink-"+strings.ReplaceAll(v, ".", ""), config.Endpoint{
				Type: kind, URL: url, Topic: "it/{{ .subject }}", ProtocolVersion: v,
				Headers: map[string]string{"x-tenant": "acme"},
			})
			for qos := range 3 {
				sink.opts.qos = byte(qos)
				msg := testMessage(fmt.Sprintf("qos%d", qos), fmt.Sprintf(`{"qos":%d}`, qos))
				_, err := sink.Deliver(deliverCtx(t), msg)
				require.NoError(t, err, "qos %d", qos)
				env := recv(t, out)
				assert.Equal(t, fmt.Sprintf("it/qos%d", qos), env.Subject)
				assert.JSONEq(t, fmt.Sprintf(`{"qos":%d}`, qos), string(env.Body))
				if v == version5 {
					assert.Equal(t, msg.ID, env.ID, "the Idempotency-Key carries the ID across")
					assert.Equal(t, []string{"acme"}, env.Headers["X-Tenant"])
					assert.Equal(t, contentJSON, env.ContentType)
				}
				settle(env, envelope.Delivered)
			}

			// A failed message is redelivered after the source reconnects.
			_, err := sink.Deliver(deliverCtx(t), testMessage("retry", "again"))
			require.NoError(t, err)
			first := recv(t, out)
			settle(first, envelope.Failed)
			again := recv(t, out)
			assert.Equal(t, "again", string(again.Body))
			assert.Equal(t, true, again.Attributes["mqtt.duplicate"])
			settle(again, envelope.Delivered)

			// The persistent session keeps what is published while the source is down.
			stop()
			for i := range 3 {
				_, err := sink.Deliver(deliverCtx(t), testMessage("down", fmt.Sprint(i)))
				require.NoError(t, err)
			}
			out2, _ := run(t, mosquittoSource(t, url, v, clientID))
			var got []string
			for range 3 {
				env := recv(t, out2)
				got = append(got, string(env.Body))
				settle(env, envelope.Delivered)
			}
			assert.ElementsMatch(t, []string{"0", "1", "2"}, got)
		})
	}
}

func TestMosquittoSharedSubscription(t *testing.T) {
	url := startMosquitto(t)
	var outs []<-chan *envelope.Envelope
	for _, id := range []string{"it-worker-a", "it-worker-b"} {
		cfg := config.Source{Name: "in", Type: kind, URL: url, Topic: "jobs/#", Group: "workers", ClientID: id}
		s, err := newSource(context.Background(), plugin.Spec{Name: "in", Kind: kind, Config: &cfg})
		require.NoError(t, err)
		out, _ := run(t, s)
		waitHealthy(t, s)
		outs = append(outs, out)
	}
	sink := newNamedSink(t, "it-jobs", config.Endpoint{Type: kind, URL: url, Topic: "jobs/{{ .subject }}"})
	const n = 20
	for i := range n {
		_, err := sink.Deliver(deliverCtx(t), testMessage(fmt.Sprint(i), fmt.Sprint(i)))
		require.NoError(t, err)
	}
	per := make([]int, 2)
	seen := map[string]bool{}
	deadline := time.After(30 * time.Second)
	for len(seen) < n {
		select {
		case env := <-outs[0]:
			per[0]++
			seen[string(env.Body)] = true
			settle(env, envelope.Delivered)
		case env := <-outs[1]:
			per[1]++
			seen[string(env.Body)] = true
			settle(env, envelope.Delivered)
		case <-deadline:
			t.Fatalf("only %d of %d messages arrived", len(seen), n)
		}
	}
	assert.Equal(t, n, per[0]+per[1], "each message once")
	assert.Positive(t, per[0])
	assert.Positive(t, per[1])
}
