//go:build integration

package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

// image is the broker the integration suite runs against: Redpanda speaks
// the Kafka protocol, with SASL/SCRAM-SHA-256 and ACLs.
const image = "docker.redpanda.com/redpandadata/redpanda:v25.2.4"

// startRedpanda runs a broker that requires SASL/SCRAM-SHA-256, with the
// users relay (a superuser) and nobody (no ACLs), and returns its address.
func startRedpanda(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	ctr, err := redpanda.Run(ctx, image,
		redpanda.WithEnableSASL(),
		redpanda.WithEnableKafkaAuthorization(),
		redpanda.WithNewServiceAccount("relay", "s3cret"),
		redpanda.WithNewServiceAccount("nobody", "pw"),
		redpanda.WithSuperusers("relay"),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)
	broker, err := ctr.KafkaSeedBroker(ctx)
	require.NoError(t, err)
	return broker
}

func TestIntegrationRedpanda(t *testing.T) {
	broker := startRedpanda(t)
	creds := &config.SASL{Mechanism: "scram-sha-256", Username: "relay", Password: "s3cret"}
	wrong := &config.SASL{Mechanism: "scram-sha-256", Username: "relay", Password: "wrong"}
	admin, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.SASL(saslMech(t, creds.Mechanism, creds.Username, creds.Password)))
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	adm := kadm.NewClient(admin)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, topic := range []string{"orders", "batched"} {
		_, err = adm.CreateTopic(ctx, 3, 1, nil, topic)
		require.NoError(t, err)
	}

	newSinkOn := func(t *testing.T, ep config.Endpoint) *Sink {
		ep.Brokers = []string{broker}
		return newTestSink(t, nil, ep)
	}
	newSourceOn := func(t *testing.T, cfg config.Source) *Source {
		cfg.Brokers = []string{broker}
		return newTestSource(t, nil, cfg)
	}
	// produced delivers bodies through a destination and returns their IDs.
	produced := func(t *testing.T, s *Sink, bodies ...string) map[string]bool {
		ids := map[string]bool{}
		for _, b := range bodies {
			env := message(b)
			res, err := s.Deliver(ctx, env)
			require.NoError(t, err)
			require.Len(t, res.Headers[resultOffset], 1)
			ids[env.ID] = true
		}
		return ids
	}
	// commitsCaughtUp waits until the group committed every partition's end.
	commitsCaughtUp := func(t *testing.T, group, topic string) {
		require.Eventually(t, func() bool {
			ends, err := adm.ListEndOffsets(ctx, topic)
			if err != nil {
				return false
			}
			done := true
			ends.Each(func(o kadm.ListedOffset) {
				if o.Offset > 0 && committed(t, admin, group, topic, o.Partition) != o.Offset {
					done = false
				}
			})
			return done
		}, time.Minute, 50*time.Millisecond)
	}

	t.Run("round trip and a group restart", func(t *testing.T) {
		sink := newSinkOn(t, config.Endpoint{Topic: "orders", Key: "{{ .json.id }}", SASL: creds, Compression: "zstd"})
		require.NoError(t, sink.Health(ctx))
		first := produced(t, sink, `{"id":"a"}`, `{"id":"b"}`, `{"id":"c"}`, `{"id":"d"}`)

		r := run(t, newSourceOn(t, config.Source{Topic: "orders", Group: "relay", SASL: creds}))
		for range first {
			env := r.next(t)
			assert.True(t, first[env.ID], "the record carries the message's Idempotency-Key: %s", env.ID)
			assert.Equal(t, "orders", env.Subject)
			assert.Equal(t, "application/json", env.ContentType)
			ack(env, envelope.Delivered)
		}
		commitsCaughtUp(t, "relay", "orders")
		r.stop(t)

		later := produced(t, sink, `{"id":"e"}`, `{"id":"f"}`)
		again := run(t, newSourceOn(t, config.Source{Topic: "orders", Group: "relay", SASL: creds}))
		for range later {
			env := again.next(t)
			assert.True(t, later[env.ID], "a restarted member resumes after the commit: %s", env.ID)
			ack(env, envelope.Delivered)
		}
		again.none(t, 2*time.Second)
		commitsCaughtUp(t, "relay", "orders")
	})

	t.Run("a failed record is delivered again after a restart", func(t *testing.T) {
		r := run(t, newSourceOn(t, config.Source{Topic: "orders", Group: "holder", SASL: creds}))
		var failed *envelope.Envelope
		for range 6 {
			env := r.next(t)
			if failed == nil {
				failed = env
				ack(env, envelope.Failed)
				continue
			}
			ack(env, envelope.Delivered)
		}
		r.stop(t)
		again := run(t, newSourceOn(t, config.Source{Topic: "orders", Group: "holder", SASL: creds}))
		env := again.next(t)
		assert.Equal(t, failed.ID, env.ID, "the partition resumes from the failed record")
		ack(env, envelope.Delivered)
	})

	t.Run("batch", func(t *testing.T) {
		sink := newSinkOn(t, config.Endpoint{Topic: "batched", SASL: creds})
		res, err := sink.DeliverBatch(ctx, []*envelope.Envelope{message(`1`), message(`2`), message(`3`)})
		require.NoError(t, err)
		assert.Len(t, res.Headers[resultOffset], 3)
		commitsAfter := run(t, newSourceOn(t, config.Source{Topic: "batched", Group: "batch", SASL: creds}))
		for range 3 {
			ack(commitsAfter.next(t), envelope.Delivered)
		}
		commitsCaughtUp(t, "batch", "batched")
	})

	t.Run("a missing topic is permanent", func(t *testing.T) {
		sink := newSinkOn(t, config.Endpoint{Topic: "missing", SASL: creds})
		_, err := sink.Deliver(ctx, message(`{}`))
		require.Error(t, err)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, "%v", err)
		assert.Equal(t, []string{"kafka:UNKNOWN_TOPIC_OR_PARTITION"}, plugin.Tags(err))
	})

	t.Run("wrong credentials", func(t *testing.T) {
		sink := newSinkOn(t, config.Endpoint{Topic: "orders", SASL: wrong})
		sink.flushTimeout = 10 * time.Millisecond
		_, err := sink.Deliver(ctx, message(`{}`))
		require.Error(t, err)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, "%v", err)
		assert.Equal(t, []string{"kafka:SASL_AUTHENTICATION_FAILED"}, plugin.Tags(err))

		src := newSourceOn(t, config.Source{Topic: "orders", Group: "wrong", SASL: wrong, Reconnect: &config.Reconnect{MaxAttempts: 2}})
		r := run(t, src)
		select {
		case err := <-r.done:
			r.done <- nil
			assert.ErrorIs(t, err, kerr.SaslAuthenticationFailed)
		case <-time.After(time.Minute):
			t.Fatal("the source keeps retrying wrong credentials")
		}
	})

	t.Run("ACLs", func(t *testing.T) {
		// A user without ACLs authenticates, but may not use the group or
		// the topic.
		nobody := &config.SASL{Mechanism: "scram-sha-256", Username: "nobody", Password: "pw"}
		src := newSourceOn(t, config.Source{Topic: "orders", Group: "acl", SASL: nobody,
			Reconnect: &config.Reconnect{Backoff: config.Duration(10 * time.Millisecond), MaxAttempts: 2}})
		r := run(t, src)
		select {
		case err := <-r.done:
			r.done <- nil
			require.Error(t, err)
			assert.True(t, authFailure(err), "%v", err)
		case <-time.After(time.Minute):
			t.Fatal("the source keeps retrying a refused group")
		}
		sink := newSinkOn(t, config.Endpoint{Topic: "orders", SASL: nobody})
		_, err = sink.Deliver(ctx, message(`{}`))
		require.Error(t, err)
		class, _ := plugin.Classify(err)
		assert.Equal(t, plugin.Permanent, class, "%v", err)
		assert.Equal(t, []string{"kafka:TOPIC_AUTHORIZATION_FAILED"}, plugin.Tags(err), "%v", err)
	})

	t.Run("source health", func(t *testing.T) {
		src := newSourceOn(t, config.Source{Topic: "orders", Group: fmt.Sprint("health-", time.Now().UnixNano()), SASL: creds})
		run(t, src)
		require.Eventually(t, func() bool { return src.Health(ctx) == nil }, time.Minute, 50*time.Millisecond)
	})
}
