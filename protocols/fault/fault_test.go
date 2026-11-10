package fault

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func init() { Register() }

func msg() *envelope.Envelope { return envelope.New("s", "t", []byte("{}")) }

func TestFailFirstThenSucceed(t *testing.T) {
	s, err := New("billing", config.Fault{FailFirst: 2, Status: 502, RetryAfter: config.Duration(time.Second)})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err := s.Deliver(context.Background(), msg())
		require.Error(t, err)
		class, ra := plugin.Classify(err)
		assert.Equal(t, plugin.Transient, class)
		assert.Equal(t, time.Second, ra)
		assert.Contains(t, plugin.Tags(err), "502")
	}
	r, err := s.Deliver(context.Background(), msg())
	require.NoError(t, err)
	assert.Equal(t, 202, r.Status)
	assert.Len(t, s.Delivered(), 1)
	assert.Equal(t, int64(3), s.Attempts())
}

func TestFailShareIsSeeded(t *testing.T) {
	run := func() []bool {
		s, err := New("x", config.Fault{Fail: 0.5, Seed: 42})
		require.NoError(t, err)
		var out []bool
		for i := 0; i < 200; i++ {
			_, err := s.Deliver(context.Background(), msg())
			out = append(out, err == nil)
		}
		return out
	}
	a, b := run(), run()
	assert.Equal(t, a, b, "the same seed fails the same attempts")
	ok := 0
	for _, x := range a {
		if x {
			ok++
		}
	}
	assert.InDelta(t, 100, ok, 30, "about half succeed")
}

func TestErrorModes(t *testing.T) {
	conn, _ := New("c", config.Fault{FailFirst: 1, Error: "connect"})
	_, err := conn.Deliver(context.Background(), msg())
	assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err))

	slow, _ := New("t", config.Fault{FailFirst: 1, Error: "timeout"})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = slow.Deliver(ctx, msg())
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)

	lat, _ := New("l", config.Fault{Latency: config.Duration(30 * time.Millisecond)})
	start := time.Now()
	_, err = lat.Deliver(context.Background(), msg())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 30*time.Millisecond)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel2()
	slowLat, _ := New("l2", config.Fault{Latency: config.Duration(time.Second)})
	_, err = slowLat.Deliver(ctx2, msg())
	class, _ = plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)

	_, err = New("bad", config.Fault{Error: "meteor"})
	assert.Error(t, err)
}

func TestWrap(t *testing.T) {
	inner, _ := New("inner", config.Fault{})
	w, err := Wrap(inner, config.Fault{FailFirst: 1})
	require.NoError(t, err)
	_, err = w.Deliver(context.Background(), msg())
	assert.Error(t, err)
	_, err = w.Deliver(context.Background(), msg())
	assert.NoError(t, err)
	assert.Len(t, inner.Delivered(), 1, "only attempts past the faults reach the real sink")
	assert.Equal(t, "inner", w.Name())
	assert.NoError(t, w.Health(context.Background()))
	assert.NoError(t, w.Close(context.Background()))
	_, err = Wrap(inner, config.Fault{Error: "meteor"})
	assert.Error(t, err)
}

func TestRegistered(t *testing.T) {
	s, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "f", Kind: "fault", Config: &config.Endpoint{Type: "fault", Fault: &config.Fault{FailFirst: 1}}})
	require.NoError(t, err)
	_, err = s.Deliver(context.Background(), msg())
	assert.Error(t, err)
	_, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "f", Kind: "fault"})
	assert.Error(t, err)
	s, err = plugin.NewSink(context.Background(), plugin.Spec{Name: "plain", Kind: "fault", Config: &config.Endpoint{Type: "fault"}})
	require.NoError(t, err)
	_, err = s.Deliver(context.Background(), msg())
	assert.NoError(t, err, "no fault block: always succeeds")
}
