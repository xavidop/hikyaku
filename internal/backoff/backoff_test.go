package backoff

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDelay(t *testing.T) {
	assert.Equal(t, time.Duration(0), Base(0, time.Second, 3))
	assert.Equal(t, 500*time.Millisecond, Base(500*time.Millisecond, 30*time.Second, 1))
	assert.Equal(t, 2*time.Second, Base(500*time.Millisecond, 30*time.Second, 3))
	assert.Equal(t, 30*time.Second, Base(500*time.Millisecond, 30*time.Second, 50))
	assert.Equal(t, time.Second, Base(time.Second, 0, 4), "max below base is base")
	half := func() float64 { return 0.5 }
	assert.Equal(t, time.Second, Jittered(2*time.Second, "full", half))
	assert.Equal(t, 1500*time.Millisecond, Jittered(2*time.Second, "equal", half))
	assert.Equal(t, 2*time.Second, Jittered(2*time.Second, "none", half))
	assert.LessOrEqual(t, Jittered(time.Second, "", nil), time.Second)
	assert.Equal(t, 500*time.Millisecond, Delay(500*time.Millisecond, time.Minute, "none", 1, nil))
}

func TestSleep(t *testing.T) {
	assert.True(t, Sleep(context.Background(), time.Millisecond))
	assert.True(t, Sleep(context.Background(), 0))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.False(t, Sleep(ctx, time.Hour))
	assert.False(t, Sleep(ctx, 0))
}
