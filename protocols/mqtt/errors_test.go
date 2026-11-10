package mqtt

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xavidop/hikyaku/plugin"
)

func TestErrorMessages(t *testing.T) {
	assert.Equal(t, "the broker refused it (0x97 quota exceeded: slow down)", (&reasonError{code: 0x97, reason: "slow down"}).Error())
	assert.Equal(t, "the broker refused it (0xf0 error)", (&reasonError{code: 0xf0}).Error())
	assert.Equal(t, "the broker refused the connection (0x86 bad user name or password)", (&connackError{code: 0x86}).Error())
	assert.Equal(t, "the broker refused the connection (return code 5 not authorized)", (&connackError{code: 5, v3: true}).Error())
	assert.Equal(t, "the broker refused the connection (return code 9 refused)", (&connackError{code: 9, v3: true}).Error())
	assert.Equal(t, "the broker disconnected (0x8e session taken over)", (&disconnectedError{code: 0x8e}).Error())
	assert.Equal(t, "mqtt:0x97", reasonTag(0x97))
	assert.Equal(t, "mqtt:0x0a", reasonTag(0x0a))
}

func TestPublishErrorClasses(t *testing.T) {
	live := context.Background()
	expired, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	cancelled, stop := context.WithCancel(context.Background())
	stop()

	for name, tc := range map[string]struct {
		ctx   context.Context
		err   error
		class plugin.Class
		tags  []string
	}{
		"reason code":     {live, &reasonError{code: 0x89}, plugin.Transient, []string{"mqtt:0x89"}},
		"wrapped reason":  {live, fmt.Errorf("x: %w", &reasonError{code: 0x87}), plugin.Permanent, []string{"mqtt:0x87"}},
		"quota":           {live, &reasonError{code: 0x97}, plugin.RateLimited, []string{"mqtt:0x97", plugin.Tag429}},
		"deadline":        {expired, context.DeadlineExceeded, plugin.Timeout, []string{plugin.TagTimeout}},
		"packet timeout":  {live, context.DeadlineExceeded, plugin.Timeout, []string{plugin.TagTimeout}},
		"shutdown":        {cancelled, context.Canceled, plugin.Transient, nil},
		"unsupported":     {live, fmt.Errorf("%w: retain", errUnsupported), plugin.Permanent, nil},
		"connection lost": {live, errNotConnected, plugin.Transient, []string{plugin.TagConnect}},
		"anything else":   {live, errors.New("EOF"), plugin.Transient, []string{plugin.TagConnect}},
	} {
		err := publishError(tc.ctx, "mqtt://b", tc.err)
		class, _ := plugin.Classify(err)
		assert.Equal(t, tc.class, class, name)
		assert.ElementsMatch(t, tc.tags, plugin.Tags(err), name)
		assert.ErrorIs(t, err, tc.err, name)
		assert.Contains(t, err.Error(), "publish to mqtt://b", name)
	}
}

func TestNotConnected(t *testing.T) {
	err := notConnected("mqtt://b", nil)
	assert.ErrorIs(t, err, errNotConnected)
	assert.Equal(t, []string{plugin.TagConnect}, plugin.Tags(err))
	why := errors.New("dial: refused")
	assert.ErrorIs(t, notConnected("mqtt://b", why), why)
}
