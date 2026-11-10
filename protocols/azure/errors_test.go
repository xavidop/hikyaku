package azure

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/plugin"
)

func TestClassify(t *testing.T) {
	cond := func(c amqp.ErrCond) error { return &amqp.Error{Condition: c, Description: "described"} }
	for name, tc := range map[string]struct {
		err   error
		class plugin.Class
		tags  []string
		after time.Duration
	}{
		"connlost":        {&azservicebus.Error{Code: azservicebus.CodeConnectionLost}, plugin.Transient, []string{"servicebus:connlost", "connect"}, 0},
		"closed":          {&azservicebus.Error{Code: azservicebus.CodeClosed}, plugin.Transient, []string{"servicebus:closed", "connect"}, 0},
		"sb timeout":      {&azservicebus.Error{Code: azservicebus.CodeTimeout}, plugin.Timeout, []string{"servicebus:timeout", "timeout"}, 0},
		"not found":       {&azservicebus.Error{Code: azservicebus.CodeNotFound}, plugin.Permanent, []string{"servicebus:notfound"}, 0},
		"unauthorized":    {&azservicebus.Error{Code: azservicebus.CodeUnauthorizedAccess}, plugin.Permanent, []string{"servicebus:unauthorized"}, 0},
		"lock lost":       {&azservicebus.Error{Code: azservicebus.CodeLockLost}, plugin.Transient, []string{"servicebus:locklost", "5xx"}, 0},
		"eh connlost":     {&azeventhubs.Error{Code: azeventhubs.ErrorCodeConnectionLost}, plugin.Transient, []string{"servicebus:connlost", "connect"}, 0},
		"server busy":     {cond("com.microsoft:server-busy"), plugin.RateLimited, []string{"servicebus:server-busy", "429"}, throttleWait},
		"quota":           {cond(amqp.ErrCondResourceLimitExceeded), plugin.RateLimited, []string{"servicebus:resource-limit-exceeded", "429"}, throttleWait},
		"service timeout": {cond("com.microsoft:timeout"), plugin.Timeout, []string{"servicebus:timeout", "timeout"}, 0},
		"amqp not found":  {cond(amqp.ErrCondNotFound), plugin.Permanent, []string{"servicebus:not-found"}, 0},
		"too large":       {cond(amqp.ErrCondMessageSizeExceeded), plugin.Permanent, []string{"servicebus:message-size-exceeded"}, 0},
		"disabled":        {cond("com.microsoft:entity-disabled"), plugin.Permanent, []string{"servicebus:entity-disabled"}, 0},
		"detached":        {&amqp.LinkError{RemoteErr: &amqp.Error{Condition: amqp.ErrCondDetachForced}}, plugin.Transient, []string{"servicebus:detach-forced", "connect"}, 0},
		"internal":        {cond(amqp.ErrCondInternalError), plugin.Transient, []string{"servicebus:internal-error", "5xx"}, 0},
		"batch too large": {fmt.Errorf("adding: %w", azservicebus.ErrMessageTooLarge), plugin.Permanent, []string{"servicebus:message_too_large"}, 0},
		"event too large": {azeventhubs.ErrEventDataTooLarge, plugin.Permanent, []string{"servicebus:message_too_large"}, 0},
		"credentials":     {&azidentity.AuthenticationFailedError{}, plugin.Permanent, []string{"servicebus:credentials"}, 0},
		"deadline":        {fmt.Errorf("send: %w", context.DeadlineExceeded), plugin.Timeout, []string{"timeout"}, 0},
		"refused":         {&net.OpError{Op: "dial", Err: errors.New("connection refused")}, plugin.Transient, []string{"connect"}, 0},
		"eof":             {io.EOF, plugin.Transient, []string{"connect"}, 0},
		"other":           {errors.New("odd"), plugin.Transient, nil, 0},
	} {
		t.Run(name, func(t *testing.T) {
			err := classify(context.Background(), kindServiceBus, "send", tc.err)
			var pe *plugin.Error
			require.ErrorAs(t, err, &pe)
			assert.Equal(t, tc.class, pe.Class)
			assert.ElementsMatch(t, tc.tags, plugin.Tags(err))
			assert.Equal(t, tc.after, pe.RetryAfter)
			assert.ErrorIs(t, err, tc.err)
			assert.Contains(t, err.Error(), "send: ")
		})
	}
	assert.NoError(t, classify(context.Background(), kindServiceBus, "x", nil))

	// The attempt's own deadline wins over what the SDK said.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := classify(ctx, kindEventHubs, "send", &azeventhubs.Error{Code: azeventhubs.ErrorCodeConnectionLost})
	class, _ := plugin.Classify(err)
	assert.Equal(t, plugin.Timeout, class)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	class, _ = plugin.Classify(classify(ctx, kindEventHubs, "send", errors.New("cancelled")))
	assert.Equal(t, plugin.Transient, class, "a shutdown is not the endpoint's fault")

	err = classify(context.Background(), kindEventHubs, "send", &azeventhubs.Error{Code: azeventhubs.ErrorCodeUnauthorizedAccess})
	assert.Equal(t, []string{"eventhubs:unauthorized"}, plugin.Tags(err))
}

func TestErrorPredicates(t *testing.T) {
	assert.True(t, hasCode(fmt.Errorf("x: %w", &azservicebus.Error{Code: azservicebus.CodeTimeout}), azservicebus.CodeTimeout))
	assert.False(t, hasCode(errors.New("x"), azservicebus.CodeTimeout))
	assert.True(t, ownershipLost(&azeventhubs.Error{Code: azeventhubs.ErrorCodeOwnershipLost}))
	assert.False(t, ownershipLost(&azeventhubs.Error{Code: azeventhubs.ErrorCodeConnectionLost}))

	sessionful := &amqp.Error{Condition: amqp.ErrCondNotAllowed,
		Description: "It is not possible for an entity that requires sessions to create a non-sessionful message receiver. TrackingId:1"}
	assert.True(t, requiresSessions(&amqp.LinkError{RemoteErr: sessionful}))
	assert.True(t, requiresSessions(fmt.Errorf("attach: %w", sessionful)))
	assert.False(t, requiresSessions(&amqp.Error{Condition: amqp.ErrCondNotAllowed, Description: "nope"}))
	assert.False(t, requiresSessions(errors.New("requires sessions")), "only the service's AMQP error counts")
}
