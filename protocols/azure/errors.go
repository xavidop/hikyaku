package azure

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"

	"github.com/xavidop/hikyaku/plugin"
)

// errClosed is what a destination answers once it is closed.
var errClosed = errors.New("destination is closed")

// throttleWait is how long a throttled client should wait: Service Bus and
// Event Hubs answer server-busy and ask for 10 seconds.
const throttleWait = 10 * time.Second

// AMQP conditions the services send that go-amqp does not name.
const (
	condServerBusy     amqp.ErrCond = "com.microsoft:server-busy"
	condTimeout        amqp.ErrCond = "com.microsoft:timeout"
	condEntityDisabled amqp.ErrCond = "com.microsoft:entity-disabled"
	condArgument       amqp.ErrCond = "com.microsoft:argument-error"
	condArgumentRange  amqp.ErrCond = "com.microsoft:argument-out-of-range"
)

// classify turns a failed send into a *plugin.Error for the retry policy.
// proto is the tag prefix (servicebus, eventhubs); what says what failed.
//
//	the attempt's deadline                   timeout     timeout
//	connlost, closed, a dropped link         transient   <proto>:<code>, connect
//	the service's timeout                    timeout     <proto>:timeout
//	server-busy, resource-limit-exceeded     rate limited (Retry-After 10s), <proto>:<condition>
//	notfound, unauthorized, not-allowed,     permanent   <proto>:<code or condition>
//	  message too large, entity disabled
//	bad credentials                          permanent   <proto>:credentials
//	other broker errors                      transient   <proto>:<code or condition>, 5xx
//	a network error                          transient   connect
func classify(ctx context.Context, proto, what string, err error) error {
	if err == nil {
		return nil
	}
	wrapped := fmt.Errorf("%s: %w", what, err)
	if ctx != nil && ctx.Err() != nil {
		return ctxError(ctx, wrapped)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, wrapped)
	}
	var sbe *azservicebus.Error
	if errors.As(err, &sbe) {
		return codeError(proto, string(sbe.Code), wrapped)
	}
	var ehe *azeventhubs.Error
	if errors.As(err, &ehe) {
		return codeError(proto, string(ehe.Code), wrapped)
	}
	if errors.Is(err, azservicebus.ErrMessageTooLarge) || errors.Is(err, azeventhubs.ErrEventDataTooLarge) {
		return plugin.Wrap(plugin.Permanent, []string{proto + ":message_too_large"}, wrapped)
	}
	var ae *amqp.Error
	if errors.As(err, &ae) {
		return conditionError(proto, ae.Condition, wrapped)
	}
	// azidentity marks its authentication failures NonRetriable.
	var cred interface {
		error
		NonRetriable()
	}
	if errors.As(err, &cred) {
		return plugin.Wrap(plugin.Permanent, []string{proto + ":credentials"}, wrapped)
	}
	class, _ := plugin.Classify(err)
	return plugin.Wrap(class, plugin.Tags(err), wrapped)
}

// ctxError classifies a failure caused by the attempt's context: a
// deadline is a timeout; a cancellation (shutdown) is transient.
func ctxError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// codeError classifies an azservicebus.Error or azeventhubs.Error by its
// code.
func codeError(proto, code string, err error) error {
	tag := proto + ":" + code
	switch code {
	case string(azservicebus.CodeConnectionLost), string(azservicebus.CodeClosed):
		return plugin.Wrap(plugin.Transient, []string{tag, plugin.TagConnect}, err)
	case string(azservicebus.CodeTimeout):
		return plugin.Wrap(plugin.Timeout, []string{tag, plugin.TagTimeout}, err)
	case string(azservicebus.CodeNotFound), string(azservicebus.CodeUnauthorizedAccess):
		return plugin.Wrap(plugin.Permanent, []string{tag}, err)
	}
	// locklost, ownershiplost and codes added later: the broker's side.
	return plugin.Wrap(plugin.Transient, []string{tag, plugin.Tag5xx}, err)
}

// conditionError classifies an AMQP error condition. The tag carries the
// condition's last part: amqp:not-found is <proto>:not-found.
func conditionError(proto string, cond amqp.ErrCond, err error) error {
	c := string(cond)
	tag := proto + ":" + c[strings.LastIndex(c, ":")+1:]
	switch cond {
	case condServerBusy, amqp.ErrCondResourceLimitExceeded:
		return &plugin.Error{Class: plugin.RateLimited, RetryAfter: throttleWait, Tags: []string{tag}, Err: err}
	case condTimeout:
		return plugin.Wrap(plugin.Timeout, []string{tag, plugin.TagTimeout}, err)
	case amqp.ErrCondNotFound, amqp.ErrCondUnauthorizedAccess, amqp.ErrCondNotAllowed,
		amqp.ErrCondMessageSizeExceeded, amqp.ErrCondInvalidField, amqp.ErrCondDecodeError,
		amqp.ErrCondNotImplemented, amqp.ErrCondPreconditionFailed, amqp.ErrCondResourceDeleted,
		condEntityDisabled, condArgument, condArgumentRange:
		return plugin.Wrap(plugin.Permanent, []string{tag}, err)
	case amqp.ErrCondConnectionForced, amqp.ErrCondDetachForced, amqp.ErrCondLinkRedirect,
		amqp.ErrCondConnectionRedirect, amqp.ErrCondStolen:
		return plugin.Wrap(plugin.Transient, []string{tag, plugin.TagConnect}, err)
	}
	return plugin.Wrap(plugin.Transient, []string{tag, plugin.Tag5xx}, err)
}

// hasCode reports whether err is an azservicebus.Error with code.
func hasCode(err error, code azservicebus.Code) bool {
	var sbe *azservicebus.Error
	return errors.As(err, &sbe) && sbe.Code == code
}

// ownershipLost reports whether a partition reader was disconnected
// because another reader opened the partition with a higher owner level.
func ownershipLost(err error) bool {
	var ehe *azeventhubs.Error
	return errors.As(err, &ehe) && ehe.Code == azeventhubs.ErrorCodeOwnershipLost
}

// requiresSessions reports whether a receive failed because the entity
// requires sessions, so it must be received with a session receiver.
func requiresSessions(err error) bool {
	var ae *amqp.Error
	if !errors.As(err, &ae) {
		return false
	}
	// "It is not possible for an entity that requires sessions to create a
	// non-sessionful message receiver."
	d := strings.ToLower(ae.Description)
	return strings.Contains(d, "requires session") || strings.Contains(d, "non-sessionful")
}
