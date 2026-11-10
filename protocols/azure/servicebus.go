package azure

import (
	"context"
	"errors"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// The Service Bus client surface the source and destination use, small so
// tests can fake it.

// sbClient is a Service Bus namespace connection.
type sbClient interface {
	// newReceiver returns a PeekLock receiver of a queue or subscription.
	newReceiver(e sbEntity) (sbReceiver, error)
	// acceptNextSession locks the next session with messages, waiting for
	// one (an *azservicebus.Error with CodeTimeout when none comes).
	acceptNextSession(ctx context.Context, e sbEntity) (sbSessionReceiver, error)
	newSender(queueOrTopic string) (sbSender, error)
	Close(ctx context.Context) error
}

// sbSettler settles received messages.
type sbSettler interface {
	CompleteMessage(ctx context.Context, m *azservicebus.ReceivedMessage, o *azservicebus.CompleteMessageOptions) error
	AbandonMessage(ctx context.Context, m *azservicebus.ReceivedMessage, o *azservicebus.AbandonMessageOptions) error
}

// sbReceiver receives from a queue or subscription without sessions.
type sbReceiver interface {
	sbSettler
	ReceiveMessages(ctx context.Context, maxMessages int, o *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	RenewMessageLock(ctx context.Context, m *azservicebus.ReceivedMessage, o *azservicebus.RenewMessageLockOptions) error
	PeekMessages(ctx context.Context, maxMessages int, o *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	Close(ctx context.Context) error
}

// sbSessionReceiver receives one locked session.
type sbSessionReceiver interface {
	sbSettler
	ReceiveMessages(ctx context.Context, maxMessages int, o *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error)
	SessionID() string
	LockedUntil() time.Time
	RenewSessionLock(ctx context.Context, o *azservicebus.RenewSessionLockOptions) error
	Close(ctx context.Context) error
}

// sbSender sends to a queue or topic.
type sbSender interface {
	SendMessage(ctx context.Context, m *azservicebus.Message, o *azservicebus.SendMessageOptions) error
	newBatch(ctx context.Context) (sbBatch, error)
	sendBatch(ctx context.Context, b sbBatch) error
	Close(ctx context.Context) error
}

// sbBatch is a message batch being filled.
type sbBatch interface {
	// AddMessage fails with azservicebus.ErrMessageTooLarge when the
	// message does not fit.
	AddMessage(m *azservicebus.Message, o *azservicebus.AddMessageOptions) error
	NumMessages() int32
}

// dialServiceBus opens a client: from the connection string, or for the
// namespace with the credential.
func dialServiceBus(c conn, retry azservicebus.RetryOptions) (sbClient, error) {
	opts := &azservicebus.ClientOptions{TLSConfig: c.tls, RetryOptions: retry, ApplicationID: "hikyaku"}
	var (
		cl  *azservicebus.Client
		err error
	)
	if c.connectionString != "" {
		cl, err = azservicebus.NewClientFromConnectionString(c.connectionString, opts)
	} else {
		cl, err = azservicebus.NewClient(c.namespace, c.credential, opts)
	}
	if err != nil {
		// The SDK's errors do not quote the connection string.
		return nil, err
	}
	return sbClientSDK{cl}, nil
}

type sbClientSDK struct{ c *azservicebus.Client }

func (s sbClientSDK) newReceiver(e sbEntity) (sbReceiver, error) {
	var (
		r   *azservicebus.Receiver
		err error
	)
	if e.queue != "" {
		r, err = s.c.NewReceiverForQueue(e.queue, nil)
	} else {
		r, err = s.c.NewReceiverForSubscription(e.topic, e.subscription, nil)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s sbClientSDK) acceptNextSession(ctx context.Context, e sbEntity) (sbSessionReceiver, error) {
	var (
		r   *azservicebus.SessionReceiver
		err error
	)
	if e.queue != "" {
		r, err = s.c.AcceptNextSessionForQueue(ctx, e.queue, nil)
	} else {
		r, err = s.c.AcceptNextSessionForSubscription(ctx, e.topic, e.subscription, nil)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s sbClientSDK) newSender(entity string) (sbSender, error) {
	snd, err := s.c.NewSender(entity, nil)
	if err != nil {
		return nil, err
	}
	return sbSenderSDK{snd}, nil
}

func (s sbClientSDK) Close(ctx context.Context) error { return s.c.Close(ctx) }

type sbSenderSDK struct{ *azservicebus.Sender }

func (s sbSenderSDK) newBatch(ctx context.Context) (sbBatch, error) {
	b, err := s.NewMessageBatch(ctx, nil)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (s sbSenderSDK) sendBatch(ctx context.Context, b sbBatch) error {
	mb, ok := b.(*azservicebus.MessageBatch)
	if !ok {
		return errors.New("servicebus: not an SDK batch")
	}
	return s.SendMessageBatch(ctx, mb, nil)
}
