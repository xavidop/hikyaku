package mqtt

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
)

// packetTimeout bounds paho's wait for a SUBACK, PUBACK or PUBCOMP; the
// caller's context usually ends sooner.
const packetTimeout = time.Minute

// v5Client is a client over MQTT 5, built on paho.golang's paho package.
// Each connection is a new paho.Client with its own session state: a
// persistent session lives on the broker, which redelivers what was not
// acknowledged, and an acknowledgement can never reach a later connection.
type v5Client struct {
	*ending
	conn           *watchedConn
	pc             *paho.Client
	session        *ackSession // a source's: it sends the acknowledgements
	sessionPresent bool
}

// ackSession is the session state of a source's connection, with the
// acknowledgements taken over. paho either acknowledges a message as soon as
// its handler returns, or, with manual acknowledgement, through a tracker it
// resets after the connection has begun handing messages over, which drops
// the acknowledgements of the first messages a resumed session delivers. So
// paho's own Ack does nothing here, and the source's ack queue sends each
// PUBACK or PUBREC through send, in arrival order, once the relay settled
// the message.
type ackSession struct{ *state.State }

// Ack is paho's automatic acknowledgement: dropped.
func (ackSession) Ack(*packets.Publish) error { return nil }

// send acknowledges a received message: PUBACK (QoS 1) or PUBREC (QoS 2).
// It does nothing once the connection is gone.
func (s ackSession) send(pb *packets.Publish) error { return s.State.Ack(pb) }

// connectV5 dials srv and connects over MQTT 5.
func connectV5(ctx context.Context, srv server, o connOptions, onMessage func(*message)) (client, error) {
	raw, err := dial(ctx, srv, o.tls)
	if err != nil {
		return nil, err
	}
	c := &v5Client{ending: newEnding(), conn: watch(raw)}
	handle := func(paho.PublishReceived) (bool, error) { return true, nil }
	if onMessage != nil {
		handle = func(pr paho.PublishReceived) (bool, error) {
			onMessage(c.message(pr))
			return true, nil
		}
	}
	cfg := paho.ClientConfig{
		ClientID:           o.clientID,
		Conn:               packets.NewThreadSafeConn(c.conn),
		PacketTimeout:      packetTimeout,
		OnPublishReceived:  []func(paho.PublishReceived) (bool, error){handle},
		OnClientError:      func(err error) { c.finish(fmt.Errorf("connection lost: %w", err)) },
		OnServerDisconnect: func(d *paho.Disconnect) { c.finish(disconnectError(d)) },
	}
	if o.manualAck {
		c.session = &ackSession{State: state.NewInMemory()}
		cfg.Session = c.session
	}
	c.pc = paho.NewClient(cfg)
	user, password, hasUser, hasPassword := o.credentials(srv)
	cp := &paho.Connect{
		ClientID:     o.clientID,
		KeepAlive:    keepAliveSeconds(o.keepAlive),
		CleanStart:   o.cleanStart,
		Username:     user,
		UsernameFlag: hasUser,
		Password:     []byte(password),
		PasswordFlag: hasPassword,
		Properties:   &paho.ConnectProperties{RequestProblemInfo: true},
	}
	if o.sessionExpiry > 0 {
		expiry := o.sessionExpiry
		cp.Properties.SessionExpiryInterval = &expiry
	}
	if o.receiveMaximum > 0 {
		receiveMax := o.receiveMaximum
		cp.Properties.ReceiveMaximum = &receiveMax
	}
	ca, err := c.pc.Connect(ctx, cp)
	if err != nil {
		_ = c.conn.Close()
		c.closeSession()
		if ca != nil && ca.ReasonCode >= 0x80 {
			reason := ""
			if ca.Properties != nil {
				reason = ca.Properties.ReasonString
			}
			return nil, &connackError{code: ca.ReasonCode, reason: reason}
		}
		return nil, fmt.Errorf("connect to %s: %w", srv.shown, err)
	}
	c.sessionPresent = ca.SessionPresent
	if c.session != nil {
		// paho closes only the session state it made itself.
		go func() {
			<-c.pc.Done()
			c.closeSession()
		}()
	}
	return c, nil
}

func (c *v5Client) closeSession() {
	if c.session != nil {
		_ = c.session.Close()
	}
}

// keepAliveSeconds rounds a keepalive up to whole seconds (0 disables it).
func keepAliveSeconds(d time.Duration) uint16 {
	if d <= 0 {
		return 0
	}
	return uint16(min((d+time.Second-1)/time.Second, 65535))
}

func (c *v5Client) SessionPresent() bool  { return c.sessionPresent }
func (c *v5Client) Lost() <-chan struct{} { return c.conn.lost }

func (c *v5Client) Subscribe(ctx context.Context, filters []string, qos byte) error {
	subs := make([]paho.SubscribeOptions, len(filters))
	for i, f := range filters {
		// Retain handling 1: retained messages come only with a new
		// subscription, not again each time a resumed session resubscribes.
		subs[i] = paho.SubscribeOptions{Topic: f, QoS: qos, RetainHandling: 1}
	}
	sctx, cancel := whileConnected(ctx, c.conn.lost)
	defer cancel()
	sa, err := c.pc.Subscribe(sctx, &paho.Subscribe{Subscriptions: subs})
	if err != nil && lostWhileWaiting(ctx, sctx) {
		return fmt.Errorf("subscribe: %w", errNotConnected)
	}
	if sa != nil {
		for i, code := range sa.Reasons {
			if code >= 0x80 && i < len(filters) {
				reason := ""
				if sa.Properties != nil {
					reason = sa.Properties.ReasonString
				}
				return fmt.Errorf("subscribe to %q: %w", filters[i], &reasonError{code: code, reason: reason})
			}
		}
	}
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	return nil
}

func (c *v5Client) Publish(ctx context.Context, p *publication) error {
	pub := &paho.Publish{QoS: p.qos, Retain: p.retain, Topic: p.topic, Payload: p.payload}
	if p.contentType != "" || len(p.userProps) > 0 {
		props := &paho.PublishProperties{ContentType: p.contentType}
		for _, u := range p.userProps {
			props.User = append(props.User, paho.UserProperty{Key: u.key, Value: u.value})
		}
		pub.Properties = props
	}
	// paho forgets a publish waiting for its acknowledgement when a
	// connection without a persistent session drops, so the wait also ends
	// with the connection.
	pctx, cancel := whileConnected(ctx, c.conn.lost)
	defer cancel()
	resp, err := c.pc.Publish(pctx, pub)
	if err != nil && lostWhileWaiting(ctx, pctx) {
		return fmt.Errorf("%w: the connection dropped before the broker acknowledged the publish", errNotConnected)
	}
	// paho reports a refused QoS 1 publish as an error, a refused QoS 2 one
	// only through the PUBREC's reason code.
	if resp != nil && resp.ReasonCode >= 0x80 {
		reason := ""
		if resp.Properties != nil {
			reason = resp.Properties.ReasonString
		}
		return &reasonError{code: resp.ReasonCode, reason: reason}
	}
	if errors.Is(err, paho.ErrInvalidArguments) {
		return fmt.Errorf("%w: %v", errUnsupported, err)
	}
	return err
}

// Disconnect sends DISCONNECT with reason 0: the broker keeps a persistent
// session.
func (c *v5Client) Disconnect(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		// A failed write means the connection was already gone.
		_ = c.pc.Disconnect(&paho.Disconnect{ReasonCode: 0})
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		_ = c.conn.Close()
		err = ctx.Err()
	}
	c.finish(errDisconnected)
	return err
}

// message converts a received PUBLISH.
func (c *v5Client) message(pr paho.PublishReceived) *message {
	p := pr.Packet
	m := &message{
		topic: p.Topic, payload: p.Payload, qos: p.QoS, retained: p.Retain,
		duplicate: p.Duplicate(), packetID: p.PacketID, lost: c.conn.lost,
	}
	if p.Properties != nil {
		m.contentType = p.Properties.ContentType
		m.correlation = p.Properties.CorrelationData
		for _, u := range p.Properties.User {
			m.userProps = append(m.userProps, userProp{key: u.Key, value: u.Value})
		}
	}
	if p.QoS > 0 && c.session != nil {
		sess := c.session
		m.ack = func() error { return sess.send(p.Packet()) }
	}
	return m
}

// disconnectError is why the broker ended a connection with DISCONNECT.
func disconnectError(d *paho.Disconnect) error {
	reason := ""
	if d.Properties != nil {
		reason = d.Properties.ReasonString
	}
	return &disconnectedError{code: d.ReasonCode, reason: reason}
}
