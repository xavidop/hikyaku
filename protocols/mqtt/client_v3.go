package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	mqtt3 "github.com/eclipse/paho.mqtt.golang"
)

// Timings of the MQTT 3.1.1 client.
const (
	// pingTimeout is how long the client waits for a PINGRESP.
	pingTimeout = 10 * time.Second
	// writeTimeout bounds a write to the broker.
	writeTimeout = 30 * time.Second
	// quiesceMillis is how long Disconnect lets the DISCONNECT go out.
	quiesceMillis = 250
)

// v3Client is a client over MQTT 3.1.1, built on paho.mqtt.golang with its
// own reconnects turned off: each connection is a new paho client, so an
// acknowledgement can never reach a later connection.
type v3Client struct {
	*ending
	lost chan struct{}
	pc   mqtt3.Client

	mu             sync.Mutex
	conn           *watchedConn
	sessionPresent bool
}

// connectV3 dials srv and connects over MQTT 3.1.1.
func connectV3(ctx context.Context, srv server, o connOptions, onMessage func(*message)) (client, error) {
	c := &v3Client{ending: newEnding(), lost: make(chan struct{})}
	user, password, _, _ := o.credentials(srv)
	opts := mqtt3.NewClientOptions()
	// paho dials through open below; the URL it is given carries no user
	// info, which paho would prefer over the credentials.
	opts.AddBroker(srv.brokerURL())
	opts.SetClientID(o.clientID)
	opts.SetUsername(user)
	opts.SetPassword(password)
	opts.SetCleanSession(o.cleanStart)
	opts.SetProtocolVersion(4) // 3.1.1, with no fallback to 3.1
	opts.SetKeepAlive(time.Duration(keepAliveSeconds(o.keepAlive)) * time.Second)
	opts.SetPingTimeout(pingTimeout)
	opts.SetConnectTimeout(o.connectTimeout)
	opts.SetWriteTimeout(writeTimeout)
	opts.SetAutoReconnect(false)
	opts.SetConnectRetry(false)
	opts.SetResumeSubs(false)
	// Handlers run one at a time in arrival order, the order acks must keep.
	opts.SetOrderMatters(true)
	opts.SetAutoAckDisabled(o.manualAck)
	opts.SetCustomOpenConnectionFn(func(*url.URL, mqtt3.ClientOptions) (net.Conn, error) {
		return c.open(ctx, srv, o.tls)
	})
	// No subscription has a handler of its own, so every message, including
	// those a resumed session delivers before SUBSCRIBE, comes here.
	opts.SetDefaultPublishHandler(func(_ mqtt3.Client, m mqtt3.Message) {
		if onMessage != nil {
			onMessage(c.message(m))
		}
	})
	opts.SetConnectionLostHandler(func(_ mqtt3.Client, err error) {
		c.finish(fmt.Errorf("connection lost: %w", err))
	})
	c.pc = mqtt3.NewClient(opts)
	tok := c.pc.Connect()
	select {
	case <-tok.Done():
	case <-ctx.Done():
		c.closeConn() // the handshake fails at once
		<-tok.Done()
		c.pc.Disconnect(0)
		return nil, fmt.Errorf("connect to %s: %w", srv.shown, ctx.Err())
	}
	if err := tok.Error(); err != nil {
		c.closeConn()
		if ct, ok := tok.(*mqtt3.ConnectToken); ok && ct.ReturnCode() > 0 && ct.ReturnCode() < 0x80 {
			return nil, &connackError{code: ct.ReturnCode(), v3: true}
		}
		return nil, fmt.Errorf("connect to %s: %w", srv.shown, err)
	}
	if ct, ok := tok.(*mqtt3.ConnectToken); ok {
		c.sessionPresent = ct.SessionPresent()
	}
	return c, nil
}

// open dials the one connection of this client.
func (c *v3Client) open(ctx context.Context, srv server, tlsCfg *tls.Config) (net.Conn, error) {
	raw, err := dial(ctx, srv, tlsCfg)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		_ = raw.Close()
		return nil, errors.New("an MQTT 3.1.1 client makes one connection")
	}
	c.conn = &watchedConn{Conn: raw, lost: c.lost}
	return c.conn, nil
}

func (c *v3Client) closeConn() {
	c.mu.Lock()
	wc := c.conn
	c.mu.Unlock()
	if wc != nil {
		_ = wc.Close()
	}
}

func (c *v3Client) SessionPresent() bool  { return c.sessionPresent }
func (c *v3Client) Lost() <-chan struct{} { return c.lost }

func (c *v3Client) Subscribe(ctx context.Context, filters []string, qos byte) error {
	m := make(map[string]byte, len(filters))
	for _, f := range filters {
		m[f] = qos
	}
	tok := c.pc.SubscribeMultiple(m, nil)
	if err := waitToken(ctx, tok, c.lost); err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	if st, ok := tok.(*mqtt3.SubscribeToken); ok {
		granted := st.Result()
		for _, f := range filters {
			if code, ok := granted[f]; ok && code >= 0x80 {
				return fmt.Errorf("subscribe to %q: the broker refused it (return code 0x%02x)", f, code)
			}
		}
	}
	return nil
}

func (c *v3Client) Publish(ctx context.Context, p *publication) error {
	return waitToken(ctx, c.pc.Publish(p.topic, p.qos, p.retain, p.payload), c.lost)
}

func (c *v3Client) Disconnect(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.pc.Disconnect(quiesceMillis)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		c.closeConn()
		err = ctx.Err()
	}
	c.finish(errDisconnected)
	return err
}

// message converts a received PUBLISH. Its acknowledgement goes out on the
// connection it came on, or nowhere once that connection is gone.
func (c *v3Client) message(m mqtt3.Message) *message {
	msg := &message{
		topic: m.Topic(), payload: m.Payload(), qos: m.Qos(), retained: m.Retained(),
		duplicate: m.Duplicate(), packetID: m.MessageID(), lost: c.lost,
	}
	if m.Qos() > 0 {
		msg.ack = func() error {
			m.Ack()
			return nil
		}
	}
	return msg
}

// waitToken waits for a paho token, the context or the connection's loss;
// a token that completed wins.
func waitToken(ctx context.Context, tok mqtt3.Token, lost <-chan struct{}) error {
	select {
	case <-tok.Done():
		return tok.Error()
	case <-ctx.Done():
	case <-lost:
	}
	select {
	case <-tok.Done():
		return tok.Error()
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return errNotConnected
}
