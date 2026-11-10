package mqtt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// dial opens the network connection to srv: TCP, TLS, or a WebSocket
// speaking the mqtt subprotocol. ctx bounds the dial and the handshakes.
func dial(ctx context.Context, srv server, tlsCfg *tls.Config) (net.Conn, error) {
	var (
		c   net.Conn
		err error
	)
	switch srv.transport {
	case transportTCP:
		var d net.Dialer
		c, err = d.DialContext(ctx, "tcp", srv.addr)
	case transportTLS:
		d := tls.Dialer{Config: tlsCfg}
		c, err = d.DialContext(ctx, "tcp", srv.addr)
	default:
		c, err = dialWebSocket(ctx, srv, tlsCfg)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", srv.shown, err)
	}
	return c, nil
}

// dialWebSocket dials a ws:// or wss:// broker.
func dialWebSocket(ctx context.Context, srv server, tlsCfg *tls.Config) (net.Conn, error) {
	d := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: defaultConnectTimeout,
		Subprotocols:     []string{"mqtt"},
	}
	if srv.transport == transportWSS {
		d.TLSClientConfig = tlsCfg
	}
	ws, resp, err := d.DialContext(ctx, srv.wsURL, nil)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("WebSocket handshake answered %s: %w", resp.Status, err)
		}
		return nil, err
	}
	return &wsConn{ws: ws}, nil
}

// wsConn carries a byte stream over WebSocket binary messages: MQTT packets
// need not align with message boundaries (MQTT 5 §6, 3.1.1 §6).
type wsConn struct {
	ws *websocket.Conn

	rmu sync.Mutex
	r   io.Reader // the message being read

	wmu sync.Mutex
}

func (c *wsConn) Read(b []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for {
		if c.r == nil {
			typ, r, err := c.ws.NextReader()
			if err != nil {
				return 0, err
			}
			if typ != websocket.BinaryMessage {
				return 0, errors.New("the broker sent a WebSocket text message; MQTT uses binary messages")
			}
			c.r = r
		}
		n, err := c.r.Read(b)
		if errors.Is(err, io.EOF) {
			c.r = nil
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (c *wsConn) Write(b []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Close closes the network connection; a broker that is going away needs no
// close handshake.
func (c *wsConn) Close() error { return c.ws.Close() }

func (c *wsConn) LocalAddr() net.Addr  { return c.ws.LocalAddr() }
func (c *wsConn) RemoteAddr() net.Addr { return c.ws.RemoteAddr() }

func (c *wsConn) SetDeadline(t time.Time) error {
	return errors.Join(c.ws.SetReadDeadline(t), c.ws.SetWriteDeadline(t))
}

func (c *wsConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }
