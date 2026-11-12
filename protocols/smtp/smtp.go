// Package smtp is hikyaku's email destination: each message becomes one
// email, sent through an SMTP server. It is the plain way to get an alert
// (notify) or a report to people.
//
// The URL's scheme picks the transport: smtps:// is TLS from the first byte
// (port 465), smtp:// upgrades with STARTTLS and refuses a server that does
// not offer it (port 587), and smtp+plain:// uses no TLS at all, for a relay
// on the same host or network (port 25).
package smtp

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/mail"
	netsmtp "net/smtp"
	"net/textproto"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/internal/httpmux"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Kind is the destination type.
const Kind = "smtp"

// Error tags, next to smtp:<code> for a server's reply.
const (
	TagAuth     = "smtp:auth"     // the server refused the user name or password
	TagStartTLS = "smtp:starttls" // smtp:// to a server without STARTTLS
	TagAddress  = "smtp:address"  // a recipient that is not an email address
)

// DefaultTimeout bounds one delivery when the attempt has no deadline.
const DefaultTimeout = 30 * time.Second

// Register makes the smtp destination available.
func Register() {
	plugin.RegisterSink(Kind, func(_ context.Context, s plugin.Spec) (plugin.Sink, error) { return newSink(s) })
}

// Sink sends one email per message.
type Sink struct {
	name     string
	addr     string // host:port
	host     string
	implicit bool // smtps://
	plain    bool // smtp+plain://
	tls      *tls.Config
	user     string
	pass     string
	from     *mail.Address
	to       []*transform.Template
	subject  *transform.Template
	body     *transform.Template
	headers  map[string]*transform.Template
	helo     string
	now      func() time.Time
}

// reserved are the headers the sink writes itself.
var reserved = map[string]bool{
	"From": true, "To": true, "Subject": true, "Date": true, "Message-Id": true, "Mime-Version": true,
	"Content-Type": true, "Content-Transfer-Encoding": true, "Bcc": true, "Cc": true,
}

func newSink(s plugin.Spec) (*Sink, error) {
	ep, ok := s.Config.(*config.Endpoint)
	if !ok || ep == nil {
		return nil, fmt.Errorf("destination %s: missing config", s.Name)
	}
	bad := func(format string, args ...any) (*Sink, error) {
		return nil, fmt.Errorf("destination %s: %s", s.Name, fmt.Sprintf(format, args...))
	}
	u, err := url.Parse(ep.URL)
	if ep.URL == "" || err != nil || u.Hostname() == "" {
		return bad("url must be smtp://host, smtps://host or smtp+plain://host")
	}
	sk := &Sink{name: s.Name, host: u.Hostname(), user: ep.Username, pass: ep.Password, headers: map[string]*transform.Template{}, now: time.Now}
	port := u.Port()
	switch u.Scheme {
	case "smtps":
		sk.implicit = true
		port = orDefault(port, "465")
	case "smtp":
		port = orDefault(port, "587")
	case "smtp+plain":
		sk.plain = true
		port = orDefault(port, "25")
	default:
		return bad("url scheme must be smtp, smtps or smtp+plain, not %q", u.Scheme)
	}
	sk.addr = net.JoinHostPort(sk.host, port)
	if u.User != nil {
		return bad("put the user name and password in username and password, not in the url")
	}
	if (sk.user == "") != (sk.pass == "") {
		return bad("username and password go together")
	}
	if sk.tls, err = httpmux.ClientTLS(ep.TLS); err != nil {
		return bad("%v", err)
	}
	if sk.tls == nil {
		sk.tls = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if sk.tls.ServerName == "" {
		sk.tls.ServerName = sk.host
	}
	if sk.plain && ep.TLS != nil {
		return bad("tls does not go with smtp+plain://")
	}
	if sk.from, err = mail.ParseAddress(ep.From); err != nil {
		return bad("from %q is not an email address", ep.From)
	}
	if len(ep.To) == 0 {
		return bad("to needs at least one address")
	}
	for i, t := range ep.To {
		tpl, err := transform.CompileTemplate(fmt.Sprintf("to[%d]", i), t)
		if err != nil {
			return bad("%v", err)
		}
		if tpl.IsStatic() {
			if _, err := mail.ParseAddressList(t); err != nil {
				return bad("to[%d] %q is not an email address", i, t)
			}
		}
		sk.to = append(sk.to, tpl)
	}
	subject := ep.Subject
	if subject == "" {
		subject = "Message from hikyaku ({{ .source }})"
	}
	if sk.subject, err = transform.CompileTemplate("subject", subject); err != nil {
		return bad("%v", err)
	}
	if ep.Body != "" {
		if sk.body, err = transform.CompileTemplate("body", ep.Body); err != nil {
			return bad("%v", err)
		}
	}
	for k, v := range ep.Headers {
		ck := textproto.CanonicalMIMEHeaderKey(k)
		if reserved[ck] {
			return bad("headers.%s is set by the destination", k)
		}
		if strings.ContainsAny(k, ": \r\n") {
			return bad("headers: %q is not a header name", k)
		}
		if sk.headers[ck], err = transform.CompileTemplate("headers."+k, v); err != nil {
			return bad("%v", err)
		}
	}
	if sk.helo, _ = os.Hostname(); sk.helo == "" {
		sk.helo = "localhost"
	}
	return sk, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// Name is the destination name.
func (s *Sink) Name() string { return s.name }

// Deliver sends the message as one email, on a connection of its own.
func (s *Sink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	rcpts, err := s.recipients(env)
	if err != nil {
		return nil, err
	}
	msg, err := s.message(env, rcpts)
	if err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultTimeout)
		defer cancel()
	}
	c, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close() }()
	if err := s.send(c, rcpts, msg); err != nil {
		if ctx.Err() != nil {
			return nil, plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, fmt.Errorf("smtp: %w", ctx.Err()))
		}
		return nil, err
	}
	_ = c.Quit()
	return &plugin.Result{Status: 250, Endpoint: s.scheme() + "://" + s.addr}, nil
}

func (s *Sink) scheme() string {
	switch {
	case s.implicit:
		return "smtps"
	case s.plain:
		return "smtp+plain"
	}
	return "smtp"
}

// recipients renders to: each entry may come out as several addresses.
func (s *Sink) recipients(env *envelope.Envelope) ([]*mail.Address, error) {
	var out []*mail.Address
	seen := map[string]bool{}
	for _, t := range s.to {
		v, err := t.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		if strings.TrimSpace(v) == "" {
			continue
		}
		list, err := mail.ParseAddressList(v)
		if err != nil {
			return nil, plugin.Errorf(plugin.Permanent, []string{TagAddress}, "smtp: %q is not an email address", v)
		}
		for _, a := range list {
			if !seen[strings.ToLower(a.Address)] {
				seen[strings.ToLower(a.Address)] = true
				out = append(out, a)
			}
		}
	}
	if len(out) == 0 {
		return nil, plugin.Errorf(plugin.Permanent, []string{TagAddress}, "smtp: the message has no recipient")
	}
	return out, nil
}

// oneLine keeps a header value on one line: a message must not be able to
// add headers of its own.
func oneLine(v string) string {
	return strings.TrimSpace(strings.NewReplacer("\r\n", " ", "\r", " ", "\n", " ").Replace(v))
}

// message builds the email: headers, then the body in base64 (any bytes,
// any line length).
func (s *Sink) message(env *envelope.Envelope, rcpts []*mail.Address) ([]byte, error) {
	subject, err := s.subject.Render(env)
	if err != nil {
		return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
	}
	body := env.Body
	if s.body != nil {
		b, err := s.body.Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		body = []byte(b)
	}
	ctype := "text/plain; charset=utf-8"
	if mt, _, err := mime.ParseMediaType(env.ContentType); err == nil && mt == "text/html" {
		ctype = "text/html; charset=utf-8"
	}
	tos := make([]string, len(rcpts))
	for i, a := range rcpts {
		tos[i] = a.String()
	}
	domain := s.from.Address[strings.LastIndex(s.from.Address, "@")+1:]
	var b bytes.Buffer
	header := func(k, v string) { fmt.Fprintf(&b, "%s: %s\r\n", k, v) }
	header("From", s.from.String())
	header("To", strings.Join(tos, ", "))
	header("Subject", mime.QEncoding.Encode("utf-8", oneLine(subject)))
	header("Date", s.now().Format(time.RFC1123Z))
	header("Message-ID", "<"+messageID(env.ID)+"@"+domain+">")
	names := make([]string, 0, len(s.headers))
	for k := range s.headers {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		v, err := s.headers[k].Render(env)
		if err != nil {
			return nil, plugin.Wrap(plugin.Permanent, []string{"template"}, err)
		}
		if v = oneLine(v); v != "" {
			header(k, mime.QEncoding.Encode("utf-8", v))
		}
	}
	header("MIME-Version", "1.0")
	header("Content-Type", ctype)
	header("Content-Transfer-Encoding", "base64")
	b.WriteString("\r\n")
	enc := base64.StdEncoding.EncodeToString(body)
	for len(enc) > 76 {
		b.WriteString(enc[:76])
		b.WriteString("\r\n")
		enc = enc[76:]
	}
	b.WriteString(enc)
	b.WriteString("\r\n")
	return b.Bytes(), nil
}

// messageID keeps the characters of id that a Message-ID may hold.
func messageID(id string) string {
	out := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			return r
		}
		return '-'
	}, id)
	if out == "" {
		return "message"
	}
	return out
}

// connect dials the server, says hello, and brings TLS up.
func (s *Sink) connect(ctx context.Context) (*netsmtp.Client, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return nil, dialError(ctx, err)
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	// A cancelled delivery must not sit in a read.
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	fail := func(err error) (*netsmtp.Client, error) {
		stop()
		_ = conn.Close()
		if ctx.Err() != nil {
			return nil, dialError(ctx, ctx.Err())
		}
		return nil, err
	}
	if s.implicit {
		tc := tls.Client(conn, s.tls)
		if err := tc.HandshakeContext(ctx); err != nil {
			return fail(plugin.Wrap(plugin.Transient, []string{plugin.TagConnect, "smtp:tls"}, fmt.Errorf("smtp: TLS handshake: %w", err)))
		}
		conn = tc
	}
	c, err := netsmtp.NewClient(conn, s.host)
	if err != nil {
		return fail(replyError("greeting", err))
	}
	if err := c.Hello(s.helo); err != nil {
		return fail(replyError("EHLO", err))
	}
	if !s.implicit && !s.plain {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return fail(plugin.Errorf(plugin.Permanent, []string{TagStartTLS}, "smtp: %s does not offer STARTTLS (use smtps:// for TLS from the start, or smtp+plain:// for no TLS)", s.addr))
		}
		if err := c.StartTLS(s.tls); err != nil {
			return fail(plugin.Wrap(plugin.Transient, []string{plugin.TagConnect, "smtp:tls"}, fmt.Errorf("smtp: STARTTLS: %w", err)))
		}
	}
	if s.user != "" {
		if err := c.Auth(s.auth(c)); err != nil {
			var te *textproto.Error
			if errors.As(err, &te) && te.Code >= 500 {
				return fail(plugin.Wrap(plugin.Permanent, []string{TagAuth, "smtp:" + strconv.Itoa(te.Code)}, fmt.Errorf("smtp: the server refused the user name or password: %w", err)))
			}
			if !errors.As(err, &te) {
				return fail(plugin.Wrap(plugin.Permanent, []string{TagAuth}, fmt.Errorf("smtp: auth: %w", err)))
			}
			return fail(replyError("AUTH", err))
		}
	}
	return c, nil
}

// auth picks PLAIN, or LOGIN for a server that offers only that (Microsoft
// 365). Both refuse to send a password over a connection without TLS,
// unless the server is this host.
func (s *Sink) auth(c *netsmtp.Client) netsmtp.Auth {
	if _, mechs := c.Extension("AUTH"); !strings.Contains(" "+strings.ToUpper(mechs)+" ", " PLAIN ") && strings.Contains(" "+strings.ToUpper(mechs)+" ", " LOGIN ") {
		return &loginAuth{user: s.user, pass: s.pass, host: s.host}
	}
	return netsmtp.PlainAuth("", s.user, s.pass, s.host)
}

func (s *Sink) send(c *netsmtp.Client, rcpts []*mail.Address, msg []byte) error {
	if err := c.Mail(s.from.Address); err != nil {
		return replyError("MAIL FROM", err)
	}
	for _, a := range rcpts {
		if err := c.Rcpt(a.Address); err != nil {
			return replyError("RCPT TO "+a.Address, err)
		}
	}
	w, err := c.Data()
	if err != nil {
		return replyError("DATA", err)
	}
	if _, err := w.Write(msg); err != nil {
		return replyError("DATA", err)
	}
	if err := w.Close(); err != nil {
		return replyError("DATA", err)
	}
	return nil
}

func dialError(ctx context.Context, err error) error {
	var ne net.Error
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, fmt.Errorf("smtp: %w", err))
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("smtp: %w", err))
}

// replyError classifies a failed step. A 4yz reply is the server asking to
// try again later (transient); a 5yz reply will not change (permanent).
// Anything else is the connection.
func replyError(step string, err error) error {
	var te *textproto.Error
	if errors.As(err, &te) {
		class := plugin.Permanent
		if te.Code >= 400 && te.Code < 500 {
			class = plugin.Transient
		}
		return plugin.Wrap(class, []string{"smtp:" + strconv.Itoa(te.Code)}, fmt.Errorf("smtp: %s: %w", step, err))
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, fmt.Errorf("smtp: %s: %w", step, err))
	}
	return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, fmt.Errorf("smtp: %s: %w", step, err))
}

// Health dials the server.
func (s *Sink) Health(ctx context.Context) error {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", s.addr, err)
	}
	return c.Close()
}

// Close does nothing: each delivery closes its own connection.
func (s *Sink) Close(context.Context) error { return nil }

// loginAuth is the LOGIN mechanism: the server asks for the user name, then
// the password.
type loginAuth struct{ user, pass, host string }

func (a *loginAuth) Start(server *netsmtp.ServerInfo) (string, []byte, error) {
	if !server.TLS && server.Name != "localhost" && server.Name != "127.0.0.1" && server.Name != "::1" {
		return "", nil, errors.New("unencrypted connection")
	}
	if server.Name != a.host {
		return "", nil, errors.New("wrong host name")
	}
	return "LOGIN", nil, nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}
	switch strings.ToLower(strings.TrimRight(string(fromServer), ": ")) {
	case "username", "user name":
		return []byte(a.user), nil
	case "password":
		return []byte(a.pass), nil
	}
	return nil, fmt.Errorf("unexpected LOGIN prompt %q", fromServer)
}
