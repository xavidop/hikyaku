package smtp

import (
	"context"
	"encoding/base64"
	"mime"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
)

func init() { Register() }

func sink(t *testing.T, ep config.Endpoint) plugin.Sink {
	t.Helper()
	ep.Type = Kind
	if ep.From == "" {
		ep.From = "Relay <relay@example.com>"
	}
	if ep.To == nil {
		ep.To = []string{"ops@example.com"}
	}
	s, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "mail", Kind: Kind, Config: &ep})
	require.NoError(t, err)
	return s
}

func msg(body string) *envelope.Envelope {
	e := envelope.New("alerts", "http", []byte(body))
	e.ID = "01JABC"
	e.ContentType = "application/json"
	return e
}

// parse reads a sent email back: its headers and its decoded body.
func parse(t *testing.T, data string) (mail.Header, string) {
	t.Helper()
	m, err := mail.ReadMessage(strings.NewReader(data))
	require.NoError(t, err)
	var b strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := m.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			break
		}
	}
	body, err := base64.StdEncoding.DecodeString(strings.NewReplacer("\r", "", "\n", "").Replace(b.String()))
	require.NoError(t, err)
	return m.Header, string(body)
}

func classOf(err error) plugin.Class {
	c, _ := plugin.Classify(err)
	return c
}

func TestSendsOverSTARTTLSWithPlainAuth(t *testing.T) {
	srv := newServer(t, func(s *server) { s.starttls, s.mechs, s.user, s.pass = true, "PLAIN LOGIN", "relay", "s3cret" })
	s := sink(t, config.Endpoint{
		URL: "smtp://" + srv.addr(), Username: "relay", Password: "s3cret", TLS: &config.TLS{CAFile: srv.caFile},
		To:      []string{"ops@example.com", "{{ .json.owner }}"},
		Subject: "Dead letter on {{ .json.destination }} ✉",
		Headers: map[string]string{"Reply-To": "team@example.com", "X-Relay": "{{ .source }}"},
	})
	body := `{"destination":"crm","owner":"Ana <ana@example.com>, bo@example.com","text":"` + strings.Repeat("long ", 60) + `"}`
	res, err := s.Deliver(t.Context(), msg(body))
	require.NoError(t, err)
	assert.Equal(t, 250, res.Status)
	assert.Equal(t, "smtp://"+srv.addr(), res.Endpoint)

	got := srv.got()
	require.Len(t, got, 1)
	m := got[0]
	assert.True(t, m.tls, "the email went over TLS")
	assert.Equal(t, "relay", m.user)
	assert.Equal(t, "relay@example.com", m.from)
	assert.Equal(t, []string{"ops@example.com", "ana@example.com", "bo@example.com"}, m.to)

	h, text := parse(t, m.data)
	assert.Equal(t, body, text)
	assert.Equal(t, `"Relay" <relay@example.com>`, h.Get("From"))
	assert.Equal(t, `<ops@example.com>, "Ana" <ana@example.com>, <bo@example.com>`, h.Get("To"))
	dec, err := new(mime.WordDecoder).DecodeHeader(h.Get("Subject"))
	require.NoError(t, err)
	assert.Equal(t, "Dead letter on crm ✉", dec)
	assert.Equal(t, "team@example.com", h.Get("Reply-To"))
	assert.Equal(t, "alerts", h.Get("X-Relay"))
	assert.Equal(t, "<01JABC@example.com>", h.Get("Message-Id"))
	assert.Equal(t, "text/plain; charset=utf-8", h.Get("Content-Type"))
	_, err = h.Date()
	assert.NoError(t, err)
	for _, l := range strings.Split(m.data, "\r\n") {
		assert.LessOrEqual(t, len(l), 998, "no line is over the SMTP limit")
	}
}

func TestSendsOverImplicitTLSWithLoginAuth(t *testing.T) {
	srv := newServer(t, func(s *server) { s.implicit, s.mechs, s.user, s.pass = true, "LOGIN XOAUTH2", "relay", "s3cret" })
	s := sink(t, config.Endpoint{URL: "smtps://" + srv.addr(), Username: "relay", Password: "s3cret", TLS: &config.TLS{CAFile: srv.caFile}, Body: "<b>{{ .json.n }}</b>"})
	e := msg(`{"n":7}`)
	e.ContentType = "text/html"
	_, err := s.Deliver(t.Context(), e)
	require.NoError(t, err)
	got := srv.got()
	require.Len(t, got, 1)
	assert.Equal(t, "s3cret", got[0].pass)
	h, text := parse(t, got[0].data)
	assert.Equal(t, "<b>7</b>", text)
	assert.Equal(t, "text/html; charset=utf-8", h.Get("Content-Type"))
	assert.Equal(t, "Message from hikyaku (alerts)", h.Get("Subject"))
}

func TestSendsWithoutTLSToALocalRelay(t *testing.T) {
	srv := newServer(t)
	s := sink(t, config.Endpoint{URL: "smtp+plain://" + srv.addr()})
	_, err := s.Deliver(t.Context(), msg(`{}`))
	require.NoError(t, err)
	require.Len(t, srv.got(), 1)
	assert.False(t, srv.got()[0].tls)
	require.NoError(t, s.Health(t.Context()))
	require.NoError(t, s.Close(t.Context()))
}

func TestRefusesAServerWithoutSTARTTLS(t *testing.T) {
	srv := newServer(t) // offers no STARTTLS
	s := sink(t, config.Endpoint{URL: "smtp://" + srv.addr(), Username: "relay", Password: "s3cret"})
	_, err := s.Deliver(t.Context(), msg(`{}`))
	require.Error(t, err)
	assert.Equal(t, plugin.Permanent, classOf(err))
	assert.Contains(t, plugin.Tags(err), TagStartTLS)
	assert.Empty(t, srv.got())
}

func TestFailures(t *testing.T) {
	t.Run("wrong password", func(t *testing.T) {
		srv := newServer(t, func(s *server) { s.starttls, s.mechs, s.user, s.pass = true, "PLAIN", "relay", "s3cret" })
		s := sink(t, config.Endpoint{URL: "smtp://" + srv.addr(), Username: "relay", Password: "wrong-pass", TLS: &config.TLS{CAFile: srv.caFile}})
		_, err := s.Deliver(t.Context(), msg(`{}`))
		require.Error(t, err)
		assert.Equal(t, plugin.Permanent, classOf(err))
		assert.Subset(t, plugin.Tags(err), []string{TagAuth, "smtp:535"})
		assert.NotContains(t, err.Error(), "wrong-pass")
	})
	t.Run("a mailbox that does not exist is permanent", func(t *testing.T) {
		srv := newServer(t, func(s *server) { s.reply["RCPT TO"] = "550 5.1.1 no such user" })
		_, err := sink(t, config.Endpoint{URL: "smtp+plain://" + srv.addr()}).Deliver(t.Context(), msg(`{}`))
		require.Error(t, err)
		assert.Equal(t, plugin.Permanent, classOf(err))
		assert.Contains(t, plugin.Tags(err), "smtp:550")
		assert.Empty(t, srv.got())
	})
	t.Run("a server asking to come back later is transient", func(t *testing.T) {
		srv := newServer(t, func(s *server) { s.reply["MAIL FROM"] = "451 4.7.1 try again later" })
		_, err := sink(t, config.Endpoint{URL: "smtp+plain://" + srv.addr()}).Deliver(t.Context(), msg(`{}`))
		require.Error(t, err)
		assert.Equal(t, plugin.Transient, classOf(err))
		assert.Contains(t, plugin.Tags(err), "smtp:451")
	})
	t.Run("no server", func(t *testing.T) {
		srv := newServer(t)
		addr := srv.addr()
		_ = srv.ln.Close()
		s := sink(t, config.Endpoint{URL: "smtp+plain://" + addr})
		_, err := s.Deliver(t.Context(), msg(`{}`))
		require.Error(t, err)
		assert.Equal(t, plugin.Transient, classOf(err))
		assert.Contains(t, plugin.Tags(err), plugin.TagConnect)
		assert.Error(t, s.Health(t.Context()))
	})
	t.Run("a server that never answers", func(t *testing.T) {
		srv := newServer(t, func(s *server) { s.hang = true })
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := sink(t, config.Endpoint{URL: "smtp+plain://" + srv.addr()}).Deliver(ctx, msg(`{}`))
		require.Error(t, err)
		assert.Equal(t, plugin.Timeout, classOf(err))
		assert.Less(t, time.Since(start), 5*time.Second)
	})
	t.Run("an untrusted certificate", func(t *testing.T) {
		srv := newServer(t, func(s *server) { s.implicit = true })
		_, err := sink(t, config.Endpoint{URL: "smtps://" + srv.addr()}).Deliver(t.Context(), msg(`{}`))
		require.Error(t, err)
		assert.Contains(t, plugin.Tags(err), "smtp:tls")
	})
}

func TestRecipientsFromTheMessage(t *testing.T) {
	srv := newServer(t)
	s := sink(t, config.Endpoint{URL: "smtp+plain://" + srv.addr(), To: []string{"{{ .json.to }}"}})

	_, err := s.Deliver(t.Context(), msg(`{"to":"not an address"}`))
	require.Error(t, err)
	assert.Equal(t, plugin.Permanent, classOf(err))
	assert.Contains(t, plugin.Tags(err), TagAddress)

	_, err = s.Deliver(t.Context(), msg(`{}`))
	assert.ErrorContains(t, err, "no recipient")

	// A message cannot add headers or recipients through a header value.
	s2 := sink(t, config.Endpoint{URL: "smtp+plain://" + srv.addr(), Subject: "{{ .json.s }}", Headers: map[string]string{"X-Note": "{{ .json.s }}"}})
	_, err = s2.Deliver(t.Context(), msg(`{"s":"hi\r\nBcc: evil@example.com\r\n\r\nbody"}`))
	require.NoError(t, err)
	got := srv.got()
	require.Len(t, got, 1)
	assert.Equal(t, []string{"ops@example.com"}, got[0].to)
	h, text := parse(t, got[0].data)
	assert.Empty(t, h.Get("Bcc"))
	assert.Contains(t, h.Get("Subject"), "hi Bcc: evil@example.com")
	assert.JSONEq(t, `{"s":"hi\r\nBcc: evil@example.com\r\n\r\nbody"}`, text)
}

func TestSettings(t *testing.T) {
	ok := config.Endpoint{Type: Kind, URL: "smtp://mail.example.com", From: "relay@example.com", To: []string{"ops@example.com"}}
	for want, change := range map[string]func(*config.Endpoint){
		"url must be smtp://host":                func(e *config.Endpoint) { e.URL = "" },
		"url scheme must be smtp, smtps or":      func(e *config.Endpoint) { e.URL = "https://mail.example.com" },
		"not in the url":                         func(e *config.Endpoint) { e.URL = "smtp://u:p@mail.example.com" },
		"username and password go together":      func(e *config.Endpoint) { e.Username = "relay" },
		"tls does not go with smtp+plain://":     func(e *config.Endpoint) { e.URL, e.TLS = "smtp+plain://mail.example.com", &config.TLS{ServerName: "x"} },
		`from "relay" is not an email address`:   func(e *config.Endpoint) { e.From = "relay" },
		"to needs at least one address":          func(e *config.Endpoint) { e.To = nil },
		`to[0] "nobody" is not an email address`: func(e *config.Endpoint) { e.To = []string{"nobody"} },
		"headers.Subject is set by the":          func(e *config.Endpoint) { e.Headers = map[string]string{"Subject": "x"} },
		"headers.bcc is set by the":              func(e *config.Endpoint) { e.Headers = map[string]string{"bcc": "x@example.com"} },
	} {
		ep := ok
		change(&ep)
		_, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "mail", Kind: Kind, Config: &ep})
		assert.ErrorContains(t, err, want)
	}
	s, err := plugin.NewSink(context.Background(), plugin.Spec{Name: "mail", Kind: Kind, Config: &ok})
	require.NoError(t, err)
	assert.Equal(t, "mail", s.Name())
	assert.Equal(t, "mail.example.com:587", s.(*Sink).addr)
}
