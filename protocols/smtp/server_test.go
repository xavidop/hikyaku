package smtp

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// received is one email a test server took.
type received struct {
	from string
	to   []string
	data string // headers and body, as sent
	tls  bool
	user string
	pass string
}

// server is a small SMTP server for tests.
type server struct {
	t        *testing.T
	ln       net.Listener
	cert     tls.Certificate
	caFile   string
	implicit bool   // TLS from the first byte
	starttls bool   // offer STARTTLS
	mechs    string // AUTH mechanisms offered ("" for none)
	user     string // the account that may sign in
	pass     string
	reply    map[string]string // command prefix → reply instead of the usual one
	hang     bool              // never greet

	mu   sync.Mutex
	mail []received
}

func newServer(t *testing.T, opts ...func(*server)) *server {
	t.Helper()
	s := &server{t: t, reply: map[string]string{}}
	s.cert, s.caFile = selfSigned(t)
	for _, o := range opts {
		o(s)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	if s.implicit {
		ln = tls.NewListener(ln, &tls.Config{Certificates: []tls.Certificate{s.cert}, MinVersion: tls.VersionTLS12})
	}
	s.ln = ln
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	return s
}

func (s *server) addr() string { return s.ln.Addr().String() }

func (s *server) got() []received {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]received(nil), s.mail...)
}

func (s *server) serve(c net.Conn) {
	defer func() { _ = c.Close() }()
	if s.hang {
		time.Sleep(time.Minute)
		return
	}
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	r := bufio.NewReader(c)
	say := func(format string, args ...any) { _, _ = fmt.Fprintf(c, format+"\r\n", args...) }
	line := func() (string, bool) {
		l, err := r.ReadString('\n')
		return strings.TrimRight(l, "\r\n"), err == nil
	}
	cur := received{tls: s.implicit}
	authed := s.mechs == ""
	say("220 test ESMTP")
	for {
		l, ok := line()
		if !ok {
			return
		}
		cmd := strings.ToUpper(l)
		if rep := s.custom(cmd); rep != "" {
			say("%s", rep)
			continue
		}
		switch {
		case strings.HasPrefix(cmd, "EHLO"):
			say("250-test")
			if s.starttls && !cur.tls {
				say("250-STARTTLS")
			}
			if s.mechs != "" {
				say("250-AUTH %s", s.mechs)
			}
			say("250 8BITMIME")
		case cmd == "STARTTLS":
			say("220 go ahead")
			tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{s.cert}, MinVersion: tls.VersionTLS12})
			if err := tc.Handshake(); err != nil {
				return
			}
			c, r, cur.tls = tc, bufio.NewReader(tc), true
		case strings.HasPrefix(cmd, "AUTH PLAIN"):
			raw, _ := base64.StdEncoding.DecodeString(strings.TrimSpace(l[len("AUTH PLAIN"):]))
			parts := strings.Split(string(raw), "\x00")
			if len(parts) == 3 {
				cur.user, cur.pass = parts[1], parts[2]
			}
			authed = s.signIn(say, cur)
		case cmd == "AUTH LOGIN":
			say("334 %s", base64.StdEncoding.EncodeToString([]byte("Username:")))
			u, _ := line()
			say("334 %s", base64.StdEncoding.EncodeToString([]byte("Password:")))
			p, _ := line()
			ub, _ := base64.StdEncoding.DecodeString(u)
			pb, _ := base64.StdEncoding.DecodeString(p)
			cur.user, cur.pass = string(ub), string(pb)
			authed = s.signIn(say, cur)
		case strings.HasPrefix(cmd, "MAIL FROM:"):
			if !authed {
				say("530 5.7.0 Authentication required")
				continue
			}
			cur.from = between(l)
			say("250 ok")
		case strings.HasPrefix(cmd, "RCPT TO:"):
			cur.to = append(cur.to, between(l))
			say("250 ok")
		case cmd == "DATA":
			say("354 go ahead")
			var b strings.Builder
			for {
				dl, ok := line()
				if !ok {
					return
				}
				if dl == "." {
					break
				}
				b.WriteString(strings.TrimPrefix(dl, "."))
				b.WriteString("\r\n")
			}
			cur.data = b.String()
			s.mu.Lock()
			s.mail = append(s.mail, cur)
			s.mu.Unlock()
			say("250 queued")
		case cmd == "QUIT":
			say("221 bye")
			return
		default:
			say("502 not implemented")
		}
	}
}

func (s *server) custom(cmd string) string {
	for prefix, rep := range s.reply {
		if strings.HasPrefix(cmd, prefix) {
			return rep
		}
	}
	return ""
}

func (s *server) signIn(say func(string, ...any), cur received) bool {
	if cur.user == s.user && cur.pass == s.pass {
		say("235 2.7.0 Authentication successful")
		return true
	}
	say("535 5.7.8 Authentication credentials invalid")
	return false
}

func between(l string) string {
	i, j := strings.Index(l, "<"), strings.Index(l, ">")
	if i < 0 || j < i {
		return ""
	}
	return l[i+1 : j]
}

// selfSigned makes a certificate for 127.0.0.1 and writes it as a CA file.
func selfSigned(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, caFile
}
