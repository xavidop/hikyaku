package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/protocols/grpc/internal/echopb"
)

// echoSet is the descriptor set of testdata/echo.proto.
const echoSet = "testdata/echo.pb"

// Full method names of the test services.
const (
	sayMethod     = "/hikyaku.test.echo.v1.Echo/Say"
	collectMethod = "/hikyaku.test.echo.v1.Echo/Collect"
	watchMethod   = "/hikyaku.test.echo.v1.Feed/Watch"
	peekMethod    = "/hikyaku.test.echo.v1.Feed/Peek"
	talkMethod    = "/hikyaku.test.echo.v1.Chat/Talk"
	echoService   = "hikyaku.test.echo.v1.Echo"
	feedService   = "hikyaku.test.echo.v1.Feed"
	chatService   = "hikyaku.test.echo.v1.Chat"
)

// echoServer is the third-party server of the tests. Say answers with the
// text upper-cased and count+1 (or fails with fail); Collect reads its
// stream, then answers how many requests it got and the last text (or
// fails with fail); Talk answers each request as Say would, and fails with
// fail after its first answer; Watch streams limit events, then holds the
// stream open while hold is set. It records the metadata and requests of
// every call.
type echoServer struct {
	echopb.UnimplementedEchoServer
	echopb.UnimplementedFeedServer
	echopb.UnimplementedChatServer

	mu       sync.Mutex
	md       []metadata.MD
	says     []*echopb.SayRequest
	collects [][]*echopb.SayRequest // the requests of each Collect call
	talks    [][]*echopb.SayRequest // the requests of each Talk call
	watches  []*echopb.WatchRequest
	fail     error
	header   metadata.MD
	trailer  metadata.MD
	delay    time.Duration
	hold     bool
	data     string // the data of every event
}

func (s *echoServer) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.md = append(s.md, md)
}

func (s *echoServer) Say(ctx context.Context, r *echopb.SayRequest) (*echopb.SayReply, error) {
	s.record(ctx)
	s.mu.Lock()
	s.says = append(s.says, r)
	fail, header, trailer, delay := s.fail, s.header, s.trailer, s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if header != nil {
		_ = grpc.SetHeader(ctx, header)
	}
	if trailer != nil {
		_ = grpc.SetTrailer(ctx, trailer)
	}
	if fail != nil {
		return nil, fail
	}
	return &echopb.SayReply{Text: strings.ToUpper(r.GetText()), Count: r.GetCount() + 1}, nil
}

func (s *echoServer) Collect(stream echopb.Echo_CollectServer) error {
	s.record(stream.Context())
	var reqs []*echopb.SayRequest
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		reqs = append(reqs, r)
	}
	s.mu.Lock()
	s.collects = append(s.collects, reqs)
	fail, header, trailer, delay := s.fail, s.header, s.trailer, s.delay
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	if header != nil {
		_ = stream.SetHeader(header)
	}
	if trailer != nil {
		stream.SetTrailer(trailer)
	}
	if fail != nil {
		return fail
	}
	reply := &echopb.CollectReply{Received: int32(len(reqs))}
	if len(reqs) > 0 {
		reply.Last = reqs[len(reqs)-1].GetText()
	}
	return stream.SendAndClose(reply)
}

func (s *echoServer) Talk(stream echopb.Chat_TalkServer) error {
	s.record(stream.Context())
	s.mu.Lock()
	call := len(s.talks)
	s.talks = append(s.talks, nil)
	fail, header := s.fail, s.header
	s.mu.Unlock()
	if header != nil {
		_ = stream.SendHeader(header)
	}
	for {
		r, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.talks[call] = append(s.talks[call], r)
		s.mu.Unlock()
		if err := stream.Send(&echopb.SayReply{Text: strings.ToUpper(r.GetText()), Count: r.GetCount() + 1}); err != nil {
			return err
		}
		if fail != nil {
			return fail
		}
	}
}

// collected is the requests of every Collect call.
func (s *echoServer) collected() [][]*echopb.SayRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]*echopb.SayRequest(nil), s.collects...)
}

// talked is the requests of every Talk call.
func (s *echoServer) talked() [][]*echopb.SayRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]*echopb.SayRequest, len(s.talks))
	for i, t := range s.talks {
		out[i] = append([]*echopb.SayRequest(nil), t...)
	}
	return out
}

func (s *echoServer) Peek(ctx context.Context, r *echopb.WatchRequest) (*echopb.Event, error) {
	s.record(ctx)
	return &echopb.Event{Topic: r.GetTopic(), Seq: 1}, nil
}

func (s *echoServer) Watch(r *echopb.WatchRequest, stream echopb.Feed_WatchServer) error {
	s.record(stream.Context())
	s.mu.Lock()
	s.watches = append(s.watches, r)
	hold, data := s.hold, s.data
	s.mu.Unlock()
	_ = stream.SendHeader(metadata.Pairs("x-feed", "test"))
	for i := int64(1); i <= int64(r.GetLimit()); i++ {
		if err := stream.Send(&echopb.Event{Topic: r.GetTopic(), Seq: i, Data: data}); err != nil {
			return err
		}
	}
	if hold {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return nil
}

func (s *echoServer) calls() []metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]metadata.MD(nil), s.md...)
}

func (s *echoServer) lastSay() *echopb.SayRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.says) == 0 {
		return nil
	}
	return s.says[len(s.says)-1]
}

func (s *echoServer) set(f func(*echoServer)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

// serverOpts shape a test server.
type serverOpts struct {
	addr         string
	noHealth     bool
	noReflection bool
	creds        credentials.TransportCredentials
	// token, when set, is required (authorization: Bearer <token>) on every
	// call, reflection and health included.
	token string
}

// requireToken refuses calls without the bearer token.
func requireToken(token string) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	check := func(ctx context.Context) error {
		md, _ := metadata.FromIncomingContext(ctx)
		if first(md, "authorization") != "Bearer "+token {
			return status.Error(codes.Unauthenticated, "no token")
		}
		return nil
	}
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if err := check(ctx); err != nil {
			return nil, err
		}
		return h(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		if err := check(ss.Context()); err != nil {
			return err
		}
		return h(srv, ss)
	}
	return unary, stream
}

// testServer is a running echo server.
type testServer struct {
	addr string
	srv  *grpc.Server
	hs   *health.Server
}

// startEcho serves es on a loopback port (or o.addr) with health and
// reflection; it stops at cleanup.
func startEcho(t *testing.T, es *echoServer, o serverOpts) *testServer {
	t.Helper()
	addr := o.addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	var ln net.Listener
	var err error
	require.Eventually(t, func() bool { ln, err = net.Listen("tcp", addr); return err == nil }, 3*time.Second, 10*time.Millisecond, "listen %s", addr)
	var opts []grpc.ServerOption
	if o.creds != nil {
		opts = append(opts, grpc.Creds(o.creds))
	}
	if o.token != "" {
		unary, stream := requireToken(o.token)
		opts = append(opts, grpc.UnaryInterceptor(unary), grpc.StreamInterceptor(stream))
	}
	srv := grpc.NewServer(opts...)
	echopb.RegisterEchoServer(srv, es)
	echopb.RegisterFeedServer(srv, es)
	echopb.RegisterChatServer(srv, es)
	ts := &testServer{addr: ln.Addr().String(), srv: srv}
	if !o.noHealth {
		ts.hs = health.NewServer()
		healthgrpc.RegisterHealthServer(srv, ts.hs)
	}
	if !o.noReflection {
		reflection.Register(srv)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ts
}

// dial is a plaintext client of addr, closed at cleanup.
func dial(t *testing.T, addr string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	cc, err := grpc.NewClient("passthrough:///"+addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

// closedAddr is an address nothing listens on.
func closedAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// msg is an envelope with a JSON body.
func msg(body string) *envelope.Envelope {
	env := envelope.New("test", "test", []byte(body))
	env.ContentType = contentJSON
	return env
}

// recv waits for the next envelope.
func recv(t *testing.T, out <-chan *envelope.Envelope) *envelope.Envelope {
	t.Helper()
	select {
	case env := <-out:
		return env
	case <-time.After(5 * time.Second):
		t.Fatal("no envelope arrived")
		return nil
	}
}

// start runs a source until cleanup (or the returned stop), checking that
// Start then returns nil.
func start(t *testing.T, s plugin.Source, out chan *envelope.Envelope) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Start(ctx, out) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-errc:
				assert.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("Start did not return after its context ended")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// fakeEngine stands in for the relay: it takes every message a source hands
// over, marks it queued, and acks it with decide's outcome (nil decide
// delivers). Taken messages are sent on the returned channel.
func fakeEngine(t *testing.T, out <-chan *envelope.Envelope, decide func(*envelope.Envelope) envelope.Outcome) <-chan *envelope.Envelope {
	t.Helper()
	seen := make(chan *envelope.Envelope, 64)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case env := <-out:
				env.Queued()
				o := envelope.Outcome{Status: envelope.Delivered}
				if decide != nil {
					o = decide(env)
				}
				seen <- env
				go env.Ack(context.Background(), o)
			case <-stop:
				return
			}
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
	return seen
}

// selfSigned writes a certificate for 127.0.0.1 and its key; client marks
// it for client authentication too.
func selfSigned(t *testing.T, name string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	kb, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	dir := t.TempDir()
	certFile, keyFile = filepath.Join(dir, name+".pem"), filepath.Join(dir, name+"-key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600))
	return certFile, keyFile
}

// clientTLS is a client TLS config that trusts caFile and presents
// cert/key when given.
func clientTLS(t *testing.T, caFile, certFile, keyFile string) *tls.Config {
	t.Helper()
	pemBytes, err := os.ReadFile(caFile)
	require.NoError(t, err)
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pemBytes))
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		require.NoError(t, err)
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg
}

// records keeps what a source logs.
type records struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (r *records) Enabled(context.Context, slog.Level) bool { return true }
func (r *records) WithAttrs([]slog.Attr) slog.Handler       { return r }
func (r *records) WithGroup(string) slog.Handler            { return r }
func (r *records) Handle(_ context.Context, rec slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recs = append(r.recs, rec.Clone())
	return nil
}

func (r *records) count(level slog.Level, msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, rec := range r.recs {
		if rec.Level == level && rec.Message == msg {
			n++
		}
	}
	return n
}

// text is everything logged, one record per line.
func (r *records) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var b strings.Builder
	for _, rec := range r.recs {
		fmt.Fprintf(&b, "%s %s", rec.Level, rec.Message)
		rec.Attrs(func(a slog.Attr) bool { fmt.Fprintf(&b, " %s=%v", a.Key, a.Value); return true })
		b.WriteByte('\n')
	}
	return b.String()
}

// sourceConfig is a grpc source's config with a name and type.
func sourceConfig(c config.Source) *config.Source {
	c.Name, c.Type = "in", kind
	return &c
}
