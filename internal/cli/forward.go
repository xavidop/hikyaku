package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/admin"
)

// Headers hikyaku forward adds to every request.
const (
	ForwardIDHeader     = "X-Hikyaku-Id"
	ForwardSourceHeader = "X-Hikyaku-Source"
)

// forwardWorkers is how many requests hikyaku forward has open at once.
const forwardWorkers = 8

// notForwarded are the headers of the first request that describe its
// connection, not the message.
var notForwarded = map[string]bool{
	"Host": true, "Content-Length": true, "Connection": true, "Keep-Alive": true, "Transfer-Encoding": true,
	"Te": true, "Trailer": true, "Upgrade": true, "Proxy-Authorization": true, "Proxy-Connection": true,
	"Accept-Encoding": true,
}

// forwarder sends the messages a running relay receives to one local URL.
type forwarder struct {
	to       *url.URL
	keepPath bool
	headers  http.Header
	client   *http.Client
	out      io.Writer
	style    Style
	now      func() time.Time

	mu sync.Mutex // one line at a time
}

// request builds the local request of a message: the method, headers and
// body it came with.
func (f *forwarder) request(ctx context.Context, env *envelope.Envelope) (*http.Request, error) {
	method := http.MethodPost
	if m, ok := env.Attributes["http.method"].(string); ok && m != "" {
		method = m
	}
	u := *f.to
	if f.keepPath {
		if p, ok := env.Attributes["http.path"].(string); ok && p != "" {
			u.Path = strings.TrimSuffix(u.Path, "/") + "/" + strings.TrimPrefix(p, "/")
		}
		if q, ok := env.Attributes["http.query"].(string); ok && q != "" {
			u.RawQuery = q
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(env.Body))
	if err != nil {
		return nil, err
	}
	for k, vs := range env.Headers {
		if notForwarded[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	if req.Header.Get("Content-Type") == "" && env.ContentType != "" {
		req.Header.Set("Content-Type", env.ContentType)
	}
	req.Header.Set(ForwardIDHeader, env.ID)
	req.Header.Set(ForwardSourceHeader, env.Source)
	for k, vs := range f.headers {
		req.Header[k] = vs
	}
	return req, nil
}

// send forwards one message and prints what the local server answered.
func (f *forwarder) send(ctx context.Context, env *envelope.Envelope) {
	start := f.now()
	line := func(result string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		fprintf(f.out, "%s %s %s %s %s\n", f.style.Dim(start.Local().Format("15:04:05.000")), f.style.Info(env.Source), env.ID,
			result, f.style.Dim("("+f.now().Sub(start).Round(time.Millisecond).String()+")"))
	}
	req, err := f.request(ctx, env)
	if err != nil {
		line(f.style.Fail("✗ " + err.Error()))
		return
	}
	resp, err := f.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		line(f.style.Fail("✗ " + err.Error()))
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	status := fmt.Sprintf("→ %d", resp.StatusCode)
	if resp.StatusCode/100 == 2 {
		line(f.style.OK(status))
		return
	}
	line(f.style.Fail(status))
}

// stream reads one tail connection to its end and forwards every message
// received. connected reports a stream that opened: an error after that is
// a relay that went away.
func (f *forwarder) stream(ctx context.Context, c *adminClient, path string, sem chan struct{}, wg *sync.WaitGroup, opened func()) (connected bool, err error) {
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return false, fmt.Errorf("admin API: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	opened()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		data, ok := strings.CutPrefix(sc.Text(), "data: ")
		if !ok {
			continue
		}
		var ev admin.Event
		if json.Unmarshal([]byte(data), &ev) != nil || ev.Envelope == nil || ev.Type != "received" {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return true, nil
		}
		wg.Add(1)
		go func(env *envelope.Envelope) {
			defer func() { <-sem; wg.Done() }()
			f.send(ctx, env)
		}(ev.Envelope)
	}
	return true, sc.Err()
}

func (a *app) newForwardCmd() *cobra.Command {
	var af adminFlags
	var to, source string
	var headers []string
	var keepPath bool
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "forward",
		Short: "Send the messages a running relay receives to a local URL too",
		Long: "Forward a copy of every message a running relay receives to a URL on this\n" +
			"machine, to work on a webhook handler with real traffic. It reads the relay's\n" +
			"live stream over the admin API, like hikyaku tail, and makes one request per\n" +
			"message with the method, headers and body the message came with. The relay\n" +
			"delivers the message as usual: what the local server answers changes nothing.\n\n" +
			"It is a tool for development. A message that arrives while the connection is\n" +
			"down is not forwarded later, and a secret value of the spec inside a message\n" +
			"is hidden, as it is in hikyaku tail.",
		Example: "  hikyaku forward --to http://localhost:3000/webhooks/stripe --source stripe\n" +
			"  hikyaku forward --to http://localhost:3000 --keep-path \\\n" +
			"    --admin https://relay.example.com:9090 --token \"$HIKYAKU_ADMIN_TOKEN\"",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if to == "" {
				return usageError(fmt.Errorf("--to is required (the local URL, like http://localhost:3000/webhooks)"))
			}
			u, err := url.Parse(to)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
				return usageError(fmt.Errorf("--to %q: want an http or https URL", to))
			}
			extra := http.Header{}
			for _, h := range headers {
				k, v, ok := strings.Cut(h, ":")
				if !ok || strings.TrimSpace(k) == "" {
					return usageError(fmt.Errorf("--header %q: want Name: value", h))
				}
				extra.Set(strings.TrimSpace(k), strings.TrimSpace(v))
			}
			ctx := cmd.Context()
			c := a.client(ctx, af)
			w := cmd.OutOrStdout()
			f := &forwarder{
				to: u, keepPath: keepPath, headers: extra, out: w, style: a.style(w), now: a.deps.Now,
				client: &http.Client{Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
			}
			path := "/v1/tail"
			if source != "" {
				path += "?source=" + url.QueryEscape(source)
			}
			sem := make(chan struct{}, forwardWorkers)
			var wg sync.WaitGroup
			defer wg.Wait()

			var once sync.Once
			opened := func() {
				once.Do(func() { fprintf(w, "%s forwarding %s to %s\n", f.style.OK("✓"), c.base, u.Redacted()) })
			}
			wait := time.Second
			for first := true; ; first = false {
				connected, err := f.stream(ctx, c, path, sem, &wg, opened)
				if ctx.Err() != nil {
					return nil
				}
				if first && !connected {
					// A wrong URL or token: say so, do not try again.
					if err == nil {
						err = fmt.Errorf("admin API at %s: the stream did not open", c.base)
					}
					return err
				}
				if connected {
					wait = time.Second
				}
				fprintf(cmd.ErrOrStderr(), "%s the relay's stream ended, connecting again in %s\n", f.style.Warn("!"), wait)
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(wait):
				}
				wait = min(wait*2, 15*time.Second)
			}
		},
	}
	af.bind(cmd)
	fl := cmd.Flags()
	fl.StringVar(&to, "to", "", "local URL every message is sent to")
	fl.StringVar(&source, "source", "", "only this source's messages")
	fl.StringArrayVar(&headers, "header", nil, "header to add or replace, as Name: value (repeatable)")
	fl.BoolVar(&keepPath, "keep-path", false, "add the path and query the message came on to --to")
	fl.DurationVar(&timeout, "timeout", 30*time.Second, "how long the local server may take to answer")
	return cmd
}
