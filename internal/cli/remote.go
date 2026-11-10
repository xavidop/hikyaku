package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/xavidop/hikyaku/engine"
	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/internal/admin"
	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/store"
)

// adminClient talks to a running relay's admin API.
type adminClient struct {
	base  string
	token string
	http  *http.Client
}

// adminFlags are --admin and --token.
type adminFlags struct {
	admin string
	token string
}

func (f *adminFlags) bind(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.admin, "admin", "", "admin API URL (default: from the spec's server.admin.listen, else http://127.0.0.1:9090)")
	cmd.Flags().StringVar(&f.token, "token", "", "admin token (default: HIKYAKU_ADMIN_TOKEN, else the spec's server.admin.token)")
}

// client resolves the admin URL and token: the flags, then the
// environment, then the spec (when one loads), then the defaults.
func (a *app) client(ctx context.Context, f adminFlags) *adminClient {
	c := &adminClient{base: f.admin, token: f.token, http: &http.Client{}}
	if c.token == "" {
		if v, ok := a.deps.LookupEnv("HIKYAKU_ADMIN_TOKEN"); ok {
			c.token = v
		}
	}
	if c.base == "" || c.token == "" {
		if l, err := a.load(ctx, true); err == nil {
			if c.base == "" {
				c.base = adminURL(l.Spec.Server.Admin.Listen)
			}
			if c.token == "" {
				c.token = l.Spec.Server.Admin.Token
			}
		}
	}
	if c.base == "" {
		c.base = "http://127.0.0.1:9090"
	}
	c.base = strings.TrimSuffix(c.base, "/")
	return c
}

func adminURL(listen string) string {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func (c *adminClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("admin API at %s: %w (is hikyaku run up? pass --admin)", c.base, err)
	}
	return resp, nil
}

// json calls the API and decodes a JSON answer; a non-2xx is an error with
// the API's message.
func (c *adminClient) json(ctx context.Context, method, path string, body, out any) error {
	resp, err := c.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(data))
		}
		return fmt.Errorf("admin API: %s: %s", resp.Status, e.Error)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (a *app) newTailCmd() *cobra.Command {
	var f adminFlags
	var source string
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Stream live envelopes from a running relay",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := a.client(cmd.Context(), f)
			q := ""
			if source != "" {
				q = "?source=" + url.QueryEscape(source)
			}
			resp, err := c.do(cmd.Context(), http.MethodGet, "/v1/tail"+q, nil)
			if err != nil {
				return err
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != 200 {
				b, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("admin API: %s: %s", resp.Status, strings.TrimSpace(string(b)))
			}
			w := cmd.OutOrStdout()
			st := a.style(w)
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 64<<10), 16<<20)
			for sc.Scan() {
				line := sc.Text()
				data, ok := strings.CutPrefix(line, "data: ")
				if !ok {
					continue
				}
				if asJSON {
					fprintln(w, data)
					continue
				}
				var ev admin.Event
				if json.Unmarshal([]byte(data), &ev) != nil || ev.Envelope == nil {
					continue
				}
				e := ev.Envelope
				ts := e.ReceivedAt.Local().Format("15:04:05.000")
				if ev.Type == "received" {
					fprintf(w, "%s %s %s %s %s\n", st.Dim(ts), st.Info(e.Source), e.ID, st.Dim(e.Subject), short(e.BodyText(), 120))
					continue
				}
				status := ""
				if ev.Report != nil {
					status = ev.Report.Status
				}
				fprintf(w, "%s %s %s %s\n", st.Dim(ts), st.Info(e.Source), e.ID, statusColor(st, status))
			}
			return sc.Err()
		},
	}
	f.bind(cmd)
	cmd.Flags().StringVar(&source, "source", "", "only this source's messages")
	cmd.Flags().BoolVar(&asJSON, "json", false, "print each event as JSON")
	return cmd
}

func statusColor(st Style, s string) string {
	switch s {
	case "delivered", "filtered":
		return st.OK("→ " + s)
	case "dead_lettered", "unrouted":
		return st.Warn("→ " + s)
	}
	return st.Fail("→ " + s)
}

func short(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func (a *app) newHistoryCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "history", Short: "Routing history of a running relay (via the admin API)"}
	var f adminFlags
	var route, key string
	show := &cobra.Command{
		Use:   "show",
		Short: "Print the past calls of a key, exactly as the model sees them",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if route == "" || key == "" {
				return usageError(fmt.Errorf("--route and --key are required"))
			}
			var v admin.HistoryView
			if err := a.client(cmd.Context(), f).json(cmd.Context(), http.MethodGet, "/v1/history/"+url.PathEscape(route)+"/"+url.PathEscape(key), nil, &v); err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			st := a.style(w)
			if len(v.Records) == 0 {
				fprintf(w, "no history for %s in route %s\n", key, route)
				return nil
			}
			fprintf(w, "%s %s\n", st.Bold("system:"), v.System)
			for _, t := range v.Turns {
				fprintf(w, "%s %s\n", st.Info(t.Role+":"), t.Content)
			}
			return nil
		},
	}
	f.bind(show)
	show.Flags().StringVar(&route, "route", "", "route")
	show.Flags().StringVar(&key, "key", "", "history key (customer, conversation, ...)")

	var cf adminFlags
	var to []string
	var note string
	correct := &cobra.Command{
		Use:   "correct <msg_id>",
		Short: "Record a correction of a past decision (the feedback loop)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(to) == 0 {
				return usageError(fmt.Errorf("--to is required"))
			}
			var rec store.CallRecord
			req := admin.FeedbackRequest{MsgID: args[0], Correct: to, Note: note, Source: "cli"}
			if err := a.client(cmd.Context(), cf).json(cmd.Context(), http.MethodPost, "/v1/feedback", req, &rec); err != nil {
				return err
			}
			fprintf(cmd.OutOrStdout(), "%s %s in route %s (key %s): correct destination %s\n", a.style(cmd.OutOrStdout()).OK("✓"), args[0], rec.Route, rec.Key, strings.Join(to, ", "))
			return nil
		},
	}
	cf.bind(correct)
	correct.Flags().StringArrayVar(&to, "to", nil, "the right destination (repeatable)")
	correct.Flags().StringVar(&note, "note", "", "why")

	var ef adminFlags
	var labeled bool
	var since, eroute string
	export := &cobra.Command{
		Use:   "export",
		Short: "Export history; --labeled writes corrections as ai test cases",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			q := url.Values{}
			if labeled {
				q.Set("labeled", "true")
			}
			if since != "" {
				q.Set("since", since)
			}
			if eroute != "" {
				q.Set("route", eroute)
			}
			resp, err := a.client(cmd.Context(), ef).do(cmd.Context(), http.MethodGet, "/v1/history?"+q.Encode(), nil)
			if err != nil {
				return err
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != 200 {
				b, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("admin API: %s: %s", resp.Status, strings.TrimSpace(string(b)))
			}
			w := cmd.OutOrStdout()
			dec := json.NewDecoder(resp.Body)
			if labeled {
				fprintln(w, "# Labeled cases from recorded corrections: hikyaku ai test --route <r> --cases <this file>")
				fprintln(w, "cases:")
			}
			for {
				var rec store.CallRecord
				if err := dec.Decode(&rec); err == io.EOF {
					return nil
				} else if err != nil {
					return err
				}
				if !labeled {
					b, _ := json.Marshal(rec)
					fprintln(w, string(b))
					continue
				}
				writeCase(w, rec)
			}
		},
	}
	ef.bind(export)
	export.Flags().BoolVar(&labeled, "labeled", false, "only corrected records, as ai test cases (YAML)")
	export.Flags().StringVar(&since, "since", "", "only records this recent, such as 30d")
	export.Flags().StringVar(&eroute, "route", "", "only this route")

	var ff adminFlags
	var froute, fkey string
	forget := &cobra.Command{
		Use:   "forget",
		Short: "Delete the history of a key (right to be forgotten)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if froute == "" || fkey == "" {
				return usageError(fmt.Errorf("--route and --key are required"))
			}
			if err := a.client(cmd.Context(), ff).json(cmd.Context(), http.MethodDelete, "/v1/history/"+url.PathEscape(froute)+"/"+url.PathEscape(fkey), nil, nil); err != nil {
				return err
			}
			fprintf(cmd.OutOrStdout(), "%s forgot %s in route %s\n", a.style(cmd.OutOrStdout()).OK("✓"), fkey, froute)
			return nil
		},
	}
	ff.bind(forget)
	forget.Flags().StringVar(&froute, "route", "", "route")
	forget.Flags().StringVar(&fkey, "key", "", "history key")
	cmd.AddCommand(show, correct, export, forget)
	return cmd
}

// writeCase writes one corrected record as an ai test case.
func writeCase(w io.Writer, rec store.CallRecord) {
	body := rec.State
	fprintf(w, "  - name: %s\n", jsonString(rec.Route+" "+rec.MsgID))
	var v any
	if json.Unmarshal([]byte(body), &v) == nil {
		b, _ := json.Marshal(v)
		fprintf(w, "    data: %s\n", b)
	} else {
		fprintf(w, "    body: %s\n", jsonString(body))
	}
	b, _ := json.Marshal(rec.Feedback.Correct)
	fprintf(w, "    expect: %s\n", b)
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func (a *app) newDLQCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "dlq", Short: "List, inspect and replay dead letters (file DLQ)"}
	var path, since string
	var asJSON bool
	read := func(ctx context.Context) ([]*envelope.Envelope, error) {
		p := path
		if p == "" {
			l, err := a.load(ctx, false)
			if err != nil {
				return nil, err
			}
			dl := l.Spec.DeadLetter
			if dl == nil || dl.Type != "file" {
				return nil, usageError(fmt.Errorf("dead_letter is not a file in the spec: pass --path"))
			}
			p = dl.Path
		}
		f, err := os.Open(p)
		if os.IsNotExist(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		defer func() { _ = f.Close() }()
		envs, err := engine.ReadDLQ(f)
		if err != nil {
			return nil, err
		}
		if since == "" {
			return envs, nil
		}
		d, err := config.ParseDuration(since)
		if err != nil {
			return nil, usageError(fmt.Errorf("--since: %w", err))
		}
		cut := a.deps.Now().Add(-d)
		var out []*envelope.Envelope
		for _, e := range envs {
			if info, ok := engine.InfoOf(e); ok && !info.At.Before(cut) {
				out = append(out, e)
			}
		}
		return out, nil
	}
	list := &cobra.Command{
		Use:   "list",
		Short: "List dead letters",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			envs, err := read(cmd.Context())
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			if asJSON {
				for _, e := range envs {
					b, _ := json.Marshal(e)
					fprintln(w, string(b))
				}
				return nil
			}
			if len(envs) == 0 {
				fprintln(w, "no dead letters")
				return nil
			}
			st := a.style(w)
			for _, e := range envs {
				info, _ := engine.InfoOf(e)
				target := info.Destination
				if target == "" {
					target = "route " + info.Route
				}
				fprintf(w, "%s  %s  %-16s %-10s ×%d  %s\n", e.ID, st.Dim(info.At.Local().Format(time.DateTime)), target, info.Reason, info.Attempts, st.Dim(short(info.Error, 80)))
			}
			fprintf(w, "%d dead letter(s)\n", len(envs))
			return nil
		},
	}
	inspect := &cobra.Command{
		Use:   "inspect <id>",
		Short: "Print one dead letter in full",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			envs, err := read(cmd.Context())
			if err != nil {
				return err
			}
			for _, e := range envs {
				if e.ID == args[0] {
					return printJSON(cmd.OutOrStdout(), e)
				}
			}
			return usageError(fmt.Errorf("no dead letter %s", args[0]))
		},
	}
	var to string
	var ids []string
	var dryRun bool
	replay := &cobra.Command{
		Use:   "replay",
		Short: "Re-inject dead letters, keeping their idempotency keys",
		Long: "Re-inject dead letters: each goes back to the destination that failed (or --to),\n" +
			"or through its route when routing itself failed, with its original ID as the\n" +
			"idempotency key. The DLQ file is append-only: a message that fails again is\n" +
			"dead-lettered again.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			envs, err := read(cmd.Context())
			if err != nil {
				return err
			}
			var pick []*envelope.Envelope
			for _, e := range envs {
				if len(ids) == 0 || contains(ids, e.ID) {
					pick = append(pick, e)
				}
			}
			w := cmd.OutOrStdout()
			st := a.style(w)
			if len(pick) == 0 {
				fprintln(w, "nothing to replay")
				return nil
			}
			var eng *engine.Engine
			if !dryRun {
				_, e, err := a.oneShot(cmd.Context(), nil)
				if err != nil {
					return err
				}
				defer e.Close(context.Background())
				eng = e
			}
			failed := 0
			for _, env := range pick {
				info, _ := engine.InfoOf(env)
				env.Attributes = withoutDLQ(env.Attributes)
				target := to
				if target == "" {
					target = info.Destination
				}
				if dryRun {
					if target != "" {
						fprintf(w, "would deliver %s to %s\n", env.ID, target)
					} else {
						fprintf(w, "would route %s through %s\n", env.ID, info.Route)
					}
					continue
				}
				if target != "" {
					rep, err := eng.DeliverTo(cmd.Context(), target, env)
					if err != nil && rep.Destination == "" {
						fprintf(w, "%s %s: %v\n", st.Fail("✗"), env.ID, err)
						failed++
						continue
					}
					a.printDelivery(w, env.ID, rep)
					if !rep.OK() {
						failed++
					}
					continue
				}
				rep, err := eng.InjectRoute(cmd.Context(), info.Route, env)
				if err != nil {
					fprintf(w, "%s %s: %v\n", st.Fail("✗"), env.ID, err)
					failed++
					continue
				}
				fprintf(w, "%s %s through %s: %s\n", st.OK("✓"), env.ID, info.Route, rep.Status)
				if rep.Status != envelope.Delivered.String() {
					failed++
				}
			}
			if failed > 0 {
				fprintf(w, "%d of %d did not go through\n", failed, len(pick))
				return silentExit(ExitFailed)
			}
			return nil
		},
	}
	for _, c := range []*cobra.Command{list, inspect, replay} {
		c.Flags().StringVar(&path, "path", "", "DLQ file (default: the spec's dead_letter.path)")
	}
	for _, c := range []*cobra.Command{list, replay} {
		c.Flags().StringVar(&since, "since", "", "only dead letters this recent, such as 1h")
	}
	list.Flags().BoolVar(&asJSON, "json", false, "print the dead letters as NDJSON")
	replay.Flags().StringVar(&to, "to", "", "deliver to this destination instead of the one that failed")
	replay.Flags().StringArrayVar(&ids, "id", nil, "only this dead letter (repeatable)")
	replay.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be replayed")
	cmd.AddCommand(list, inspect, replay)
	return cmd
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func withoutDLQ(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if k != "dlq" {
			out[k] = v
		}
	}
	return out
}
