package inbound

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // Twilio signs with HMAC-SHA1
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/hikyaku/internal/config"
)

// DefaultWebhookTolerance is how old a signed timestamp may be (stripe,
// slack) when auth.webhook.tolerance is not set. Both senders suggest it.
const DefaultWebhookTolerance = 5 * time.Minute

// webhookCheck verifies a request the way one well-known sender signs it.
type webhookCheck struct {
	provider  string
	secret    []byte
	tolerance time.Duration
	url       string // twilio: the public URL, when set
	now       func() time.Time
}

func newWebhookCheck(w *config.WebhookAuth) (*webhookCheck, error) {
	if !slices.Contains(config.WebhookProviders, w.Provider) {
		return nil, fmt.Errorf("auth.webhook.provider %q: use one of %s", w.Provider, strings.Join(config.WebhookProviders, ", "))
	}
	if w.Secret == "" {
		return nil, fmt.Errorf("auth.webhook.secret is required")
	}
	c := &webhookCheck{provider: w.Provider, secret: []byte(w.Secret), tolerance: w.Tolerance.Std(), url: w.URL, now: time.Now}
	if c.tolerance == 0 {
		c.tolerance = DefaultWebhookTolerance
	}
	return c, nil
}

func (c *webhookCheck) check(r *http.Request, body []byte) error {
	if body == nil {
		return fmt.Errorf("%w: the %s signature needs a request body", ErrUnauthorized, c.provider)
	}
	switch c.provider {
	case "github":
		return c.github(r, body)
	case "stripe":
		return c.stripe(r, body)
	case "slack":
		return c.slack(r, body)
	case "shopify":
		return c.shopify(r, body)
	default:
		return c.twilio(r, body)
	}
}

func (c *webhookCheck) mac(h func() hash.Hash, parts ...[]byte) []byte {
	m := hmac.New(h, c.secret)
	for _, p := range parts {
		m.Write(p)
	}
	return m.Sum(nil)
}

func (c *webhookCheck) bad(header string) error {
	return fmt.Errorf("%w: bad %s signature", ErrUnauthorized, header)
}

func missing(header string) error {
	return fmt.Errorf("%w: missing %s", ErrUnauthorized, header)
}

// fresh checks a signed Unix timestamp against the tolerance, both ways:
// a sender's clock may run a little ahead.
func (c *webhookCheck) fresh(header, ts string) error {
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: %s has no valid timestamp", ErrUnauthorized, header)
	}
	age := c.now().Sub(time.Unix(sec, 0))
	if age > c.tolerance || age < -c.tolerance {
		return fmt.Errorf("%w: the %s timestamp is outside the %s tolerance", ErrUnauthorized, header, c.tolerance)
	}
	return nil
}

// github: X-Hub-Signature-256 is "sha256=" and the hex HMAC-SHA256 of the
// body.
func (c *webhookCheck) github(r *http.Request, body []byte) error {
	const header = "X-Hub-Signature-256"
	got := strings.TrimSpace(r.Header.Get(header))
	if got == "" {
		return missing(header)
	}
	sig, err := hex.DecodeString(strings.TrimPrefix(got, "sha256="))
	if err != nil || !hmac.Equal(sig, c.mac(sha256.New, body)) {
		return c.bad(header)
	}
	return nil
}

// stripe: Stripe-Signature is "t=<unix>,v1=<hex>[,v1=<hex>...]". Each v1
// is the HMAC-SHA256 of "<t>.<body>"; there are two while a secret is
// rolled, and one match is enough.
func (c *webhookCheck) stripe(r *http.Request, body []byte) error {
	const header = "Stripe-Signature"
	raw := r.Header.Get(header)
	if raw == "" {
		return missing(header)
	}
	var ts string
	var sigs [][]byte
	for _, part := range strings.Split(raw, ",") {
		k, v, _ := strings.Cut(strings.TrimSpace(part), "=")
		switch k {
		case "t":
			ts = v
		case "v1":
			if b, err := hex.DecodeString(v); err == nil {
				sigs = append(sigs, b)
			}
		}
	}
	if err := c.fresh(header, ts); err != nil {
		return err
	}
	want := c.mac(sha256.New, []byte(ts), []byte("."), body)
	for _, s := range sigs {
		if hmac.Equal(s, want) {
			return nil
		}
	}
	return c.bad(header)
}

// slack: X-Slack-Signature is "v0=" and the hex HMAC-SHA256 of
// "v0:<X-Slack-Request-Timestamp>:<body>".
func (c *webhookCheck) slack(r *http.Request, body []byte) error {
	const header, tsHeader = "X-Slack-Signature", "X-Slack-Request-Timestamp"
	got := strings.TrimSpace(r.Header.Get(header))
	if got == "" {
		return missing(header)
	}
	ts := strings.TrimSpace(r.Header.Get(tsHeader))
	if ts == "" {
		return missing(tsHeader)
	}
	if err := c.fresh(tsHeader, ts); err != nil {
		return err
	}
	sig, err := hex.DecodeString(strings.TrimPrefix(got, "v0="))
	if err != nil || !hmac.Equal(sig, c.mac(sha256.New, []byte("v0:"), []byte(ts), []byte(":"), body)) {
		return c.bad(header)
	}
	return nil
}

// shopify: X-Shopify-Hmac-Sha256 is the base64 HMAC-SHA256 of the body.
func (c *webhookCheck) shopify(r *http.Request, body []byte) error {
	const header = "X-Shopify-Hmac-Sha256"
	got := strings.TrimSpace(r.Header.Get(header))
	if got == "" {
		return missing(header)
	}
	sig, err := base64.StdEncoding.DecodeString(got)
	if err != nil || !hmac.Equal(sig, c.mac(sha256.New, body)) {
		return c.bad(header)
	}
	return nil
}

// twilio: X-Twilio-Signature is the base64 HMAC-SHA1, keyed with the auth
// token, of the URL Twilio called. For a form post, every parameter's name
// and value follow the URL, sorted by name. For a JSON post nothing
// follows, and the URL's bodySHA256 parameter is the hex SHA-256 of the
// body.
func (c *webhookCheck) twilio(r *http.Request, body []byte) error {
	const header = "X-Twilio-Signature"
	got := strings.TrimSpace(r.Header.Get(header))
	if got == "" {
		return missing(header)
	}
	sig, err := base64.StdEncoding.DecodeString(got)
	if err != nil {
		return c.bad(header)
	}
	var params string
	if sum := r.URL.Query().Get("bodySHA256"); sum != "" {
		have := sha256.Sum256(body)
		want, err := hex.DecodeString(sum)
		if err != nil || !hmac.Equal(want, have[:]) {
			return fmt.Errorf("%w: the body does not match bodySHA256", ErrUnauthorized)
		}
	} else if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt == "application/x-www-form-urlencoded" {
		form, err := url.ParseQuery(string(body))
		if err != nil {
			return fmt.Errorf("%w: the form body cannot be parsed", ErrUnauthorized)
		}
		params = twilioParams(form)
	}
	for _, u := range c.twilioURLs(r) {
		if hmac.Equal(sig, c.mac(sha1.New, []byte(u), []byte(params))) {
			return nil
		}
	}
	return c.bad(header)
}

// twilioParams joins a form's names and values the way Twilio signs them:
// sorted by name, and a name's distinct values sorted too.
func twilioParams(form url.Values) string {
	names := make([]string, 0, len(form))
	for k := range form {
		names = append(names, k)
	}
	slices.Sort(names)
	var b strings.Builder
	for _, k := range names {
		vals := slices.Clone(form[k])
		slices.Sort(vals)
		for _, v := range slices.Compact(vals) {
			b.WriteString(k)
			b.WriteString(v)
		}
	}
	return b.String()
}

// twilioURLs are the spellings of the called URL that Twilio may have
// signed: as configured or as the request shows it, and that URL with the
// scheme's default port added or removed (Twilio's own libraries try both).
func (c *webhookCheck) twilioURLs(r *http.Request) []string {
	full := c.url
	if full == "" {
		scheme := "http"
		if r.TLS != nil {
			scheme = "https"
		}
		if p := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); p == "http" || p == "https" {
			scheme = p
		}
		full = scheme + "://" + r.Host + r.URL.RequestURI()
	} else if !strings.Contains(full, "?") && r.URL.RawQuery != "" {
		full += "?" + r.URL.RawQuery
	}
	out := []string{full}
	u, err := url.Parse(full)
	if err != nil || u.Host == "" {
		return out
	}
	def := map[string]string{"http": "80", "https": "443"}[u.Scheme]
	if def == "" {
		return out
	}
	rest := strings.TrimPrefix(full, u.Scheme+"://"+u.Host)
	switch u.Port() {
	case "":
		out = append(out, u.Scheme+"://"+u.Host+":"+def+rest)
	case def:
		out = append(out, u.Scheme+"://"+u.Hostname()+rest)
	}
	return out
}
