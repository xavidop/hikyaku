package inbound

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // Twilio signs with HMAC-SHA1
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
)

func webhook(t *testing.T, w config.WebhookAuth, now time.Time) *Authenticator {
	t.Helper()
	a, err := New(context.Background(), &config.Auth{Webhook: &w})
	require.NoError(t, err)
	require.True(t, a.NeedsBody())
	a.webhook.now = func() time.Time { return now }
	return a
}

func post(target string, body []byte, headers map[string]string) *http.Request {
	r := httptest.NewRequest("POST", target, bytes.NewReader(body))
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	return r
}

func hexMAC(secret, msg string) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write([]byte(msg))
	return hex.EncodeToString(m.Sum(nil))
}

// The example of GitHub's "Validating webhook deliveries" page.
func TestWebhookGitHub(t *testing.T) {
	a := webhook(t, config.WebhookAuth{Provider: "github", Secret: "It's a Secret to Everybody"}, time.Now())
	body := []byte("Hello, World!")
	const sig = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17"
	assert.NoError(t, a.Check(post("/hook", body, map[string]string{"X-Hub-Signature-256": sig}), body))
	assert.ErrorIs(t, a.Check(post("/hook", []byte("Hello, World?"), map[string]string{"X-Hub-Signature-256": sig}), []byte("Hello, World?")), ErrUnauthorized)
	assert.ErrorContains(t, a.Check(post("/hook", body, nil), body), "missing X-Hub-Signature-256")
	assert.ErrorContains(t, a.Check(post("/hook", body, map[string]string{"X-Hub-Signature-256": "sha256=zz"}), body), "bad X-Hub-Signature-256")
	assert.ErrorContains(t, a.Check(post("/hook", nil, map[string]string{"X-Hub-Signature-256": sig}), nil), "needs a request body")
}

// The example of Slack's "Verifying requests from Slack" page.
func TestWebhookSlack(t *testing.T) {
	const secret, ts = "8f742231b10e8888abcd99yyyzzz85a5", "1531420618"
	body := []byte("token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c")
	h := map[string]string{
		"X-Slack-Signature":         "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503",
		"X-Slack-Request-Timestamp": ts,
	}
	sent := time.Unix(1531420618, 0)
	a := webhook(t, config.WebhookAuth{Provider: "slack", Secret: secret}, sent.Add(time.Minute))
	assert.NoError(t, a.Check(post("/slack", body, h), body))

	// A replay after the tolerance is refused, even with a good signature.
	old := webhook(t, config.WebhookAuth{Provider: "slack", Secret: secret}, sent.Add(6*time.Minute))
	assert.ErrorContains(t, old.Check(post("/slack", body, h), body), "outside the 5m0s tolerance")
	long := webhook(t, config.WebhookAuth{Provider: "slack", Secret: secret, Tolerance: config.Duration(time.Hour)}, sent.Add(6*time.Minute))
	assert.NoError(t, long.Check(post("/slack", body, h), body))

	// The timestamp is signed: changing it breaks the signature.
	moved := map[string]string{"X-Slack-Signature": h["X-Slack-Signature"], "X-Slack-Request-Timestamp": "1531420619"}
	assert.ErrorContains(t, a.Check(post("/slack", body, moved), body), "bad X-Slack-Signature")
	assert.ErrorContains(t, a.Check(post("/slack", body, map[string]string{"X-Slack-Signature": h["X-Slack-Signature"]}), body), "missing X-Slack-Request-Timestamp")
	assert.ErrorContains(t, a.Check(post("/slack", body, nil), body), "missing X-Slack-Signature")
}

func TestWebhookStripe(t *testing.T) {
	const secret = "whsec_test"
	body := []byte(`{"id":"evt_1","type":"invoice.paid"}`)
	now := time.Unix(1700000000, 0)
	ts := fmt.Sprint(now.Unix())
	good := hexMAC(secret, ts+"."+string(body))
	a := webhook(t, config.WebhookAuth{Provider: "stripe", Secret: secret}, now.Add(10*time.Second))

	assert.NoError(t, a.Check(post("/stripe", body, map[string]string{"Stripe-Signature": "t=" + ts + ",v1=" + good}), body))
	// While a secret is rolled Stripe sends one v1 per secret, and other
	// schemes may come along.
	rolled := "t=" + ts + ",v1=" + hexMAC("whsec_old", ts+"."+string(body)) + ", v1=" + good + ",v0=ignored"
	assert.NoError(t, a.Check(post("/stripe", body, map[string]string{"Stripe-Signature": rolled}), body))

	assert.ErrorContains(t, a.Check(post("/stripe", body, map[string]string{"Stripe-Signature": "t=" + ts + ",v1=" + hexMAC("other", ts+"."+string(body))}), body), "bad Stripe-Signature")
	// A signature of the body alone is not Stripe's.
	assert.ErrorContains(t, a.Check(post("/stripe", body, map[string]string{"Stripe-Signature": "t=" + ts + ",v1=" + hexMAC(secret, string(body))}), body), "bad Stripe-Signature")
	assert.ErrorContains(t, a.Check(post("/stripe", body, map[string]string{"Stripe-Signature": "v1=" + good}), body), "no valid timestamp")
	assert.ErrorContains(t, a.Check(post("/stripe", body, nil), body), "missing Stripe-Signature")

	late := webhook(t, config.WebhookAuth{Provider: "stripe", Secret: secret}, now.Add(time.Hour))
	assert.ErrorContains(t, late.Check(post("/stripe", body, map[string]string{"Stripe-Signature": "t=" + ts + ",v1=" + good}), body), "tolerance")
	// A timestamp from the future is refused the same way.
	early := webhook(t, config.WebhookAuth{Provider: "stripe", Secret: secret}, now.Add(-time.Hour))
	assert.ErrorContains(t, early.Check(post("/stripe", body, map[string]string{"Stripe-Signature": "t=" + ts + ",v1=" + good}), body), "tolerance")
}

func TestWebhookShopify(t *testing.T) {
	const secret = "shpss_test"
	body := []byte(`{"id":820982911946154508}`)
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	sig := base64.StdEncoding.EncodeToString(m.Sum(nil))
	a := webhook(t, config.WebhookAuth{Provider: "shopify", Secret: secret}, time.Now())
	assert.NoError(t, a.Check(post("/shopify", body, map[string]string{"X-Shopify-Hmac-Sha256": sig}), body))
	assert.ErrorContains(t, a.Check(post("/shopify", []byte(`{}`), map[string]string{"X-Shopify-Hmac-Sha256": sig}), []byte(`{}`)), "bad X-Shopify-Hmac-Sha256")
	assert.ErrorContains(t, a.Check(post("/shopify", body, map[string]string{"X-Shopify-Hmac-Sha256": "not base64!"}), body), "bad X-Shopify-Hmac-Sha256")
	assert.ErrorContains(t, a.Check(post("/shopify", body, nil), body), "missing X-Shopify-Hmac-Sha256")
}

// The example of Twilio's "Validating requests" page: auth token 12345.
func TestWebhookTwilioForm(t *testing.T) {
	const public = "https://mycompany.com/myapp.php?foo=1&bar=2"
	const sig = "RSOYDt4T1cUTdK1PDd93/VVr8B8="
	body := []byte("To=%2B18005551212&From=%2B14158675309&Digits=1234&Caller=%2B14158675309&CallSid=CA1234567890ABCDE")
	form := map[string]string{"X-Twilio-Signature": sig, "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

	// Behind a proxy the relay sees another host: url names the public one.
	a := webhook(t, config.WebhookAuth{Provider: "twilio", Secret: "12345", URL: public}, time.Now())
	assert.NoError(t, a.Check(post("http://10.0.0.7:8080/myapp.php?foo=1&bar=2", body, form), body))
	// url may leave the query to the request.
	short := webhook(t, config.WebhookAuth{Provider: "twilio", Secret: "12345", URL: "https://mycompany.com/myapp.php"}, time.Now())
	assert.NoError(t, short.Check(post("http://10.0.0.7:8080/myapp.php?foo=1&bar=2", body, form), body))

	// Without url, the request's own host and the forwarded scheme are used.
	direct := webhook(t, config.WebhookAuth{Provider: "twilio", Secret: "12345"}, time.Now())
	r := post("http://mycompany.com/myapp.php?foo=1&bar=2", body, form)
	assert.ErrorContains(t, direct.Check(r, body), "bad X-Twilio-Signature", "http is not what Twilio called")
	r.Header.Set("X-Forwarded-Proto", "https")
	assert.NoError(t, direct.Check(r, body))
	// Twilio may sign the URL with its default port.
	ported := post("http://mycompany.com:443/myapp.php?foo=1&bar=2", body, form)
	ported.Header.Set("X-Forwarded-Proto", "https")
	assert.NoError(t, direct.Check(ported, body))

	changed := []byte("To=%2B18005551212&From=%2B14158675309&Digits=9999&Caller=%2B14158675309&CallSid=CA1234567890ABCDE")
	assert.ErrorContains(t, a.Check(post("/myapp.php?foo=1&bar=2", changed, form), changed), "bad X-Twilio-Signature")
	assert.ErrorContains(t, a.Check(post("/myapp.php?foo=1&bar=2", body, map[string]string{"Content-Type": form["Content-Type"]}), body), "missing X-Twilio-Signature")
}

func TestWebhookTwilioJSON(t *testing.T) {
	const token = "12345"
	body := []byte(`{"CallSid":"CA1234567890ABCDE"}`)
	sum := sha256.Sum256(body)
	public := "https://mycompany.com/hook?bodySHA256=" + hex.EncodeToString(sum[:])
	sig := base64.StdEncoding.EncodeToString((&webhookCheck{secret: []byte(token)}).mac(sha1.New, []byte(public)))
	h := map[string]string{"X-Twilio-Signature": sig, "Content-Type": "application/json"}
	a := webhook(t, config.WebhookAuth{Provider: "twilio", Secret: token, URL: "https://mycompany.com/hook"}, time.Now())

	target := "/hook?bodySHA256=" + hex.EncodeToString(sum[:])
	assert.NoError(t, a.Check(post(target, body, h), body))
	other := []byte(`{"CallSid":"CAother"}`)
	assert.ErrorContains(t, a.Check(post(target, other, h), other), "does not match bodySHA256")
}

func TestWebhookSettings(t *testing.T) {
	_, err := New(context.Background(), &config.Auth{Webhook: &config.WebhookAuth{Provider: "paypal", Secret: "x"}})
	assert.ErrorContains(t, err, "auth.webhook.provider")
	_, err = New(context.Background(), &config.Auth{Webhook: &config.WebhookAuth{Provider: "github"}})
	assert.ErrorContains(t, err, "secret is required")
}
