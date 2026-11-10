package gcp

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const serviceAccount = `{"type":"service_account","project_id":"p","private_key":"-----BEGIN PRIVATE KEY-----\nSECRET\n-----END PRIVATE KEY-----\n","client_email":"x@p.iam.gserviceaccount.com"}`

func TestParseGCPEndpoints(t *testing.T) {
	cases := []struct {
		kind, endpoint, want string
		emulator             bool
	}{
		{kindPubSub, "localhost:8085", "localhost:8085", true},
		{kindPubSub, "http://127.0.0.1:8085", "127.0.0.1:8085", true},
		{kindPubSub, "us-east1-pubsub.googleapis.com", "us-east1-pubsub.googleapis.com:443", false},
		{kindPubSub, "us-east1-pubsub.googleapis.com:443", "us-east1-pubsub.googleapis.com:443", false},
		{kindGCS, "http://localhost:4443/storage/v1/", "http://localhost:4443/storage/v1/", true},
		{kindGCS, "localhost:4443", "http://localhost:4443/storage/v1/", true},
		{kindGCS, "http://localhost:4443/storage/v1", "http://localhost:4443/storage/v1/", true},
		{kindGCS, "storage.googleapis.com", "https://storage.googleapis.com/storage/v1/", false},
	}
	for _, c := range cases {
		t.Run(c.kind+" "+c.endpoint, func(t *testing.T) {
			got, err := parseGCP(&config.GCP{Endpoint: c.endpoint}, c.kind)
			require.NoError(t, err)
			assert.Equal(t, c.want, got.endpoint)
			assert.Equal(t, c.emulator, got.emulator)
			assert.NotEmpty(t, got.opts)
		})
	}

	none, err := parseGCP(nil, kindPubSub)
	require.NoError(t, err)
	assert.Empty(t, none.opts, "no options: Application Default Credentials")

	for _, bad := range []string{"localhost", "http://", "::"} {
		_, err := parseGCP(&config.GCP{Endpoint: bad}, kindPubSub)
		assert.Error(t, err, bad)
	}
	_, err = parseGCP(&config.GCP{Endpoint: "ftp://host/x"}, kindGCS)
	assert.Error(t, err)
}

func TestParseGCPCredentials(t *testing.T) {
	file := filepath.Join(t.TempDir(), "sa.json")
	require.NoError(t, os.WriteFile(file, []byte(serviceAccount), 0o600))

	c, err := parseGCP(&config.GCP{CredentialsFile: file}, kindPubSub)
	require.NoError(t, err)
	assert.Len(t, c.opts, 1)
	c, err = parseGCP(&config.GCP{CredentialsJSON: serviceAccount, Endpoint: "europe-west1-pubsub.googleapis.com:443"}, kindPubSub)
	require.NoError(t, err)
	assert.Len(t, c.opts, 2, "a googleapis.com endpoint keeps the credentials")

	_, err = parseGCP(&config.GCP{CredentialsFile: file, CredentialsJSON: serviceAccount}, kindPubSub)
	assert.ErrorContains(t, err, "not both")
	_, err = parseGCP(&config.GCP{CredentialsJSON: serviceAccount, Endpoint: "localhost:8085"}, kindPubSub)
	assert.ErrorContains(t, err, "emulator")
	_, err = parseGCP(&config.GCP{CredentialsFile: filepath.Join(t.TempDir(), "missing.json")}, kindPubSub)
	assert.ErrorContains(t, err, "credentials_file")

	_, err = parseGCP(&config.GCP{CredentialsJSON: `not json SECRET`}, kindGCS)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "SECRET", "an error never quotes the credentials")
}

func TestCredentialsType(t *testing.T) {
	for typ, want := range map[string]option.CredentialsType{
		"service_account":              option.ServiceAccount,
		"authorized_user":              option.AuthorizedUser,
		"impersonated_service_account": option.ImpersonatedServiceAccount,
		"external_account":             option.ExternalAccount,
	} {
		got, err := credentialsType([]byte(`{"type":"` + typ + `"}`))
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
	_, err := credentialsType([]byte(`{"private_key":"SECRET"}`))
	assert.ErrorContains(t, err, `no "type"`)
	_, err = credentialsType([]byte(`{"type":"gdch_service_account"}`))
	assert.ErrorContains(t, err, "not supported")
}

func TestResourceName(t *testing.T) {
	full, project, err := resourceName("subscription", "projects/p1/subscriptions/s", "subscriptions", "")
	require.NoError(t, err)
	assert.Equal(t, "projects/p1/subscriptions/s", full)
	assert.Equal(t, "p1", project)

	full, project, err = resourceName("topic", "t", "topics", "p2")
	require.NoError(t, err)
	assert.Equal(t, "projects/p2/topics/t", full)
	assert.Equal(t, "p2", project)

	for _, bad := range []string{"", "projects/p/topics/t", "projects/p/subscriptions", "a/b"} {
		_, _, err := resourceName("subscription", bad, "subscriptions", "p")
		assert.Error(t, err, bad)
	}
	_, _, err = resourceName("topic", "t", "topics", "")
	assert.ErrorContains(t, err, "gcp.project is required")
}

func TestPubSubSourceConfigErrors(t *testing.T) {
	gcp := &config.GCP{Project: "p", Endpoint: "localhost:1"}
	cases := map[string]struct {
		src  config.Source
		want string
	}{
		"no subscription": {config.Source{GCP: gcp}, "subscription is required"},
		"no project":      {config.Source{Subscription: "s"}, "gcp.project is required"},
		"bad path":        {config.Source{Subscription: "projects/p/topics/t", GCP: gcp}, "not a projects/<project>/subscriptions/<name> path"},
		"listen":          {config.Source{Subscription: "s", GCP: gcp, Mode: "listen"}, "subscribe mode"},
		"ack":             {config.Source{Subscription: "s", GCP: gcp, Ack: "later"}, "ack must be"},
		"prefetch":        {config.Source{Subscription: "s", GCP: gcp, Prefetch: -1}, "prefetch"},
		"max body":        {config.Source{Subscription: "s", GCP: gcp, MaxBodyBytes: -1}, "max_body_bytes"},
		"jitter":          {config.Source{Subscription: "s", GCP: gcp, Reconnect: &config.Reconnect{Jitter: "some"}}, "reconnect.jitter"},
		"attempts":        {config.Source{Subscription: "s", GCP: gcp, Reconnect: &config.Reconnect{MaxAttempts: -1}}, "max_attempts"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newPubSubSource(context.Background(), plugin.Spec{Name: "in", Kind: kindPubSub, Config: &c.src})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "source in: ")
			assert.Contains(t, err.Error(), c.want)
		})
	}
	_, err := newPubSubSource(context.Background(), plugin.Spec{Name: "in", Kind: kindPubSub})
	assert.ErrorContains(t, err, "missing config")
}

func TestPubSubSourceConfigDefaults(t *testing.T) {
	o, err := parsePubSubSource(plugin.Spec{Name: "in", Config: &config.Source{Subscription: "projects/p/subscriptions/s",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Millisecond)}}})
	require.NoError(t, err)
	assert.Equal(t, "p", o.client.project, "the project comes from the path")
	assert.Equal(t, defaultPrefetch, o.prefetch)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, time.Second, o.reconnect.maxBackoff, "max_backoff is at least backoff")
	assert.Equal(t, defaultJitter, o.reconnect.jitter)
}

func TestPubSubSinkConfigErrors(t *testing.T) {
	gcp := &config.GCP{Project: "p", Endpoint: "localhost:1"}
	cases := map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no topic":         {config.Endpoint{GCP: gcp}, "topic is required"},
		"no project":       {config.Endpoint{Topic: "t"}, "gcp.project is required"},
		"templated no prj": {config.Endpoint{Topic: "{{ .subject }}"}, "gcp.project is required"},
		"bad path":         {config.Endpoint{Topic: "projects/p/subscriptions/s", GCP: gcp}, "not a projects/<project>/topics/<name> path"},
		"bad template":     {config.Endpoint{Topic: "{{ .x", GCP: gcp}, "template topic"},
		"bad key":          {config.Endpoint{Topic: "t", Key: "{{", GCP: gcp}, "template key"},
		"bad header":       {config.Endpoint{Topic: "t", Headers: map[string]string{"X": "{{"}, GCP: gcp}, "headers.X"},
		"bad body":         {config.Endpoint{Topic: "t", Body: "{{", GCP: gcp}, "template body"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newPubSubSink(context.Background(), plugin.Spec{Name: "out", Kind: kindPubSub, Config: &c.ep})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "destination out: ")
			assert.Contains(t, err.Error(), c.want)
		})
	}

	o, err := parsePubSubSink(plugin.Spec{Name: "out", Config: &config.Endpoint{Topic: "projects/p9/topics/{{ .subject }}"}})
	require.NoError(t, err)
	assert.Equal(t, "p9", o.client.project, "a templated full path still names its project")
	assert.Empty(t, o.static)
}

func TestGCSSourceConfigErrors(t *testing.T) {
	gcp := &config.GCP{Endpoint: "localhost:1"}
	cases := map[string]struct {
		src  config.Source
		want string
	}{
		"no bucket":     {config.Source{GCP: gcp}, "bucket is required"},
		"bucket path":   {config.Source{Bucket: "b/x", GCP: gcp}, "without a path"},
		"after":         {config.Source{Bucket: "b", After: "archive", GCP: gcp}, "after must be"},
		"move no dest":  {config.Source{Bucket: "b", After: "move", GCP: gcp}, "move_to is required"},
		"move_to alone": {config.Source{Bucket: "b", MoveTo: "done/", GCP: gcp}, "move_to applies"},
		"split":         {config.Source{Bucket: "b", Split: ".items[", GCP: gcp}, "split"},
		"mode":          {config.Source{Bucket: "b", Mode: "subscribe", GCP: gcp}, "poll mode"},
		"ack":           {config.Source{Bucket: "b", Ack: "accepted", GCP: gcp}, "ack must be delivered"},
		"interval":      {config.Source{Bucket: "b", Interval: -1, GCP: gcp}, "interval"},
		"timeout":       {config.Source{Bucket: "b", Timeout: -1, GCP: gcp}, "timeout"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newGCSSource(context.Background(), plugin.Spec{Name: "in", Kind: kindGCS, Config: &c.src})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "source in: ")
			assert.Contains(t, err.Error(), c.want)
		})
	}

	_, err := newGCSSource(context.Background(), plugin.Spec{Name: "in", Kind: kindGCS,
		Config: &config.Source{Bucket: "b", After: "keep", GCP: gcp}})
	assert.ErrorContains(t, err, "needs a store")

	o, err := parseGCSSource(plugin.Spec{Name: "in", Config: &config.Source{Bucket: "gs://b/", GCP: gcp}})
	require.NoError(t, err)
	assert.Equal(t, "b", o.bucket)
	assert.Equal(t, afterDelete, o.after)
	assert.Equal(t, config.DefaultPollInterval, o.interval)
	assert.Equal(t, config.DefaultPollTimeout, o.timeout)

	st := store.NewMemory("m")
	s, err := newGCSSource(context.Background(), plugin.Spec{Name: "in", Kind: kindGCS, Env: testEnv(st),
		Config: &config.Source{Bucket: "b", After: "keep", GCP: gcp}})
	require.NoError(t, err)
	assert.NoError(t, s.Close(context.Background()))
}

func TestGCSSinkConfigErrors(t *testing.T) {
	gcp := &config.GCP{Endpoint: "localhost:1"}
	cases := map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no bucket":  {config.Endpoint{GCP: gcp}, "bucket is required"},
		"bad key":    {config.Endpoint{Bucket: "b", Key: "{{", GCP: gcp}, "template key"},
		"bad header": {config.Endpoint{Bucket: "b", Headers: map[string]string{"": "x"}, GCP: gcp}, "name is empty"},
		"bad body":   {config.Endpoint{Bucket: "b", Body: "{{ .x", GCP: gcp}, "template body"},
		"creds":      {config.Endpoint{Bucket: "b", GCP: &config.GCP{Endpoint: "localhost:1", CredentialsJSON: serviceAccount}}, "emulator"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newGCSSink(context.Background(), plugin.Spec{Name: "out", Kind: kindGCS, Config: &c.ep})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "destination out: ")
			assert.Contains(t, err.Error(), c.want)
		})
	}
	_, err := newGCSSink(context.Background(), plugin.Spec{Name: "out", Kind: kindGCS})
	assert.ErrorContains(t, err, "missing config")
}
