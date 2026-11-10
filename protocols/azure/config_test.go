package azure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/internal/config"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/store"
)

const testConnString = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=root;SharedAccessKey=c2VjcmV0"

func connStr() *config.Azure { return &config.Azure{ConnectionString: testConnString} }

func memEnv() plugin.Env {
	mem := store.NewMemory("default")
	return plugin.Env{Relay: "relay", Store: func(name string) (store.Store, error) {
		if name != "" && name != "default" {
			return nil, errors.New("store " + name + " is not declared")
		}
		return mem, nil
	}}
}

func TestParseConn(t *testing.T) {
	c, err := parseConn(connStr(), nil)
	require.NoError(t, err)
	assert.Equal(t, "ns.servicebus.windows.net", c.namespace)
	assert.Empty(t, c.entityPath)
	assert.Nil(t, c.credential)
	assert.Equal(t, "ns.servicebus.windows.net", c.String(), "String never shows the connection string")

	c, err = parseConn(&config.Azure{ConnectionString: testConnString + ";EntityPath=orders"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "orders", c.entityPath)

	c, err = parseConn(&config.Azure{Namespace: "sb://ns.servicebus.windows.net/"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "ns.servicebus.windows.net", c.namespace)
	assert.NotNil(t, c.credential, "DefaultAzureCredential")

	c, err = parseConn(&config.Azure{Namespace: "ns.servicebus.windows.net", TenantID: "00000000-0000-0000-0000-000000000000",
		ClientID: "11111111-1111-1111-1111-111111111111", ClientSecret: "s3cret"}, nil)
	require.NoError(t, err)
	assert.NotNil(t, c.credential, "ClientSecretCredential")

	_, err = parseConn(&config.Azure{Namespace: "ns.servicebus.windows.net", ClientID: "x"}, nil)
	assert.ErrorContains(t, err, "tenant_id, client_id and client_secret")

	for _, a := range []*config.Azure{nil, {}} {
		_, err = parseConn(a, nil)
		assert.ErrorContains(t, err, "needs connection_string")
	}
	_, err = parseConn(&config.Azure{ConnectionString: testConnString, Namespace: "ns"}, nil)
	assert.ErrorContains(t, err, "not both")
	_, err = parseConn(&config.Azure{Namespace: "bad name"}, nil)
	assert.ErrorContains(t, err, "fully qualified namespace")

	_, err = parseConn(&config.Azure{ConnectionString: "Endpoint=sb://ns/;SharedAccessKey=hunter2"}, nil)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "hunter2", "the connection string is never quoted")

	_, err = parseConn(connStr(), &config.TLS{CAFile: "/nonexistent/ca.pem"})
	assert.Error(t, err)
}

func TestParseSBSource(t *testing.T) {
	spec := func(src config.Source) plugin.Spec {
		if src.Azure == nil {
			src.Azure = connStr()
		}
		return plugin.Spec{Name: "in", Kind: kindServiceBus, Config: &src, Env: memEnv()}
	}
	o, err := parseSBSource(spec(config.Source{Queue: "orders"}))
	require.NoError(t, err)
	assert.Equal(t, sbEntity{queue: "orders"}, o.entity)
	assert.Equal(t, "orders", o.entity.path())
	assert.Equal(t, "orders", o.entity.name())
	assert.Equal(t, defaultPrefetch, o.prefetch)
	assert.Equal(t, int64(config.DefaultMaxBodyBytes), o.maxBytes)
	assert.Equal(t, ackDelivered, o.ack)
	assert.Equal(t, reconnectOptions{backoff: defaultBackoff, maxBackoff: defaultMaxBackoff, jitter: "full"}, o.reconnect)

	o, err = parseSBSource(spec(config.Source{Topic: "events", Subscription: "audit", Prefetch: 10, MaxBodyBytes: 1024, Ack: "accepted",
		Reconnect: &config.Reconnect{Backoff: config.Duration(time.Second), MaxBackoff: config.Duration(time.Minute), Jitter: "none", MaxAttempts: 3}}))
	require.NoError(t, err)
	assert.Equal(t, "events/subscriptions/audit", o.entity.path())
	assert.Equal(t, "events", o.entity.name())
	assert.Equal(t, 10, o.prefetch)
	assert.Equal(t, int64(1024), o.maxBytes)
	assert.Equal(t, ackAccepted, o.ack)
	assert.Equal(t, reconnectOptions{backoff: time.Second, maxBackoff: time.Minute, jitter: "none", maxAttempts: 3}, o.reconnect)

	for name, tc := range map[string]struct {
		src  config.Source
		want string
	}{
		"no entity":           {config.Source{}, "queue is required"},
		"both":                {config.Source{Queue: "q", Topic: "t", Subscription: "s"}, "not both"},
		"topic no sub":        {config.Source{Topic: "t"}, "subscription is required"},
		"queue with sub":      {config.Source{Queue: "q", Subscription: "s"}, "subscription applies to a topic"},
		"negative prefetch":   {config.Source{Queue: "q", Prefetch: -1}, "prefetch"},
		"huge prefetch":       {config.Source{Queue: "q", Prefetch: maxPrefetch + 1}, "at most 5000"},
		"negative body":       {config.Source{Queue: "q", MaxBodyBytes: -1}, "max_body_bytes"},
		"bad ack":             {config.Source{Queue: "q", Ack: "sometimes"}, "ack must be"},
		"bad mode":            {config.Source{Queue: "q", Mode: "poll"}, "subscribe mode"},
		"bad jitter":          {config.Source{Queue: "q", Reconnect: &config.Reconnect{Jitter: "wild"}}, "jitter"},
		"negative attempts":   {config.Source{Queue: "q", Reconnect: &config.Reconnect{MaxAttempts: -1}}, "max_attempts"},
		"no azure":            {config.Source{Queue: "q", Azure: &config.Azure{}}, "needs connection_string"},
		"bad tls":             {config.Source{Queue: "q", TLS: &config.TLS{CertFile: "/nonexistent.pem", KeyFile: "/nonexistent.key"}}, ""},
		"bad conn":            {config.Source{Queue: "q", Azure: &config.Azure{ConnectionString: "nonsense"}}, "connection_string"},
		"wrong config struct": {config.Source{}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			s := spec(tc.src)
			if name == "wrong config struct" {
				s.Config = &config.Endpoint{}
			}
			_, err := parseSBSource(s)
			require.Error(t, err)
			assert.ErrorContains(t, err, "source in")
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

func TestParseEHSource(t *testing.T) {
	spec := func(src config.Source) plugin.Spec {
		if src.Azure == nil {
			src.Azure = connStr()
		}
		return plugin.Spec{Name: "in", Kind: kindEventHubs, Config: &src, Env: memEnv()}
	}
	o, err := parseEHSource(spec(config.Source{Topic: "telemetry"}))
	require.NoError(t, err)
	assert.Equal(t, "telemetry", o.hub)
	assert.Equal(t, defaultConsumerGroup, o.group)
	assert.Equal(t, startLatest, o.start)
	assert.NotNil(t, o.store)

	o, err = parseEHSource(spec(config.Source{Azure: &config.Azure{ConnectionString: testConnString + ";EntityPath=telemetry"},
		Group: "relay", Start: "earliest", StateStore: "default"}))
	require.NoError(t, err)
	assert.Equal(t, "telemetry", o.hub, "EntityPath names the hub")
	assert.Equal(t, "relay", o.group)
	assert.Equal(t, startEarliest, o.start)

	_, err = parseEHSource(spec(config.Source{Azure: &config.Azure{ConnectionString: testConnString + ";EntityPath=telemetry"}, Topic: "telemetry"}))
	assert.NoError(t, err, "topic may repeat the EntityPath")

	for name, tc := range map[string]struct {
		src  config.Source
		env  *plugin.Env
		want string
	}{
		"no hub":        {src: config.Source{}, want: "topic is required"},
		"two hubs":      {src: config.Source{Azure: &config.Azure{ConnectionString: testConnString + ";EntityPath=a"}, Topic: "b"}, want: "differs"},
		"bad start":     {src: config.Source{Topic: "t", Start: "new"}, want: "start must be earliest or latest"},
		"unknown store": {src: config.Source{Topic: "t", StateStore: "nope"}, want: "state_store"},
		"no store":      {src: config.Source{Topic: "t"}, env: &plugin.Env{}, want: "no store"},
		"nil store": {src: config.Source{Topic: "t"}, env: &plugin.Env{Store: func(string) (store.Store, error) { return nil, nil }},
			want: "no store"},
	} {
		t.Run(name, func(t *testing.T) {
			s := spec(tc.src)
			if tc.env != nil {
				s.Env = *tc.env
			}
			_, err := parseEHSource(s)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

func TestParseSinks(t *testing.T) {
	sb := func(ep config.Endpoint) plugin.Spec {
		if ep.Azure == nil {
			ep.Azure = connStr()
		}
		return plugin.Spec{Name: "out", Kind: kindServiceBus, Config: &ep}
	}
	o, err := parseSBSink(sb(config.Endpoint{Queue: "orders", GroupID: "{{ .json.customer }}", Headers: map[string]string{"X-Team": "core"}, Body: "{{ .bodyText }}"}))
	require.NoError(t, err)
	assert.Equal(t, "orders", o.entity)
	assert.NotNil(t, o.session)
	assert.Len(t, o.out.headers, 1)
	assert.NotNil(t, o.out.body)

	o, err = parseSBSink(sb(config.Endpoint{Topic: "events"}))
	require.NoError(t, err)
	assert.Equal(t, "events", o.entity)

	for name, tc := range map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no entity":       {config.Endpoint{}, "queue is required"},
		"both":            {config.Endpoint{Queue: "q", Topic: "t"}, "not both"},
		"templated queue": {config.Endpoint{Queue: "{{ .subject }}"}, "per-message template"},
		"bad group_id":    {config.Endpoint{Queue: "q", GroupID: "{{ .x "}, "group_id"},
		"bad header":      {config.Endpoint{Queue: "q", Headers: map[string]string{"A": "{{"}}, "headers.A"},
		"bad body":        {config.Endpoint{Queue: "q", Body: "{{ end }}"}, "body"},
		"no azure":        {config.Endpoint{Queue: "q", Azure: &config.Azure{}}, "needs connection_string"},
	} {
		t.Run("servicebus "+name, func(t *testing.T) {
			_, err := parseSBSink(sb(tc.ep))
			assert.ErrorContains(t, err, "destination out")
			assert.ErrorContains(t, err, tc.want)
		})
	}
	_, err = parseSBSink(plugin.Spec{Name: "out", Config: &config.Source{}})
	assert.ErrorContains(t, err, "missing config")

	eh := func(ep config.Endpoint) plugin.Spec {
		if ep.Azure == nil {
			ep.Azure = connStr()
		}
		return plugin.Spec{Name: "out", Kind: kindEventHubs, Config: &ep}
	}
	e, err := parseEHSink(eh(config.Endpoint{Topic: "telemetry", Key: "{{ .json.device }}"}))
	require.NoError(t, err)
	assert.Equal(t, "telemetry", e.hub)
	assert.NotNil(t, e.key)
	for name, tc := range map[string]struct {
		ep   config.Endpoint
		want string
	}{
		"no hub":        {config.Endpoint{}, "topic is required"},
		"templated hub": {config.Endpoint{Topic: "{{ .subject }}"}, "per-message template"},
		"bad key":       {config.Endpoint{Topic: "t", Key: "{{"}, "key"},
		"bad header":    {config.Endpoint{Topic: "t", Headers: map[string]string{"A": "{{"}}, "headers.A"},
	} {
		t.Run("eventhubs "+name, func(t *testing.T) {
			_, err := parseEHSink(eh(tc.ep))
			assert.ErrorContains(t, err, tc.want)
		})
	}
	_, err = parseEHSink(plugin.Spec{Name: "out"})
	assert.ErrorContains(t, err, "missing config")
}

func TestRegister(t *testing.T) {
	Register()
	for _, kind := range []string{kindServiceBus, kindEventHubs} {
		_, ok := plugin.LookupSource(kind)
		assert.True(t, ok, kind)
		_, ok = plugin.LookupSink(kind)
		assert.True(t, ok, kind)
	}
	// The factories build real clients from a connection string without
	// connecting.
	ctx := context.Background()
	src, err := plugin.NewSource(ctx, plugin.Spec{Name: "in", Kind: kindServiceBus,
		Config: &config.Source{Queue: "q", Azure: connStr()}, Env: memEnv()})
	require.NoError(t, err)
	assert.Equal(t, "in", src.Name())
	assert.NoError(t, src.(plugin.Closer).Close(ctx))

	src, err = plugin.NewSource(ctx, plugin.Spec{Name: "hub", Kind: kindEventHubs,
		Config: &config.Source{Topic: "h", Azure: connStr()}, Env: memEnv()})
	require.NoError(t, err)
	assert.Equal(t, "hub", src.Name())
	assert.NoError(t, src.(plugin.Closer).Close(ctx))

	for _, kind := range []string{kindServiceBus, kindEventHubs} {
		sink, err := plugin.NewSink(ctx, plugin.Spec{Name: "out", Kind: kind,
			Config: &config.Endpoint{Queue: "q", Topic: map[string]string{kindServiceBus: "", kindEventHubs: "h"}[kind], Azure: connStr()}})
		require.NoError(t, err, kind)
		assert.Equal(t, "out", sink.Name())
		assert.NoError(t, sink.Close(ctx))
		assert.NoError(t, sink.Close(ctx), "closing twice is fine")
	}

	_, err = plugin.NewSource(ctx, plugin.Spec{Name: "in", Kind: kindServiceBus, Config: &config.Source{Azure: connStr()}})
	assert.ErrorContains(t, err, "queue is required")
	_, err = plugin.NewSink(ctx, plugin.Spec{Name: "out", Kind: kindEventHubs, Config: &config.Endpoint{Azure: connStr()}})
	assert.ErrorContains(t, err, "topic is required")
}
