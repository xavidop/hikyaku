package config

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validateYAML loads a one-route spec around the given source and
// destination and returns the validation errors.
func validateYAML(t *testing.T, source, destination string) string {
	t.Helper()
	doc := "version: 1\nname: relay\nsources:\n  - name: in\n" + indent(source) +
		"destinations:\n  - name: out\n" + indent(destination) +
		"routes: [{ name: r, from: [in], to: [out] }]\n"
	_, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte(doc), LoadOptions{})
	if err == nil {
		return ""
	}
	return err.Error()
}

func indent(block string) string {
	var b strings.Builder
	for _, l := range strings.Split(strings.TrimSpace(block), "\n") {
		b.WriteString("    " + strings.TrimSpace(l) + "\n")
	}
	return b.String()
}

const stdout = "type: stdout"
const stdin = "type: stdin"

func TestProtocolSourcesValid(t *testing.T) {
	for name, src := range map[string]string{
		"grpc listen":    "type: grpc\nlisten: :50051",
		"grpc subscribe": "type: grpc\nmode: subscribe\nurl: grpc://feed:443\nmethod: /feed.v1.Feed/Watch",
		"tcp":            "type: tcp\nlisten: :9000\nframe: length\nlength_bytes: 2",
		"udp":            "type: udp\nlisten: :9001\nframe: raw",
		"udp dtls":       "type: udp\nlisten: :5684\ntls: { cert_file: c.pem, key_file: k.pem, client_ca_file: ca.pem }",
		"kafka":          "type: kafka\nbrokers: [k:9092]\ntopics: [a, b]\ngroup: g\nstart: earliest\nsasl: { mechanism: scram-sha-512, username: u, password: p }",
		"nats":           "type: nats\nurl: nats://n:4222\nsubject: orders.>\nqueue: workers",
		"jetstream":      "type: jetstream\nurl: nats://n:4222\nstream: ORDERS\nstart: all",
		"mqtt":           "type: mqtt\nurl: mqtt://m:1883\ntopic: sensors/#\nqos: 2\ngroup: g",
		"amqp":           "type: amqp\nurl: amqp://r:5672/\nqueue: jobs\nexchange: events\nrouting_key: job.*\ndeclare: true",
		"redis stream":   "type: redis\nurl: redis://r:6379/0\nstream: events\ngroup: hikyaku",
		"redis channel":  "type: redis\nurl: redis://r:6379/0\nchannel: news.*",
		"sqs":            "type: sqs\nqueue_url: https://sqs.eu-west-1.amazonaws.com/1/q\nprefetch: 10\nwait_time: 20s",
		"sns":            "type: sns\nqueue_url: https://sqs.eu-west-1.amazonaws.com/1/q",
		"s3":             "type: s3\nbucket: b\nprefix: in/\nafter: move\nmove_to: done/",
		"pubsub":         "type: pubsub\nsubscription: s\ngcp: { project: p }",
		"pubsub path":    "type: pubsub\nsubscription: projects/p/subscriptions/s",
		"servicebus":     "type: servicebus\ntopic: t\nsubscription: s\nazure: { connection_string: Endpoint=sb://x/ }",
		"eventhubs":      "type: eventhubs\nazure: { connection_string: \"Endpoint=sb://x/;EntityPath=hub\" }",
		"openai":         "type: openai\nlisten: :8080\nmodels: [auto, fast]",
	} {
		t.Run(name, func(t *testing.T) {
			assert.Empty(t, validateYAML(t, src, stdout))
		})
	}
}

func TestProtocolSourcesInvalid(t *testing.T) {
	for _, c := range []struct{ name, src, want string }{
		{"grpc subscribe without method", "type: grpc\nmode: subscribe\nurl: grpc://f:1", `.method: is required`},
		{"tcp bad frame", "type: tcp\nlisten: :9000\nframe: crlf", `.frame: must be newline, length or raw`},
		{"length bytes without length frame", "type: tcp\nlisten: :9000\nlength_bytes: 4", `applies with frame: length only`},
		{"kafka without group", "type: kafka\nbrokers: [k:9092]\ntopic: t", `.group: is required`},
		{"kafka topic and topics", "type: kafka\nbrokers: [k:9092]\ntopic: t\ntopics: [u]\ngroup: g", `set topic or topics, not both`},
		{"kafka bad sasl", "type: kafka\nbrokers: [k:9092]\ntopic: t\ngroup: g\nsasl: { mechanism: gssapi, username: u, password: p }", `sasl.mechanism: must be plain`},
		{"nats without subject", "type: nats\nurl: nats://n:4222", `.subject: is required`},
		{"nats without url", "type: nats\nsubject: s", `.url: is required in subscribe mode`},
		{"jetstream bad start", "type: jetstream\nurl: nats://n:4222\nstream: S\nstart: earliest", `.start: must be all or new or last`},
		{"mqtt v3 shared", "type: mqtt\nurl: mqtt://m:1883\ntopic: t\ngroup: g\nprotocol_version: \"3.1.1\"", `shared subscriptions need MQTT 5`},
		{"mqtt bad qos", "type: mqtt\nurl: mqtt://m:1883\ntopic: t\nqos: 3", `.qos: must be 0, 1 or 2`},
		{"amqp without queue", "type: amqp\nurl: amqp://r/", `.queue: is required`},
		{"amqp exchange without declare", "type: amqp\nurl: amqp://r/\nqueue: q\nexchange: e", `.exchange: needs declare: true`},
		{"redis both", "type: redis\nurl: redis://r\nstream: s\ngroup: g\nchannel: c", `set stream or channel, not both`},
		{"redis stream without group", "type: redis\nurl: redis://r\nstream: s", `.group: is required`},
		{"sqs prefetch", "type: sqs\nqueue_url: https://q\nprefetch: 20", `must be 1 to 10`},
		{"sqs wait", "type: sqs\nqueue_url: https://q\nwait_time: 30s", `at most 20s`},
		{"s3 move without move_to", "type: s3\nbucket: b\nafter: move", `.move_to: is required with after: move`},
		{"pubsub without project", "type: pubsub\nsubscription: s", `.gcp.project: is required`},
		{"servicebus without azure", "type: servicebus\nqueue: q", `.azure: needs connection_string`},
		{"servicebus topic without subscription", "type: servicebus\ntopic: t\nazure: { namespace: n.servicebus.windows.net }", `.subscription: is required with topic`},
		{"eventhubs without hub", "type: eventhubs\nazure: { connection_string: Endpoint=sb://x/ }", `.topic: is required`},
		{"accepted on a listen tcp", "type: tcp\nlisten: :9000\nack: accepted", `accepted applies to http and grpc listen sources and to subscribe sources`},
		{"grpc path", "type: grpc\nlisten: :50051\npath: /x", `.path: does not apply to grpc`},
	} {
		t.Run(c.name, func(t *testing.T) {
			assert.Contains(t, validateYAML(t, c.src, stdout), c.want)
		})
	}
}

func TestProtocolDestinations(t *testing.T) {
	valid := map[string]string{
		"grpc":        "type: grpc\nurl: grpc://svc:50051\nmethod: /pkg.Svc/Do",
		"tcp":         "type: tcp\nurl: tcp://h:9000\nframe: newline",
		"udp dtls":    "type: udp\nurl: dtls://h:5684\ntls: { ca_file: ca.pem }\nframe: raw",
		"kafka":       "type: kafka\nbrokers: [k:9092]\ntopic: t\nkey: \"{{ .json.id }}\"\nacks: all\ncompression: zstd",
		"nats":        "type: nats\nurl: nats://n:4222\nsubject: out",
		"jetstream":   "type: jetstream\nurl: nats://n:4222\nsubject: out",
		"mqtt":        "type: mqtt\nurl: mqtt://m:1883\ntopic: out\nqos: 1\nretain: true",
		"amqp":        "type: amqp\nurl: amqp://r/\nexchange: events\nrouting_key: \"{{ .subject }}\"",
		"redis":       "type: redis\nurl: redis://r:6379\nstream: out\nmax_len: 1000",
		"sqs":         "type: sqs\nqueue_url: https://q\ngroup_id: \"{{ .json.customer }}\"\ndelay: 10s",
		"sns":         "type: sns\ntopic: arn:aws:sns:eu-west-1:1:t",
		"eventbridge": "type: eventbridge\nevent_bus: bus",
		"s3":          "type: s3\nbucket: b\nkey: \"in/{{ .id }}.json\"\naws: { region: eu-west-1, endpoint: \"http://localhost:4566\" }",
		"gcs":         "type: gcs\nbucket: b",
		"pubsub":      "type: pubsub\ntopic: projects/p/topics/t",
		"servicebus":  "type: servicebus\nqueue: q\nazure: { connection_string: Endpoint=sb://x/ }",
		"eventhubs":   "type: eventhubs\ntopic: hub\nazure: { namespace: x.servicebus.windows.net }",
		"model":       "type: model\nmodel: googleai/gemini-2.5-flash\ngeneration: { temperature: 0.2, max_tokens: 512 }",
	}
	for name, dst := range valid {
		t.Run("valid/"+name, func(t *testing.T) {
			assert.Empty(t, validateYAML(t, stdin, dst))
		})
	}
	for _, c := range []struct{ name, dst, want string }{
		{"grpc scheme", "type: grpc\nurl: http://svc\nmethod: /a.B/C", `must start with grpc:// or grpcs://`},
		{"grpc without method", "type: grpc\nurl: grpc://svc:1", `.method: is required`},
		{"udp scheme", "type: udp\nurl: tls://h:1", `must start with udp:// or dtls://`},
		{"dtls scheme on tcp", "type: tcp\nurl: dtls://h:1", `must start with tcp:// or tls://`},
		{"amqp without key", "type: amqp\nurl: amqp://r/", `.routing_key: is required`},
		{"redis max_len on a channel", "type: redis\nurl: redis://r\nchannel: c\nmax_len: 5", `.max_len: applies to streams only`},
		{"sqs delay", "type: sqs\nqueue_url: https://q\ndelay: 20m", `at most 15m`},
		{"pubsub bad path", "type: pubsub\ntopic: projects/p/subscriptions/s", `is not a topics path`},
		{"azure both", "type: servicebus\nqueue: q\nazure: { connection_string: x, namespace: y }", `not both`},
		{"model without provider", "type: model\nmodel: gemini", `must be provider/name`},
		{"ws frame", "type: ws\nurl: ws://h\nframe: newline", `must be text or binary for ws`},
		{"frame on http", "type: http\nurl: http://h\nframe: raw", `applies to ws, tcp and udp destinations only`},
		{"auth on http", "type: http\nurl: http://h\nauth: { bearer: { tokens: [t] } }", `.auth: applies to sse destinations only`},
		{"hmac on sse", "type: sse\nlisten: :8090\nauth: { hmac: { header: X-Sig, secret: s } }", `.auth.hmac: does not apply: a client reading the stream sends no body to sign`},
	} {
		t.Run(c.name, func(t *testing.T) {
			assert.Contains(t, validateYAML(t, stdin, c.dst), c.want)
		})
	}
}

func TestNotInBuild(t *testing.T) {
	require.True(t, FullOnly("kafka"))
	assert.False(t, FullOnly("tcp"))
	assert.ErrorContains(t, NotInBuild("source", "in", "sqs"), "source in: protocol sqs is not in this build: hikyaku-lite")
	assert.EqualError(t, NotInBuild("destination", "out", "tcp"), "destination out: protocol tcp is not in this build")
	assert.True(t, IsBuiltinSource("openai"))
	assert.True(t, IsBuiltinSink("model"))
	assert.False(t, IsBuiltinSink("openai"))
}

func TestBatchAndBufferValidation(t *testing.T) {
	assert.Empty(t, validateYAML(t, stdin, "type: stdout\nbatch: { max_messages: 50, max_bytes: 512KiB, max_wait: 2s, format: json_array }"))
	assert.Contains(t, validateYAML(t, stdin, "type: stdout\nbatch: { format: csv }"), `batch.format: must be ndjson or json_array`)
	assert.Contains(t, validateYAML(t, stdin, "type: stdout\nbatch: { max_messages: -1 }"), `batch.max_messages: must not be negative`)
	assert.Contains(t, validateYAML(t, stdin, "type: group\nmembers: [{ type: stdout }]\nbatch: { format: csv }"), `batch.format: must be ndjson or json_array`, "a group's batch is checked too")
	assert.Contains(t, validateYAML(t, stdin, "type: model\nmodel: ollama/llama3.2\nbatch: { max_messages: 10 }"), "batch: does not apply to a model destination")
	assert.Contains(t, validateYAML(t, stdin, "type: group\nmembers: [{ type: model, model: ollama/llama3.2 }]\nbatch: { max_messages: 10 }"), "batch: does not apply to a model destination")

	doc := func(buffer string) string {
		return "version: 1\nname: relay\n" + buffer + "\nsources: [{ name: in, type: stdin }]\ndestinations: [{ name: out, type: stdout }]\nroutes: [{ name: r, from: [in], to: [out] }]\n"
	}
	_, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte(doc("buffer: { type: memory, path: x.db }")), LoadOptions{})
	assert.ErrorContains(t, err, "path and sync apply to a disk buffer")
	_, err = LoadBytes(context.Background(), "hikyaku.yaml", []byte(doc("buffer: { type: disk, path: /var/lib/hikyaku/buffer.db, sync: false }")), LoadOptions{})
	assert.NoError(t, err)
}

func TestLintAcceptedAndOrderedBatches(t *testing.T) {
	l, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte(`
version: 1
name: relay
sources:
  - { name: orders, type: kafka, brokers: [k:9092], topic: orders, group: g, ack: accepted, ordering: {} }
destinations:
  - { name: lake, type: s3, bucket: b, batch: { max_messages: 500 } }
routes: [{ name: r, from: [orders], to: [lake] }]
`), LoadOptions{})
	require.NoError(t, err)
	var text []string
	for _, w := range Lint(l.Spec) {
		text = append(text, w.Path+": "+w.Msg)
	}
	all := strings.Join(text, "\n")
	assert.Contains(t, all, `source "orders".ack: accepted with a memory buffer`)
	assert.Contains(t, all, `route "r": source orders is ordered and destination lake batches`)
}

// Keys a source type would ignore are refused, not accepted silently.
func TestIgnoredSourceKeysAreRefused(t *testing.T) {
	assert.Empty(t, validateYAML(t, "type: mqtt\nurl: mqtt://a:1883\nfailover_urls: [mqtt://b:1883]\ntopic: t", stdout))
	assert.Contains(t, validateYAML(t, "type: sqs\nqueue_url: https://sqs.eu-west-1.amazonaws.com/1/q\nfailover_urls: [https://b]", stdout),
		"failover_urls: applies to ws, wss, sse, grpc, nats, jetstream, mqtt, amqp and redis subscribe sources only")
	assert.Contains(t, validateYAML(t, "type: gcs\nbucket: b\nretry: { attempts: 5 }", stdout), "retry: does not apply to gcs")
	assert.Empty(t, validateYAML(t, "type: s3\nbucket: b\nretry: { attempts: 5 }\ntimeout: 30s", stdout))
	assert.Empty(t, validateYAML(t, "type: sqs\nqueue_url: https://sqs.eu-west-1.amazonaws.com/1/q\ndedupe: { key: \"{{ .json.id }}\" }", stdout), "dedupe applies to any source")
}

func TestLintFallbackWithItsOwnBatch(t *testing.T) {
	l, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte(`
version: 1
name: relay
sources: [{ name: in, type: stdin }]
destinations:
  - { name: api, type: stdout, fallback: [lake] }
  - { name: lake, type: stdout, batch: { max_messages: 500 } }
  - { name: bulk, type: stdout, batch: { max_messages: 10 }, fallback: [lake] }
routes: [{ name: r, from: [in], to: [api, bulk] }]
`), LoadOptions{})
	require.NoError(t, err)
	var text []string
	for _, w := range Lint(l.Spec) {
		text = append(text, w.Path+": "+w.Msg)
	}
	all := strings.Join(text, "\n")
	assert.Contains(t, all, `destination "api".fallback: lake batches, but as a fallback of api it delivers api's messages one message at a time`)
	assert.Contains(t, all, `destination "bulk".fallback: lake batches, but as a fallback of bulk it delivers bulk's messages in bulk's batches`)
}

func TestListenersNeedTheirOwnPort(t *testing.T) {
	doc := func(sources string) string {
		return "version: 1\nname: relay\nsources:\n" + sources + "destinations: [{ name: out, type: stdout }]\nroutes: [{ name: r, from: [a, b], to: [out] }]\n"
	}
	load := func(sources string) error {
		_, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte(doc(sources)), LoadOptions{})
		return err
	}
	assert.NoError(t, load("  - { name: a, type: http, listen: \":8080\", path: /a }\n  - { name: b, type: http, listen: \":8080\", path: /b }\n"), "http sources share a port by path")
	assert.NoError(t, load("  - { name: a, type: tcp, listen: \":9000\" }\n  - { name: b, type: udp, listen: \":9000\" }\n"), "tcp and udp ports are different ports")
	assert.ErrorContains(t, load("  - { name: a, type: grpc, listen: \":50051\" }\n  - { name: b, type: grpc, listen: \":50051\" }\n"), `source "b".listen: :50051 is also source "a"'s`)
	assert.ErrorContains(t, load("  - { name: a, type: http, listen: \":8080\" }\n  - { name: b, type: tcp, listen: \":8080\" }\n"), "a tcp listener needs a port of its own")
	assert.ErrorContains(t, load("  - { name: a, type: grpc, listen: \":8080\" }\n  - { name: b, type: openai, listen: \":8080\" }\n"), "a grpc listener needs a port of its own")
}

func TestGrpcListenAccepted(t *testing.T) {
	assert.Empty(t, validateYAML(t, "type: grpc\nlisten: :50051\nack: accepted", stdout))
}

func TestPluginBinariesValidation(t *testing.T) {
	doc := func(plugins string) string {
		return "version: 1\nname: relay\nplugins:\n" + plugins + "sources: [{ name: in, type: stdin }]\ndestinations: [{ name: out, type: stdout }]\nroutes: [{ name: r, from: [in], to: [out] }]\n"
	}
	load := func(plugins string) error {
		_, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte(doc(plugins)), LoadOptions{})
		return err
	}
	assert.NoError(t, load("  - { name: crm, command: ./bin/crm, sha256: "+strings.Repeat("ab", 32)+" }\n"))
	assert.ErrorContains(t, load("  - { name: crm, command: a }\n  - { name: crm, command: b }\n"), `plugin "crm".name: is declared twice`)
	assert.ErrorContains(t, load("  - { name: crm, command: a, sha256: xyz }\n"), "must be the 64 hex digits")
	assert.ErrorContains(t, load("  - { name: crm, command: \" \" }\n"), `plugin "crm".command: is required`)
}

func TestLintGatedLastRouter(t *testing.T) {
	lint := func(router string) string {
		l, err := LoadBytes(context.Background(), "hikyaku.yaml", []byte("version: 1\nname: relay\nsources: [{ name: in, type: stdin }]\ndestinations: [{ name: a, type: stdout }, { name: b, type: stdout }]\nroutes:\n  - name: r\n    from: [in]\n    router: "+router+"\n"), LoadOptions{})
		require.NoError(t, err)
		var out []string
		for _, w := range Lint(l.Spec) {
			out = append(out, w.Path+": "+w.Msg)
		}
		return strings.Join(out, "\n")
	}
	assert.Contains(t, lint(`{ type: static, to: [a], when: "has(msg.json.text)" }`), "the messages it leaves out go to the DLQ (reason no_route)")
	assert.NotContains(t, lint(`{ type: static, to: [a], when: "has(msg.json.text)", fallback: [{ type: static, to: [b] }] }`), "leaves out")
}
