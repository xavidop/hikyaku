// Package kafka is hikyaku's Kafka protocol: the kafka source and destination
// types, built on franz-go.
//
// A source is a consumer group member (subscribe mode). It hands each record
// to the relay as one envelope and commits offsets itself, in the background
// (every second, when partitions are revoked or lost, and at Close), never
// per record. A partition's committed offset only moves past a contiguous
// prefix of records whose outcome is final: delivered, dead-lettered,
// unrouted, filtered, or skipped for max_body_bytes. Records finish out of
// order; the commit waits for the earliest one still in flight. A record
// that fails (the dead-letter queue failed too, or shutdown cut its delivery)
// holds its partition's commit: the partition resumes from that record on the
// next restart or rebalance, and every record after it is delivered again
// then (at least once). With ack: accepted a record is final as soon as the
// relay holds it.
//
// Envelopes carry the record's headers, its topic as the subject, and the
// attributes kafka.topic, kafka.partition, kafka.offset, kafka.key,
// kafka.timestamp and ordering_key (<topic>/<partition>). The ID is the
// record's Idempotency-Key header, else <topic>-<partition>-<offset>, which
// is stable across redeliveries.
//
// A destination produces each message as one record with the idempotent
// producer (acks: all) unless acks says otherwise. Every record carries an
// Idempotency-Key header, so consumers can drop the duplicates a retry
// produces. It implements plugin.BatchSink: a batch is one produce call.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/xavidop/hikyaku/plugin"
)

// Kind is the protocol type of the source and the destination.
const Kind = "kafka"

// Register makes the kafka source and destination types available.
func Register() {
	plugin.RegisterSource(Kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		return newSource(ctx, spec)
	})
	plugin.RegisterSink(Kind, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		return newSink(ctx, spec)
	})
}

// Header names a record carries.
const (
	headerIdempotency = "Idempotency-Key"
	headerContentType = "content-type"
)

// Content types of received records without a content-type header.
const (
	contentJSON   = "application/json"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// maxIdempotencyKey is the longest Idempotency-Key header a source takes as
// the envelope ID, as the http source does.
const maxIdempotencyKey = 256

// tp is a topic partition.
type tp struct {
	topic     string
	partition int32
}

func (t tp) String() string { return t.topic + "/" + strconv.Itoa(int(t.partition)) }

// tps lists the partitions of a franz-go partition map.
func tps(m map[string][]int32) []tp {
	var out []tp
	for topic, parts := range m {
		for _, p := range parts {
			out = append(out, tp{topic, p})
		}
	}
	slices.SortFunc(out, func(a, b tp) int {
		if c := strings.Compare(a.topic, b.topic); c != 0 {
			return c
		}
		return int(a.partition) - int(b.partition)
	})
	return out
}

// describe renders partitions for logs: orders/0 orders/1.
func describe(parts []tp) string {
	s := make([]string, len(parts))
	for i, p := range parts {
		s[i] = p.String()
	}
	return strings.Join(s, " ")
}

// header returns the first value of a record header, matched
// case-insensitively.
func header(headers []kgo.RecordHeader, name string) (string, bool) {
	for _, h := range headers {
		if strings.EqualFold(h.Key, name) {
			return string(h.Value), true
		}
	}
	return "", false
}

// detectContentType is the content type of a record without a content-type
// header: JSON when the value parses as JSON, text when it is UTF-8, else
// binary. An empty value (a tombstone) has none.
func detectContentType(b []byte) string {
	switch {
	case len(b) == 0:
		return ""
	case json.Valid(b):
		return contentJSON
	case utf8.Valid(b):
		return contentText
	}
	return contentBinary
}

// clientID is the Kafka client.id: client_id, else hikyaku-<relay>-<name>.
func clientID(configured, relay, name string) string {
	if configured != "" {
		return configured
	}
	if relay == "" {
		return "hikyaku-" + name
	}
	return "hikyaku-" + relay + "-" + name
}

// logger adapts slog to franz-go's logger. The client's info and debug
// lines go out at debug, and only when debug is on: they are chatty.
type logger struct{ log *slog.Logger }

func (l logger) Level() kgo.LogLevel {
	if l.log.Enabled(context.Background(), slog.LevelDebug) {
		return kgo.LogLevelInfo
	}
	return kgo.LogLevelWarn
}

func (l logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	msg = "kafka client: " + msg
	switch level {
	case kgo.LogLevelError:
		l.log.Error(msg, keyvals...)
	case kgo.LogLevelWarn:
		l.log.Warn(msg, keyvals...)
	default:
		l.log.Debug(msg, keyvals...)
	}
}

// fail formats a configuration error of an entry: "source in: ...".
func fail(kind, name, format string, args ...any) error {
	return fmt.Errorf("%s %s: %s", kind, name, fmt.Sprintf(format, args...))
}
