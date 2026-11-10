// Package aws is hikyaku's AWS protocol, on aws-sdk-go-v2: the sqs, sns
// and eventbridge source and destination types, and the s3 poll source and
// destination.
//
// Sources. An sqs source long-polls its queue (ReceiveMessage). The sns and
// eventbridge sources read the same way from the SQS queue subscribed to the
// topic, or the rule's SQS target, and unwrap the SNS notification or the
// EventBridge event; anything else passes through as a plain SQS message
// (raw message delivery, an input transformer). A message is deleted
// (DeleteMessageBatch, every 100ms or 10 messages) once the relay is done
// with it: delivered, dead-lettered, unrouted or filtered. A message the
// relay failed (the DLQ failed too, or shutdown cut the delivery) is made
// visible again after a backoff on its receive count, so SQS redelivers it.
// While a message is in flight longer than half the visibility timeout, its
// visibility is extended, so SQS does not redeliver it under the relay.
//
// An s3 source lists its bucket every interval and turns each object, or
// each of its lines (split: lines), into messages. Once every message of an
// object is final the object is deleted, moved under move_to, or remembered
// in a store (after: delete, move or keep). An object with a failed message
// is left for the next poll.
//
// Destinations. sqs sends (SendMessage, SendMessageBatch), sns publishes
// (Publish, PublishBatch), eventbridge puts events (PutEvents) and s3 writes
// one object per message (PutObject), or one NDJSON object per batch. FIFO
// queues and topics get a message group and the envelope ID as the
// deduplication ID. Errors carry the AWS error code as aws:<Code> and
// <service>:<Code> tags.
//
// Attributes: sqs.message_id, sqs.receive_count, sqs.sent_timestamp,
// sqs.group_id; sns.topic_arn, sns.message_id; eventbridge.source,
// eventbridge.id, eventbridge.time, eventbridge.account, eventbridge.region;
// s3.bucket, s3.key, s3.etag, s3.size, s3.line. ordering_key is the SQS
// message group of a FIFO queue, or the S3 key.
package aws

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/xavidop/hikyaku/plugin"
)

// The kinds this package registers.
const (
	kindSQS         = "sqs"
	kindSNS         = "sns"
	kindEventBridge = "eventbridge"
	kindS3          = "s3"
)

// Register makes the sqs, sns, eventbridge and s3 source and destination
// types available.
func Register() {
	for _, kind := range []string{kindSQS, kindSNS, kindEventBridge} {
		plugin.RegisterSource(kind, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
			s, err := newQueueSource(ctx, spec)
			if err != nil {
				return nil, err
			}
			return s, nil
		})
	}
	plugin.RegisterSource(kindS3, func(ctx context.Context, spec plugin.Spec) (plugin.Source, error) {
		s, err := newS3Source(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
	plugin.RegisterSink(kindSQS, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		s, err := newSQSSink(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
	plugin.RegisterSink(kindSNS, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		s, err := newSNSSink(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
	plugin.RegisterSink(kindEventBridge, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		s, err := newEventBridgeSink(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
	plugin.RegisterSink(kindS3, func(ctx context.Context, spec plugin.Spec) (plugin.Sink, error) {
		s, err := newS3Sink(ctx, spec)
		if err != nil {
			return nil, err
		}
		return s, nil
	})
}

// Content types.
const (
	contentJSON   = "application/json"
	contentNDJSON = "application/x-ndjson"
	contentText   = "text/plain; charset=utf-8"
	contentBinary = "application/octet-stream"
)

// Message attributes (and S3 metadata) the relay reads and writes itself.
const (
	attrContentType     = "content-type"
	attrTransferEncoder = "content-transfer-encoding"
	attrIdempotencyKey  = "Idempotency-Key"
	encodingBase64      = "base64"
)

// detectContentType names a body that came without a content type: JSON
// when it parses as JSON, text when it is UTF-8 without control characters
// (tabs and line breaks aside), else binary.
func detectContentType(b []byte) string {
	switch {
	case len(b) == 0:
		return ""
	case json.Valid(b):
		return contentJSON
	case textSafe(b):
		return contentText
	}
	return contentBinary
}

// relayName is the relay name used in defaults (the S3 key prefix, the
// EventBridge source), "hikyaku" when the spec has none.
func relayName(spec plugin.Spec) string {
	if r := strings.TrimSpace(spec.Env.Relay); r != "" {
		return r
	}
	return "hikyaku"
}
