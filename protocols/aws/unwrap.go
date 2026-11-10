package aws

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/xavidop/hikyaku/envelope"
)

// snsNotification is the JSON SNS writes to a subscribed SQS queue, unless
// the subscription has raw message delivery.
type snsNotification struct {
	Type              string                  `json:"Type"`
	MessageID         string                  `json:"MessageId"`
	TopicArn          string                  `json:"TopicArn"`
	Subject           *string                 `json:"Subject"`
	Message           *string                 `json:"Message"`
	Timestamp         string                  `json:"Timestamp"`
	MessageAttributes map[string]snsAttribute `json:"MessageAttributes"`
}

// snsAttribute is a message attribute in an SNS notification.
type snsAttribute struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// SNS notification types.
const (
	snsTypeNotification = "Notification"
	snsTypeSubscribe    = "SubscriptionConfirmation"
	snsTypeUnsubscribe  = "UnsubscribeConfirmation"
)

// unwrapSNS turns an SNS notification into the message published: its
// Message is the body, its Subject the subject, its MessageId (or an
// Idempotency-Key attribute) the ID, and its String and Number attributes
// headers. A subscription confirmation is logged and dropped. A body that
// is no notification (raw message delivery) stays as SQS gave it.
func unwrapSNS(s *queueSource, env *envelope.Envelope) bool {
	var n snsNotification
	if !looksLikeObject(env.Body) || json.Unmarshal(env.Body, &n) != nil || n.TopicArn == "" || n.MessageID == "" {
		return true
	}
	switch n.Type {
	case snsTypeNotification:
	case snsTypeSubscribe, snsTypeUnsubscribe:
		s.log.Info("dropping an SNS "+n.Type+" message; the subscription is managed in SNS", "source", s.name,
			"topic_arn", n.TopicArn, "sns_message_id", n.MessageID)
		return false
	default:
		return true
	}
	if n.Message == nil {
		return true
	}
	body := []byte(*n.Message)
	attr := func(name string) string {
		for k, v := range n.MessageAttributes {
			if strings.EqualFold(k, name) {
				return v.Value
			}
		}
		return ""
	}
	if strings.EqualFold(attr(attrTransferEncoder), encodingBase64) {
		if dec, err := base64.StdEncoding.DecodeString(*n.Message); err == nil {
			body = dec
		} else {
			s.log.Warn("SNS message says base64 but is not; passing it on as it is", "source", s.name,
				"sns_message_id", n.MessageID, "err", err)
		}
	}
	env.SetBody(body)
	env.ID = n.MessageID
	if key := attr(attrIdempotencyKey); key != "" {
		env.ID = key
	}
	env.Subject = topicName(n.TopicArn)
	if n.Subject != nil && *n.Subject != "" {
		env.Subject = *n.Subject
	}
	for k, v := range n.MessageAttributes {
		if !reservedAttr(k) && textType(v.Type) {
			env.Headers[k] = []string{v.Value}
		}
	}
	env.ContentType = attr(attrContentType)
	if env.ContentType == "" {
		env.ContentType = detectContentType(body)
	}
	env.Attributes["sns.topic_arn"] = n.TopicArn
	env.Attributes["sns.message_id"] = n.MessageID
	return true
}

// topicName is the last part of a topic ARN.
func topicName(arn string) string {
	return arn[strings.LastIndex(arn, ":")+1:]
}

// eventBridgeEvent is the JSON EventBridge sends a rule's SQS target,
// unless the target has an input transformer.
type eventBridgeEvent struct {
	ID         string          `json:"id"`
	DetailType *string         `json:"detail-type"`
	Source     *string         `json:"source"`
	Account    string          `json:"account"`
	Time       string          `json:"time"`
	Region     string          `json:"region"`
	Resources  []string        `json:"resources"`
	Detail     json.RawMessage `json:"detail"`
}

// unwrapEventBridge turns an EventBridge event into its detail: the detail
// JSON is the body, detail-type the subject, id the ID, and source, time,
// account and region attributes. A body that is no event stays as SQS gave
// it.
func unwrapEventBridge(_ *queueSource, env *envelope.Envelope) bool {
	var e eventBridgeEvent
	if !looksLikeObject(env.Body) || json.Unmarshal(env.Body, &e) != nil ||
		e.ID == "" || e.DetailType == nil || e.Source == nil || len(e.Detail) == 0 {
		return true
	}
	env.SetBody([]byte(e.Detail))
	env.ContentType = contentJSON
	env.ID = e.ID
	env.Subject = *e.DetailType
	env.Attributes["eventbridge.id"] = e.ID
	env.Attributes["eventbridge.source"] = *e.Source
	env.Attributes["eventbridge.time"] = e.Time
	env.Attributes["eventbridge.account"] = e.Account
	env.Attributes["eventbridge.region"] = e.Region
	if len(e.Resources) > 0 {
		env.Attributes["eventbridge.resources"] = e.Resources
	}
	return true
}

// looksLikeObject is a cheap test before parsing a body as a JSON object.
func looksLikeObject(b []byte) bool {
	b = bytes.TrimSpace(b)
	return len(b) >= 2 && b[0] == '{' && b[len(b)-1] == '}'
}
