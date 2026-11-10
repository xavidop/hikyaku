package aws

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"

	"github.com/xavidop/hikyaku/envelope"
	"github.com/xavidop/hikyaku/plugin"
	"github.com/xavidop/hikyaku/transform"
)

// Limits and defaults of EventBridge events.
const (
	maxDetailType     = 128
	defaultDetailType = "hikyaku.message"
	defaultBus        = "default"
	headerEventID     = "eventbridge-event-id"
)

// eventBridgeSink is an eventbridge destination: PutEvents.
type eventBridgeSink struct {
	name       string
	bus        string // "" is the account's default bus
	source     string
	detailType *transform.Template
	body       *transform.Template
	client     eventBridgePutter
}

func newEventBridgeSink(ctx context.Context, spec plugin.Spec) (*eventBridgeSink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	s, err := parseEventBridgeSink(spec)
	if err != nil {
		return nil, err
	}
	cfg, err := loadAWS(ctx, ep.AWS, regionFromARN(s.bus), sinkAttempts, roleSessionName(spec))
	if err != nil {
		return nil, fail("%v", err)
	}
	s.client = eventbridge.NewFromConfig(cfg)
	return s, nil
}

// parseEventBridgeSink builds an eventbridge destination without its
// client.
func parseEventBridgeSink(spec plugin.Spec) (*eventBridgeSink, error) {
	ep, fail, err := sinkConfig(spec)
	if err != nil {
		return nil, err
	}
	for field, v := range map[string]string{"event_bus": ep.EventBus, "event_source": ep.EventSource} {
		if err := static(field, v); err != nil {
			return nil, fail("%v", err)
		}
	}
	s := &eventBridgeSink{name: spec.Name, bus: ep.EventBus, source: ep.EventSource}
	if s.source == "" {
		s.source = "hikyaku." + relayName(spec)
	}
	if strings.HasPrefix(s.source, "aws.") {
		return nil, fail("event_source %q: the aws. prefix is reserved for AWS services", s.source)
	}
	if len(ep.Headers) > 0 {
		return nil, fail("headers: EventBridge events carry no headers; put what you need in the detail with body:")
	}
	if s.detailType, err = optionalTemplate("detail_type", ep.DetailType); err != nil {
		return nil, fail("%v", err)
	}
	if s.body, err = optionalTemplate("body", ep.Body); err != nil {
		return nil, fail("%v", err)
	}
	return s, nil
}

// Name is the destination name.
func (s *eventBridgeSink) Name() string { return s.name }

// entry is the event of one envelope: source event_source, detail-type the
// detail_type template (else the subject, else hikyaku.message), and the
// body as detail when it is a JSON object, else wrapped as {"body": text}
// or {"body_base64": ...}.
func (s *eventBridgeSink) entry(env *envelope.Envelope) (types.PutEventsRequestEntry, int, error) {
	dt := ""
	if s.detailType != nil {
		v, err := s.detailType.Render(env)
		if err != nil {
			return types.PutEventsRequestEntry{}, 0, templateError(err)
		}
		dt = strings.TrimSpace(v)
	}
	if dt == "" {
		dt = strings.TrimSpace(env.Subject)
	}
	if dt == "" {
		dt = defaultDetailType
	}
	if utf8.RuneCountInString(dt) > maxDetailType {
		dt = string([]rune(dt)[:maxDetailType])
	}
	raw, err := bodyOf(s.body, env)
	if err != nil {
		return types.PutEventsRequestEntry{}, 0, err
	}
	detail, err := detailOf(raw)
	if err != nil {
		return types.PutEventsRequestEntry{}, 0, plugin.Wrap(plugin.Permanent, []string{plugin.Tag4xx}, err)
	}
	e := types.PutEventsRequestEntry{Source: aws.String(s.source), DetailType: aws.String(dt), Detail: aws.String(detail)}
	if s.bus != "" {
		e.EventBusName = aws.String(s.bus)
	}
	return e, len(s.source) + len(dt) + len(detail), nil
}

// detailOf is an event's detail: the body when it is a JSON object, else
// the body wrapped in one.
func detailOf(b []byte) (string, error) {
	if t := bytes.TrimSpace(b); len(t) > 0 && t[0] == '{' && json.Valid(t) {
		return string(t), nil
	}
	var v map[string]string
	if utf8.Valid(b) {
		v = map[string]string{"body": string(b)}
	} else {
		v = map[string]string{"body_base64": base64.StdEncoding.EncodeToString(b)}
	}
	out, err := json.Marshal(v)
	return string(out), err
}

// put sends up to 10 events in one PutEvents and returns their IDs. A
// failed entry fails the call, tagged with its ErrorCode.
func (s *eventBridgeSink) put(ctx context.Context, entries []types.PutEventsRequestEntry) ([]string, error) {
	out, err := s.client.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: entries})
	if err != nil {
		return nil, classify(kindEventBridge, err)
	}
	if out.FailedEntryCount > 0 {
		var failed []entryFailure
		for _, r := range out.Entries {
			if r.ErrorCode != nil {
				failed = append(failed, entryFailure{code: *r.ErrorCode, message: aws.ToString(r.ErrorMessage)})
			}
		}
		if len(failed) == 0 {
			failed = []entryFailure{{code: "InternalFailure", message: "PutEvents reported failed entries without codes"}}
		}
		return nil, batchError(kindEventBridge, failed, len(entries))
	}
	ids := make([]string, len(out.Entries))
	for i, r := range out.Entries {
		ids[i] = aws.ToString(r.EventId)
	}
	return ids, nil
}

// Deliver puts one event.
func (s *eventBridgeSink) Deliver(ctx context.Context, env *envelope.Envelope) (*plugin.Result, error) {
	e, _, err := s.entry(env)
	if err != nil {
		return nil, err
	}
	ids, err := s.put(ctx, []types.PutEventsRequestEntry{e})
	if err != nil {
		return nil, err
	}
	return &plugin.Result{Endpoint: s.endpoint(), Headers: map[string][]string{headerEventID: ids}}, nil
}

// DeliverBatch puts a batch, 10 events and 256KiB per PutEvents. Any failed
// entry fails the batch, and the relay retries all of it.
func (s *eventBridgeSink) DeliverBatch(ctx context.Context, envs []*envelope.Envelope) (*plugin.Result, error) {
	type sized struct {
		e    types.PutEventsRequestEntry
		size int
	}
	entries := make([]sized, len(envs))
	for i, env := range envs {
		e, n, err := s.entry(env)
		if err != nil {
			return nil, err
		}
		entries[i] = sized{e, n}
	}
	var ids []string
	for _, c := range chunk(entries, func(e sized) int { return e.size }, sqsBatchEntries, maxBatchBytes) {
		req := make([]types.PutEventsRequestEntry, len(c))
		for i, e := range c {
			req[i] = e.e
		}
		got, err := s.put(ctx, req)
		if err != nil {
			return nil, err
		}
		ids = append(ids, got...)
	}
	return &plugin.Result{Endpoint: s.endpoint(), Headers: map[string][]string{headerEventID: ids}}, nil
}

func (s *eventBridgeSink) endpoint() string {
	if s.bus == "" {
		return "eventbridge:" + defaultBus
	}
	return "eventbridge:" + s.bus
}

// Health describes the bus: a cheap call that needs the bus to exist and
// the credentials to work.
func (s *eventBridgeSink) Health(ctx context.Context) error {
	name := s.bus
	if name == "" {
		name = defaultBus
	}
	if _, err := s.client.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: aws.String(name)}); err != nil {
		return fmt.Errorf("eventbridge bus %s: %w", name, classify(kindEventBridge, err))
	}
	return nil
}

// Close has nothing to flush: every delivery is one call.
func (s *eventBridgeSink) Close(context.Context) error { return nil }
