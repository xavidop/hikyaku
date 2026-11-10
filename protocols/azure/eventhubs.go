package azure

import (
	"context"
	"errors"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
)

// The Event Hubs client surface the source and destination use, small so
// tests can fake it.

// ehConsumer reads an event hub in a consumer group.
type ehConsumer interface {
	GetEventHubProperties(ctx context.Context, o *azeventhubs.GetEventHubPropertiesOptions) (azeventhubs.EventHubProperties, error)
	GetPartitionProperties(ctx context.Context, partitionID string, o *azeventhubs.GetPartitionPropertiesOptions) (azeventhubs.PartitionProperties, error)
	newPartitionClient(partitionID string, o *azeventhubs.PartitionClientOptions) (ehPartition, error)
	Close(ctx context.Context) error
}

// ehPartition receives the events of one partition.
type ehPartition interface {
	ReceiveEvents(ctx context.Context, count int, o *azeventhubs.ReceiveEventsOptions) ([]*azeventhubs.ReceivedEventData, error)
	Close(ctx context.Context) error
}

// ehProducer sends to an event hub.
type ehProducer interface {
	GetEventHubProperties(ctx context.Context, o *azeventhubs.GetEventHubPropertiesOptions) (azeventhubs.EventHubProperties, error)
	newBatch(ctx context.Context, o *azeventhubs.EventDataBatchOptions) (ehBatch, error)
	sendBatch(ctx context.Context, b ehBatch) error
	Close(ctx context.Context) error
}

// ehBatch is an event batch being filled.
type ehBatch interface {
	// AddEventData fails with azeventhubs.ErrEventDataTooLarge when the
	// event does not fit.
	AddEventData(e *azeventhubs.EventData, o *azeventhubs.AddEventDataOptions) error
	NumEvents() int32
}

// dialConsumer opens a consumer client.
func dialConsumer(c conn, hub, group string, retry azeventhubs.RetryOptions) (ehConsumer, error) {
	opts := &azeventhubs.ConsumerClientOptions{TLSConfig: c.tls, RetryOptions: retry, ApplicationID: "hikyaku"}
	var (
		cc  *azeventhubs.ConsumerClient
		err error
	)
	if c.connectionString != "" {
		// With an EntityPath, the SDK takes the hub from the connection
		// string and wants an empty one here.
		h := hub
		if c.entityPath != "" {
			h = ""
		}
		cc, err = azeventhubs.NewConsumerClientFromConnectionString(c.connectionString, h, group, opts)
	} else {
		cc, err = azeventhubs.NewConsumerClient(c.namespace, hub, group, c.credential, opts)
	}
	if err != nil {
		return nil, err
	}
	return ehConsumerSDK{cc}, nil
}

type ehConsumerSDK struct{ *azeventhubs.ConsumerClient }

func (c ehConsumerSDK) newPartitionClient(id string, o *azeventhubs.PartitionClientOptions) (ehPartition, error) {
	pc, err := c.NewPartitionClient(id, o)
	if err != nil {
		return nil, err
	}
	return pc, nil
}

// dialProducer opens a producer client.
func dialProducer(c conn, hub string, retry azeventhubs.RetryOptions) (ehProducer, error) {
	opts := &azeventhubs.ProducerClientOptions{TLSConfig: c.tls, RetryOptions: retry, ApplicationID: "hikyaku"}
	var (
		pc  *azeventhubs.ProducerClient
		err error
	)
	if c.connectionString != "" {
		h := hub
		if c.entityPath != "" {
			h = ""
		}
		pc, err = azeventhubs.NewProducerClientFromConnectionString(c.connectionString, h, opts)
	} else {
		pc, err = azeventhubs.NewProducerClient(c.namespace, hub, c.credential, opts)
	}
	if err != nil {
		return nil, err
	}
	return ehProducerSDK{pc}, nil
}

type ehProducerSDK struct{ *azeventhubs.ProducerClient }

func (p ehProducerSDK) newBatch(ctx context.Context, o *azeventhubs.EventDataBatchOptions) (ehBatch, error) {
	b, err := p.NewEventDataBatch(ctx, o)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (p ehProducerSDK) sendBatch(ctx context.Context, b ehBatch) error {
	eb, ok := b.(*azeventhubs.EventDataBatch)
	if !ok {
		return errors.New("eventhubs: not an SDK batch")
	}
	return p.SendEventDataBatch(ctx, eb, nil)
}
