package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// The calls each part makes, so tests can put a fake in place of a service
// client. *sqs.Client, *sns.Client, *eventbridge.Client and *s3.Client
// implement them.

// sqsReceiver is what a queue source calls.
type sqsReceiver interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, opts ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, opts ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// sqsSender is what an sqs destination calls.
type sqsSender interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput, opts ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	SendMessageBatch(ctx context.Context, in *sqs.SendMessageBatchInput, opts ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	GetQueueAttributes(ctx context.Context, in *sqs.GetQueueAttributesInput, opts ...func(*sqs.Options)) (*sqs.GetQueueAttributesOutput, error)
}

// snsPublisher is what an sns destination calls.
type snsPublisher interface {
	Publish(ctx context.Context, in *sns.PublishInput, opts ...func(*sns.Options)) (*sns.PublishOutput, error)
	PublishBatch(ctx context.Context, in *sns.PublishBatchInput, opts ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
	GetTopicAttributes(ctx context.Context, in *sns.GetTopicAttributesInput, opts ...func(*sns.Options)) (*sns.GetTopicAttributesOutput, error)
}

// eventBridgePutter is what an eventbridge destination calls.
type eventBridgePutter interface {
	PutEvents(ctx context.Context, in *eventbridge.PutEventsInput, opts ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
	DescribeEventBus(ctx context.Context, in *eventbridge.DescribeEventBusInput, opts ...func(*eventbridge.Options)) (*eventbridge.DescribeEventBusOutput, error)
}

// s3API is what the s3 source and destination call.
type s3API interface {
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, opts ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	CopyObject(ctx context.Context, in *s3.CopyObjectInput, opts ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, opts ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput, opts ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput, opts ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Compile-time checks that the SDK clients fit.
var (
	_ sqsReceiver       = (*sqs.Client)(nil)
	_ sqsSender         = (*sqs.Client)(nil)
	_ snsPublisher      = (*sns.Client)(nil)
	_ eventBridgePutter = (*eventbridge.Client)(nil)
	_ s3API             = (*s3.Client)(nil)
)

// newS3Client is the S3 client of a configuration: path-style addressing
// against a custom endpoint (LocalStack, MinIO), which rarely serves
// virtual-hosted buckets, and checksums only where the API requires them,
// which every S3-compatible server supports.
func newS3Client(cfg aws.Config, customEndpoint bool) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if customEndpoint {
			o.UsePathStyle = true
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}
	})
}
