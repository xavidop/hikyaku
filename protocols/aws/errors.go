package aws

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"syscall"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/xavidop/hikyaku/plugin"
)

// The AWS error codes with a fixed class. Codes not listed are classified
// by their HTTP status: 429 rate limited, 408 and 504 timeouts, other 5xx
// transient, other 4xx permanent; and without one by who is at fault.
var (
	// throttlingCodes are AWS asking the caller to slow down.
	throttlingCodes = codeSet(
		"Throttling", "ThrottlingException", "ThrottledException", "RequestThrottled",
		"RequestThrottledException", "SlowDown", "TooManyRequestsException",
		"KMSThrottlingException", "KMSThrottling", "KmsThrottled",
		"ProvisionedThroughputExceededException", "RequestLimitExceeded",
		"BandwidthLimitExceeded", "OverLimit", "AWS.SimpleQueueService.OverLimit",
	)
	// permanentCodes will fail the same way again: a missing queue, topic,
	// bus or bucket, a refused request, bad credentials.
	permanentCodes = codeSet(
		// sqs
		"NonExistentQueue", "AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist",
		"QueueDeletedRecently", "AWS.SimpleQueueService.QueueDeletedRecently",
		"InvalidMessageContents", "UnsupportedOperation", "AWS.SimpleQueueService.UnsupportedOperation",
		"InvalidAddress", "InvalidAttributeName", "InvalidAttributeValue",
		"InvalidBatchEntryId", "AWS.SimpleQueueService.InvalidBatchEntryId",
		"BatchEntryIdsNotDistinct", "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
		"BatchRequestTooLong", "AWS.SimpleQueueService.BatchRequestTooLong",
		"EmptyBatchRequest", "AWS.SimpleQueueService.EmptyBatchRequest",
		"TooManyEntriesInBatchRequest", "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
		"KmsAccessDenied", "KmsDisabled", "KmsInvalidKeyUsage", "KmsInvalidState", "KmsNotFound",
		"KmsOptInRequired",
		// sns
		"NotFound", "NotFoundException", "AuthorizationError", "InvalidParameter",
		"InvalidParameterValue", "InvalidParameterException", "EndpointDisabled",
		"KMSAccessDenied", "KMSDisabled", "KMSInvalidState", "KMSNotFound", "KMSOptInRequired",
		// eventbridge
		"ResourceNotFoundException", "ValidationException", "MalformedDetail", "InvalidArgument",
		"NotAuthorizedForSourceException", "NotAuthorizedForDetailTypeException",
		// s3
		"NoSuchBucket", "NoSuchKey", "AccessDenied", "Forbidden", "InvalidBucketName",
		"InvalidObjectState", "EntityTooLarge", "KeyTooLongError", "MetadataTooLarge",
		"InvalidRequest", "MalformedXML", "MethodNotAllowed",
		// every service: credentials, signing and request validation
		"AccessDeniedException", "InvalidClientTokenId", "UnrecognizedClientException",
		"SignatureDoesNotMatch", "InvalidAccessKeyId", "MissingAuthenticationToken",
		"IncompleteSignature", "InvalidSignatureException", "InvalidSecurity", "OptInRequired",
		"UnauthorizedOperation", "MissingParameter", "InvalidAction", "InvalidQueryParameter",
		"MalformedQueryString", "ValidationError",
	)
	// transientCodes may not happen again: AWS failed, or the request or its
	// credentials went stale.
	transientCodes = codeSet(
		"InternalError", "InternalFailure", "InternalException", "InternalServerError",
		"InternalServiceError", "ServiceUnavailable", "ServiceUnavailableException",
		"RequestTimeout", "RequestTimeoutException", "ExpiredToken", "ExpiredTokenException",
		"RequestExpired", "RequestTimeTooSkewed", "IDPCommunicationError", "KMSInternalException",
		"KMSInternal",
	)
)

func codeSet(codes ...string) map[string]bool {
	m := make(map[string]bool, len(codes))
	for _, c := range codes {
		m[c] = true
	}
	return m
}

// classOf is the class of an AWS error code, with the response status (0
// when there was none) and the side at fault as tie breakers.
func classOf(code string, status int, fault smithy.ErrorFault) plugin.Class {
	switch {
	case throttlingCodes[code]:
		return plugin.RateLimited
	case permanentCodes[code]:
		return plugin.Permanent
	case transientCodes[code]:
		return plugin.Transient
	case status == 429:
		return plugin.RateLimited
	case status == 408 || status == 504:
		return plugin.Timeout
	case status >= 500:
		return plugin.Transient
	case status >= 400:
		return plugin.Permanent
	case fault == smithy.FaultClient:
		return plugin.Permanent
	}
	return plugin.Transient
}

// codeTags are the tags of an AWS error: aws:<Code> and <service>:<Code>,
// then the tag of its class that the default retry policy reads: 429 for a
// throttle, 4xx for a permanent failure, 5xx for a transient one (with the
// status when the response had one), timeout for a timeout.
func codeTags(service, code string, class plugin.Class, status int) []string {
	var tags []string
	if code != "" {
		tags = append(tags, "aws:"+code, service+":"+code)
	}
	switch class {
	case plugin.RateLimited:
		tags = append(tags, plugin.Tag429)
	case plugin.Permanent:
		if status >= 400 && status < 500 {
			tags = append(tags, strconv.Itoa(status))
		}
		tags = append(tags, plugin.Tag4xx)
	case plugin.Transient:
		if status >= 500 {
			tags = append(tags, strconv.Itoa(status))
		}
		tags = append(tags, plugin.Tag5xx)
	case plugin.Timeout:
		if status > 0 {
			tags = append(tags, strconv.Itoa(status))
		}
		tags = append(tags, plugin.TagTimeout)
	}
	return tags
}

// classify turns an error of an AWS call into a *plugin.Error for the retry
// policy. service is sqs, sns, eventbridge, s3 or sts.
func classify(service string, err error) error {
	if err == nil {
		return nil
	}
	var pe *plugin.Error
	if errors.As(err, &pe) {
		return err
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	if errors.Is(err, context.Canceled) {
		// Shutdown: the engine does not count it as the endpoint's failure.
		return plugin.Wrap(plugin.Transient, nil, err)
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code, status := ae.ErrorCode(), httpStatus(err)
		class := classOf(code, status, ae.ErrorFault())
		return &plugin.Error{Class: class, Tags: codeTags(service, code, class, status), Err: err}
	}
	var ipe smithy.InvalidParamsError
	var se *smithy.SerializationError
	if errors.As(err, &ipe) || errors.As(err, &se) {
		return plugin.Wrap(plugin.Permanent, []string{plugin.Tag4xx}, err)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return plugin.Wrap(plugin.Timeout, []string{plugin.TagTimeout}, err)
	}
	var rse *smithyhttp.RequestSendError
	if errors.As(err, &rse) || isNetwork(err) {
		return plugin.Wrap(plugin.Transient, []string{plugin.TagConnect}, err)
	}
	return plugin.Wrap(plugin.Transient, nil, err)
}

// httpStatus is the status of the response an error came with, 0 when the
// request got no response.
func httpStatus(err error) int {
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		return re.HTTPStatusCode()
	}
	return 0
}

// isNetwork reports a failure to reach AWS at all.
func isNetwork(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) {
		return true
	}
	var oe *net.OpError
	var de *net.DNSError
	return errors.As(err, &oe) || errors.As(err, &de)
}

// entryFailure is one failed entry of a batch call (SendMessageBatch,
// PublishBatch, PutEvents).
type entryFailure struct {
	code    string
	message string
	// sender: the entry was refused for what it holds (SenderFault), not
	// because AWS failed.
	sender bool
}

// batchError is the error of a batch call some entries of which failed. A
// batch fails as a whole, so its class is the worst of the entries' (a
// permanent one first: retrying cannot help). It carries the code tags of
// every failed entry and the class tag of that worst class only, so the
// retry policy decides on the batch as a whole.
func batchError(service string, failed []entryFailure, total int) error {
	class := plugin.Transient
	rank := func(c plugin.Class) int {
		switch c {
		case plugin.Permanent:
			return 3
		case plugin.RateLimited:
			return 2
		case plugin.Timeout:
			return 1
		}
		return 0
	}
	var tags, reasons []string
	for i, f := range failed {
		fault := smithy.FaultServer
		if f.sender {
			fault = smithy.FaultClient
		}
		if c := classOf(f.code, 0, fault); i == 0 || rank(c) > rank(class) {
			class = c
		}
		for _, t := range codeTags(service, f.code, -1, 0) {
			if !slices.Contains(tags, t) {
				tags = append(tags, t)
			}
		}
		if len(reasons) < 3 {
			reasons = append(reasons, strings.TrimSpace(f.code+" "+f.message))
		}
	}
	tags = append(tags, codeTags(service, "", class, 0)...)
	return &plugin.Error{Class: class, Tags: tags, Err: fmt.Errorf("%s: %d of %d entries failed: %s",
		service, len(failed), total, strings.Join(reasons, "; "))}
}
