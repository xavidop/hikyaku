package aws

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/plugin"
)

func TestClassifyAPIErrors(t *testing.T) {
	for _, c := range []struct {
		code   string
		status int
		fault  smithy.ErrorFault
		class  plugin.Class
		tags   []string // besides aws:<code> and sqs:<code>
	}{
		// Throttles are rate limited (429), whatever the status says.
		{"Throttling", 400, smithy.FaultClient, plugin.RateLimited, []string{"429"}},
		{"ThrottlingException", 400, smithy.FaultClient, plugin.RateLimited, []string{"429"}},
		{"RequestThrottled", 403, smithy.FaultClient, plugin.RateLimited, []string{"429"}},
		{"SlowDown", 503, smithy.FaultServer, plugin.RateLimited, []string{"429"}},
		{"TooManyRequestsException", 429, smithy.FaultClient, plugin.RateLimited, []string{"429"}},
		{"KMSThrottlingException", 400, smithy.FaultClient, plugin.RateLimited, []string{"429"}},
		// Missing resources, refusals and bad credentials are permanent (4xx).
		{"AWS.SimpleQueueService.NonExistentQueue", 400, smithy.FaultClient, plugin.Permanent, []string{"400", "4xx"}},
		{"NonExistentQueue", 400, smithy.FaultClient, plugin.Permanent, []string{"400", "4xx"}},
		{"QueueDoesNotExist", 400, smithy.FaultClient, plugin.Permanent, []string{"400", "4xx"}},
		{"NotFound", 404, smithy.FaultClient, plugin.Permanent, []string{"404", "4xx"}},
		{"NoSuchBucket", 404, smithy.FaultClient, plugin.Permanent, []string{"404", "4xx"}},
		{"AccessDenied", 403, smithy.FaultClient, plugin.Permanent, []string{"403", "4xx"}},
		{"InvalidParameterValue", 400, smithy.FaultClient, plugin.Permanent, []string{"400", "4xx"}},
		{"InvalidMessageContents", 400, smithy.FaultClient, plugin.Permanent, []string{"400", "4xx"}},
		{"ValidationException", 400, smithy.FaultClient, plugin.Permanent, []string{"400", "4xx"}},
		{"AuthorizationError", 403, smithy.FaultClient, plugin.Permanent, []string{"403", "4xx"}},
		// AWS failing is transient (5xx), with its status when it has one.
		{"InternalError", 500, smithy.FaultServer, plugin.Transient, []string{"500", "5xx"}},
		{"ServiceUnavailable", 503, smithy.FaultServer, plugin.Transient, []string{"503", "5xx"}},
		{"ExpiredToken", 400, smithy.FaultClient, plugin.Transient, []string{"5xx"}},
		// Unknown codes follow their status, then who is at fault.
		{"Mystery", 500, smithy.FaultUnknown, plugin.Transient, []string{"500", "5xx"}},
		{"Mystery", 429, smithy.FaultUnknown, plugin.RateLimited, []string{"429"}},
		{"Mystery", 504, smithy.FaultUnknown, plugin.Timeout, []string{"504", "timeout"}},
		{"Mystery", 408, smithy.FaultUnknown, plugin.Timeout, []string{"408", "timeout"}},
		{"Mystery", 409, smithy.FaultUnknown, plugin.Permanent, []string{"409", "4xx"}},
		{"Mystery", 0, smithy.FaultClient, plugin.Permanent, []string{"4xx"}},
		{"Mystery", 0, smithy.FaultServer, plugin.Transient, []string{"5xx"}},
	} {
		name := fmt.Sprintf("%s/%d", c.code, c.status)
		err := apiError(c.code, c.status, c.fault)
		if c.status == 0 {
			err = &smithy.GenericAPIError{Code: c.code, Fault: c.fault}
		}
		got := classify("sqs", err)
		require.True(t, isPluginError(got), name)
		class, tags := errorTags(got)
		assert.Equal(t, c.class, class, name)
		assert.Equal(t, append([]string{"aws:" + c.code, "sqs:" + c.code}, c.tags...), tags, name)
		assert.ErrorIs(t, got, err, name)
	}
}

func TestClassifyTransportErrors(t *testing.T) {
	refused := &smithyhttp.RequestSendError{Err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}}
	class, tags := errorTags(classify("s3", refused))
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{plugin.TagConnect}, tags)

	class, tags = errorTags(classify("s3", fmt.Errorf("get: %w", syscall.ECONNRESET)))
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{plugin.TagConnect}, tags)

	deadline := &smithyhttp.RequestSendError{Err: context.DeadlineExceeded}
	class, tags = errorTags(classify("sqs", deadline))
	assert.Equal(t, plugin.Timeout, class)
	assert.Equal(t, []string{plugin.TagTimeout}, tags)

	class, tags = errorTags(classify("sqs", fmt.Errorf("op: %w", context.Canceled)))
	assert.Equal(t, plugin.Transient, class)
	assert.Empty(t, tags, "shutdown is not the endpoint's fault")

	class, tags = errorTags(classify("sqs", smithy.InvalidParamsError{Context: "SendMessageInput"}))
	assert.Equal(t, plugin.Permanent, class)
	assert.Equal(t, []string{plugin.Tag4xx}, tags)

	class, tags = errorTags(classify("sqs", errors.New("something else")))
	assert.Equal(t, plugin.Transient, class)
	assert.Empty(t, tags)

	assert.NoError(t, classify("sqs", nil))
	already := plugin.Permanentf("mine")
	assert.Same(t, already, classify("sqs", already))
}

func TestBatchErrorIsTheWorstEntry(t *testing.T) {
	err := batchError("sqs", []entryFailure{
		{code: "InternalError", message: "boom"},
		{code: "InvalidMessageContents", message: "bad", sender: true},
	}, 10)
	class, tags := errorTags(err)
	assert.Equal(t, plugin.Permanent, class, "retrying cannot fix a refused entry")
	assert.Equal(t, []string{"aws:InternalError", "sqs:InternalError", "aws:InvalidMessageContents",
		"sqs:InvalidMessageContents", "4xx"}, tags)
	assert.Contains(t, err.Error(), "2 of 10 entries failed")

	err = batchError("sns", []entryFailure{{code: "Throttling"}, {code: "InternalError"}}, 2)
	class, tags = errorTags(err)
	assert.Equal(t, plugin.RateLimited, class)
	assert.Contains(t, tags, "429")
	assert.NotContains(t, tags, "5xx", "only the batch's class tag")

	err = batchError("eventbridge", []entryFailure{{code: "InternalFailure"}}, 1)
	class, tags = errorTags(err)
	assert.Equal(t, plugin.Transient, class)
	assert.Equal(t, []string{"aws:InternalFailure", "eventbridge:InternalFailure", "5xx"}, tags)
}
