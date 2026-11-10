package pluginpb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// The method names are the protocol: a plugin in another language serves
// exactly these.
func TestServiceMethods(t *testing.T) {
	sd := File_hikyaku_plugin_v1_plugin_proto.Services().ByName("Plugin")
	require.NotNil(t, sd)
	assert.Equal(t, "hikyaku.plugin.v1.Plugin", string(sd.FullName()))
	var names []string
	for i := range sd.Methods().Len() {
		names = append(names, string(sd.Methods().Get(i).Name()))
	}
	assert.Equal(t, []string{
		"Describe", "OpenSink", "Deliver", "DeliverBatch", "SinkHealth", "CloseSink",
		"OpenSource", "StartSource", "Settle", "SourceHealth", "CloseSource",
	}, names)
	assert.True(t, sd.Methods().ByName("StartSource").IsStreamingServer())
	assert.Equal(t, "/hikyaku.plugin.v1.Plugin/Deliver", Plugin_Deliver_FullMethodName)
}

func TestEnvelopeWire(t *testing.T) {
	in := &Envelope{
		Id:             "01J",
		Headers:        map[string]*HeaderValues{"A": {Values: []string{"1", "2"}}},
		Body:           []byte{0, 1, 2},
		AttributesJson: `{"k":"v"}`,
		Trace:          &Trace{Traceparent: "00-0102-03-01"},
	}
	b, err := proto.Marshal(in)
	require.NoError(t, err)
	var out Envelope
	require.NoError(t, proto.Unmarshal(b, &out))
	assert.True(t, proto.Equal(in, &out))

	resp := &DeliverResponse{Outcome: &DeliverResponse_Error{Error: &Error{Class: ErrorClass_ERROR_CLASS_RATE_LIMITED, RetryAfterMs: 1500}}}
	b, err = proto.Marshal(resp)
	require.NoError(t, err)
	var got DeliverResponse
	require.NoError(t, proto.Unmarshal(b, &got))
	assert.Equal(t, ErrorClass_ERROR_CLASS_RATE_LIMITED, got.GetError().GetClass())
	assert.Nil(t, got.GetResult())
	assert.Equal(t, ErrorClass_ERROR_CLASS_TRANSIENT, (&Error{}).GetClass(), "an unset class is transient")
}
