package relaypb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// The wire contract callers build clients from: field numbers and names
// must not change.
func TestRelayContract(t *testing.T) {
	sd := File_relaypb_relay_proto.Services().ByName("Relay")
	require.NotNil(t, sd)
	assert.Equal(t, "hikyaku.relay.v1.Relay", string(sd.FullName()))
	send := sd.Methods().ByName("Send")
	require.NotNil(t, send)
	assert.Equal(t, "/hikyaku.relay.v1.Relay/Send", Relay_Send_FullMethodName)
	assert.False(t, send.IsStreamingClient() || send.IsStreamingServer())

	fields := func(m proto.Message) map[string]int {
		out := map[string]int{}
		fs := m.ProtoReflect().Descriptor().Fields()
		for i := 0; i < fs.Len(); i++ {
			out[string(fs.Get(i).Name())] = int(fs.Get(i).Number())
		}
		return out
	}
	assert.Equal(t, map[string]int{"id": 1, "subject": 2, "headers": 3, "content_type": 4, "body": 5}, fields(&Message{}))
	assert.Equal(t, map[string]int{"id": 1, "status": 2}, fields(&SendReply{}))

	_, err := protoregistry.GlobalFiles.FindFileByPath("relaypb/relay.proto")
	assert.NoError(t, err, "registered for reflection")
}

func TestMessageRoundTrip(t *testing.T) {
	in := &Message{Id: "evt_1", Subject: "orders", Headers: map[string]string{"x-tenant": "acme"}, ContentType: "application/json", Body: []byte(`{}`)}
	b, err := proto.Marshal(in)
	require.NoError(t, err)
	var out Message
	require.NoError(t, proto.Unmarshal(b, &out))
	assert.True(t, proto.Equal(in, &out))
}
