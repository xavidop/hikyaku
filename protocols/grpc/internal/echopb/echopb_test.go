package echopb

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

// The generated code and testdata/echo.pb come from the same echo.proto:
// regenerate both together (go generate ./... in protocols/grpc).
func TestGeneratedCodeMatchesTheDescriptorSet(t *testing.T) {
	b, err := os.ReadFile("../../testdata/echo.pb")
	require.NoError(t, err)
	var set descriptorpb.FileDescriptorSet
	require.NoError(t, proto.Unmarshal(b, &set))
	var fromSet *descriptorpb.FileDescriptorProto
	for _, fd := range set.GetFile() {
		if fd.GetName() == "echo.proto" {
			fromSet = fd
		}
	}
	require.NotNil(t, fromSet, "echo.pb describes echo.proto")
	generated := protodesc.ToFileDescriptorProto(File_echo_proto)
	fromSet.SourceCodeInfo, generated.SourceCodeInfo = nil, nil
	assert.True(t, proto.Equal(fromSet, generated), "regenerate testdata/echo.pb and internal/echopb together")
}
