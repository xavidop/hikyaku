package grpc

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
)

// echoFiles is the test descriptor set, parsed.
func echoFiles(t *testing.T) []*descriptorpb.FileDescriptorProto {
	t.Helper()
	b, err := os.ReadFile(echoSet)
	require.NoError(t, err)
	var set descriptorpb.FileDescriptorSet
	require.NoError(t, proto.Unmarshal(b, &set))
	return set.GetFile()
}

func TestLoadDescriptorSet(t *testing.T) {
	reg, err := loadDescriptorSet(echoSet)
	require.NoError(t, err)
	var names []string
	for _, sd := range reg.services() {
		names = append(names, string(sd.FullName()))
	}
	assert.Equal(t, []string{"hikyaku.test.echo.v1.Chat", "hikyaku.test.echo.v1.Echo", "hikyaku.test.echo.v1.Feed"}, names)

	md, err := reg.method(sayMethod)
	require.NoError(t, err)
	assert.Equal(t, sayMethod, fullMethod(md))
	assert.Equal(t, "unary", streaming(md))
	md, err = reg.method("hikyaku.test.echo.v1.Echo/Collect")
	require.NoError(t, err, "the leading slash may be left out")
	assert.Equal(t, "client-streaming", streaming(md))
	md, err = reg.method(watchMethod)
	require.NoError(t, err)
	assert.Equal(t, "server-streaming", streaming(md))
	md, err = reg.method("/hikyaku.test.echo.v1.Chat/Talk")
	require.NoError(t, err)
	assert.Equal(t, "bidi-streaming", streaming(md))

	_, err = reg.method("/hikyaku.test.echo.v1.SayRequest/Text")
	assert.EqualError(t, err, "hikyaku.test.echo.v1.SayRequest is not a service")
	_, err = reg.method("/hikyaku.test.echo.v1.Echo/Shout")
	assert.EqualError(t, err, "service hikyaku.test.echo.v1.Echo has no method Shout (it has Say, Collect)")
}

func TestSplitMethod(t *testing.T) {
	for _, ok := range []string{"/a.B/C", "a.B/C", " /pkg.v1.Svc/Do_It "} {
		_, _, err := splitMethod(ok)
		assert.NoError(t, err, ok)
	}
	for _, bad := range []string{"", "/", "a.B", "/a.B/", "//C", "/a.B/C/D", "/a..B/C", "/a.B/C-D"} {
		_, _, err := splitMethod(bad)
		assert.Error(t, err, bad)
	}
}

func TestLinkUsesTheLinkedWellKnownTypes(t *testing.T) {
	var own []*descriptorpb.FileDescriptorProto
	for _, fd := range echoFiles(t) {
		if fd.GetName() != "google/protobuf/timestamp.proto" {
			own = append(own, fd)
		}
	}
	reg, err := link(own)
	require.NoError(t, err, "a set built without --include_imports still finds timestamp.proto")
	_, err = reg.method(sayMethod)
	assert.NoError(t, err)
}

func TestLinkErrors(t *testing.T) {
	files := echoFiles(t)
	echo := files[len(files)-1]
	require.Equal(t, "echo.proto", echo.GetName())

	orphan := proto.Clone(echo).(*descriptorpb.FileDescriptorProto)
	orphan.Dependency = append(orphan.Dependency, "company/missing.proto")
	_, err := link([]*descriptorpb.FileDescriptorProto{orphan})
	assert.EqualError(t, err, "echo.proto imports company/missing.proto, which is missing (build the set with --include_imports)")

	other := proto.Clone(echo).(*descriptorpb.FileDescriptorProto)
	other.Package = proto.String("another.pkg")
	_, err = link([]*descriptorpb.FileDescriptorProto{echo, other})
	assert.EqualError(t, err, "echo.proto is described twice, differently")

	a := &descriptorpb.FileDescriptorProto{Name: proto.String("a.proto"), Dependency: []string{"b.proto"}, Syntax: proto.String("proto3")}
	b := &descriptorpb.FileDescriptorProto{Name: proto.String("b.proto"), Dependency: []string{"a.proto"}, Syntax: proto.String("proto3")}
	_, err = link([]*descriptorpb.FileDescriptorProto{a, b})
	assert.ErrorContains(t, err, "import cycle")

	dir := t.TempDir()
	empty := filepath.Join(dir, "empty.pb")
	require.NoError(t, os.WriteFile(empty, nil, 0o600))
	_, err = loadDescriptorSet(empty)
	assert.ErrorContains(t, err, "holds no files")
}

func TestJSONMapping(t *testing.T) {
	reg, err := loadDescriptorSet(echoSet)
	require.NoError(t, err)
	md, err := reg.method(sayMethod)
	require.NoError(t, err)
	m := dynamicpb.NewMessage(md.Input())
	require.NoError(t, fromJSON([]byte(`{"text":"a","count":3,"at":"2026-01-02T03:04:05Z","nope":1}`), m, reg.types))
	out, err := toJSON(m, reg.types)
	require.NoError(t, err)
	assert.Equal(t, `{"text":"a","count":3,"at":"2026-01-02T03:04:05Z"}`, string(out), "compact, unknown fields dropped")

	empty := dynamicpb.NewMessage(md.Input())
	require.NoError(t, fromJSON([]byte("  "), empty, reg.types), "a blank body is an empty message")
	out, err = toJSON(empty, reg.types)
	require.NoError(t, err)
	assert.Equal(t, `{"text":"","count":0,"at":null}`, string(out), "unpopulated fields included")

	assert.Error(t, fromJSON([]byte(`{"count":"x"}`), dynamicpb.NewMessage(md.Input()), reg.types))
}

func TestResolverFindsLinkedTypes(t *testing.T) {
	r := resolver{}
	mt, err := r.FindMessageByName("google.protobuf.Duration")
	require.NoError(t, err)
	assert.Equal(t, "google.protobuf.Duration", string(mt.Descriptor().FullName()))
	_, err = r.FindMessageByURL("type.googleapis.com/google.protobuf.Duration")
	assert.NoError(t, err)
	_, err = r.FindExtensionByName("nope.ext")
	assert.Error(t, err)
	_, err = r.FindExtensionByNumber("nope.Msg", 1)
	assert.Error(t, err)

	reg, err := loadDescriptorSet(echoSet)
	require.NoError(t, err)
	mt, err = reg.types.FindMessageByURL("type.googleapis.com/hikyaku.test.echo.v1.Event")
	require.NoError(t, err, "the set's own types first")
	assert.Equal(t, "hikyaku.test.echo.v1.Event", string(mt.Descriptor().FullName()))

	any, err := anypb.New(durationpb.New(1))
	require.NoError(t, err)
	out, err := toJSON(any, reg.types)
	require.NoError(t, err)
	assert.JSONEq(t, `{"@type":"type.googleapis.com/google.protobuf.Duration","value":"0.000000001s"}`, string(out))
}

func TestMediaTypes(t *testing.T) {
	assert.True(t, isProtobuf("application/x-protobuf"))
	assert.True(t, isProtobuf("Application/Protobuf; proto=hikyaku.test.echo.v1.SayRequest"))
	assert.False(t, isProtobuf("application/json"))
	assert.False(t, isProtobuf(""))
	assert.Equal(t, contentJSON, sniff([]byte(`{"a":1}`)))
	assert.Equal(t, contentText, sniff([]byte("plain")))
	assert.Equal(t, contentBinary, sniff([]byte{0xff}))
}
