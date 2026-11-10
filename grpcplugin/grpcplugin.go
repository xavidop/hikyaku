// Package grpcplugin runs hikyaku plugins out of process: separate binaries,
// in any language, that hikyaku starts and talks gRPC to through
// hashicorp/go-plugin (the protocol is proto/hikyaku/plugin/v1/plugin.proto).
//
// # Host
//
// The spec's plugins: section lists the binaries. LoadAll launches each one
// (Launch: resolve the command, check its sha256, start it with a minimal
// environment, handshake over AutoMTLS, Describe), and Set.Register makes
// every kind they declare a source or destination type, with its settings
// under options:, exactly like an in-process Go plugin. Set.Extensions gives
// hikyaku validate and hikyaku schema those types, with each options: block
// checked against the JSON Schema the plugin declares.
//
// A registered kind's factory opens a handle in the plugin and returns a
// proxy: a plugin.Sink (and plugin.BatchSink when the plugin's sink batches)
// or a plugin.Source (and plugin.Closer). Failures cross the wire with their
// class, retry-after hint and tags. When the plugin process dies, the call
// in flight fails as transient with the tag connect, and the next call
// relaunches the process (with backoff when relaunching fails) and re-opens
// the handles; a source restarts its stream the same way. The plugin's
// stderr logs are forwarded to the host's logger with their levels.
//
// # Plugin
//
// A Go plugin is ordinary plugin.Sink and plugin.Source code, served by
// Serve:
//
//	func main() {
//		grpcplugin.Serve(grpcplugin.Plugins{
//			Sinks: map[string]grpcplugin.SinkDef{
//				"echo": {Factory: newEcho, Options: EchoOptions{}, Description: "..."},
//			},
//		})
//	}
//
// The options: block arrives as plugin.Spec.Options, so spec.Decode(&opts)
// works as it does in process, and the JSON Schema hikyaku validates with is
// generated from the Options struct (its yaml and jsonschema tags). A source
// settles its messages with plugin.OnSettle(env, grpcplugin.AckMode(spec),
// ...): hikyaku's outcome comes back through the Settle call. See
// example/hikyaku-plugin-echo.
package grpcplugin

import (
	"context"
	"encoding/json"

	goplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"

	"github.com/xavidop/hikyaku/grpcplugin/pluginpb"
)

// Handshake is the go-plugin handshake of every hikyaku plugin: the host
// sets HIKYAKU_PLUGIN to the cookie value, and the plugin refuses to run
// without it (it is not meant to be run by hand).
var Handshake = goplugin.HandshakeConfig{
	ProtocolVersion:  1,
	MagicCookieKey:   "HIKYAKU_PLUGIN",
	MagicCookieValue: "hikyaku.plugin.v1.9f4c2b7e",
}

// pluginName is the name hikyaku dispenses the plugin service under.
const pluginName = "hikyaku"

// maxMessageBytes caps one gRPC message in either direction: a message body
// with its envelope, or a batch.
const maxMessageBytes = 64 << 20

// logLevelEnv tells a plugin the host's log level, so it does not format
// records the host would drop.
const logLevelEnv = "HIKYAKU_PLUGIN_LOG_LEVEL"

// Role says whether a kind is a source or a sink (destination) type.
type Role int

// Roles.
const (
	RoleSource Role = iota + 1
	RoleSink
)

func (r Role) String() string {
	switch r {
	case RoleSource:
		return "source"
	case RoleSink:
		return "destination"
	}
	return "unknown"
}

// Kind is a source or destination type a plugin serves.
type Kind struct {
	Name        string
	Role        Role
	Description string
	// Schema is the JSON Schema of the kind's options: block; empty when
	// the plugin declares none (any object is accepted).
	Schema json.RawMessage
}

// grpcPlugin plugs the protocol into go-plugin: the plugin side registers
// the server, the host side dispenses a client.
type grpcPlugin struct {
	goplugin.NetRPCUnsupportedPlugin
	server pluginpb.PluginServer
}

func (p *grpcPlugin) GRPCServer(_ *goplugin.GRPCBroker, s *grpc.Server) error {
	pluginpb.RegisterPluginServer(s, p.server)
	return nil
}

func (p *grpcPlugin) GRPCClient(_ context.Context, _ *goplugin.GRPCBroker, c *grpc.ClientConn) (any, error) {
	return pluginpb.NewPluginClient(c), nil
}

// pluginSet is what both sides hand go-plugin.
func pluginSet(server pluginpb.PluginServer) goplugin.PluginSet {
	return goplugin.PluginSet{pluginName: &grpcPlugin{server: server}}
}
