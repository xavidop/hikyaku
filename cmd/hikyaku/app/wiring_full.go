//go:build !lite

package app

import (
	"context"
	mqttproto "github.com/xavidop/hikyaku/protocols/mqtt"
	"log/slog"

	"github.com/xavidop/hikyaku/grpcplugin"
	"github.com/xavidop/hikyaku/internal/cli"
	"github.com/xavidop/hikyaku/internal/config"
	amqpproto "github.com/xavidop/hikyaku/protocols/amqp"
	awsproto "github.com/xavidop/hikyaku/protocols/aws"
	azureproto "github.com/xavidop/hikyaku/protocols/azure"
	gcpproto "github.com/xavidop/hikyaku/protocols/gcp"
	grpcproto "github.com/xavidop/hikyaku/protocols/grpc"
	kafkaproto "github.com/xavidop/hikyaku/protocols/kafka"
	natsproto "github.com/xavidop/hikyaku/protocols/nats"
	redisproto "github.com/xavidop/hikyaku/protocols/redis"
	// Cloud secret providers for mamori refs (aws-sm://, gcp-sm://,
	// azure-kv://, vault://, k8s-secret://, op://, sops://, doppler://,
	// infisical://). The lite build resolves env:, file:// and dotenv://
	// only.
	_ "github.com/xavidop/mamori/providers/aws"
	_ "github.com/xavidop/mamori/providers/azure"
	_ "github.com/xavidop/mamori/providers/doppler"
	_ "github.com/xavidop/mamori/providers/gcp"
	_ "github.com/xavidop/mamori/providers/infisical"
	_ "github.com/xavidop/mamori/providers/k8s"
	_ "github.com/xavidop/mamori/providers/onepassword"
	_ "github.com/xavidop/mamori/providers/sops"
	_ "github.com/xavidop/mamori/providers/vault"
)

// registerFull registers the protocols with heavy SDKs: the brokers and the
// clouds, which hikyaku-lite leaves out.
func registerFull() {
	mqttproto.Register()
	azureproto.Register()
	gcpproto.Register()
	kafkaproto.Register()
	natsproto.Register()
	grpcproto.Register()
	awsproto.Register()
	redisproto.Register()
	amqpproto.Register()
}

// Out-of-process plugins (plugins:) are a full-build feature: the host
// links gRPC and hashicorp/go-plugin.
func init() {
	pluginLoader = func(ctx context.Context, bins []config.PluginBinary, baseDir string, log *slog.Logger) (cli.PluginSet, error) {
		return grpcplugin.LoadAll(ctx, bins, baseDir, log)
	}
}
