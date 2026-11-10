// Package hikyaku runs hikyaku relays from Go: load a spec, then run it.
//
//	import (
//		"github.com/xavidop/hikyaku"
//		"github.com/xavidop/hikyaku/protocols/file"
//		httpproto "github.com/xavidop/hikyaku/protocols/http"
//	)
//
//	func main() {
//		file.Register()
//		httpproto.Register()
//		ctx := context.Background()
//		spec, err := hikyaku.Load(ctx, []string{"hikyaku.yaml"})
//		if err != nil {
//			log.Fatal(err)
//		}
//		log.Fatal(hikyaku.Run(ctx, spec, hikyaku.WithLogger(slog.Default())))
//	}
//
// The root module holds the engine and the public plugin API (packages
// envelope, plugin, store, router, transform, metrics) but no protocol: a
// program links the protocols it registers, and the hikyaku binary
// (cmd/hikyaku) registers them all. AI routers and model destinations come
// from the llm module: pass an llm.Factory with WithAI.
//
// One process can run several relays, each with its own spec. Listen
// sources of different relays may share an address when they serve
// different paths. The admin API (/healthz, /metrics, /reload) is the
// CLI's: a program serves its own, from Relay.Health and the
// Prometheus registry it passes with WithMetrics.
package hikyaku
