---
layout: ../../layouts/DocsLayout.astro
title: Embed it (Go)
---

# Embed it (Go)

This page explains how to use hikyaku as a Go library, inside your own program, instead of running
the `hikyaku` binary. Read it when your service should load a spec and run the relay itself.

The root module (`github.com/xavidop/hikyaku`) is a library: load a spec, then run it.

```go
import (
	"github.com/xavidop/hikyaku"
	"github.com/xavidop/hikyaku/protocols/file"
	httpproto "github.com/xavidop/hikyaku/protocols/http"
	"github.com/xavidop/hikyaku/protocols/kafka"
)

func main() {
	file.Register()
	httpproto.Register()
	kafka.Register()
	ctx := context.Background()
	spec, err := hikyaku.Load(ctx, []string{"hikyaku.yaml"})
	if err != nil {
		log.Fatal(err)
	}
	log.Fatal(hikyaku.Run(ctx, spec, hikyaku.WithLogger(slog.Default())))
}
```

- **Load and Parse** read, merge and validate specs. They run the same checks as
  `hikyaku validate`: references, cycles, every CEL expression, jq program and template, and plugin
  options. They resolve the declared secrets through mamori (`env:`, `file://`, and the providers
  your program links). `WithoutSecrets()` validates without credentials. `WithEnvLookup` replaces
  the environment for `{{env.X}}`.
- **Protocols are opt-in.** The root module links no protocol. Register the ones you use; each is
  a module of its own (`protocols/<name>`).
- **Require a release.** Each release tags every module at the same version (`vX.Y.Z` for the
  root, `protocols/http/vX.Y.Z`, `llm/vX.Y.Z` and so on). In those tags, each module requires the
  others at that version, so `go get github.com/xavidop/hikyaku/protocols/http@vX.Y.Z` works from
  any module. To work on hikyaku at the same time, put your module in a clone, keep the `replace`
  lines and add it to `go.work`, as
  [`examples/custom-destination`](https://github.com/xavidop/hikyaku/tree/main/examples/custom-destination)
  does.
- **Options.**
  - `WithAI(llm.New(...))` turns on AI routers and model destinations.
  - `WithStoreFactory("redis", ...)` opens Redis stores, and `WithStore` hands one in.
  - `WithMetrics(metrics.New())` records Prometheus metrics into a registry you serve.
- **Relay.** `hikyaku.New` builds a relay without running it. `Relay.Send` pushes one message
  through the routes and reports what happened. `Relay.Health` reports readiness: `Ready` is false
  while a required destination and its whole fallback chain are down, and `ListenersUp` is false
  while a listen source is down. It also carries the state of each destination, source and store.
- **Several relays.** One process can run several relays. Listen sources share an address when
  they serve different paths.

The admin API (`/healthz`, `/metrics`, `/reload`, `/v1/tail`) belongs to the CLI. A program
serves its own from `Relay.Health` and its metrics registry.
