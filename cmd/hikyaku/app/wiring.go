package app

import (
	"github.com/xavidop/hikyaku/protocols/fault"
	"github.com/xavidop/hikyaku/protocols/file"
	httpproto "github.com/xavidop/hikyaku/protocols/http"
	"github.com/xavidop/hikyaku/protocols/openai"
	"github.com/xavidop/hikyaku/protocols/schedule"
	tcpproto "github.com/xavidop/hikyaku/protocols/tcp"
)

// registerBuiltins registers every protocol of this build. The protocols
// with heavy SDKs (brokers and clouds, from milestone M2) register in
// wiring_full.go, which the lite build leaves out.
func registerBuiltins() {
	file.Register()
	httpproto.Register()
	openai.Register()
	fault.Register()
	tcpproto.Register()
	schedule.Register()
	registerWeb()
	registerFull()
}
