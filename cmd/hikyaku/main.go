// Command hikyaku reads from one or more endpoints and delivers to one or
// more endpoints, configured by a YAML spec, with an AI router and retries
// and fallbacks at every layer. See https://xavidop.github.io/hikyaku/.
package main

import (
	"os"
	"runtime/debug"
	"strings"

	"github.com/xavidop/hikyaku/cmd/hikyaku/app"
)

// Set by the release build (-ldflags -X main.version=...).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	bi, ok := debug.ReadBuildInfo()
	os.Exit(app.Main(os.Args[1:], app.WithBuild(buildVersion(version, bi, ok), commit, date)))
}

// buildVersion is the version the CLI reports: v when the release build set
// it, else the module version go install built
// (go install github.com/xavidop/hikyaku/cmd/hikyaku@v1.2.3 reports v1.2.3).
// A build in a checkout stays "dev": the go command stamps it from the VCS,
// and it is no release.
func buildVersion(v string, bi *debug.BuildInfo, ok bool) string {
	if v != "dev" || !ok || !strings.HasPrefix(bi.Main.Version, "v") {
		return v
	}
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" {
			return v
		}
	}
	return bi.Main.Version
}
