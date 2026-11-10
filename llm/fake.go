//go:build llmfake

package llm

import (
	"github.com/firebase/genkit/go/core/api"

	"github.com/xavidop/hikyaku/llm/llmfake"
)

// A build with -tags llmfake registers the fake model providers (a
// generative model and a System One decision server, both in-process and
// deterministic), so the whole pipeline runs without network or keys. A
// default build never contains them.
func init() {
	fakePlugins = func() []api.Plugin { return llmfake.Plugins() }
}
