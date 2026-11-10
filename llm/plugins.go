package llm

import (
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"

	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/plugins/anthropic"
	"github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/firebase/genkit/go/plugins/compat_oai/openrouter"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/firebase/genkit/go/plugins/ollama"
	systemonex "github.com/firebase/genkit/go/plugins/systemone/exp"
	"github.com/openai/openai-go/option"

	"github.com/xavidop/hikyaku/internal/config"
)

// fakePlugins is set by fake.go in -tags llmfake builds; a default build
// never contains the fake provider.
var fakePlugins func() []api.Plugin

// providerPlugin maps a model's provider prefix to the plugin (and System
// One server) that serves it.
var providerPlugin = map[string][2]string{
	"googleai":             {"googlegenai", ""},
	"anthropic":            {"anthropic", ""},
	"openai":               {"openai", ""},
	"openrouter":           {"openrouter", ""},
	"ollama":               {"ollama", ""},
	"typesafe":             {"systemone", "typesafe"},
	"openrouter-decisions": {"systemone", "openrouter"},
	"ollama-decisions":     {"systemone", "ollama"},
	"cloudflare-decisions": {"systemone", "workersai"},
	"llmfake":              {"llmfake", ""},
	"llmfake-decisions":    {"llmfake", ""},
}

// allModels lists every model the spec's AI routers may call.
func allModels(spec *config.Spec) []string {
	var out []string
	add := func(ms ...string) {
		for _, m := range ms {
			if m != "" && !slices.Contains(out, m) {
				out = append(out, m)
			}
		}
	}
	add(spec.Models.Default, spec.Models.Decision)
	for _, d := range spec.Destinations {
		if d.Type == "model" {
			add(d.Model)
		}
		for _, m := range d.Members {
			if m.Type == "model" {
				add(m.Model)
			}
		}
	}
	add(spec.Models.Fallback.Default...)
	add(spec.Models.Fallback.Decision...)
	for _, r := range spec.Routes {
		var walk func(rs *config.RouterSpec)
		walk = func(rs *config.RouterSpec) {
			if rs == nil {
				return
			}
			if rs.Type == "ai" {
				add(spec.ModelChain(*rs)...)
			}
			for i := range rs.Fallback {
				walk(&rs.Fallback[i])
			}
		}
		walk(r.Router)
	}
	return out
}

// pluginsFor builds the Genkit plugins of a spec.
func pluginsFor(spec *config.Spec, o Options) ([]api.Plugin, error) {
	cred := func(names ...string) string {
		if o.Credentials == nil {
			return ""
		}
		for _, n := range names {
			if v, ok := o.Credentials(n); ok && v != "" {
				return v
			}
		}
		return ""
	}
	names := slices.Clone(spec.Genkit.Plugins)
	servers := slices.Clone(spec.Genkit.SystemOne.Servers)
	custom := map[string]bool{}
	for _, c := range spec.Genkit.SystemOne.Custom {
		custom[c.Provider] = true
	}
	for _, m := range allModels(spec) {
		prefix, _, _ := strings.Cut(m, "/")
		if custom[prefix] {
			if !slices.Contains(names, "systemone") {
				names = append(names, "systemone")
			}
			continue
		}
		p, ok := providerPlugin[prefix]
		if !ok {
			return nil, fmt.Errorf("model %s: unknown provider %q (known: googleai, anthropic, openai, openrouter, ollama, typesafe, openrouter-decisions, ollama-decisions, cloudflare-decisions, or a genkit.systemone.custom provider)", m, prefix)
		}
		if !slices.Contains(names, p[0]) {
			names = append(names, p[0])
		}
		if p[1] != "" && !slices.Contains(servers, p[1]) {
			servers = append(servers, p[1])
		}
	}
	ollamaURL := spec.Genkit.Ollama.URL
	if ollamaURL == "" {
		ollamaURL = "http://localhost:11434"
	}
	// key finds an API key in the declared secrets, then the environment;
	// the providers panic at init without one, so it is checked here.
	key := func(plugin string, envs ...string) (string, error) {
		if v := cred(envs...); v != "" {
			return v, nil
		}
		for _, e := range envs {
			if v := os.Getenv(e); v != "" {
				return v, nil
			}
		}
		return "", fmt.Errorf("%s needs %s: declare it under secrets: (%s: env:%s) or export it", plugin, strings.Join(envs, " or "), envs[0], envs[0])
	}
	var out []api.Plugin
	out = append(out, o.Plugins...)
	for _, name := range names {
		switch name {
		case "googlegenai":
			k, err := key("googlegenai", "GEMINI_API_KEY", "GOOGLE_API_KEY")
			if err != nil {
				return nil, err
			}
			out = append(out, &googlegenai.GoogleAI{APIKey: k})
		case "anthropic":
			k, err := key("anthropic", "ANTHROPIC_API_KEY")
			if err != nil {
				return nil, err
			}
			out = append(out, &anthropic.Anthropic{APIKey: k})
		case "openai":
			p, err := openAIPlugin(spec.Genkit.OpenAI, key)
			if err != nil {
				return nil, err
			}
			out = append(out, p)
		case "openrouter":
			k, err := key("openrouter", "OPENROUTER_API_KEY")
			if err != nil {
				return nil, err
			}
			out = append(out, &openrouter.OpenRouter{APIKey: k})
		case "ollama":
			out = append(out, &ollama.Ollama{ServerAddress: ollamaURL})
		case "systemone":
			ps, err := systemOnePlugins(spec, servers, key)
			if err != nil {
				return nil, err
			}
			out = append(out, ps...)
		case "llmfake":
			if fakePlugins == nil {
				if suppliedFakes(o.Plugins) {
					continue // the caller passed the fakes itself (tests)
				}
				return nil, fmt.Errorf("the fake model provider is not in this build (build with -tags llmfake: make build-llmfake)")
			}
			out = append(out, fakePlugins()...)
		default:
			return nil, fmt.Errorf("unknown genkit plugin %q", name)
		}
	}
	// One plugin per name: Genkit refuses to register an action twice.
	seen := map[string]bool{}
	uniq := out[:0]
	for _, p := range out {
		if seen[p.Name()] {
			continue
		}
		seen[p.Name()] = true
		uniq = append(uniq, p)
	}
	return uniq, nil
}

// DefaultOpenAIBaseURL is the server of the openai plugin unless
// genkit.openai.base_url or OPENAI_BASE_URL names another.
const DefaultOpenAIBaseURL = "https://api.openai.com/v1"

// openAIPlugin builds the openai plugin for its server. OpenAI itself needs
// OPENAI_API_KEY; another OpenAI-compatible server (vLLM, LM Studio, a
// gateway) may need none, and then no Authorization header is sent.
func openAIPlugin(c config.OpenAI, key func(string, ...string) (string, error)) (*openai.OpenAI, error) {
	base := c.BaseURL
	if base == "" {
		base = os.Getenv("OPENAI_BASE_URL")
	}
	if base == "" {
		base = DefaultOpenAIBaseURL
	}
	p := &openai.OpenAI{Opts: []option.RequestOption{option.WithBaseURL(base)}}
	k, err := key("openai", "OPENAI_API_KEY")
	switch {
	case err == nil:
		p.APIKey = k
	case strings.TrimSuffix(base, "/") == DefaultOpenAIBaseURL:
		return nil, err
	default:
		// The plugin refuses an empty key: give it one, and send none.
		p.APIKey = "none"
		p.Opts = append(p.Opts, option.WithHeaderDel("Authorization"))
	}
	return p, nil
}

// systemOnePlugins builds one plugin per System One server.
func systemOnePlugins(spec *config.Spec, servers []string, key func(string, ...string) (string, error)) ([]api.Plugin, error) {
	so := spec.Genkit.SystemOne
	var out []api.Plugin
	for _, s := range servers {
		switch s {
		case "typesafe":
			k, err := key("the typesafe decision server", "TYPESAFE_API_KEY")
			if err != nil {
				return nil, err
			}
			p := systemonex.TypeSafe()
			p.APIKey = k
			if so.TypeSafeBaseURL != "" {
				p.BaseURL = so.TypeSafeBaseURL
			}
			out = append(out, p)
		case "openrouter":
			k, err := key("the openrouter decision server", "OPENROUTER_API_KEY")
			if err != nil {
				return nil, err
			}
			p := systemonex.OpenRouter()
			p.APIKey = k
			if so.OpenRouterBaseURL != "" {
				p.BaseURL = so.OpenRouterBaseURL
			}
			out = append(out, p)
		case "workersai":
			k, err := key("the workersai decision server", "CLOUDFLARE_API_TOKEN")
			if err != nil {
				return nil, err
			}
			p := systemonex.WorkersAI(so.WorkersAIAccount)
			p.APIKey = k
			out = append(out, p)
		case "ollama":
			p := systemonex.Ollama()
			if so.OllamaURL != "" {
				p.BaseURL = so.OllamaURL
			} else if spec.Genkit.Ollama.URL != "" {
				p.BaseURL = spec.Genkit.Ollama.URL
			}
			out = append(out, p)
		}
	}
	for _, c := range so.Custom {
		p := &systemonex.SystemOne{Provider: c.Provider, BaseURL: c.BaseURL, Path: c.Path, ModelsPath: c.ModelsPath, APIKey: c.APIKey}
		if len(c.Headers) > 0 {
			p.Headers = http.Header{}
			for k, v := range c.Headers {
				p.Headers.Set(k, v)
			}
		}
		if c.Images {
			p.Images = systemonex.MediaSupported
		}
		out = append(out, p)
	}
	return out, nil
}

// suppliedFakes reports whether Options.Plugins already holds the fake
// providers, so a build without -tags llmfake can still run them.
func suppliedFakes(ps []api.Plugin) bool {
	for _, p := range ps {
		if p.Name() == "llmfake" {
			return true
		}
	}
	return false
}
