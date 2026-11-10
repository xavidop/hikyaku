package registry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xavidop/hikyaku/plugin"
)

type webhookOptions struct {
	URL     string `yaml:"url"`
	Retries int    `yaml:"retries,omitempty"`
}

func noSink(context.Context, plugin.Spec) (plugin.Sink, error)     { return nil, nil }
func noSource(context.Context, plugin.Spec) (plugin.Source, error) { return nil, nil }

// Extensions lists the registered types the protocol matrix does not know,
// with a strict check of their options: blocks.
func TestExtensions(t *testing.T) {
	plugin.RegisterSink("registry-test-hook", noSink, plugin.WithOptions(webhookOptions{}), plugin.WithDescription("a test webhook"))
	plugin.RegisterSource("registry-test-feed", noSource)
	plugin.RegisterSink("stdout", noSink)    // a built-in type: not an extension
	plugin.RegisterSource("stdin", noSource) // likewise

	x := Extensions()
	hook, ok := x.Sinks["registry-test-hook"]
	require.True(t, ok)
	assert.Equal(t, "a test webhook", hook.Description)
	assert.Equal(t, webhookOptions{}, hook.Options)
	require.NotNil(t, hook.Check)
	assert.NoError(t, hook.Check(map[string]any{"url": "https://example.com", "retries": 2}))
	assert.Error(t, hook.Check(map[string]any{"url": "https://example.com", "nope": 1}), "unknown keys are refused")
	assert.Error(t, hook.Check(map[string]any{"retries": "many"}), "so are wrong types")

	feed, ok := x.Sources["registry-test-feed"]
	require.True(t, ok)
	assert.Nil(t, feed.Check, "a type without an options struct accepts any options")

	assert.NotContains(t, x.Sinks, "stdout", "built-in types are the matrix's, not extensions")
	assert.NotContains(t, x.Sources, "stdin")
}
