package app

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hk(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := Main(append(args, "--color", "never"), WithStreams(strings.NewReader(""), &out, &errb),
		WithEnv(func(string) (string, bool) { return "", false }))
	return code, out.String(), errb.String()
}

// Every example spec, real and offline, validates against this binary.
func TestExamplesValidate(t *testing.T) {
	specs, err := filepath.Glob("../../../examples/*/hikyaku*.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, specs)
	for _, s := range specs {
		if strings.HasSuffix(s, ".test.yaml") {
			continue // tests of an example, not a spec (TestExamplesTests runs them)
		}
		if strings.Contains(s, "custom-destination") || strings.Contains(s, "-plugin") {
			continue // their types come from their own binaries (TestCRMPluginExample and TestPostgresPluginExample validate them)
		}
		t.Run(filepath.Base(filepath.Dir(s))+"/"+filepath.Base(s), func(t *testing.T) {
			code, out, errOut := hk(t, "validate", "-f", s)
			assert.Equal(t, 0, code, errOut)
			assert.Contains(t, out, "valid")
		})
	}
}

// Every example that ships tests passes them, with no secret set.
func TestExamplesTests(t *testing.T) {
	files, err := filepath.Glob("../../../examples/*/hikyaku.test.yaml")
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		dir := filepath.Dir(f)
		t.Run(filepath.Base(dir), func(t *testing.T) {
			code, out, errOut := hk(t, "test", "-f", filepath.Join(dir, "hikyaku.yaml"), f)
			assert.Equal(t, 0, code, out+errOut)
			assert.Contains(t, out, "passed")
		})
	}
}

func TestVersionNamesGenkit(t *testing.T) {
	code, out, _ := hk(t, "version")
	require.Equal(t, 0, code)
	assert.Contains(t, out, "genkit v1.")
}
