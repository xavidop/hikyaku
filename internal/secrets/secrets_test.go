package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xavidop/mamori"
)

// fakeProvider resolves fake://<path>[#key] from maps.
type fakeProvider struct {
	values map[string][]byte
	errs   map[string]error
	calls  []string
}

func (p *fakeProvider) Scheme() string { return "fake" }

func (p *fakeProvider) Resolve(_ context.Context, ref mamori.Ref) (mamori.Value, error) {
	p.calls = append(p.calls, ref.Path)
	if err, ok := p.errs[ref.Path]; ok {
		return mamori.Value{}, err
	}
	v, ok := p.values[ref.Path]
	if !ok {
		return mamori.Value{}, fmt.Errorf("fake: %s: %w", ref.Path, mamori.ErrNotFound)
	}
	if ref.Key != "" {
		b, err := mamori.SelectKey(v, ref.Key)
		if err != nil {
			return mamori.Value{}, err
		}
		v = b
	}
	return mamori.Value{Bytes: v, Sensitive: true}, nil
}

func TestResolve(t *testing.T) {
	fake := &fakeProvider{
		values: map[string][]byte{
			"present":  []byte("v1"),
			"json":     []byte(`{"token":"tok"}`),
			"prod/key": []byte("prodkey"),
		},
		errs: map[string]error{"down": fmt.Errorf("fake: down: %w", mamori.ErrUnavailable)},
	}
	opts := Options{Providers: []mamori.Provider{fake}, RefVars: map[string]string{"ENV": "prod"}}
	tests := []struct {
		name       string
		ref        string
		opts       Options
		want       string
		wantSource Source
		wantConfig string
		wantInfra  mamori.Kind
	}{
		{name: "single", ref: "fake://present", opts: opts, want: "v1", wantSource: Source{Name: "S", Scheme: "fake", Ref: "fake://present", Link: 1, Links: 1}},
		{name: "chain falls through", ref: "fake://missing,fake://present", opts: opts, want: "v1", wantSource: Source{Name: "S", Scheme: "fake", Ref: "fake://present", Link: 2, Links: 2}},
		{name: "key selection", ref: "fake://json#token", opts: opts, want: "tok", wantSource: Source{Name: "S", Scheme: "fake", Ref: "fake://json#token", Link: 1, Links: 1}},
		{name: "ref var", ref: "fake://${ENV}/key", opts: opts, want: "prodkey", wantSource: Source{Name: "S", Scheme: "fake", Ref: "fake://prod/key", Link: 1, Links: 1}},
		{name: "ref var unset", ref: "fake://${REGION}/key", opts: Options{Providers: opts.Providers}, wantConfig: "ref var REGION is not set"},
		{name: "all missing", ref: "fake://missing,fake://also", opts: opts, wantConfig: "not found in any of 2 ref(s)"},
		{name: "provider down", ref: "fake://down,fake://present", opts: opts, wantInfra: mamori.KindUnavailable},
		{name: "unknown scheme", ref: "nosuch://x", opts: opts, wantConfig: "nosuch://x"},
		{name: "not a ref", ref: "plain-text", opts: opts, wantConfig: "invalid ref"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, err := Resolve(context.Background(), map[string]string{"S": tt.ref}, tt.opts)
			switch {
			case tt.wantConfig != "":
				require.Error(t, err)
				assert.True(t, IsConfig(err), "want ConfigError, got %T: %v", err, err)
				assert.False(t, IsInfra(err))
				assert.ErrorContains(t, err, tt.wantConfig)
				assert.ErrorContains(t, err, "secret S")
			case tt.wantInfra != "":
				require.Error(t, err)
				var ie *InfraError
				require.True(t, errors.As(err, &ie), "want InfraError, got %T: %v", err, err)
				assert.Equal(t, tt.wantInfra, ie.Kind)
				assert.Equal(t, "fake", ie.Scheme)
				assert.True(t, IsInfra(err))
			default:
				require.NoError(t, err)
				got, err := store.Reveal("S")
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
				src, ok := store.Source("S")
				require.True(t, ok)
				assert.Equal(t, tt.wantSource, src)
				v, _ := store.Get("S")
				assert.Equal(t, "[REDACTED]", v.String())
				assert.Equal(t, "[REDACTED]", fmt.Sprint(v))
			}
		})
	}
}

func TestResolveNeverPutsValuesInErrors(t *testing.T) {
	fake := &fakeProvider{values: map[string][]byte{"json": []byte(`{"a":"hidden-value"}`)}}
	_, err := Resolve(context.Background(), map[string]string{"S": "fake://json#nope"}, Options{Providers: []mamori.Provider{fake}})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "hidden-value")
}

func TestResolveEnvFallsBackToEnvFile(t *testing.T) {
	dir := t.TempDir()
	envFile := filepath.Join(dir, ".env")
	require.NoError(t, os.WriteFile(envFile, []byte("TAMESU_TEST_FROM_FILE=fromfile\n"), 0o600))
	t.Setenv("TAMESU_TEST_FROM_PROCESS", "fromprocess")

	store, err := Resolve(context.Background(), map[string]string{
		"A": "env:TAMESU_TEST_FROM_PROCESS",
		"B": "env:TAMESU_TEST_FROM_FILE",
	}, Options{EnvFile: envFile})
	require.NoError(t, err)
	a, _ := store.Reveal("A")
	b, _ := store.Reveal("B")
	assert.Equal(t, "fromprocess", a)
	assert.Equal(t, "fromfile", b)
	srcA, _ := store.Source("A")
	srcB, _ := store.Source("B")
	assert.Equal(t, "env", srcA.Scheme)
	assert.Equal(t, "dotenv", srcB.Scheme)
	assert.Equal(t, []string{"A", "B"}, store.Names())

	// Without the env file the second one is a config error.
	_, err = Resolve(context.Background(), map[string]string{"B": "env:TAMESU_TEST_FROM_FILE"}, Options{})
	require.Error(t, err)
	assert.True(t, IsConfig(err))

	lookup := EnvLookup(envFile, os.LookupEnv)
	v, ok := lookup("TAMESU_TEST_FROM_FILE")
	assert.True(t, ok)
	assert.Equal(t, "fromfile", v)
	_, ok = lookup("TAMESU_TEST_NOT_ANYWHERE")
	assert.False(t, ok)
}

func TestNewStore(t *testing.T) {
	store := NewStore(nil)
	_, err := store.Reveal("X")
	assert.True(t, IsConfig(err))
	var nilStore *Store
	_, ok := nilStore.Get("X")
	assert.False(t, ok)
	assert.Nil(t, nilStore.Names())
}
