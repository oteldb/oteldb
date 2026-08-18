package config_test

import (
	"os"
	"path/filepath"
	"testing"

	fyaml "github.com/go-faster/figureout/source/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
)

// TestResolveRejectsUnknownKey is oteldb#1285: a key no field claims — here a Loki setting written
// into the Prometheus block — is a startup error naming the path, instead of a setting that looks
// applied and is not.
func TestResolveRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.yml")
	require.NoError(t, os.WriteFile(path, []byte("prometheus:\n  max_sample_rows: 10\n"), 0o600))

	_, _, err := config.Resolve(diffDescriptor(t), path, config.LoadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "prometheus.max_sample_rows")
}

func TestResolveFallback(t *testing.T) {
	t.Parallel()

	d := diffDescriptor(t)
	missing := filepath.Join(t.TempDir(), "missing.yml")

	t.Run("Optional", func(t *testing.T) {
		t.Parallel()

		cfg, _, err := config.Resolve(d, "", config.LoadOptions{Fallback: missing, Optional: true})
		require.NoError(t, err)
		assert.Zero(t, cfg)
	})
	t.Run("Required", func(t *testing.T) {
		t.Parallel()

		_, _, err := config.Resolve(d, "", config.LoadOptions{Fallback: missing})
		require.Error(t, err)
	})
	t.Run("ExplicitPathIsRequired", func(t *testing.T) {
		t.Parallel()

		_, _, err := config.Resolve(d, missing, config.LoadOptions{Fallback: missing, Optional: true})
		require.Error(t, err)
	})
}

// TestResolveListenerShape pins that the descriptor spells a listener flat, the way the `,inline`
// embedding does. A nested "listener" key would break every config file in existence.
func TestResolveListenerShape(t *testing.T) {
	t.Parallel()

	d := diffDescriptor(t)
	_, _, err := d.Resolve(fyaml.Bytes([]byte(
		"prometheus:\n  listener:\n    bind: \":9090\"\n"), fyaml.DisallowUnknownFields()))
	require.Error(t, err, "the listener must not be a key of its own")

	cfg, _, err := d.Resolve(fyaml.Bytes([]byte(
		"prometheus:\n  bind: \":9090\"\n  auth:\n    - type: none\n"), fyaml.DisallowUnknownFields()))
	require.NoError(t, err)
	assert.Equal(t, ":9090", cfg.Prometheus.Bind)
	require.Len(t, cfg.Prometheus.Auth, 1)
}

// TestResolveDisabledBind pins that "-" reaches the config as itself. It is what disables an API in
// odbselect, and nothing about the descriptor may interpret it.
func TestResolveDisabledBind(t *testing.T) {
	t.Parallel()

	cfg, _, err := diffDescriptor(t).Resolve(fyaml.Bytes([]byte("prometheus:\n  bind: \"-\"\n")))
	require.NoError(t, err)
	assert.Equal(t, "-", cfg.Prometheus.Bind)
}

// TestResolveDefaultsUnchanged pins that resolution does not default anything, so a binary can
// still apply its environment overrides first and default afterwards.
func TestResolveDefaultsUnchanged(t *testing.T) {
	t.Parallel()

	cfg, _, err := diffDescriptor(t).Resolve(fyaml.Bytes([]byte("{}\n")))
	require.NoError(t, err)
	assert.Empty(t, cfg.Prometheus.Bind)
	assert.Zero(t, cfg.Prometheus.MaxSamples)
	assert.Nil(t, cfg.Prometheus.EnableNegativeOffset)

	cfg.Prometheus.SetDefaults()
	assert.Equal(t, ":9090", cfg.Prometheus.Bind)
	require.NotNil(t, cfg.Prometheus.EnableNegativeOffset)
	assert.True(t, *cfg.Prometheus.EnableNegativeOffset)
}
