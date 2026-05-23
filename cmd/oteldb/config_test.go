package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	data := `
prometheus:
  cache:
    max_bytes: 100MiB
max_result_bytes: 1GB
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	assert.Equal(t, int64(100*1024*1024), int64(cfg.Prometheus.Cache.MaxBytes))
	assert.Equal(t, int64(1000*1000*1000), int64(cfg.MaxResultBytes))
}
