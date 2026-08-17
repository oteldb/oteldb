package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/xbytes"
)

// TestStorageClusterConfigYAML checks the cluster block parses from the documented YAML shape via
// the real config loader.
func TestStorageClusterConfigYAML(t *testing.T) {
	const data = `
storage:
  backend: file
  dir: /data
  cluster:
    etcd: ["http://etcd-1:2379", "http://etcd-2:2379"]
    id: oteldb-1
    zone: a
    addr: oteldb-1:7946
    rf: 3
    shards_per_tenant: 8
    root: /oteldb
    private_backend: true
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.Equal(t, "file", cfg.Storage.Backend)
	require.NotNil(t, cfg.Storage.Cluster)
	require.Equal(t, []string{"http://etcd-1:2379", "http://etcd-2:2379"}, cfg.Storage.Cluster.Etcd)
	require.Equal(t, "oteldb-1", cfg.Storage.Cluster.ID)
	require.Equal(t, "a", cfg.Storage.Cluster.Zone)
	require.Equal(t, "oteldb-1:7946", cfg.Storage.Cluster.Addr)
	require.Equal(t, 3, cfg.Storage.Cluster.RF)
	require.Equal(t, 8, cfg.Storage.Cluster.ShardsPerTenant)
	require.Equal(t, "/oteldb", cfg.Storage.Cluster.Root)
	require.True(t, cfg.Storage.Cluster.PrivateBackend)
}

// TestStoragePolicyConfigYAML checks the policy block parses from the documented YAML shape. What
// the parsed policy resolves to in the engine is covered by the storagebackend tests.
func TestStoragePolicyConfigYAML(t *testing.T) {
	const data = `
storage:
  backend: file
  dir: /data
  policy:
    precision:
      - after: 168h
        bits: 32
    downsample:
      - after: 24h
        interval: 5m
        agg: avg
    recompress:
      after: 336h
      level: 9
    retention:
      max_age: 720h
      max_bytes: 100GB
    limits:
      ingest_bytes_per_second: 10MB
      max_series: 1000000
      max_series_soft: 800000
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	p := cfg.Storage.Policy
	require.NotNil(t, p)
	require.Equal(t, []storagebackend.PrecisionTierConfig{{After: 168 * time.Hour, Bits: 32}}, p.Precision)
	require.Equal(t, []storagebackend.DownsampleTierConfig{
		{After: 24 * time.Hour, Interval: 5 * time.Minute, Agg: "avg"},
	}, p.Downsample)
	require.Equal(t, &storagebackend.RecompressConfig{After: 336 * time.Hour, Level: 9}, p.Recompress)
	require.Equal(t, &storagebackend.RetentionConfig{
		MaxAge:   720 * time.Hour,
		MaxBytes: 100_000_000_000,
	}, p.Retention)
	require.Equal(t, &storagebackend.LimitsConfig{
		IngestBytesPerSecond: 10_000_000,
		MaxSeries:            1_000_000,
		MaxSeriesSoft:        800_000,
	}, p.Limits)
}

// TestStorageS3ConfigYAML checks the s3 block parses from the documented YAML shape via the real
// config loader.
func TestStorageS3ConfigYAML(t *testing.T) {
	const data = `
storage:
  backend: s3
  wal_dir: /var/lib/oteldb/wal
  s3:
    bucket: oteldb
    prefix: data/
    region: us-east-1
    endpoint: http://minio:9000
    force_path_style: true
    access_key_id: key
    secret_access_key: secret
    session_token: token
    retry: lossy
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.Equal(t, "s3", cfg.Storage.Backend)
	require.Equal(t, "/var/lib/oteldb/wal", cfg.Storage.WALDir)
	require.NotNil(t, cfg.Storage.S3)
	require.Equal(t, "oteldb", cfg.Storage.S3.Bucket)
	require.Equal(t, "data/", cfg.Storage.S3.Prefix)
	require.Equal(t, "us-east-1", cfg.Storage.S3.Region)
	require.Equal(t, "http://minio:9000", cfg.Storage.S3.Endpoint)
	require.True(t, cfg.Storage.S3.ForcePathStyle)
	require.Equal(t, "key", cfg.Storage.S3.AccessKeyID)
	require.Equal(t, "secret", cfg.Storage.S3.SecretAccessKey)
	require.Equal(t, "token", cfg.Storage.S3.SessionToken)
	require.Equal(t, "lossy", cfg.Storage.S3.Retry)
}

// TestStorageCacheConfigYAML checks the cache block parses from the documented YAML shape, including
// the opt-out semantics (an explicit 0 is distinguishable from unset, so it disables a byte cache;
// see the storagebackend tests for how the parsed values resolve).
func TestStorageCacheConfigYAML(t *testing.T) {
	const data = `
storage:
  backend: file
  dir: /data
  read_cache_bytes: "0"
  decode_cache_bytes: "64MiB"
  decode_memory_bytes: "256MiB"
  aggregate_stats: false
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.Equal(t, new(xbytes.Bytes(0)), cfg.Storage.ReadCacheBytes, "explicit 0 parses as set-to-zero")
	require.Equal(t, new(xbytes.Bytes(64<<20)), cfg.Storage.DecodeCacheBytes)
	require.Equal(t, new(xbytes.Bytes(256<<20)), cfg.Storage.DecodeMemoryBytes)
	require.Equal(t, new(false), cfg.Storage.AggregateStats)
}
