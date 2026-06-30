package main

import (
	"context"
	"math"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// applyOption applies a storage.Option to a fresh Options and returns it, so tests can inspect what
// clusterOption configured.
func applyOption(t *testing.T, opt storage.Option) storage.Options {
	t.Helper()
	var o storage.Options
	require.NotNil(t, opt)
	opt(&o)
	return o
}

func TestClusterOption(t *testing.T) {
	lg := zap.NewNop()

	t.Run("DisabledWhenNoEtcd", func(t *testing.T) {
		opt, err := clusterOption(nil, lg)
		require.NoError(t, err)
		require.Nil(t, opt)

		opt, err = clusterOption(&StorageClusterConfig{ID: "n1"}, lg) // no etcd endpoints
		require.NoError(t, err)
		require.Nil(t, opt)
	})

	t.Run("Explicit", func(t *testing.T) {
		opt, err := clusterOption(&StorageClusterConfig{
			Etcd: []string{"http://etcd:2379"}, ID: "node-1", Zone: "z1",
			Addr: "node-1:9000", RF: 2, ShardsPerTenant: 4, Root: "/x",
		}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.NotNil(t, o.Cluster)
		require.Equal(t, []string{"http://etcd:2379"}, o.Cluster.Etcd)
		require.Equal(t, "node-1", o.Cluster.Self.ID)
		require.Equal(t, "z1", o.Cluster.Self.Zone)
		require.Equal(t, "node-1:9000", o.Cluster.Self.Addr)
		require.Equal(t, 2, o.Cluster.RF)
		require.Equal(t, 4, o.Cluster.ShardsPerTenant)
		require.Equal(t, "/x", o.Cluster.Root)
	})

	t.Run("AddrDerivedFromID", func(t *testing.T) {
		opt, err := clusterOption(&StorageClusterConfig{Etcd: []string{"e"}, ID: "node-7"}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.Equal(t, "node-7:7946", o.Cluster.Self.Addr, "default replication port")

		opt, err = clusterOption(&StorageClusterConfig{Etcd: []string{"e"}, ID: "node-7", Port: 1234}, lg)
		require.NoError(t, err)
		o = applyOption(t, opt)
		require.Equal(t, "node-7:1234", o.Cluster.Self.Addr)
	})

	t.Run("IDAndAddrDefaultToHostname", func(t *testing.T) {
		host, err := os.Hostname()
		require.NoError(t, err)
		opt, err := clusterOption(&StorageClusterConfig{Etcd: []string{"e"}}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.Equal(t, host, o.Cluster.Self.ID)
		require.Equal(t, host+":7946", o.Cluster.Self.Addr)
	})
}

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
}

func TestTenancyOption(t *testing.T) {
	t.Run("DisabledWhenEmpty", func(t *testing.T) {
		opt, err := tenancyOption(nil)
		require.NoError(t, err)
		require.Nil(t, opt)

		opt, err = tenancyOption(&StoragePolicyConfig{}) // no tiers, no recompress
		require.NoError(t, err)
		require.Nil(t, opt)
	})

	t.Run("ResolvesPolicyForEveryTenant", func(t *testing.T) {
		opt, err := tenancyOption(&StoragePolicyConfig{
			Precision: []PrecisionTierConfig{
				{After: 7 * 24 * time.Hour, Bits: 32},
				{After: 30 * 24 * time.Hour, Bits: 16},
			},
			Downsample: []DownsampleTierConfig{
				{After: 24 * time.Hour, Interval: 5 * time.Minute, Agg: "avg"},
				{After: 7 * 24 * time.Hour, Interval: time.Hour}, // Agg defaults to last.
			},
			Recompress: &RecompressConfig{After: 14 * 24 * time.Hour, Level: 9},
		})
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.NotNil(t, o.Tenancy)

		// The static resolver returns the same policy regardless of tenant.
		p := o.Tenancy.Resolve(signal.TenantID("any"))

		require.Len(t, p.Precision.Tiers, 2)
		require.Equal(t, 7*24*time.Hour, p.Precision.Tiers[0].After)
		require.Equal(t, uint8(32), p.Precision.Tiers[0].Bits)
		require.Equal(t, uint8(16), p.Precision.Tiers[1].Bits)

		require.Len(t, p.Downsample.Tiers, 2)
		require.Equal(t, 5*time.Minute, p.Downsample.Tiers[0].Interval)
		require.Equal(t, signal.AggAvg, p.Downsample.Tiers[0].Agg)
		require.Equal(t, signal.AggLast, p.Downsample.Tiers[1].Agg, "empty agg defaults to last")

		require.Equal(t, 14*24*time.Hour, p.Recompress.After)
		require.Equal(t, 9, p.Recompress.Level)
	})

	t.Run("UnknownAggIsAnError", func(t *testing.T) {
		_, err := tenancyOption(&StoragePolicyConfig{
			Downsample: []DownsampleTierConfig{{Interval: time.Minute, Agg: "median"}},
		})
		require.Error(t, err)
	})
}

// TestStoragePolicyConfigYAML checks the policy block parses from the documented YAML shape and
// resolves into the storage tenancy policy.
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
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.NotNil(t, cfg.Storage.Policy)
	opt, err := tenancyOption(cfg.Storage.Policy)
	require.NoError(t, err)
	o := applyOption(t, opt)

	p := o.Tenancy.Resolve("default")
	require.Len(t, p.Precision.Tiers, 1)
	require.Equal(t, 168*time.Hour, p.Precision.Tiers[0].After)
	require.Equal(t, uint8(32), p.Precision.Tiers[0].Bits)
	require.Len(t, p.Downsample.Tiers, 1)
	require.Equal(t, signal.AggAvg, p.Downsample.Tiers[0].Agg)
	require.Equal(t, 336*time.Hour, p.Recompress.After)
	require.Equal(t, 9, p.Recompress.Level)
}

func TestS3Backend(t *testing.T) {
	ctx := context.Background()

	t.Run("RequiresBucket", func(t *testing.T) {
		_, err := s3Backend(ctx, nil)
		require.Error(t, err)

		_, err = s3Backend(ctx, &StorageS3Config{Region: "us-east-1"}) // no bucket
		require.Error(t, err)
	})

	t.Run("StaticCredentials", func(t *testing.T) {
		b, err := s3Backend(ctx, &StorageS3Config{
			Bucket:          "oteldb",
			Prefix:          "data/",
			Region:          "us-east-1",
			Endpoint:        "http://minio:9000",
			ForcePathStyle:  true,
			AccessKeyID:     "key",
			SecretAccessKey: "secret",
		})
		require.NoError(t, err)
		require.NotNil(t, b)
		require.False(t, b.IsEphemeral())
	})

	t.Run("UnknownRetryProfile", func(t *testing.T) {
		_, err := s3Backend(ctx, &StorageS3Config{Bucket: "oteldb", AccessKeyID: "k", SecretAccessKey: "s", Retry: "bogus"})
		require.Error(t, err)
	})

	t.Run("RetryProfiles", func(t *testing.T) {
		for _, profile := range []string{"", "none", "default", "lossy"} {
			b, err := s3Backend(ctx, &StorageS3Config{Bucket: "oteldb", AccessKeyID: "k", SecretAccessKey: "s", Retry: profile})
			require.NoError(t, err, "profile %q", profile)
			require.NotNil(t, b)
		}
	})
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

func TestResolveCacheSettings(t *testing.T) {
	t.Run("DefaultsEnableAll", func(t *testing.T) {
		s := resolveCacheSettings(StorageConfig{})
		// All three are opt-out for oteldb: on by default.
		require.True(t, s.AggregateStats)
		require.Greater(t, s.ReadCache, int64(0))
		require.Greater(t, s.DecodeCache, int64(0))
	})

	t.Run("ExplicitSizesHonored", func(t *testing.T) {
		rc, dc := xbytes.Bytes(1<<20), xbytes.Bytes(2<<20)
		s := resolveCacheSettings(StorageConfig{
			ReadCacheBytes:   &rc,
			DecodeCacheBytes: &dc,
		})
		require.Equal(t, int64(1<<20), s.ReadCache)
		require.Equal(t, int64(2<<20), s.DecodeCache)
	})

	t.Run("ZeroDisablesByteCache", func(t *testing.T) {
		zero := xbytes.Bytes(0)
		s := resolveCacheSettings(StorageConfig{ReadCacheBytes: &zero, DecodeCacheBytes: &zero})
		require.Equal(t, int64(0), s.ReadCache)
		require.Equal(t, int64(0), s.DecodeCache)
	})

	t.Run("AggregateStatsFalseDisables", func(t *testing.T) {
		s := resolveCacheSettings(StorageConfig{AggregateStats: new(bool)})
		require.False(t, s.AggregateStats)
	})
}

// TestDefaultDecodeCacheBytesRSSSafe checks the decode-cache default sizes off detected memory and
// stays clamped to [64 MiB, 512 MiB] — a small box gets the floor rather than an unconditional
// 512 MiB, a large box is capped at the ceiling. See oteldb#1112.
func TestDefaultDecodeCacheBytesRSSSafe(t *testing.T) {
	const (
		floor   = int64(64 << 20)
		ceiling = int64(512 << 20)
	)
	// The test drives the process memory limit directly (detectMemoryBytes reads it); restore after.
	orig := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(orig) })

	for _, tc := range []struct {
		name  string
		limit int64
		want  int64
	}{
		{"small box floored", 1 << 30, floor},        // 1 GiB / 32 = 32 MiB -> floor
		{"mid box proportional", 8 << 30, 256 << 20}, // 8 GiB / 32 = 256 MiB
		{"exact ceiling", 16 << 30, ceiling},         // 16 GiB / 32 = 512 MiB
		{"large box capped", 256 << 30, ceiling},     // 256 GiB / 32 -> capped
	} {
		t.Run(tc.name, func(t *testing.T) {
			debug.SetMemoryLimit(tc.limit)
			require.Equal(t, tc.want, defaultDecodeCacheBytes())
		})
	}

	// With no explicit limit it sizes off detected host RAM, but always within the clamp.
	debug.SetMemoryLimit(math.MaxInt64)
	got := defaultDecodeCacheBytes()
	require.GreaterOrEqual(t, got, floor)
	require.LessOrEqual(t, got, ceiling)
}

func TestCacheOptions(t *testing.T) {
	apply := func(t *testing.T, opts []storage.Option) storage.Options {
		t.Helper()
		var o storage.Options
		for _, opt := range opts {
			opt(&o)
		}
		return o
	}

	t.Run("Enabled", func(t *testing.T) {
		o := apply(t, cacheOptions(cacheSettings{ReadCache: 100, DecodeCache: 200, AggregateStats: true}))
		require.Equal(t, int64(100), o.ReadCacheBytes)
		require.Equal(t, int64(200), o.DecodeCacheBytes)
		require.True(t, o.AggregateStats)
	})

	t.Run("AggregateStatsOffOmitsSidecar", func(t *testing.T) {
		o := apply(t, cacheOptions(cacheSettings{AggregateStats: false}))
		require.False(t, o.AggregateStats, "AggregateStats stays the library default when disabled")
	})
}

// TestStorageCacheConfigYAML checks the cache block parses from the documented YAML shape, including
// the opt-out semantics (an explicit 0 disables a byte cache, aggregate_stats: false disables the
// sidecar).
func TestStorageCacheConfigYAML(t *testing.T) {
	const data = `
storage:
  backend: file
  dir: /data
  read_cache_bytes: "0"
  decode_cache_bytes: "64MiB"
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

	s := resolveCacheSettings(cfg.Storage)
	require.Equal(t, int64(0), s.ReadCache, "explicit 0 disables the read cache")
	require.Equal(t, int64(64<<20), s.DecodeCache)
	require.False(t, s.AggregateStats, "explicit aggregate_stats:false disables the sidecar")
}
