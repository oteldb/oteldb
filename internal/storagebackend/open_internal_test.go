package storagebackend

import (
	"context"
	"math"
	"os"
	"runtime/debug"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

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

		opt, err = clusterOption(&ClusterConfig{ID: "n1"}, lg) // no etcd endpoints
		require.NoError(t, err)
		require.Nil(t, opt)
	})

	t.Run("Explicit", func(t *testing.T) {
		opt, err := clusterOption(&ClusterConfig{
			Etcd: []string{"http://etcd:2379"}, ID: "node-1", Zone: "z1",
			Addr: "node-1:9000", RF: 2, ShardsPerTenant: 4, Root: "/x", PrivateBackend: true,
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
		require.True(t, o.Cluster.PrivateBackend)
	})

	t.Run("PrivateBackendDefaultsToShared", func(t *testing.T) {
		opt, err := clusterOption(&ClusterConfig{Etcd: []string{"e"}, ID: "n1"}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.False(t, o.Cluster.PrivateBackend, "unset keeps the shared-store model")
	})

	t.Run("AddrDerivedFromID", func(t *testing.T) {
		opt, err := clusterOption(&ClusterConfig{Etcd: []string{"e"}, ID: "node-7"}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.Equal(t, "node-7:7946", o.Cluster.Self.Addr, "default replication port")

		opt, err = clusterOption(&ClusterConfig{Etcd: []string{"e"}, ID: "node-7", Port: 1234}, lg)
		require.NoError(t, err)
		o = applyOption(t, opt)
		require.Equal(t, "node-7:1234", o.Cluster.Self.Addr)
	})

	t.Run("IDAndAddrDefaultToHostname", func(t *testing.T) {
		host, err := os.Hostname()
		require.NoError(t, err)
		opt, err := clusterOption(&ClusterConfig{Etcd: []string{"e"}}, lg)
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.Equal(t, host, o.Cluster.Self.ID)
		require.Equal(t, host+":7946", o.Cluster.Self.Addr)
	})
}

func TestTenancyOption(t *testing.T) {
	t.Run("DisabledWhenEmpty", func(t *testing.T) {
		opt, err := tenancyOption(nil)
		require.NoError(t, err)
		require.Nil(t, opt)

		opt, err = tenancyOption(&PolicyConfig{}) // no tiers, no recompress
		require.NoError(t, err)
		require.Nil(t, opt)
	})

	t.Run("ResolvesPolicyForEveryTenant", func(t *testing.T) {
		opt, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			Precision: []PrecisionTierConfig{
				{After: 7 * 24 * time.Hour, Bits: 32},
				{After: 30 * 24 * time.Hour, Bits: 16},
			},
			Downsample: []DownsampleTierConfig{
				{After: 24 * time.Hour, Interval: 5 * time.Minute, Agg: "avg"},
				{After: 7 * 24 * time.Hour, Interval: time.Hour}, // Agg defaults to last.
			},
			Recompress: &RecompressConfig{After: 14 * 24 * time.Hour, Level: 9},
			EC:         &ECConfig{Data: 4, Parity: 2, After: 30 * 24 * time.Hour},
			Retention:  &RetentionConfig{MaxAge: 90 * 24 * time.Hour, MaxBytes: 1 << 30},
			Limits: &LimitsConfig{
				IngestBytesPerSecond: 10 << 20,
				MaxInFlightBytes:     64 << 20,
				MaxSeries:            1_000_000,
				MaxSeriesSoft:        800_000,
				MaxPartSize:          32 << 20,
				MaxMergePartSize:     512 << 20,
			},
		}})
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

		require.NotNil(t, p.Durability.EC)
		require.Equal(t, 4, p.Durability.EC.Data)
		require.Equal(t, 2, p.Durability.EC.Parity)
		require.Equal(t, 30*24*time.Hour, p.Durability.EC.After)
		// EC fixes the owner count at Data+Parity, so the tenant RF is left alone.
		require.Zero(t, p.Durability.RF)

		require.Equal(t, 90*24*time.Hour, p.Retention.MaxAge)
		require.Equal(t, int64(1<<30), p.Retention.MaxBytes)

		require.Equal(t, int64(10<<20), p.Limits.IngestBytesPerSecond)
		require.Equal(t, int64(64<<20), p.Limits.MaxInFlightBytes)
		require.Equal(t, int64(1_000_000), p.Limits.MaxSeries)
		require.Equal(t, int64(800_000), p.Limits.MaxSeriesSoft)
		require.Equal(t, int64(32<<20), p.Limits.MaxPartSize)
		// Merged parts are capped separately, on disk rather than uncompressed; leaving it zero
		// derives the cap from free space instead.
		require.Equal(t, int64(512<<20), p.Limits.MaxMergePartSize)
	})

	t.Run("RetentionOnlyInstallsResolver", func(t *testing.T) {
		opt, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			Retention: &RetentionConfig{MaxAge: 14 * 24 * time.Hour},
		}})
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.NotNil(t, o.Tenancy, "a retention-only policy must still install a resolver")
		require.Equal(t, 14*24*time.Hour, o.Tenancy.Resolve("default").Retention.MaxAge)
	})

	t.Run("LimitsOnlyInstallsResolver", func(t *testing.T) {
		opt, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			Limits: &LimitsConfig{MaxSeries: 1000},
		}})
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.NotNil(t, o.Tenancy)
		require.Equal(t, int64(1000), o.Tenancy.Resolve("default").Limits.MaxSeries)
	})

	t.Run("ECOnlyInstallsResolver", func(t *testing.T) {
		opt, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			EC: &ECConfig{Data: 6, Parity: 3},
		}})
		require.NoError(t, err)
		o := applyOption(t, opt)
		require.NotNil(t, o.Tenancy, "an EC-only policy must still install a resolver")

		p := o.Tenancy.Resolve("default")
		require.NotNil(t, p.Durability.EC)
		require.Equal(t, 6, p.Durability.EC.Data)
		require.Equal(t, 3, p.Durability.EC.Parity)
		require.Zero(t, p.Durability.EC.After, "zero After erasure-codes every part")
	})

	t.Run("InvalidECSchemeIsAnError", func(t *testing.T) {
		for name, cfg := range map[string]*ECConfig{
			"NoParity":      {Data: 4, Parity: 0},
			"NoData":        {Data: 0, Parity: 2},
			"Negative":      {Data: -1, Parity: 2},
			"TooManyShards": {Data: 200, Parity: 100},
			"NegativeAfter": {Data: 4, Parity: 2, After: -time.Hour},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{EC: cfg}})
				require.Error(t, err)
			})
		}
	})

	t.Run("UnknownAggIsAnError", func(t *testing.T) {
		_, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			Downsample: []DownsampleTierConfig{{Interval: time.Minute, Agg: "median"}},
		}})
		require.Error(t, err)
	})

	t.Run("NegativeRetentionIsAnError", func(t *testing.T) {
		_, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			Retention: &RetentionConfig{MaxAge: -time.Hour},
		}})
		require.Error(t, err)
	})

	t.Run("SoftSeriesAboveHardIsAnError", func(t *testing.T) {
		_, err := tenancyOption(&PolicyConfig{PolicyRules: PolicyRules{
			Limits: &LimitsConfig{MaxSeries: 100, MaxSeriesSoft: 200},
		}})
		require.Error(t, err)
	})
}

func TestWarnECInert(t *testing.T) {
	sharedNothing := &ClusterConfig{Etcd: []string{"http://etcd:2379"}, PrivateBackend: true}
	sharedStore := &ClusterConfig{Etcd: []string{"http://etcd:2379"}}
	ecPolicy := &PolicyConfig{PolicyRules: PolicyRules{EC: &ECConfig{Data: 4, Parity: 2}}}

	for name, tt := range map[string]struct {
		cluster *ClusterConfig
		policy  *PolicyConfig
		warns   bool
	}{
		"SharedNothing":      {sharedNothing, ecPolicy, false},
		"SharedStore":        {sharedStore, ecPolicy, true},
		"SingleNode":         {nil, ecPolicy, true},
		"ClusterWithoutEtcd": {&ClusterConfig{ID: "n1"}, ecPolicy, true},
		"NoECPolicy":         {sharedStore, &PolicyConfig{PolicyRules: PolicyRules{Retention: &RetentionConfig{}}}, false},
		"NoPolicy":           {sharedStore, nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			core, logs := observer.New(zap.WarnLevel)
			warnECInert(tt.cluster, tt.policy, zap.New(core))
			if !tt.warns {
				require.Zero(t, logs.Len())
				return
			}
			require.Equal(t, 1, logs.Len())
			require.Contains(t, logs.All()[0].Message, "erasure coding")
		})
	}
}

func TestS3Backend(t *testing.T) {
	ctx := context.Background()

	t.Run("RequiresBucket", func(t *testing.T) {
		_, err := s3Backend(ctx, nil)
		require.Error(t, err)

		_, err = s3Backend(ctx, &S3Config{Region: "us-east-1"}) // no bucket
		require.Error(t, err)
	})

	t.Run("StaticCredentials", func(t *testing.T) {
		b, err := s3Backend(ctx, &S3Config{
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
		_, err := s3Backend(ctx, &S3Config{Bucket: "oteldb", AccessKeyID: "k", SecretAccessKey: "s", Retry: "bogus"})
		require.Error(t, err)
	})

	t.Run("RetryProfiles", func(t *testing.T) {
		for _, profile := range []string{"", "none", "default", "lossy"} {
			b, err := s3Backend(ctx, &S3Config{Bucket: "oteldb", AccessKeyID: "k", SecretAccessKey: "s", Retry: profile})
			require.NoError(t, err, "profile %q", profile)
			require.NotNil(t, b)
		}
	})
}

func TestResolveCacheSettings(t *testing.T) {
	t.Run("DefaultsEnableAll", func(t *testing.T) {
		s := resolveCacheSettings(Config{})
		// All four are opt-out for oteldb: on by default.
		require.True(t, s.AggregateStats)
		require.Greater(t, s.ReadCache, int64(0))
		require.Greater(t, s.DecodeCache, int64(0))
		require.Greater(t, s.DecodeMemory, int64(0))
		// Merge memory is the exception: unset stays 0 so the library derives it.
		require.Equal(t, int64(0), s.MergeMemory)
	})

	t.Run("MergeMemoryPassesThrough", func(t *testing.T) {
		mm := xbytes.Bytes(4 << 20)
		s := resolveCacheSettings(Config{MergeMemoryBytes: &mm})
		require.Equal(t, int64(4<<20), s.MergeMemory)

		unbounded := xbytes.Bytes(-1)
		s = resolveCacheSettings(Config{MergeMemoryBytes: &unbounded})
		require.Equal(t, int64(-1), s.MergeMemory, "negative means unbounded, not unset")
	})

	t.Run("ExplicitSizesHonored", func(t *testing.T) {
		rc, dc, dm := xbytes.Bytes(1<<20), xbytes.Bytes(2<<20), xbytes.Bytes(3<<20)
		s := resolveCacheSettings(Config{
			ReadCacheBytes:    &rc,
			DecodeCacheBytes:  &dc,
			DecodeMemoryBytes: &dm,
		})
		require.Equal(t, int64(1<<20), s.ReadCache)
		require.Equal(t, int64(2<<20), s.DecodeCache)
		require.Equal(t, int64(3<<20), s.DecodeMemory)
	})

	t.Run("ZeroDisablesByteCache", func(t *testing.T) {
		zero := xbytes.Bytes(0)
		s := resolveCacheSettings(Config{ReadCacheBytes: &zero, DecodeCacheBytes: &zero, DecodeMemoryBytes: &zero})
		require.Equal(t, int64(0), s.ReadCache)
		require.Equal(t, int64(0), s.DecodeCache)
		require.Equal(t, int64(0), s.DecodeMemory, "explicit 0 disables decode admission control")
	})

	t.Run("AggregateStatsFalseDisables", func(t *testing.T) {
		s := resolveCacheSettings(Config{AggregateStats: new(bool)})
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

// TestDefaultDecodeMemoryBytesFitsLimit checks the decode-admission budget default fits the process
// memory budget: half the detected limit minus the caches, floored at 64 MiB, so under concurrent
// query load the live heap stays under GOMEMLIMIT instead of tripping the pacer. See oteldb#1124.
func TestDefaultDecodeMemoryBytesFitsLimit(t *testing.T) {
	const floor = int64(64 << 20)
	// The test drives the process memory limit directly (detectMemoryBytes reads it); restore after.
	orig := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(orig) })

	for _, tc := range []struct {
		name                   string
		limit                  int64
		readCache, decodeCache int64
		want                   int64
	}{
		// The benchmark box (#1124): 1 GiB limit with the default caches leaves 512−128−64.
		{"1GiB fits around caches", 1 << 30, 128 << 20, 64 << 20, 320 << 20},
		{"8GiB scales up", 8 << 30, 512 << 20, 256 << 20, 3328 << 20},
		{"small box floored", 256 << 20, 128 << 20, 64 << 20, floor},
		{"caches larger than half floored", 1 << 30, 512 << 20, 512 << 20, floor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			debug.SetMemoryLimit(tc.limit)
			require.Equal(t, tc.want, defaultDecodeMemoryBytes(tc.readCache, tc.decodeCache))
		})
	}

	// With no explicit limit it sizes off detected host RAM: still enabled, still floored.
	debug.SetMemoryLimit(math.MaxInt64)
	require.GreaterOrEqual(t, defaultDecodeMemoryBytes(128<<20, 64<<20), floor)
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
		o := apply(t, cacheOptions(cacheSettings{
			ReadCache: 100, DecodeCache: 200, DecodeMemory: 300, MergeMemory: 400, AggregateStats: true,
		}))
		require.Equal(t, int64(100), o.ReadCacheBytes)
		require.Equal(t, int64(200), o.DecodeCacheBytes)
		require.Equal(t, int64(300), o.DecodeMemoryBytes)
		require.Equal(t, int64(400), o.MergeMemoryBytes)
		require.True(t, o.AggregateStats)
	})

	t.Run("MergeMemoryUnsetLeavesLibraryDefault", func(t *testing.T) {
		o := apply(t, cacheOptions(cacheSettings{DecodeMemory: 300}))
		require.Equal(t, int64(0), o.MergeMemoryBytes, "0 lets storage size merge memory from GOMEMLIMIT")
	})

	t.Run("AggregateStatsOffOmitsSidecar", func(t *testing.T) {
		o := apply(t, cacheOptions(cacheSettings{AggregateStats: false}))
		require.False(t, o.AggregateStats, "AggregateStats stays the library default when disabled")
	})
}
