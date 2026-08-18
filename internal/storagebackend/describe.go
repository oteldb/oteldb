package storagebackend

import (
	"github.com/go-faster/figureout"
)

// The describe functions below register the embedded engine's fields against the schema of
// whatever root embeds it, the way the API blocks in internal/config do: figureout binds by
// pointer address inside the root, so a block stays a function over the root's schema rather than
// a descriptor of its own.
//
// Nest the whole thing under its own key by wrapping the call in [figureout.Group]:
//
//	figureout.Group(s, "storage", func(s *figureout.Schema[Config]) {
//		storagebackend.DescribeConfig(s, &c.Storage)
//	})

// DescribeConfig registers the embedded storage engine block.
//
// The pointer fields are pointers because nil is a value: an unset cache size is "fit it to the
// machine", not zero, and an unset policy is the library's default rather than a policy that
// configures nothing. They are registered as such so that resolution keeps the difference.
func DescribeConfig[R any](s *figureout.Schema[R], cfg *Config) {
	figureout.Value(s, &cfg.Backend, "backend")
	figureout.Value(s, &cfg.Dir, "dir")
	figureout.Value(s, &cfg.WALDir, "wal_dir")
	figureout.OptionalObjectFunc(s, &cfg.S3, "s3", describeS3)
	figureout.Value(s, &cfg.FlushInterval, "flush_interval")
	figureout.Value(s, &cfg.LogQueryParallelism, "log_query_parallelism")
	figureout.OptionalPtr(s, &cfg.ReadCacheBytes, "read_cache_bytes")
	figureout.OptionalPtr(s, &cfg.DecodeCacheBytes, "decode_cache_bytes")
	figureout.OptionalPtr(s, &cfg.DecodeMemoryBytes, "decode_memory_bytes")
	figureout.OptionalPtr(s, &cfg.MergeMemoryBytes, "merge_memory_bytes")
	figureout.OptionalPtr(s, &cfg.AggregateStats, "aggregate_stats")
	figureout.OptionalObjectFunc(s, &cfg.Policy, "policy", describePolicy)
	figureout.OptionalObjectFunc(s, &cfg.Cluster, "cluster", describeCluster)
}

func describeS3(cfg *S3Config, s *figureout.Schema[S3Config]) {
	figureout.Value(s, &cfg.Bucket, "bucket")
	figureout.Value(s, &cfg.Prefix, "prefix")
	figureout.Value(s, &cfg.Region, "region")
	figureout.Value(s, &cfg.Endpoint, "endpoint")
	figureout.Value(s, &cfg.ForcePathStyle, "force_path_style")
	figureout.Value(s, &cfg.AccessKeyID, "access_key_id")
	figureout.Value(s, &cfg.SecretAccessKey, "secret_access_key", figureout.Secret())
	figureout.Value(s, &cfg.SessionToken, "session_token", figureout.Secret())
	figureout.Value(s, &cfg.Retry, "retry")
}

func describeCluster(cfg *ClusterConfig, s *figureout.Schema[ClusterConfig]) {
	figureout.Value(s, &cfg.Etcd, "etcd")
	figureout.Value(s, &cfg.ID, "id")
	figureout.Value(s, &cfg.Zone, "zone")
	figureout.Value(s, &cfg.Addr, "addr")
	figureout.Value(s, &cfg.Port, "port")
	figureout.Value(s, &cfg.RF, "rf")
	figureout.Value(s, &cfg.ShardsPerTenant, "shards_per_tenant")
	figureout.Value(s, &cfg.Root, "root")
	figureout.Value(s, &cfg.PrivateBackend, "private_backend")
}

func describePolicy(cfg *PolicyConfig, s *figureout.Schema[PolicyConfig]) {
	figureout.ListOf(s, &cfg.Precision, "precision", describePrecisionTier)
	figureout.ListOf(s, &cfg.Downsample, "downsample", describeDownsampleTier)
	figureout.OptionalObjectFunc(s, &cfg.Recompress, "recompress", describeRecompress)
	figureout.OptionalObjectFunc(s, &cfg.EC, "ec", describeEC)
	figureout.OptionalObjectFunc(s, &cfg.Retention, "retention", describeRetention)
	figureout.OptionalObjectFunc(s, &cfg.Limits, "limits", describeLimits)
}

func describePrecisionTier(cfg *PrecisionTierConfig, s *figureout.Schema[PrecisionTierConfig]) {
	figureout.Value(s, &cfg.After, "after")
	figureout.Value(s, &cfg.Bits, "bits")
}

func describeDownsampleTier(cfg *DownsampleTierConfig, s *figureout.Schema[DownsampleTierConfig]) {
	figureout.Value(s, &cfg.After, "after")
	figureout.Value(s, &cfg.Interval, "interval")
	figureout.Value(s, &cfg.Agg, "agg")
}

func describeRecompress(cfg *RecompressConfig, s *figureout.Schema[RecompressConfig]) {
	figureout.Value(s, &cfg.After, "after")
	figureout.Value(s, &cfg.Level, "level")
}

func describeEC(cfg *ECConfig, s *figureout.Schema[ECConfig]) {
	figureout.Value(s, &cfg.Data, "data")
	figureout.Value(s, &cfg.Parity, "parity")
	figureout.Value(s, &cfg.After, "after")
}

func describeRetention(cfg *RetentionConfig, s *figureout.Schema[RetentionConfig]) {
	figureout.Value(s, &cfg.MaxAge, "max_age")
	figureout.Value(s, &cfg.MaxBytes, "max_bytes")
}

func describeLimits(cfg *LimitsConfig, s *figureout.Schema[LimitsConfig]) {
	figureout.Value(s, &cfg.IngestBytesPerSecond, "ingest_bytes_per_second")
	figureout.Value(s, &cfg.MaxInFlightBytes, "max_in_flight_bytes")
	figureout.Value(s, &cfg.MaxSeries, "max_series")
	figureout.Value(s, &cfg.MaxSeriesSoft, "max_series_soft")
	figureout.Value(s, &cfg.MaxPartSize, "max_part_size")
	figureout.Value(s, &cfg.MaxMergePartSize, "max_merge_part_size")
}
