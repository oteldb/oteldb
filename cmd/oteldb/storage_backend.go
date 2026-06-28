package main

import (
	"cmp"
	"context"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	backends3 "github.com/oteldb/storage/backend/s3"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"
	"github.com/oteldb/storage/reliability"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/xbytes"
)

// setupStorageBackend constructs the embedded storage engine and an adapter implementing
// oteldb's metric query and ingestion interfaces. The returned close func stops and flushes
// the engine. It is used when [Config.MetricsBackend] is [MetricsBackendStorage].
func setupStorageBackend(ctx context.Context, cfg StorageConfig, lg *zap.Logger, m *app.Telemetry) (*storagebackend.Backend, func(context.Context) error, error) {
	// The engine logs, traces, and meters through the injected providers (no-op if absent).
	opts := []storage.Option{
		storage.WithLogger(lg),
		storage.WithTracerProvider(m.TracerProvider()),
		storage.WithMeterProvider(m.MeterProvider()),
	}
	switch cfg.Backend {
	case "", "memory":
		opts = append(opts,
			storage.WithBackend(backend.Memory()),
			storage.WithDurability(storage.DurabilityEphemeral),
		)
	case "file":
		if cfg.Dir == "" {
			return nil, nil, errors.New("storage.dir is required for the file backend")
		}
		fb, err := backendfile.New(cfg.Dir)
		if err != nil {
			return nil, nil, errors.Wrap(err, "open file backend")
		}
		// Keep the WAL alongside the parts so the unflushed head survives a restart, not just the
		// flushed parts.
		opts = append(opts,
			storage.WithBackend(fb),
			storage.WithWALDir(filepath.Join(cfg.Dir, "wal")),
		)
		if cfg.FlushInterval > 0 {
			opts = append(opts, storage.WithFlushInterval(int64(cfg.FlushInterval)))
		}
	case "s3":
		sb, err := s3Backend(ctx, cfg.S3)
		if err != nil {
			return nil, nil, errors.Wrap(err, "open s3 backend")
		}
		opts = append(opts, storage.WithBackend(sb))
		// The object store is stateless; keep an optional local WAL so the unflushed head survives a
		// restart rather than only the flushed objects.
		if cfg.WALDir != "" {
			opts = append(opts, storage.WithWALDir(cfg.WALDir))
		}
		if cfg.FlushInterval > 0 {
			opts = append(opts, storage.WithFlushInterval(int64(cfg.FlushInterval)))
		}
	default:
		return nil, nil, errors.Errorf("unknown storage backend %q", cfg.Backend)
	}

	if clusterOpt, err := clusterOption(cfg.Cluster, lg); err != nil {
		return nil, nil, err
	} else if clusterOpt != nil {
		opts = append(opts, clusterOpt)
	}

	if tenancyOpt, err := tenancyOption(cfg.Policy); err != nil {
		return nil, nil, errors.Wrap(err, "storage policy")
	} else if tenancyOpt != nil {
		opts = append(opts, tenancyOpt)
		lg.Info("Applying embedded storage tenancy policy",
			zap.Int("precision_tiers", len(cfg.Policy.Precision)),
			zap.Int("downsample_tiers", len(cfg.Policy.Downsample)),
			zap.Bool("recompress", cfg.Policy.Recompress != nil),
		)
	}

	caches := resolveCacheSettings(cfg)
	opts = append(opts, cacheOptions(caches)...)

	store, err := storage.Open(ctx, storage.Options{}, opts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "open storage")
	}

	lg.Info("Using embedded storage engine for metrics",
		zap.String("backend", cmp.Or(cfg.Backend, "memory")),
		zap.Int("log_query_parallelism", cfg.LogQueryParallelism),
		zap.Int64("read_cache_bytes", caches.ReadCache),
		zap.Int64("decode_cache_bytes", caches.DecodeCache),
		zap.Bool("aggregate_stats", caches.AggregateStats),
	)
	b := storagebackend.New(store,
		storagebackend.WithLogParallelism(cfg.LogQueryParallelism),
	)
	return b, store.Close, nil
}

// clusterOption builds the storage cluster option from the config, or returns (nil, nil) when
// clustering is not configured (no etcd endpoints). The node identity and replication address
// default to the OS hostname, so every node in a deployment can share one config file.
func clusterOption(cfg *StorageClusterConfig, lg *zap.Logger) (storage.Option, error) {
	if cfg == nil || len(cfg.Etcd) == 0 {
		return nil, nil
	}

	id := cfg.ID
	if id == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, errors.Wrap(err, "resolve hostname for cluster id")
		}
		id = host
	}

	addr := cfg.Addr
	if addr == "" {
		port := cfg.Port
		if port == 0 {
			port = 7946
		}
		addr = net.JoinHostPort(id, strconv.Itoa(port))
	}

	lg.Info("Joining storage cluster",
		zap.Strings("etcd", cfg.Etcd),
		zap.String("id", id),
		zap.String("zone", cfg.Zone),
		zap.String("addr", addr),
		zap.Int("rf", cfg.RF),
		zap.Int("shards_per_tenant", cfg.ShardsPerTenant),
	)
	return storage.WithCluster(&cluster.Config{
		Etcd:            cfg.Etcd,
		Self:            etcd.Member{ID: id, Zone: cfg.Zone, Addr: addr},
		RF:              cfg.RF,
		ShardsPerTenant: cfg.ShardsPerTenant,
		Root:            cfg.Root,
	}), nil
}

// s3Backend builds the embedded storage engine's S3 object-store backend from the config. It uses
// static credentials when both keys are set, otherwise the default AWS credential chain
// (environment, shared config, IAM role), and applies the requested resilience profile.
func s3Backend(ctx context.Context, cfg *StorageS3Config) (backend.Backend, error) {
	if cfg == nil || cfg.Bucket == "" {
		return nil, errors.New("storage.s3.bucket is required for the s3 backend")
	}

	o := awss3.Options{
		Region:       cfg.Region,
		UsePathStyle: cfg.ForcePathStyle,
	}
	if cfg.Endpoint != "" {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		o.Credentials = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	} else {
		// Fall back to the default chain (env, shared config, IAM role); resolved lazily on first use.
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "load aws config")
		}
		o.Credentials = awsCfg.Credentials
		if o.Region == "" {
			o.Region = awsCfg.Region
		}
	}

	var s3Opts []backends3.Option
	switch cfg.Retry {
	case "", "none":
		// No extra resilience layer; the AWS SDK's own retryer still applies.
	case "default":
		s3Opts = append(s3Opts, backends3.WithRetry(reliability.Default()))
	case "lossy":
		s3Opts = append(s3Opts, backends3.WithRetry(reliability.LossyEnvironment()))
	default:
		return nil, errors.Errorf("unknown s3 retry profile %q", cfg.Retry)
	}

	store := backends3.NewAWS(awss3.New(o), cfg.Bucket)
	return backends3.New(store, cfg.Prefix, s3Opts...), nil
}

// cacheSettings is the resolved, effective cache/optimization configuration that oteldb applies to
// the storage engine. Unlike the storage library (where these are opt-in), oteldb enables all three
// by default and treats them as opt-out.
type cacheSettings struct {
	ReadCache      int64
	DecodeCache    int64
	AggregateStats bool
}

// resolveCacheSettings resolves the cache/optimization settings from config. Unset fields are sized
// from the Go memory limit (e.g. a cgroup limit applied by automemlimit), falling back to
// conservative absolute defaults when no limit is detectable. An explicit zero byte size disables
// that cache, and an explicit AggregateStats=false disables the sidecar.
func resolveCacheSettings(cfg StorageConfig) cacheSettings {
	return cacheSettings{
		ReadCache:      resolveCacheBytes(cfg.ReadCacheBytes, defaultReadCacheBytes),
		DecodeCache:    resolveCacheBytes(cfg.DecodeCacheBytes, defaultDecodeCacheBytes),
		AggregateStats: cfg.AggregateStats == nil || *cfg.AggregateStats,
	}
}

// cacheOptions builds the storage options for the resolved cache/optimization settings.
func cacheOptions(s cacheSettings) []storage.Option {
	opts := []storage.Option{
		storage.WithReadCache(s.ReadCache),
		storage.WithDecodeCache(s.DecodeCache),
	}
	if s.AggregateStats {
		opts = append(opts, storage.WithAggregateStats())
	}
	return opts
}

// resolveCacheBytes returns an explicit configured byte size (including 0 to disable) or, when
// unset, the default returned by def.
func resolveCacheBytes(cfg *xbytes.Bytes, def func() int64) int64 {
	if cfg != nil {
		return int64(*cfg)
	}
	return def()
}

// defaultReadCacheBytes sizes the backend object read cache (~1/16 of RAM, floor 128 MiB).
func defaultReadCacheBytes() int64 { return defaultCacheBytes(1.0/16, 128<<20) }

// defaultDecodeCacheBytes sizes the per-tenant decoded-column cache (~1/32 of RAM, floor 64 MiB).
func defaultDecodeCacheBytes() int64 { return defaultCacheBytes(1.0/32, 64<<20) }

// defaultCacheBytes sizes a cache as the given fraction of the Go memory limit, floored at minBytes.
// When no limit is set (the default of math.MaxInt64, meaning unbounded), it returns minBytes so the
// cache gets a sane absolute size rather than a RAM-proportional blow-up.
func defaultCacheBytes(fraction float64, minBytes int64) int64 {
	limit := debug.SetMemoryLimit(-1)
	if limit <= 0 || limit == math.MaxInt64 {
		return minBytes
	}
	if v := int64(float64(limit) * fraction); v > minBytes {
		return v
	}
	return minBytes
}
