package main

import (
	"cmp"
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/etcd"

	"github.com/oteldb/oteldb/internal/storagebackend"
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
	default:
		return nil, nil, errors.Errorf("unknown storage backend %q", cfg.Backend)
	}

	if clusterOpt, err := clusterOption(cfg.Cluster, lg); err != nil {
		return nil, nil, err
	} else if clusterOpt != nil {
		opts = append(opts, clusterOpt)
	}

	store, err := storage.Open(ctx, storage.Options{}, opts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "open storage")
	}

	lg.Info("Using embedded storage engine for metrics",
		zap.String("backend", cmp.Or(cfg.Backend, "memory")),
		zap.Int("log_query_parallelism", cfg.LogQueryParallelism),
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
