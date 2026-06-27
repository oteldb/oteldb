package main

import (
	"cmp"
	"context"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	backendfile "github.com/oteldb/storage/backend/file"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// setupStorageBackend constructs the embedded storage engine and an adapter implementing
// oteldb's metric query and ingestion interfaces. The returned close func stops and flushes
// the engine. It is used when [Config.MetricsBackend] is [MetricsBackendStorage].
func setupStorageBackend(ctx context.Context, cfg StorageConfig, lg *zap.Logger) (*storagebackend.Backend, func(context.Context) error, error) {
	var opts []storage.Option
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
		opts = append(opts, storage.WithBackend(fb))
		if cfg.FlushInterval > 0 {
			opts = append(opts, storage.WithFlushInterval(int64(cfg.FlushInterval)))
		}
	default:
		return nil, nil, errors.Errorf("unknown storage backend %q", cfg.Backend)
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
