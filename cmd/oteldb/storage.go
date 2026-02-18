package main

import (
	"context"
	"os"
	"strings"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/promql"
	"github.com/go-faster/oteldb/internal/traceql/traceqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

type otelStorage struct {
	logQuerier     logQuerier
	traceQuerier   traceQuerier
	metricsQuerier metricQuerier
}

type logQuerier interface {
	logstorage.Querier
	logqlengine.Querier
}

type traceQuerier interface {
	tracestorage.Querier
	traceqlengine.Querier
}

type metricQuerier interface {
	promql.Querier
	metricstorage.MetadataQuerier
}

func setupCH(
	ctx context.Context,
	dsn string,
	cfg Config,
	lg *zap.Logger,
	m *app.Telemetry,
) (store otelStorage, _ error) {
	c, err := chstorage.Dial(ctx, dsn, chstorage.DialOptions{
		MeterProvider:  m.MeterProvider(),
		TracerProvider: m.TracerProvider(),
		Logger:         lg.WithOptions(zap.IncreaseLevel(cfg.CHLogLevel)),
	})
	if err != nil {
		return store, errors.Wrap(err, "dial clickhouse")
	}

	tables := chstorage.DefaultTables()
	if err := migrate(ctx, c, tables, cfg); err != nil {
		return store, errors.Wrap(err, "migrate schema")
	}

	tracker, err := globalmetric.NewTracker(m.MeterProvider(), m.TracerProvider())
	if err != nil {
		return store, errors.Wrap(err, "create global metric tracker")
	}
	globalmetric.SetTracker(tracker)

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:            tables,
		CHLogLevel:        cfg.CHLogLevel,
		MeterProvider:     m.MeterProvider(),
		TracerProvider:    m.TracerProvider(),
		Tracker:           tracker,
		MetricSeriesLimit: cfg.Prometheus.MaxTimeseries,
	})
	if err != nil {
		return store, errors.Wrap(err, "create querier")
	}

	return otelStorage{
		logQuerier:     querier,
		traceQuerier:   querier,
		metricsQuerier: querier,
	}, nil
}

func migrate(ctx context.Context, client chstorage.ClickHouseClient, tables chstorage.Tables, cfg Config) error {
	lg := zctx.From(ctx).Named("migrator").WithOptions(zap.IncreaseLevel(cfg.CHLogLevel))
	ctx = zctx.Base(ctx, lg)

	migratorCfg := chstorage.MigratorOptions{
		Tables:     tables,
		Cluster:    cfg.Cluster,
		Replicated: cfg.Replicated,
		TTL:        cfg.TTL,
	}
	if migratorCfg.Replicated && migratorCfg.Cluster == "" {
		migratorCfg.Cluster = "{cluster}"
		lg.Warn("Using default macro for cluster name", zap.String("cluster", migratorCfg.Cluster))
	}

	migrator := chstorage.NewMigrator(client, migratorCfg)
	switch mode := os.Getenv("OTELDB_MIGRATION_MODE"); strings.ToLower(mode) {
	case "none":
		// Do not perform any schema migration at all.
		lg.Info("Migration is disabled, validating existing schema")
		if err := migrator.Validate(ctx); err != nil {
			return errors.Wrap(err, "validate existing schema")
		}
		lg.Info("Existing schema is compatible")
		return nil
	case "", "auto":
		lg.Info("Performing automatic migration if needed")
		// Best effort to migrate schema, but do fail if there is existing database with incompatible schema.
		if err := migrator.Create(ctx); err != nil {
			return errors.Wrap(err, "perform a migration")
		}
		lg.Info("Migration completed")
		return nil
	case "test", "debug":
		// In test and debug modes, we want to recreate tables on each run to ensure that schema is up to date.
		lg.Info("Recreating tables for test/debug mode")
		if err := migrator.DropIfExists(ctx, func(database, table string) {
			lg.Warn("DROPPING table", zap.String("database", database), zap.String("table", table))
		}); err != nil {
			return errors.Wrap(err, "drop existing schema")
		}
		if err := migrator.Create(ctx); err != nil {
			return errors.Wrap(err, "create schema")
		}
		lg.Info("Migration completed")
		return nil
	default:
		return errors.Errorf("unknown migration mode %q", mode)
	}
}
