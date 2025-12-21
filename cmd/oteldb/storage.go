package main

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/app"
	"github.com/go-faster/sdk/zctx"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage"
	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/logstorage"
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

type metricQuerier = promql.Querier

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
		Logger:         lg,
	})
	if err != nil {
		return store, errors.Wrap(err, "dial clickhouse")
	}

	// FIXME(tdakkota): this is not a good place for migration
	tables := chstorage.DefaultTables()
	tables.TTL = cfg.TTL
	tables.Cluster = cfg.Cluster
	tables.Replicated = cfg.Replicated
	if tables.Replicated && tables.Cluster == "" {
		tables.Cluster = "{cluster}"
		zctx.From(ctx).Warn("Using default macro for cluster name", zap.String("cluster", tables.Cluster))
	}

	if err := tables.Create(ctx, c); err != nil {
		return store, errors.Wrap(err, "create tables")
	}

	zctx.From(ctx).Info("Tables are ready")

	tracker, err := globalmetric.NewTracker(m.MeterProvider(), m.TracerProvider())
	if err != nil {
		return store, errors.Wrap(err, "create global metric tracker")
	}

	globalmetric.SetTracker(tracker)

	querier, err := chstorage.NewQuerier(c, chstorage.QuerierOptions{
		Tables:            tables,
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
