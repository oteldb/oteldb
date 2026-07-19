// Package ch2storagebackend migrates data out of chstorage's ClickHouse tables into the
// embedded storagebackend engine, by scanning ClickHouse directly (bypassing chstorage's
// selector-oriented queriers) and re-ingesting the decoded records as OTLP pdata.
//
// Logs, traces, and metrics are supported. Metrics are migrated verbatim: chstorage already
// stores them as decomposed Prometheus-style series (histograms/summaries exploded into
// _count/_sum/_bucket{le}/{quantile} series), so each stored series is re-ingested 1:1 as a gauge
// number point, and exponential histograms (stored natively) are reconstructed as OTLP
// exponential-histogram datapoints. Exemplars are not migrated (the target engine drops them).
package ch2storagebackend

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chstorage"
	"github.com/oteldb/oteldb/internal/logstorage"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// LogsStats reports the outcome of a [Migrator.MigrateLogs] run.
type LogsStats struct {
	// Records is the total number of log records migrated.
	Records int
	// Batches is the number of batches ConsumeLogs was called with.
	Batches int
}

// TracesStats reports the outcome of a [Migrator.MigrateTraces] run.
type TracesStats struct {
	// Spans is the total number of spans migrated.
	Spans int
	// Batches is the number of batches ConsumeTraces was called with.
	Batches int
}

// MetricsStats reports the outcome of a [Migrator.MigrateMetrics] run.
type MetricsStats struct {
	// Points is the total number of decomposed number points migrated.
	Points int
	// ExpHistograms is the total number of exponential-histogram datapoints migrated.
	ExpHistograms int
	// Batches is the number of batches ConsumeMetrics was called with.
	Batches int
}

// Migrator copies data from chstorage's ClickHouse tables into a [storagebackend.Backend].
type Migrator struct {
	logs     *chstorage.LogsSource
	traces   *chstorage.TracesSource
	metrics  *chstorage.MetricsSource
	back     *storagebackend.Backend
	logger   *zap.Logger
	throttle time.Duration
}

// Option configures a [Migrator].
type Option func(*Migrator)

// WithThrottle sleeps d after every ConsumeLogs/ConsumeTraces batch. A bulk migration can
// ingest orders of magnitude faster than the storage engine's background flush/compaction
// loop can drain, so without a cap the head grows unbounded in RAM until the process OOMs
// (see [storagebackend], the FlushInterval/FlushThresholdBytes options alone do not apply
// backpressure on the write path). Zero (the default) applies no throttling.
func WithThrottle(d time.Duration) Option {
	return func(m *Migrator) { m.throttle = d }
}

// NewMigrator creates a new [Migrator].
func NewMigrator(client chstorage.ClickHouseClient, tables chstorage.Tables, back *storagebackend.Backend, logger *zap.Logger, opts ...Option) *Migrator {
	if logger == nil {
		logger = zap.NewNop()
	}
	m := &Migrator{
		logs:    chstorage.NewLogsSource(client, tables, logger.Named("logs_source")),
		traces:  chstorage.NewTracesSource(client, tables, logger.Named("traces_source")),
		metrics: chstorage.NewMetricsSource(client, tables, logger.Named("metrics_source")),
		back:    back,
		logger:  logger,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// sleep pauses for the configured throttle, or returns ctx.Err() if ctx is canceled first.
func (m *Migrator) sleep(ctx context.Context) error {
	if m.throttle <= 0 {
		return nil
	}
	t := time.NewTimer(m.throttle)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// MigrateLogs migrates log records stored in ClickHouse into the storagebackend engine,
// converting and ingesting up to batchSize records at a time. When since is positive, only
// the last since of data (relative to the most recent record) is migrated.
func (m *Migrator) MigrateLogs(ctx context.Context, since time.Duration, batchSize int) (LogsStats, error) {
	var stats LogsStats
	err := m.logs.Do(ctx, since, batchSize, func(ctx context.Context, records []logstorage.Record) error {
		ld := logstorage.RecordsToLogs(records)
		if err := m.back.ConsumeLogs(ctx, ld); err != nil {
			return errors.Wrap(err, "consume logs")
		}
		stats.Records += len(records)
		stats.Batches++
		m.logger.Info("Migrated logs batch",
			zap.Int("batch_records", len(records)),
			zap.Int("total_records", stats.Records),
		)
		return m.sleep(ctx)
	})
	if err != nil {
		return stats, errors.Wrap(err, "migrate logs")
	}
	return stats, nil
}

// MigrateTraces migrates spans stored in ClickHouse into the storagebackend engine,
// converting and ingesting up to batchSize spans at a time. When since is positive, only
// the last since of data (relative to the most recent span) is migrated.
func (m *Migrator) MigrateTraces(ctx context.Context, since time.Duration, batchSize int) (TracesStats, error) {
	var stats TracesStats
	err := m.traces.Do(ctx, since, batchSize, func(ctx context.Context, spans []tracestorage.Span) error {
		td := tracestorage.SpansToTraces(spans)
		if err := m.back.ConsumeTraces(ctx, td); err != nil {
			return errors.Wrap(err, "consume traces")
		}
		stats.Spans += len(spans)
		stats.Batches++
		m.logger.Info("Migrated traces batch",
			zap.Int("batch_spans", len(spans)),
			zap.Int("total_spans", stats.Spans),
		)
		return m.sleep(ctx)
	})
	if err != nil {
		return stats, errors.Wrap(err, "migrate traces")
	}
	return stats, nil
}

// MigrateMetrics migrates metrics stored in ClickHouse into the storagebackend engine. Decomposed
// number series (gauges/sums and histogram/summary components) are ingested verbatim as gauge
// datapoints; exponential histograms are reconstructed natively. When since is positive, only the
// last since of data (relative to the most recent point) is migrated.
func (m *Migrator) MigrateMetrics(ctx context.Context, since time.Duration, batchSize int) (MetricsStats, error) {
	var stats MetricsStats
	err := m.metrics.Do(ctx, since, batchSize,
		func(ctx context.Context, points []metricstorage.NumberPoint) error {
			md := metricstorage.NumberPointsToMetrics(points)
			if err := m.back.ConsumeMetrics(ctx, md); err != nil {
				return errors.Wrap(err, "consume metrics")
			}
			stats.Points += len(points)
			stats.Batches++
			m.logger.Info("Migrated metric points batch",
				zap.Int("batch_points", len(points)),
				zap.Int("total_points", stats.Points),
			)
			return m.sleep(ctx)
		},
		func(ctx context.Context, points []metricstorage.ExpHistogramPoint) error {
			md := metricstorage.ExpHistogramsToMetrics(points)
			if err := m.back.ConsumeMetrics(ctx, md); err != nil {
				return errors.Wrap(err, "consume exp histograms")
			}
			stats.ExpHistograms += len(points)
			stats.Batches++
			m.logger.Info("Migrated exp histograms batch",
				zap.Int("batch_points", len(points)),
				zap.Int("total_exp_histograms", stats.ExpHistograms),
			)
			return m.sleep(ctx)
		},
	)
	if err != nil {
		return stats, errors.Wrap(err, "migrate metrics")
	}
	return stats, nil
}
