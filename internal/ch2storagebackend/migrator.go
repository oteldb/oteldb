// Package ch2storagebackend migrates data out of chstorage's ClickHouse tables into the
// embedded storagebackend engine, by scanning ClickHouse directly (bypassing chstorage's
// selector-oriented queriers) and re-ingesting the decoded rows.
//
// Rows are converted straight into the engine's native signal types (see convert.go). The one
// exception is exponential histograms, which go through OTLP pdata because the engine decomposes
// them into classic bucket series inside its own bridge.
//
// Logs, traces, and metrics are supported. Metrics are migrated verbatim: chstorage already
// stores them as decomposed Prometheus-style series (histograms/summaries exploded into
// _count/_sum/_bucket{le}/{quantile} series), so each stored series is re-ingested 1:1 as a gauge
// number point, and exponential histograms (stored natively) are reconstructed as OTLP
// exponential-histogram datapoints. Exemplars are not migrated (the target engine drops them).
//
// A migration is driven one UTC day at a time. That is the unit of work, of progress reporting,
// and of resumption: with a [Checkpoint] configured, a completed day is journalled once its data is
// durable in the target, and a restarted run skips it. See [Estimate] for the pre-flight sizing
// that says what a given window will cost before it is started.
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

// Signal names, as recorded in the [Checkpoint] journal.
const (
	SignalLogs    = "logs"
	SignalTraces  = "traces"
	SignalMetrics = "metrics"
)

// LogsStats reports the outcome of a [Migrator.MigrateLogs] run.
type LogsStats struct {
	// Records is the total number of log records migrated.
	Records int
	// Batches is the number of batches written to the target.
	Batches int
	// DaysDone is the number of UTC days migrated; DaysSkipped were already in the checkpoint.
	DaysDone    int
	DaysSkipped int
}

// TracesStats reports the outcome of a [Migrator.MigrateTraces] run.
type TracesStats struct {
	// Spans is the total number of spans migrated.
	Spans int
	// Batches is the number of batches written to the target.
	Batches int
	// DaysDone is the number of UTC days migrated; DaysSkipped were already in the checkpoint.
	DaysDone    int
	DaysSkipped int
}

// MetricsStats reports the outcome of a [Migrator.MigrateMetrics] run.
type MetricsStats struct {
	// Points is the total number of decomposed number points migrated.
	Points int
	// ExpHistograms is the total number of exponential-histogram datapoints migrated.
	ExpHistograms int
	// Batches is the number of batches written to the target.
	Batches int
	// DaysDone is the number of UTC days migrated; DaysSkipped were already in the checkpoint.
	DaysDone    int
	DaysSkipped int
}

// Migrator copies data from chstorage's ClickHouse tables into a [storagebackend.Backend].
type Migrator struct {
	logs       *chstorage.LogsSource
	traces     *chstorage.TracesSource
	metrics    *chstorage.MetricsSource
	back       *storagebackend.Backend
	logger     *zap.Logger
	throttle   time.Duration
	checkpoint *Checkpoint
	sync       func(context.Context) error
	// attrs memoizes attribute-set projections across the whole migration (see convert.go). It is
	// shared by every signal: a resource attribute set is typically common to all of them.
	attrs *attrConv
}

// Option configures a [Migrator].
type Option func(*Migrator)

// WithThrottle sleeps d after every ingested batch. A bulk migration can
// ingest orders of magnitude faster than the storage engine's background flush/compaction
// loop can drain, so without a cap the head grows unbounded in RAM until the process OOMs
// (see [storagebackend], the FlushInterval/FlushThresholdBytes options alone do not apply
// backpressure on the write path). Zero (the default) applies no throttling.
func WithThrottle(d time.Duration) Option {
	return func(m *Migrator) { m.throttle = d }
}

// WithCheckpoint makes the migration resumable: completed UTC days are journalled to c and skipped
// on a later run. Without a sync hook (see [WithSync]) a day is journalled once it has been handed
// to the target engine, which is only as durable as the engine's own flush cadence — pair the two
// for a resume point that cannot lose data.
func WithCheckpoint(c *Checkpoint) Option {
	return func(m *Migrator) { m.checkpoint = c }
}

// WithSync installs a barrier run at the end of each UTC day, before that day is marked complete.
// It should make everything ingested so far durable in the target (for the embedded engine, flush
// every tenant/signal head to a part). Besides making the checkpoint trustworthy, it bounds the
// head at one day's data rather than letting it grow for the whole migration.
func WithSync(fn func(context.Context) error) Option {
	return func(m *Migrator) { m.sync = fn }
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
		attrs:   newAttrConv(),
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

// finishDay runs the durability barrier and journals the day as complete, in that order.
func (m *Migrator) finishDay(ctx context.Context, signal string, day time.Time, rows int) error {
	if m.sync != nil {
		if err := m.sync(ctx); err != nil {
			return errors.Wrap(err, "sync target")
		}
	}
	if err := m.checkpoint.Mark(signal, day, rows); err != nil {
		return errors.Wrap(err, "mark checkpoint")
	}
	return nil
}

// plan resolves w against a source's range and returns the day slices to scan. ok is false when
// the source is empty or the window selects nothing.
func plan(w chstorage.Window, mint, maxt time.Time) (days []chstorage.DayRange, from, to time.Time, ok bool) {
	if mint.IsZero() && maxt.IsZero() {
		return nil, from, to, false
	}
	from, to, ok = w.Resolve(mint, maxt)
	if !ok {
		return nil, from, to, false
	}
	return chstorage.Days(from, to), from, to, true
}

// dayLogger annotates the logger with the slice being migrated, so per-day progress lines carry
// their position in the run.
func dayLogger(lg *zap.Logger, day time.Time, i, total int) *zap.Logger {
	return lg.With(
		zap.Time("day", day),
		zap.String("progress", formatProgress(i+1, total)),
	)
}

// MigrateLogs migrates log records stored in ClickHouse into the storagebackend engine, one UTC day
// at a time, converting and ingesting up to batchSize records at a time. The window w selects which
// span of the table to migrate; the zero window migrates all of it.
func (m *Migrator) MigrateLogs(ctx context.Context, w chstorage.Window, batchSize int) (LogsStats, error) {
	var stats LogsStats

	mint, maxt, err := m.logs.Range(ctx)
	if err != nil {
		return stats, errors.Wrap(err, "resolve range")
	}
	days, from, to, ok := plan(w, mint, maxt)
	if !ok {
		m.logger.Info("No logs to migrate")
		return stats, nil
	}
	m.logger.Info("Migrating logs",
		zap.Time("from", from), zap.Time("to", to), zap.Int("days", len(days)),
	)

	for i, d := range days {
		lg := dayLogger(m.logger, d.Day, i, len(days))
		if m.checkpoint.Done(SignalLogs, d.Day) {
			stats.DaysSkipped++
			lg.Info("Skipping day, already migrated")
			continue
		}

		dayRecords := 0
		err := m.logs.Scan(ctx, d.From, d.To, batchSize, func(ctx context.Context, records []logstorage.Record) error {
			if err := m.back.WriteLogs(ctx, ConvertLogs(records, m.attrs)); err != nil {
				return errors.Wrap(err, "write logs")
			}
			dayRecords += len(records)
			stats.Records += len(records)
			stats.Batches++
			return m.sleep(ctx)
		})
		if err != nil {
			return stats, errors.Wrapf(err, "migrate logs %s", d.Day.Format(time.DateOnly))
		}

		if err := m.finishDay(ctx, SignalLogs, d.Day, dayRecords); err != nil {
			return stats, errors.Wrapf(err, "finish logs %s", d.Day.Format(time.DateOnly))
		}
		stats.DaysDone++
		lg.Info("Migrated day",
			zap.Int("day_records", dayRecords),
			zap.Int("total_records", stats.Records),
		)
	}
	return stats, nil
}

// MigrateTraces migrates spans stored in ClickHouse into the storagebackend engine, one UTC day at
// a time, converting and ingesting up to batchSize spans at a time. The window w selects which span
// of the table to migrate; the zero window migrates all of it.
func (m *Migrator) MigrateTraces(ctx context.Context, w chstorage.Window, batchSize int) (TracesStats, error) {
	var stats TracesStats

	mint, maxt, err := m.traces.Range(ctx)
	if err != nil {
		return stats, errors.Wrap(err, "resolve range")
	}
	days, from, to, ok := plan(w, mint, maxt)
	if !ok {
		m.logger.Info("No traces to migrate")
		return stats, nil
	}
	m.logger.Info("Migrating traces",
		zap.Time("from", from), zap.Time("to", to), zap.Int("days", len(days)),
	)

	for i, d := range days {
		lg := dayLogger(m.logger, d.Day, i, len(days))
		if m.checkpoint.Done(SignalTraces, d.Day) {
			stats.DaysSkipped++
			lg.Info("Skipping day, already migrated")
			continue
		}

		daySpans := 0
		err := m.traces.Scan(ctx, d.From, d.To, batchSize, func(ctx context.Context, spans []tracestorage.Span) error {
			if err := m.back.WriteTraces(ctx, ConvertTraces(spans, m.attrs)); err != nil {
				return errors.Wrap(err, "write traces")
			}
			daySpans += len(spans)
			stats.Spans += len(spans)
			stats.Batches++
			return m.sleep(ctx)
		})
		if err != nil {
			return stats, errors.Wrapf(err, "migrate traces %s", d.Day.Format(time.DateOnly))
		}

		if err := m.finishDay(ctx, SignalTraces, d.Day, daySpans); err != nil {
			return stats, errors.Wrapf(err, "finish traces %s", d.Day.Format(time.DateOnly))
		}
		stats.DaysDone++
		lg.Info("Migrated day",
			zap.Int("day_spans", daySpans),
			zap.Int("total_spans", stats.Spans),
		)
	}
	return stats, nil
}

// MigrateMetrics migrates metrics stored in ClickHouse into the storagebackend engine, one UTC day
// at a time. Decomposed number series (gauges/sums and histogram/summary components) are ingested
// verbatim as gauge datapoints; exponential histograms are reconstructed natively. The window w
// selects which span of the tables to migrate; the zero window migrates all of them.
func (m *Migrator) MigrateMetrics(ctx context.Context, w chstorage.Window, batchSize int) (MetricsStats, error) {
	var stats MetricsStats

	mint, maxt, err := m.metrics.Range(ctx)
	if err != nil {
		return stats, errors.Wrap(err, "resolve range")
	}
	days, from, to, ok := plan(w, mint, maxt)
	if !ok {
		m.logger.Info("No metrics to migrate")
		return stats, nil
	}

	// The series set is loaded once for the whole window: it is small relative to the point volume,
	// and reloading it per day would re-read metrics_timeseries for every day of the run.
	scan, err := m.metrics.Prepare(ctx, from, to)
	if err != nil {
		return stats, errors.Wrap(err, "prepare metrics scan")
	}
	m.logger.Info("Migrating metrics",
		zap.Time("from", from), zap.Time("to", to),
		zap.Int("days", len(days)), zap.Int("series", scan.Series()),
	)

	for i, d := range days {
		lg := dayLogger(m.logger, d.Day, i, len(days))
		if m.checkpoint.Done(SignalMetrics, d.Day) {
			stats.DaysSkipped++
			lg.Info("Skipping day, already migrated")
			continue
		}

		dayPoints := 0
		err := scan.ScanNumbers(ctx, d.From, d.To, batchSize, func(ctx context.Context, points []metricstorage.NumberPoint) error {
			if err := m.back.WriteMetrics(ctx, ConvertNumberPoints(points, m.attrs)); err != nil {
				return errors.Wrap(err, "write metrics")
			}
			dayPoints += len(points)
			stats.Points += len(points)
			stats.Batches++
			return m.sleep(ctx)
		})
		if err != nil {
			return stats, errors.Wrapf(err, "migrate number points %s", d.Day.Format(time.DateOnly))
		}

		err = scan.ScanExpHistograms(ctx, d.From, d.To, batchSize, func(ctx context.Context, points []metricstorage.ExpHistogramPoint) error {
			md := metricstorage.ExpHistogramsToMetrics(points)
			if err := m.back.ConsumeMetrics(ctx, md); err != nil {
				return errors.Wrap(err, "consume exp histograms")
			}
			dayPoints += len(points)
			stats.ExpHistograms += len(points)
			stats.Batches++
			return m.sleep(ctx)
		})
		if err != nil {
			return stats, errors.Wrapf(err, "migrate exp histograms %s", d.Day.Format(time.DateOnly))
		}

		if err := m.finishDay(ctx, SignalMetrics, d.Day, dayPoints); err != nil {
			return stats, errors.Wrapf(err, "finish metrics %s", d.Day.Format(time.DateOnly))
		}
		stats.DaysDone++
		lg.Info("Migrated day",
			zap.Int("day_points", dayPoints),
			zap.Int("total_points", stats.Points),
			zap.Int("total_exp_histograms", stats.ExpHistograms),
		)
	}

	if missing := scan.Missing(); missing > 0 {
		m.logger.Warn("Skipped points with no matching series", zap.Int("count", missing))
	}
	return stats, nil
}
