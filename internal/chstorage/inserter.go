package chstorage

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/autometric"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/semconv"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Inserter = (*Inserter)(nil)

// Inserter implements tracestorage.Inserter using Clickhouse.
type Inserter struct {
	ch     ClickHouseClient
	tables Tables

	chLogLevel zapcore.LevelEnabler
	stats      inserterStats
	tracer     trace.Tracer
	tracker    globalmetric.Tracker
}

type inserterStats struct {
	// Logs.
	InsertedRecords   metric.Int64Counter `name:"logs.inserted_records" description:"Number of inserted log records"`
	InsertedLogLabels metric.Int64Counter `name:"logs.inserted_log_labels" description:"Number of inserted log labels"`
	// Metrics.
	InsertedSeries       metric.Int64Counter `name:"metrics.inserted_series" description:"Number of inserted series"`
	InsertedPoints       metric.Int64Counter `name:"metrics.inserted_points" description:"Number of inserted points"`
	InsertedHistograms   metric.Int64Counter `name:"metrics.inserted_histograms" description:"Number of inserted exponential (native) histograms"`
	InsertedExemplars    metric.Int64Counter `name:"metrics.inserted_exemplars" description:"Number of inserted exemplars"`
	InsertedMetricLabels metric.Int64Counter `name:"metrics.inserted_metric_labels" description:"Number of inserted metric labels"`
	// Traces.
	InsertedSpans metric.Int64Counter `name:"traces.inserted_spans" description:"Number of inserted spans"`
	InsertedTags  metric.Int64Counter `name:"traces.inserted_tags" description:"Number of inserted trace attributes"`
	// Common.
	Inserts   metric.Int64Counter   `name:"inserts" description:"Number of insert invocations"`
	BatchSize metric.Int64Histogram `name:"batch_size" description:"Histogram of the batch size"`
}

func (s *inserterStats) Init(meter metric.Meter) error {
	return autometric.Init(meter, s, autometric.InitOptions{
		Prefix: "chstorage.",
	})
}

// InserterOptions is Inserter's options.
type InserterOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// CHLogLevel sets log level for ch-go.
	CHLogLevel zapcore.LevelEnabler
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
	// Tracker provides global metric tracker.
	Tracker globalmetric.Tracker
}

func (opts *InserterOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = DefaultTables()
	}
	if opts.CHLogLevel == nil {
		opts.CHLogLevel = zap.DebugLevel
	}
	if opts.MeterProvider == nil {
		opts.MeterProvider = otel.GetMeterProvider()
	}
	if opts.TracerProvider == nil {
		opts.TracerProvider = otel.GetTracerProvider()
	}
	if opts.Tracker == nil {
		opts.Tracker = globalmetric.NewNoopTracker()
	}
}

// NewInserter creates new Inserter.
func NewInserter(c ClickHouseClient, opts InserterOptions) (*Inserter, error) {
	opts.Tracker = globalmetric.GetTracker()
	opts.setDefaults()

	inserter := &Inserter{
		ch:         c,
		tables:     opts.Tables,
		chLogLevel: opts.CHLogLevel,
		tracer:     opts.TracerProvider.Tracer("chstorage.Inserter"),
		tracker:    opts.Tracker,
	}

	meter := opts.MeterProvider.Meter("chstorage.Inserter")
	if err := inserter.stats.Init(meter); err != nil {
		return nil, errors.Wrap(err, "init stats")
	}

	totalSignals, err := meter.Int64ObservableGauge("chstorage.total_signals",
		metric.WithDescription("Total number of inserted signals"),
		metric.WithUnit("{signals}"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create total inserted siganls counter")
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		type total struct {
			Table    string
			Signal   semconv.SignalType
			Observed metric.Int64Observable
		}
		for _, t := range []total{
			{
				Table:    inserter.tables.Logs,
				Signal:   semconv.SignalLogs,
				Observed: totalSignals,
			},
			{
				Table:    inserter.tables.Points,
				Signal:   semconv.SignalMetrics,
				Observed: totalSignals,
			},
			{
				Table:    inserter.tables.Spans,
				Signal:   semconv.SignalTraces,
				Observed: totalSignals,
			},
		} {
			v, err := inserter.totals(ctx, t.Table)
			if err != nil {
				zctx.From(ctx).Error("Failed to get totals",
					zap.String("table", t.Table),
					zap.Error(err),
				)
				return errors.Wrapf(err, "get totals for table %q", t.Table)
			}
			o.ObserveInt64(t.Observed, v, metric.WithAttributes(
				semconv.Signal(t.Signal),
			))
		}

		return nil
	},
		totalSignals,
	)
	if err != nil {
		return nil, errors.Wrap(err, "register totals callback")
	}

	return inserter, nil
}
