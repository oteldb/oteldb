package chstorage

import (
	"context"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/autometric"
	"github.com/go-faster/sdk/zctx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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
	InsertedRecords   metric.Int64Counter `name:"logs.inserted_records" description:"Number of inserted log records" unit:"{records}"`
	InsertedLogLabels metric.Int64Counter `name:"logs.inserted_log_labels" description:"Number of inserted log labels" unit:"{labels}"`
	// Metrics.
	InsertedSeries       metric.Int64Counter `name:"metrics.inserted_series" description:"Number of inserted series" unit:"{series}"`
	InsertedPoints       metric.Int64Counter `name:"metrics.inserted_points" description:"Number of inserted points" unit:"{points}"`
	InsertedHistograms   metric.Int64Counter `name:"metrics.inserted_histograms" description:"Number of inserted exponential (native) histograms" unit:"{histograms}"`
	InsertedExemplars    metric.Int64Counter `name:"metrics.inserted_exemplars" description:"Number of inserted exemplars" unit:"{exemplars}"`
	InsertedMetricLabels metric.Int64Counter `name:"metrics.inserted_metric_labels" description:"Number of inserted metric labels" unit:"{labels}"`
	// Traces.
	InsertedSpans metric.Int64Counter `name:"traces.inserted_spans" description:"Number of inserted spans" unit:"{spans}"`
	InsertedTags  metric.Int64Counter `name:"traces.inserted_tags" description:"Number of inserted trace attributes" unit:"{attributes}"`
	// Common.
	Inserts       metric.Int64Counter   `name:"inserts" description:"Number of insert invocations" unit:"{inserts}"`
	InsertedBytes metric.Int64Counter   `name:"inserted_bytes" description:"Total number of inserted bytes by signal" unit:"By"`
	InsertedRows  metric.Int64Counter   `name:"inserted_rows" description:"Total number of inserted rows by signal" unit:"{rows}"`
	BatchSize     metric.Int64Histogram `name:"batch_size" description:"Histogram of the batch size" unit:"{items}"`
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

func (i *Inserter) do(ctx context.Context, signal semconv.SignalType, table, query string, input proto.Input) error {
	set := attribute.NewSet(
		semconv.Signal(signal),
		attribute.String("chstorage.table", table),
	)
	ctx, track := i.tracker.Start(ctx, globalmetric.WithAttributes(set.ToSlice()...))
	defer track.End()

	var (
		lg                    = zctx.From(ctx).Named("ch").WithOptions(zap.IncreaseLevel(i.chLogLevel))
		totalBytes, totalRows uint64
	)
	if err := i.ch.Do(ctx, ch.Query{
		Logger: lg,
		Body:   query,
		Input:  input,
		OnProgress: func(ctx context.Context, p proto.Progress) error {
			totalBytes = p.WroteBytes
			totalRows = p.WroteRows
			return nil
		},
		OnProfileEvents: track.OnProfiles,
	}); err != nil {
		return err
	}

	var batchSize int
	if len(input) > 0 {
		batchSize = input[0].Data.Rows()
	}
	i.stats.BatchSize.Record(ctx, int64(batchSize), metric.WithAttributeSet(set))
	i.stats.InsertedBytes.Add(ctx, int64(totalBytes), metric.WithAttributeSet(set))
	i.stats.InsertedRows.Add(ctx, int64(totalRows), metric.WithAttributeSet(set))

	return nil
}
