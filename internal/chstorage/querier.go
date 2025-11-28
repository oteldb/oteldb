package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	singleflight "github.com/go-faster/sdk/singleflightx"
	"github.com/go-faster/sdk/zctx"
	"github.com/prometheus/prometheus/storage"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/globalmetric"
	"github.com/go-faster/oteldb/internal/logql/logqlengine"
	"github.com/go-faster/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch         ClickHouseClient
	tables     Tables
	labelLimit int

	timeseries *timeseriesQuerier
	metricsSg  *singleflight.Group[xxh3.Uint128, *seriesSet[storage.Series]]

	clickhouseRequestHistogram metric.Float64Histogram
	tracer                     trace.Tracer
	tracker                    globalmetric.Tracker
}

// QuerierOptions is Querier's options.
type QuerierOptions struct {
	// Tables provides table paths to query.
	Tables Tables
	// LabelLimit defines limit for label lookup in the main table.
	LabelLimit int
	// MeterProvider provides OpenTelemetry meter for this querier.
	MeterProvider metric.MeterProvider
	// TracerProvider provides OpenTelemetry tracer for this querier.
	TracerProvider trace.TracerProvider
	// Tracker tracks global metrics.
	Tracker globalmetric.Tracker
}

func (opts *QuerierOptions) setDefaults() {
	if opts.Tables == (Tables{}) {
		opts.Tables = DefaultTables()
	}
	if opts.LabelLimit == 0 {
		opts.LabelLimit = 1000
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

// NewQuerier creates new Querier.
func NewQuerier(c ClickHouseClient, opts QuerierOptions) (*Querier, error) {
	opts.setDefaults()

	meter := opts.MeterProvider.Meter("chstorage.Querier")
	clickhouseRequestHistogram, err := meter.Float64Histogram("chstorage.clickhouse.request",
		metric.WithUnit("s"),
		metric.WithDescription("Clickhouse request duration in seconds"),
	)
	if err != nil {
		return nil, errors.Wrap(err, "create clickhouse.request histogram metric")
	}
	q := &Querier{
		ch:         c,
		tables:     opts.Tables,
		labelLimit: opts.LabelLimit,
		metricsSg:  new(singleflight.Group[xxh3.Uint128, *seriesSet[storage.Series]]),

		tracer:                     opts.TracerProvider.Tracer("chstorage.Querier"),
		tracker:                    opts.Tracker,
		clickhouseRequestHistogram: clickhouseRequestHistogram,
	}
	q.timeseries = newTimeseriesQuerier(q)

	return q, nil
}

type selectQuery struct {
	Query    *chsql.SelectQuery
	OnResult chsql.OnResult

	ExternalData  []proto.InputColumn
	ExternalTable string

	Type   string
	Signal string
	Table  string
}

func (q *Querier) do(ctx context.Context, s selectQuery) error {
	lg := zctx.From(ctx)

	ctx, track := q.tracker.Start(ctx, globalmetric.WithAttributes(
		attribute.String("chstorage.query_type", s.Type),
		attribute.String("chstorage.table", s.Table),
		attribute.String("chstorage.signal", s.Signal),
	))
	defer track.End()

	query, err := s.Query.Prepare(s.OnResult)
	if err != nil {
		return errors.Wrap(err, "build query")
	}
	query.ExternalData = s.ExternalData
	query.ExternalTable = s.ExternalTable
	query.Logger = lg.Named("ch")
	query.OnProfileEvents = track.OnProfiles

	if logqlengine.IsExplainQuery(ctx) {
		query.Settings = append(query.Settings, ch.Setting{
			Key:       "send_logs_level",
			Value:     "trace",
			Important: true,
		})
	}

	queryStartTime := time.Now()
	err = q.ch.Do(ctx, query)
	took := time.Since(queryStartTime)
	if ce := lg.Check(zap.DebugLevel, "Query Clickhouse"); ce != nil {
		ce.Write(
			zap.String("query_type", s.Type),
			zap.String("table", s.Table),
			zap.String("signal", s.Signal),
			zap.Duration("took", took),
			zap.Error(err),
		)
	}
	if err != nil {
		return errors.Wrapf(err, "execute %s (signal: %s)", s.Type, s.Signal)
	}

	q.clickhouseRequestHistogram.Record(ctx, took.Seconds(),
		metric.WithAttributes(
			attribute.String("chstorage.query_type", s.Type),
			attribute.String("chstorage.table", s.Table),
			attribute.String("chstorage.signal", s.Signal),
		),
	)

	return nil
}
