package chstorage

import (
	"context"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	singleflight "github.com/go-faster/sdk/singleflightx"
	"github.com/go-faster/sdk/zctx"
	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/globalmetric"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/metricscache"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

var _ tracestorage.Querier = (*Querier)(nil)

// Querier implements tracestorage.Querier using Clickhouse.
type Querier struct {
	ch              ClickHouseClient
	tables          Tables
	labelLimit      int
	timeseriesLimit int
	exemplarsLimit  int

	maxResultRows    int
	maxResultBytes   int
	maxExecutionTime time.Duration

	disableRateOffloading   bool
	disableMetricOffloading bool

	timeseries   *timeseriesQuerier
	metricsCache *metricscache.Cache
	metricsSg    *singleflight.Group[xxh3.Uint128, metricSelectResult]

	chLogLevel                 zapcore.LevelEnabler
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
	// MetricSeriesLimit defines limit for total number of series requested by the query.
	MetricSeriesLimit int
	// MetricExemplarsLimit defines limit for total number of exemplars returned by a single query.
	MetricExemplarsLimit int

	// MaxResultRows defines max number of rows to read from ClickHouse.
	MaxResultRows int
	// MaxResultBytes defines max number of bytes to read from ClickHouse.
	MaxResultBytes int
	// MaxExecutionTime defines max execution time for ClickHouse query.
	MaxExecutionTime time.Duration

	// DisableRateOffloading disables rate/increase/delta/etc. offloading to ClickHouse.
	DisableRateOffloading bool
	// DisableMetricOffloading disables all metric offloading to ClickHouse.
	DisableMetricOffloading bool

	// MetricsCacheOptions configures metrics cache.
	MetricsCacheOptions MetricsCacheOptions
	// CHLogLevel sets log level for ch-go.
	CHLogLevel zapcore.LevelEnabler
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
	if opts.MetricSeriesLimit == 0 {
		opts.MetricSeriesLimit = 1_000_000
	}
	if opts.MetricExemplarsLimit == 0 {
		opts.MetricExemplarsLimit = 1_000
	}
	if opts.MaxResultRows == 0 {
		opts.MaxResultRows = 10_000_000
	}
	if opts.MaxResultBytes == 0 {
		opts.MaxResultBytes = 1024 * 1024 * 1024 // 1 GiB
	}
	if opts.MaxExecutionTime == 0 {
		opts.MaxExecutionTime = 30 * time.Second
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
	var metricsCache *metricscache.Cache
	if opts.MetricsCacheOptions.MaxBytes > 0 {
		var err error
		cacheOpts := opts.MetricsCacheOptions
		if cacheOpts.MeterProvider == nil {
			cacheOpts.MeterProvider = opts.MeterProvider
		}
		metricsCache, err = metricscache.New(cacheOpts)
		if err != nil {
			return nil, errors.Wrap(err, "create metrics cache")
		}
	}

	q := &Querier{
		ch:              c,
		tables:          opts.Tables,
		labelLimit:      opts.LabelLimit,
		timeseriesLimit: opts.MetricSeriesLimit,
		exemplarsLimit:  opts.MetricExemplarsLimit,

		maxResultRows:    opts.MaxResultRows,
		maxResultBytes:   opts.MaxResultBytes,
		maxExecutionTime: opts.MaxExecutionTime,

		disableRateOffloading:   opts.DisableRateOffloading,
		disableMetricOffloading: opts.DisableMetricOffloading,

		metricsCache: metricsCache,
		metricsSg:    new(singleflight.Group[xxh3.Uint128, metricSelectResult]),

		chLogLevel:                 opts.CHLogLevel,
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
	query.Logger = lg.Named("ch").WithOptions(zap.IncreaseLevel(q.chLogLevel))
	query.OnProfileEvents = track.OnProfiles

	if q.maxResultRows > 0 {
		query.Settings = append(query.Settings, ch.Setting{
			Key:   "max_result_rows",
			Value: strconv.Itoa(q.maxResultRows),
		})
	}
	if q.maxResultBytes > 0 {
		query.Settings = append(query.Settings, ch.Setting{
			Key:   "max_result_bytes",
			Value: strconv.Itoa(q.maxResultBytes),
		})
	}
	if q.maxExecutionTime > 0 {
		query.Settings = append(query.Settings, ch.Setting{
			Key:   "max_execution_time",
			Value: strconv.Itoa(int(q.maxExecutionTime.Seconds())),
		})
	}

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
