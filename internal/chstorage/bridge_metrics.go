package chstorage

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/metricstorage"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

// NumberPointsBatchFunc is called with each decoded batch of number points read by [MetricsScan].
type NumberPointsBatchFunc func(ctx context.Context, points []metricstorage.NumberPoint) error

// ExpHistogramsBatchFunc is called with each decoded batch of exponential histograms.
type ExpHistogramsBatchFunc func(ctx context.Context, points []metricstorage.ExpHistogramPoint) error

// seriesMeta is the per-series identity resolved from metrics_timeseries by hash: the (already
// suffixed, for histogram/summary components) metric name, its unit/description, and the
// attribute maps (attribute already carries the le/quantile label for bucket/quantile series).
type seriesMeta struct {
	name        string
	unit        string
	description string
	attrs       otelstorage.Attrs
	scope       otelstorage.Attrs
	resource    otelstorage.Attrs
}

// MetricsSource reads metrics stored in ClickHouse for migration into another storage engine.
// Scanning is two-staged: [MetricsSource.Prepare] loads the series set from metrics_timeseries
// (small relative to the point volume) into an in-memory hash→[seriesMeta] map, and the returned
// [MetricsScan] then reads metrics_points and metrics_exp_histograms a day at a time, resolving
// each row's series by hash. Exemplars are not read (the target engine drops them).
// metrics_labels is not read (it is an autocomplete index, deriving nothing the timeseries rows do
// not already carry).
type MetricsSource struct {
	client     ClickHouseClient
	timeseries string
	points     string
	expHistos  string
	logger     *zap.Logger
}

// NewMetricsSource creates a new [MetricsSource].
func NewMetricsSource(client ClickHouseClient, tables Tables, logger *zap.Logger) *MetricsSource {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &MetricsSource{
		client:     client,
		timeseries: tables.Timeseries,
		points:     tables.Points,
		expHistos:  tables.ExpHistograms,
		logger:     logger,
	}
}

// Range returns the timestamp bounds across both point tables. Both are zero when they are empty.
func (s *MetricsSource) Range(ctx context.Context) (mint, maxt time.Time, _ error) {
	mint, maxt, err := queryMinMaxTimestamp(ctx, s.client,
		[2]string{s.points, "timestamp"},
		[2]string{s.expHistos, "timestamp"},
	)
	if err != nil {
		return mint, maxt, errors.Wrap(err, "query min/max timestamp")
	}
	return mint, maxt, nil
}

// Counts returns the per-UTC-day point counts within [from, to], summing both point tables.
func (s *MetricsSource) Counts(ctx context.Context, from, to time.Time) ([]DayCount, error) {
	numbers, err := dayCounts(ctx, s.client, s.points, "timestamp", proto.PrecisionMilli, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "count number points")
	}
	exp, err := dayCounts(ctx, s.client, s.expHistos, "timestamp", proto.PrecisionMilli, from, to)
	if err != nil {
		return nil, errors.Wrap(err, "count exp histograms")
	}
	return mergeDayCounts(numbers, exp), nil
}

// Size returns the combined active-part footprint of both point tables, for pre-flight sizing.
func (s *MetricsSource) Size(ctx context.Context) (TableSize, error) {
	numbers, err := tableSize(ctx, s.client, s.points)
	if err != nil {
		return TableSize{}, errors.Wrap(err, "size number points")
	}
	exp, err := tableSize(ctx, s.client, s.expHistos)
	if err != nil {
		return TableSize{}, errors.Wrap(err, "size exp histograms")
	}
	return TableSize{
		Rows:              numbers.Rows + exp.Rows,
		CompressedBytes:   numbers.CompressedBytes + exp.CompressedBytes,
		UncompressedBytes: numbers.UncompressedBytes + exp.UncompressedBytes,
	}, nil
}

// MetricsScan is a prepared metrics scan: the series set for a time range, plus the day-at-a-time
// readers over the point tables. Obtain it from [MetricsSource.Prepare]. It is not safe for
// concurrent use.
type MetricsScan struct {
	src     *MetricsSource
	series  map[[16]byte]seriesMeta
	missing int
}

// Prepare loads the series set covering [mint, maxt] — series whose [first_seen, last_seen]
// overlaps the range — and returns a scan bound to it.
func (s *MetricsSource) Prepare(ctx context.Context, mint, maxt time.Time) (*MetricsScan, error) {
	series, err := s.loadSeries(ctx, mint, maxt)
	if err != nil {
		return nil, errors.Wrap(err, "load series")
	}
	s.logger.Info("Loaded metric series", zap.Int("series", len(series)))
	return &MetricsScan{src: s, series: series}, nil
}

// Series returns the number of series loaded for the scan.
func (m *MetricsScan) Series() int { return len(m.series) }

// Missing returns the running count of points skipped because their hash was absent from the
// series set.
func (m *MetricsScan) Missing() int { return m.missing }

// loadSeries reads metrics_timeseries into a hash→meta map, restricted to series whose
// [first_seen, last_seen] overlaps [mint, maxt].
func (s *MetricsSource) loadSeries(ctx context.Context, mint, maxt time.Time) (map[[16]byte]seriesMeta, error) {
	c := newTimeseriesColumns()
	prec := c.timestampPrecision()

	query := chsql.Select(s.timeseries, c.ChsqlResult()...).
		Where(
			chsql.Lte(chsql.Ident("first_seen"), chsql.DateTime64(maxt, prec)),
			chsql.Gte(chsql.Ident("last_seen"), chsql.DateTime64(mint, prec)),
		)

	out := map[[16]byte]seriesMeta{}
	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		for i := 0; i < c.hash.Rows(); i++ {
			out[c.hash.Row(i)] = seriesMeta{
				name:        c.name.Row(i),
				unit:        c.unit.Row(i),
				description: c.description.Row(i),
				attrs:       c.attributes.Row(i),
				scope:       c.scope.Row(i),
				resource:    c.resource.Row(i),
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "prepare query")
	}
	if err := s.client.Do(ctx, chq); err != nil {
		return nil, errors.Wrap(err, "execute query")
	}
	return out, nil
}

// ScanNumbers reads the number points in [from, to], invoking fn with batches of up to batchSize
// decoded points. Rows whose hash is absent from the series set are skipped and counted
// (see [MetricsScan.Missing]).
func (m *MetricsScan) ScanNumbers(ctx context.Context, from, to time.Time, batchSize int, fn NumberPointsBatchFunc) error {
	var (
		c   = newPointColumns()
		buf []metricstorage.NumberPoint
	)

	flush := func(ctx context.Context) error {
		if len(buf) == 0 {
			return nil
		}
		if err := fn(ctx, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}

	query := chsql.Select(m.src.points, c.ChsqlResult()...).
		Where(chsql.InTimeRange("timestamp", from, to, proto.PrecisionMilli))

	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		defer c.Columns().Reset()
		for i := 0; i < c.timestamp.Rows(); i++ {
			meta, ok := m.series[c.hash.Row(i)]
			if !ok {
				m.missing++
				continue
			}
			buf = append(buf, metricstorage.NumberPoint{
				Name:        meta.name,
				Unit:        meta.unit,
				Description: meta.description,
				Resource:    meta.resource,
				Scope:       meta.scope,
				Attrs:       meta.attrs,
				Timestamp:   otelstorage.NewTimestampFromTime(c.timestamp.Row(i)),
				Value:       c.value.Row(i),
			})
			if len(buf) >= batchSize {
				if err := flush(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "prepare query")
	}
	if err := m.src.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "execute query")
	}
	return flush(ctx)
}

// ScanExpHistograms reads the exponential-histogram points in [from, to], invoking fn with batches
// of up to batchSize decoded points.
func (m *MetricsScan) ScanExpHistograms(ctx context.Context, from, to time.Time, batchSize int, fn ExpHistogramsBatchFunc) error {
	var (
		c   = newExpHistogramColumns()
		buf []metricstorage.ExpHistogramPoint
	)

	flush := func(ctx context.Context) error {
		if len(buf) == 0 {
			return nil
		}
		if err := fn(ctx, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}

	nullable := func(v proto.Nullable[float64]) *float64 {
		if !v.Set {
			return nil
		}
		return &v.Value
	}

	query := chsql.Select(m.src.expHistos, c.ChsqlResult()...).
		Where(chsql.InTimeRange("timestamp", from, to, proto.PrecisionMilli))

	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		defer c.Columns().Reset()
		for i := 0; i < c.timestamp.Rows(); i++ {
			meta, ok := m.series[c.hash.Row(i)]
			if !ok {
				m.missing++
				continue
			}
			buf = append(buf, metricstorage.ExpHistogramPoint{
				Name:                 meta.name,
				Unit:                 meta.unit,
				Description:          meta.description,
				Resource:             meta.resource,
				Scope:                meta.scope,
				Attrs:                meta.attrs,
				Timestamp:            otelstorage.NewTimestampFromTime(c.timestamp.Row(i)),
				Count:                c.count.Row(i),
				Sum:                  nullable(c.sum.Row(i)),
				Min:                  nullable(c.min.Row(i)),
				Max:                  nullable(c.max.Row(i)),
				Scale:                c.scale.Row(i),
				ZeroCount:            c.zerocount.Row(i),
				PositiveOffset:       c.positiveOffset.Row(i),
				PositiveBucketCounts: c.positiveBucketCounts.Row(i),
				NegativeOffset:       c.negativeOffset.Row(i),
				NegativeBucketCounts: c.negativeBucketCounts.Row(i),
				Flags:                uint32(c.flags.Row(i)),
			})
			if len(buf) >= batchSize {
				if err := flush(ctx); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, "prepare query")
	}
	if err := m.src.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "execute query")
	}
	return flush(ctx)
}
