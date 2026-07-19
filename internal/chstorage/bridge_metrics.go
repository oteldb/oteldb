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

// NumberPointsBatchFunc is called with each decoded batch of number points read by [MetricsSource].
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

// MetricsSource reads metrics stored in ClickHouse for migration into another storage engine. It
// first loads the series set from metrics_timeseries (small relative to the point volume) into an
// in-memory hash→[seriesMeta] map, then day-bucket scans metrics_points and metrics_exp_histograms
// (mirroring [Backup]) and resolves each row's series by hash. Exemplars are not read (the target
// engine drops them). metrics_labels is not read (it is an autocomplete index, deriving nothing
// the timeseries rows do not already carry).
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

// Do migrates metrics: it loads the series set (restricted to the scan window when since is
// positive), then scans number points and exponential histograms, invoking numberFn and expFn
// with batches of up to batchSize decoded points. Rows whose hash is absent from the series set
// are skipped and counted (logged at the end).
func (s *MetricsSource) Do(
	ctx context.Context,
	since time.Duration,
	batchSize int,
	numberFn NumberPointsBatchFunc,
	expFn ExpHistogramsBatchFunc,
) error {
	mint, maxt, err := queryMinMaxTimestamp(ctx, s.client,
		[2]string{s.points, "timestamp"},
		[2]string{s.expHistos, "timestamp"},
	)
	if err != nil {
		return errors.Wrap(err, "query min/max timestamp")
	}
	if mint.IsZero() && maxt.IsZero() {
		s.logger.Info("No metrics to migrate")
		return nil
	}
	if since > 0 {
		if cut := maxt.Add(-since); cut.After(mint) {
			mint = cut
		}
	}

	series, err := s.loadSeries(ctx, mint, maxt)
	if err != nil {
		return errors.Wrap(err, "load series")
	}
	s.logger.Info("Loaded metric series", zap.Int("series", len(series)))

	var missing int
	if err := s.scanNumberPoints(ctx, series, mint, maxt, batchSize, &missing, numberFn); err != nil {
		return errors.Wrap(err, "scan number points")
	}
	if err := s.scanExpHistograms(ctx, series, mint, maxt, batchSize, &missing, expFn); err != nil {
		return errors.Wrap(err, "scan exp histograms")
	}
	if missing > 0 {
		s.logger.Warn("Skipped points with no matching series", zap.Int("count", missing))
	}
	return nil
}

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

func (s *MetricsSource) scanNumberPoints(
	ctx context.Context,
	series map[[16]byte]seriesMeta,
	mint, maxt time.Time,
	batchSize int,
	missing *int,
	fn NumberPointsBatchFunc,
) error {
	return forEachDayBucket(mint, maxt, func(from, to time.Time) error {
		if err := s.scanNumberDay(ctx, series, from, to, batchSize, missing, fn); err != nil {
			return errors.Wrapf(err, "scan %s", from)
		}
		return nil
	})
}

func (s *MetricsSource) scanNumberDay(
	ctx context.Context,
	series map[[16]byte]seriesMeta,
	start, end time.Time,
	batchSize int,
	missing *int,
	fn NumberPointsBatchFunc,
) error {
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

	query := chsql.Select(s.points, c.ChsqlResult()...).
		Where(chsql.InTimeRange("timestamp", start, end, proto.PrecisionMilli))

	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		defer c.Columns().Reset()
		for i := 0; i < c.timestamp.Rows(); i++ {
			meta, ok := series[c.hash.Row(i)]
			if !ok {
				*missing++
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
	if err := s.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "execute query")
	}
	return flush(ctx)
}

func (s *MetricsSource) scanExpHistograms(
	ctx context.Context,
	series map[[16]byte]seriesMeta,
	mint, maxt time.Time,
	batchSize int,
	missing *int,
	fn ExpHistogramsBatchFunc,
) error {
	return forEachDayBucket(mint, maxt, func(from, to time.Time) error {
		if err := s.scanExpDay(ctx, series, from, to, batchSize, missing, fn); err != nil {
			return errors.Wrapf(err, "scan %s", from)
		}
		return nil
	})
}

func (s *MetricsSource) scanExpDay(
	ctx context.Context,
	series map[[16]byte]seriesMeta,
	start, end time.Time,
	batchSize int,
	missing *int,
	fn ExpHistogramsBatchFunc,
) error {
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

	query := chsql.Select(s.expHistos, c.ChsqlResult()...).
		Where(chsql.InTimeRange("timestamp", start, end, proto.PrecisionMilli))

	chq, err := query.Prepare(func(ctx context.Context, block proto.Block) error {
		defer c.Columns().Reset()
		for i := 0; i < c.timestamp.Rows(); i++ {
			meta, ok := series[c.hash.Row(i)]
			if !ok {
				*missing++
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
	if err := s.client.Do(ctx, chq); err != nil {
		return errors.Wrap(err, "execute query")
	}
	return flush(ctx)
}
