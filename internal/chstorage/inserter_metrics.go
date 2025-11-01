package chstorage

import (
	"cmp"
	"context"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-faster/errors"
	"github.com/go-faster/sdk/zctx"
	"github.com/google/uuid"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

func (i *Inserter) insertBatch(ctx context.Context, b *metricsBatch) (rerr error) {
	ctx, span := i.tracer.Start(ctx, "chstorage.metrics.insertBatch")
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		} else {
			i.stats.InsertedPoints.Add(ctx, int64(b.points.value.Rows()))
			i.stats.InsertedHistograms.Add(ctx, int64(b.expHistograms.count.Rows()))
			i.stats.InsertedExemplars.Add(ctx, int64(b.exemplars.value.Rows()))
			i.stats.InsertedMetricLabels.Add(ctx, int64(len(b.labels)))

			i.stats.Inserts.Add(ctx, 1, metric.WithAttributes(
				attribute.String("chstorage.signal", "metrics"),
			))
		}
		span.End()
	}()
	return b.Insert(ctx, i.tables, i.ch)
}

// ConsumeMetrics inserts given metrics.
func (i *Inserter) ConsumeMetrics(ctx context.Context, metrics pmetric.Metrics) error {
	b := newMetricBatch()
	if err := b.mapMetrics(metrics); err != nil {
		return errors.Wrap(err, "map metrics")
	}
	if err := i.insertBatch(ctx, b); err != nil {
		return errors.Wrap(err, "insert batch")
	}
	return nil
}

type metricsBatch struct {
	points        *pointColumns
	expHistograms *expHistogramColumns
	exemplars     *exemplarColumns
	labels        map[[2]string]labelScope
}

func (b *metricsBatch) Reset() {
	b.points.Columns().Reset()
	b.expHistograms.Columns().Reset()
	b.exemplars.Columns().Reset()
	maps.Clear(b.labels)
}

func newMetricBatch() *metricsBatch {
	return &metricsBatch{
		points:        newPointColumns(),
		expHistograms: newExpHistogramColumns(),
		exemplars:     newExemplarColumns(),
		labels:        map[[2]string]labelScope{},
	}
}

type labelScope uint8

const (
	labelScopeNone     labelScope = 0
	labelScopeResource labelScope = 1 << (iota - 1)
	labelScopeInstrumentation
	labelScopeAttribute
)

func (b *metricsBatch) Insert(ctx context.Context, tables Tables, client ClickHouseClient) error {
	lg := zctx.From(ctx)

	labelColumns := newLabelsColumns()
	insertLabel := func(
		key string,
		value string,
		scope labelScope,
	) {
		labelColumns.name.Append(key)
		labelColumns.value.Append(value)
		labelColumns.scope.Append(proto.Enum8(scope))
	}
	for pair, scopes := range b.labels {
		key, value := pair[0], pair[1]

		if scopes == 0 {
			insertLabel(key, value, labelScopeNone)
		} else {
			for _, scope := range [3]labelScope{
				labelScopeResource,
				labelScopeInstrumentation,
				labelScopeAttribute,
			} {
				if scopes&scope == 0 {
					continue
				}
				insertLabel(key, value, scope)
			}
		}
	}

	grp, grpCtx := errgroup.WithContext(ctx)
	type columns interface {
		Input() proto.Input
	}
	for _, table := range []struct {
		name    string
		columns columns
	}{
		{tables.Points, b.points},
		{tables.ExpHistograms, b.expHistograms},
		{tables.Exemplars, b.exemplars},
		{tables.Labels, labelColumns},
	} {
		table := table
		grp.Go(func() error {
			ctx := grpCtx

			var (
				queryID = uuid.New().String()
				lg      = lg.With(
					zap.String("query_id", queryID),
					zap.String("table", table.name),
				)

				input  = table.columns.Input()
				insert = func() error {
					err := client.Do(ctx, ch.Query{
						Body:   input.Into(table.name),
						Input:  input,
						Logger: zctx.From(ctx).Named("ch"),
					})
					if pe, ok := errors.Into[proto.Error](err); ok && pe == proto.ErrQueryWithSameIDIsAlreadyRunning {
						lg.Debug("Query already running")
						err = nil
					}
					return err
				}
			)

			rows := -1
			if len(input) > 0 {
				rows = input[0].Data.Rows()
			}
			lg.Debug("Inserting metrics", zap.Int("rows", rows))

			eb := backoff.NewExponentialBackOff()
			bo := backoff.WithContext(eb, ctx)
			if err := backoff.RetryNotify(insert, bo, func(err error, d time.Duration) {
				lg.Warn("Metrics insert failed",
					zap.Error(err),
					zap.Duration("retry_after", d),
				)
			}); err != nil {
				return errors.Wrapf(err, "insert %q", table.name)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return errors.Wrap(err, "insert")
	}

	return nil
}

func (b *metricsBatch) addPoints(name string, res, scope lazyAttributes, slice pmetric.NumberDataPointSlice) error {
	c := b.points

	for i := 0; i < slice.Len(); i++ {
		point := slice.At(i)
		ts := point.Timestamp().AsTime()
		flags := point.Flags()
		attrs := lazyAttributes{
			orig: point.Attributes(),
		}

		var val float64
		switch typ := point.ValueType(); typ {
		case pmetric.NumberDataPointValueTypeEmpty:
			// Just ignore it.
			continue
		case pmetric.NumberDataPointValueTypeInt:
			// TODO(tdakkota): check for overflow
			val = float64(point.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			val = point.DoubleValue()
		default:
			return errors.Errorf("unexpected metric %q value type: %v", name, typ)
		}

		b.addName(name)
		b.addLabels(labelScopeAttribute, attrs)

		if err := b.addExemplars(
			exemplarSeries{
				Name:       name,
				Timestamp:  ts,
				Attributes: attrs,
				Scope:      scope,
				Resource:   res,
			},
			point.Exemplars(),
		); err != nil {
			return errors.Wrap(err, "map exemplars")
		}
		c.name.Append(name)
		c.timestamp.Append(ts)
		c.mapping.Append(proto.Enum8(noMapping))
		c.value.Append(val)
		c.flags.Append(uint8(flags))
		c.attributes.Append(attrs.Attributes())
		c.scope.Append(scope.Attributes())
		c.resource.Append(res.Attributes())
	}
	return nil
}

func (b *metricsBatch) addHistogramPoints(name string, res, scope lazyAttributes, slice pmetric.HistogramDataPointSlice) error {
	for i := 0; i < slice.Len(); i++ {
		point := slice.At(i)
		ts := point.Timestamp().AsTime()
		flags := point.Flags()
		attrs := lazyAttributes{
			orig: point.Attributes(),
		}
		count := point.Count()
		sum := proto.Nullable[float64]{
			Set:   point.HasSum(),
			Value: point.Sum(),
		}
		_min := proto.Nullable[float64]{
			Set:   point.HasMin(),
			Value: point.Min(),
		}
		_max := proto.Nullable[float64]{
			Set:   point.HasMax(),
			Value: point.Max(),
		}
		bucketCounts := point.BucketCounts().AsRaw()
		explicitBounds := point.ExplicitBounds().AsRaw()

		// Do not add metric name as-is, since we mapping it into multiple series with prefixes.
		b.addLabels(labelScopeAttribute, attrs)

		// Map histogram as set of series for Prometheus compatibility.
		series := mappedSeries{
			Timestamp:  ts,
			Flags:      flags,
			Attributes: attrs,
			Scope:      scope,
			Resource:   res,
		}
		if sum.Set {
			b.addMappedSample(
				series,
				name+"_sum",
				histogramSum,
				sum.Value,
			)
		}
		if _min.Set {
			b.addMappedSample(
				series,
				name+"_min",
				histogramMin,
				_min.Value,
			)
		}
		if _max.Set {
			b.addMappedSample(
				series,
				name+"_max",
				histogramMax,
				_max.Value,
			)
		}
		b.addMappedSample(
			series,
			name+"_count",
			histogramCount,
			float64(count),
		)

		var (
			cumCount     uint64
			bucketName   = name + "_bucket"
			bucketBounds = make([]histogramBucketBounds, 0, len(bucketCounts))
		)
		for i := 0; i < min(len(bucketCounts), len(explicitBounds)); i++ {
			bound := explicitBounds[i]
			cumCount += bucketCounts[i]

			key := [2]string{
				"le",
				strconv.FormatFloat(bound, 'f', -1, 64),
			}
			bucketBounds = append(bucketBounds, histogramBucketBounds{
				bound:     bound,
				bucketKey: key,
			})

			// Generate series with "_bucket" suffix and "le" label.
			b.addMappedSample(
				series,
				bucketName,
				histogramBucket,
				float64(cumCount),
				key,
			)
		}
		// Generate series with "_bucket" suffix and "le" label.
		{
			key := [2]string{
				"le",
				"+Inf",
			}
			bucketBounds = append(bucketBounds, histogramBucketBounds{
				bound:     math.Inf(1),
				bucketKey: key,
			})

			b.addMappedSample(
				series,
				bucketName,
				histogramBucket,
				float64(cumCount),
				key,
			)
		}

		if err := b.addHistogramExemplars(
			exemplarSeries{
				// Note: we're using the "_bucket" name, not the original.
				Name:       bucketName,
				Timestamp:  ts,
				Attributes: attrs,
				Scope:      scope,
				Resource:   res,
			},
			point.Exemplars(),
			bucketBounds,
		); err != nil {
			return errors.Wrap(err, "map exemplars")
		}
	}
	return nil
}

func (b *metricsBatch) addHistogramExemplars(
	p exemplarSeries,
	exemplars pmetric.ExemplarSlice,
	bounds []histogramBucketBounds,
) error {
	slices.SortFunc(bounds, func(a, b histogramBucketBounds) int {
		return cmp.Compare(a.bound, b.bound)
	})
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		for _, bound := range bounds {
			var val float64
			switch typ := e.ValueType(); typ {
			case pmetric.ExemplarValueTypeEmpty:
				// Just ignore it.
				return nil
			case pmetric.ExemplarValueTypeInt:
				// TODO(tdakkota): check for overflow
				val = float64(e.IntValue())
			case pmetric.ExemplarValueTypeDouble:
				val = e.DoubleValue()
			default:
				return errors.Errorf("unexpected exemplar value type: %v", typ)
			}

			if val <= bound.bound {
				if err := b.addExemplar(p, e, bound.bucketKey); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

type histogramBucketBounds struct {
	bound     float64
	bucketKey [2]string
}

func (b *metricsBatch) addExpHistogramPoints(name string, res, scope lazyAttributes, slice pmetric.ExponentialHistogramDataPointSlice) error {
	var (
		c          = b.expHistograms
		mapBuckets = func(b pmetric.ExponentialHistogramDataPointBuckets) (offset int32, counts []uint64) {
			offset = b.Offset()
			counts = b.BucketCounts().AsRaw()
			return offset, counts
		}
	)
	for i := 0; i < slice.Len(); i++ {
		var (
			point     = slice.At(i)
			ts        = point.Timestamp().AsTime()
			flags     = point.Flags()
			attrs     = lazyAttributes{orig: point.Attributes()}
			count     = point.Count()
			sum       = proto.Nullable[float64]{Set: point.HasSum(), Value: point.Sum()}
			vmin      = proto.Nullable[float64]{Set: point.HasMin(), Value: point.Min()}
			vmax      = proto.Nullable[float64]{Set: point.HasMax(), Value: point.Max()}
			scale     = point.Scale()
			zerocount = point.ZeroCount()
		)

		positiveOffset, positiveBucketCounts := mapBuckets(point.Positive())
		negativeOffset, negativeBucketCounts := mapBuckets(point.Negative())

		b.addName(name)
		b.addLabels(labelScopeAttribute, attrs)
		if err := b.addExemplars(
			exemplarSeries{
				Name:       name,
				Timestamp:  ts,
				Attributes: attrs,
				Scope:      scope,
				Resource:   res,
			},
			point.Exemplars(),
		); err != nil {
			return errors.Wrap(err, "map exemplars")
		}
		c.name.Append(name)
		c.timestamp.Append(ts)
		c.count.Append(count)
		c.sum.Append(sum)
		c.min.Append(vmin)
		c.max.Append(vmax)
		c.scale.Append(scale)
		c.zerocount.Append(zerocount)
		c.positiveOffset.Append(positiveOffset)
		c.positiveBucketCounts.Append(positiveBucketCounts)
		c.negativeOffset.Append(negativeOffset)
		c.negativeBucketCounts.Append(negativeBucketCounts)
		c.flags.Append(uint8(flags))
		c.attributes.Append(attrs.Attributes())
		c.scope.Append(scope.Attributes())
		c.resource.Append(res.Attributes())
	}
	return nil
}

func (b *metricsBatch) addSummaryPoints(name string, res, scope lazyAttributes, slice pmetric.SummaryDataPointSlice) error {
	for i := 0; i < slice.Len(); i++ {
		var (
			point = slice.At(i)
			ts    = point.Timestamp().AsTime()
			flags = point.Flags()
			attrs = lazyAttributes{orig: point.Attributes()}
			count = point.Count()
			sum   = point.Sum()
			qv    = point.QuantileValues()
		)
		var (
			quantiles = make([]float64, qv.Len())
			values    = make([]float64, qv.Len())
		)
		for i := 0; i < qv.Len(); i++ {
			p := qv.At(i)

			quantiles[i] = p.Quantile()
			values[i] = p.Value()
		}

		b.addName(name)
		b.addLabels(labelScopeAttribute, attrs)

		ms := mappedSeries{
			Timestamp:  ts,
			Flags:      flags,
			Attributes: attrs,
			Scope:      scope,
			Resource:   res,
		}
		b.addMappedSample(ms, name+"_count", summaryCount, float64(count))
		b.addMappedSample(ms, name+"_sum", summarySum, sum)

		for i := 0; i < min(len(quantiles), len(values)); i++ {
			quantile := quantiles[i]
			value := values[i]

			// Generate series with "quantile" label.
			b.addMappedSample(ms, name, summaryQuantile, value, [2]string{
				"quantile",
				strconv.FormatFloat(quantile, 'f', -1, 64),
			})
		}
	}
	return nil
}

type mappedSeries struct {
	Timestamp  time.Time
	Flags      pmetric.DataPointFlags
	Attributes lazyAttributes
	Scope      lazyAttributes
	Resource   lazyAttributes
}

func (b *metricsBatch) addMappedSample(
	series mappedSeries,
	name string,
	mapping metricMapping,
	val float64,
	bucketKey ...[2]string,
) {
	c := b.points
	b.addName(name)
	c.name.Append(name)
	c.timestamp.Append(series.Timestamp)
	c.mapping.Append(proto.Enum8(mapping))
	c.value.Append(val)
	c.flags.Append(uint8(series.Flags))
	c.attributes.Append(series.Attributes.Attributes(bucketKey...))
	c.scope.Append(series.Scope.Attributes())
	c.resource.Append(series.Resource.Attributes())
}

type exemplarSeries struct {
	Name       string
	Timestamp  time.Time
	Attributes lazyAttributes
	Scope      lazyAttributes
	Resource   lazyAttributes
}

func (b *metricsBatch) addExemplars(p exemplarSeries, exemplars pmetric.ExemplarSlice) error {
	for i := 0; i < exemplars.Len(); i++ {
		if err := b.addExemplar(p, exemplars.At(i)); err != nil {
			return err
		}
	}
	return nil
}

func (b *metricsBatch) addExemplar(p exemplarSeries, e pmetric.Exemplar, bucketKey ...[2]string) error {
	c := b.exemplars

	var val float64
	switch typ := e.ValueType(); typ {
	case pmetric.ExemplarValueTypeEmpty:
		// Just ignore it.
		return nil
	case pmetric.ExemplarValueTypeInt:
		// TODO(tdakkota): check for overflow
		val = float64(e.IntValue())
	case pmetric.ExemplarValueTypeDouble:
		val = e.DoubleValue()
	default:
		return errors.Errorf("unexpected exemplar value type: %v", typ)
	}

	c.name.Append(p.Name)
	c.timestamp.Append(p.Timestamp)

	c.filteredAttributes.Append(encodeAttributes(e.FilteredAttributes()))
	c.exemplarTimestamp.Append(e.Timestamp().AsTime())
	c.value.Append(val)
	c.spanID.Append(e.SpanID())
	c.traceID.Append(e.TraceID())

	c.attributes.Append(p.Attributes.Attributes(bucketKey...))
	c.scope.Append(p.Scope.Attributes())
	c.resource.Append(p.Resource.Attributes())
	return nil
}

func (b *metricsBatch) addName(name string) {
	b.labels[[2]string{labels.MetricName, name}] |= 0
}

func (b *metricsBatch) addLabels(scope labelScope, attrs lazyAttributes) {
	attrs.orig.Range(func(key string, value pcommon.Value) bool {
		pair := [2]string{
			key,
			// FIXME(tdakkota): annoying allocations
			value.AsString(),
		}
		b.labels[pair] |= scope
		return true
	})
}

func (b *metricsBatch) mapMetrics(metrics pmetric.Metrics) error {
	resMetrics := metrics.ResourceMetrics()
	for i := 0; i < resMetrics.Len(); i++ {
		var (
			resMetric = resMetrics.At(i)
			resAttrs  = lazyAttributes{
				orig: resMetric.Resource().Attributes(),
			}
		)
		b.addLabels(labelScopeResource, resAttrs)

		scopeMetrics := resMetric.ScopeMetrics()
		for i := 0; i < scopeMetrics.Len(); i++ {
			var (
				scopeMetric = scopeMetrics.At(i)
				scopeAttrs  = lazyAttributes{
					orig: scopeMetric.Scope().Attributes(),
				}
			)
			b.addLabels(labelScopeInstrumentation, scopeAttrs)

			records := scopeMetric.Metrics()
			for i := 0; i < records.Len(); i++ {
				record := records.At(i)
				name := record.Name()

				switch typ := record.Type(); typ {
				case pmetric.MetricTypeGauge:
					gauge := record.Gauge()
					if err := b.addPoints(name, resAttrs, scopeAttrs, gauge.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeSum:
					sum := record.Sum()
					if err := b.addPoints(name, resAttrs, scopeAttrs, sum.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeHistogram:
					hist := record.Histogram()
					if err := b.addHistogramPoints(name, resAttrs, scopeAttrs, hist.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeExponentialHistogram:
					hist := record.ExponentialHistogram()
					if err := b.addExpHistogramPoints(name, resAttrs, scopeAttrs, hist.DataPoints()); err != nil {
						return err
					}
				case pmetric.MetricTypeSummary:
					summary := record.Summary()
					if err := b.addSummaryPoints(name, resAttrs, scopeAttrs, summary.DataPoints()); err != nil {
						return err
					}
				default:
					return errors.Errorf("unexpected metric %q type %v", name, typ)
				}
			}
		}
	}

	return nil
}
