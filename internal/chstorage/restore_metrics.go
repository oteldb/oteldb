package chstorage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/go-faster/oteldb/internal/metricstorage"
	"github.com/go-faster/oteldb/internal/otelstorage"
)

type metricsRestore struct {
	client ClickHouseClient
	tables Tables

	timeseriesMux  sync.Mutex
	timeseries     *timeseriesColumns
	seenTimeseries map[[16]byte]struct{}
	labelsMux      sync.Mutex
	labels         map[[2]string]labelScope

	logger *zap.Logger
}

func (r *metricsRestore) Do(ctx context.Context, root string) error {
	dirs, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			r.logger.Info("No metrics to restore")
			return nil
		}
		return err
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		step := filepath.Join(root, d.Name())
		if err := r.restore(ctx, step); err != nil {
			return errors.Wrapf(err, "restore dir %q", step)
		}
	}
	return nil
}

func (r *metricsRestore) restore(ctx context.Context, dir string) error {
	stopwatch := time.Now()

	r.timeseries.Columns().Reset()
	clear(r.seenTimeseries)
	clear(r.labels)
	r.logger.Info("Restoring metrics dir", zap.String("dir", dir))

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		ctx := grpCtx
		if err := r.restorePoints(ctx, dir); err != nil {
			return errors.Wrap(err, "restore points")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := r.restoreExpHistograms(ctx, dir); err != nil {
			return errors.Wrap(err, "restore exp histograms")
		}
		return nil
	})
	grp.Go(func() error {
		ctx := grpCtx
		if err := r.restoreExemplars(ctx, dir); err != nil {
			return errors.Wrap(err, "restore exemplars")
		}
		return nil
	})
	if err := grp.Wait(); err != nil {
		return err
	}

	{
		input := r.timeseries.Input()
		if err := r.client.Do(ctx, ch.Query{
			Body:  input.Into(r.tables.Timeseries),
			Input: input,
		}); err != nil {
			return errors.Wrap(err, "insert timeseries")
		}
	}
	{
		lc := newLabelsColumns()
		lc.AppendMap(r.labels)

		input := lc.Input()
		if err := r.client.Do(ctx, ch.Query{
			Body:  input.Into(r.tables.Labels),
			Input: input,
		}); err != nil {
			return errors.Wrap(err, "insert labels")
		}
	}

	r.logger.Info("Restored metrics dir", zap.Duration("took", time.Since(stopwatch)), zap.String("dir", dir))
	return nil
}

func (r *metricsRestore) restorePoints(ctx context.Context, dir string) error {
	cfg := restoreTable{
		File: "metrics_points",
		NewColumns: func() (proto.Input, Columns, func(), func() int) {
			var (
				name        = new(proto.ColStr).LowCardinality()
				unit        = new(proto.ColStr).LowCardinality()
				description proto.ColStr

				attributes = NewAttributes(colAttrs)
				scope      = NewAttributes(colScope)
				resource   = NewAttributes(colResource)

				timestamp = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
				value     proto.ColFloat64

				mapping proto.ColEnum8
				flags   proto.ColUInt8

				columns = MergeColumns(
					Columns{
						{Name: "timestamp", Data: timestamp},
						{Name: "value", Data: &value},

						{Name: "mapping", Data: proto.Wrap(&mapping, metricMappingDDL)},
						{Name: "flags", Data: &flags},
					},
					Columns{
						{Name: "name", Data: name},
						{Name: "unit", Data: unit},
						{Name: "description", Data: &description},
					},
					attributes.Columns(),
					scope.Columns(),
					resource.Columns(),
				)

				points = newPointColumns()
				add    = func() {
					for i := 0; i < timestamp.Rows(); i++ {
						var (
							timestamp   = timestamp.Row(i)
							mapping     = metricMapping(mapping.Row(i))
							name        = name.Row(i)
							unit        = unit.Row(i)
							description = description.Row(i)
							resource    = resource.Row(i)
							scope       = scope.Row(i)
							attributes  = attributes.Row(i)
						)
						hash := r.collectTimeseries(
							timestamp,
							name, unit, description,
							resource, scope, attributes,
						)
						r.collectLabels(name, mapping, resource, scope, attributes)

						points.timestamp.Append(timestamp)
						points.hash.Append(hash)
					}
					points.value.AppendArr(value)
					points.mapping.AppendArr(mapping)
					points.flags.AppendArr(flags)
				}
				rows = func() int {
					return points.hash.Rows()
				}
			)
			return points.Input(), columns, add, rows
		},
		Logger: r.logger,
	}
	return cfg.Do(ctx, dir, r.tables.Points, r.client)
}

func (r *metricsRestore) restoreExpHistograms(ctx context.Context, dir string) error {
	cfg := restoreTable{
		File: "metrics_exp_histograms",
		NewColumns: func() (proto.Input, Columns, func(), func() int) {
			var (
				name        = new(proto.ColStr).LowCardinality()
				unit        = new(proto.ColStr).LowCardinality()
				description proto.ColStr

				attributes = NewAttributes(colAttrs)
				scope      = NewAttributes(colScope)
				resource   = NewAttributes(colResource)

				timestamp            = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
				count                proto.ColUInt64
				sum                  = new(proto.ColFloat64).Nullable()
				cmin                 = new(proto.ColFloat64).Nullable()
				cmax                 = new(proto.ColFloat64).Nullable()
				scale                proto.ColInt32
				zerocount            proto.ColUInt64
				positiveOffset       proto.ColInt32
				positiveBucketCounts = new(proto.ColUInt64).Array()
				negativeOffset       proto.ColInt32
				negativeBucketCounts = new(proto.ColUInt64).Array()

				flags proto.ColUInt8

				columns = MergeColumns(
					Columns{
						{Name: "timestamp", Data: timestamp},
						{Name: "exp_histogram_count", Data: &count},
						{Name: "exp_histogram_sum", Data: sum},
						{Name: "exp_histogram_min", Data: cmin},
						{Name: "exp_histogram_max", Data: cmax},
						{Name: "exp_histogram_scale", Data: &scale},
						{Name: "exp_histogram_zerocount", Data: &zerocount},
						{Name: "exp_histogram_positive_offset", Data: &positiveOffset},
						{Name: "exp_histogram_positive_bucket_counts", Data: positiveBucketCounts},
						{Name: "exp_histogram_negative_offset", Data: &negativeOffset},
						{Name: "exp_histogram_negative_bucket_counts", Data: negativeBucketCounts},

						{Name: "flags", Data: &flags},
					},
					Columns{
						{Name: "name", Data: name},
						{Name: "unit", Data: unit},
						{Name: "description", Data: &description},
					},
					attributes.Columns(),
					scope.Columns(),
					resource.Columns(),
				)

				histograms = newExpHistogramColumns()
				add        = func() {
					for i := 0; i < timestamp.Rows(); i++ {
						var (
							timestamp   = timestamp.Row(i)
							name        = name.Row(i)
							unit        = unit.Row(i)
							description = description.Row(i)
							resource    = resource.Row(i)
							scope       = scope.Row(i)
							attributes  = attributes.Row(i)
						)
						hash := r.collectTimeseries(
							timestamp,
							name, unit, description,
							resource, scope, attributes,
						)
						r.collectLabels(name, noMapping, resource, scope, attributes)

						histograms.hash.Append(hash)
						histograms.timestamp.Append(timestamp)
						histograms.sum.Append(sum.Row(i))
						histograms.min.Append(cmin.Row(i))
						histograms.max.Append(cmax.Row(i))
						histograms.positiveBucketCounts.Append(positiveBucketCounts.Row(i))
						histograms.negativeBucketCounts.Append(negativeBucketCounts.Row(i))
					}
					histograms.count.AppendArr(count)
					histograms.scale.AppendArr(scale)
					histograms.zerocount.AppendArr(zerocount)
					histograms.positiveOffset.AppendArr(positiveOffset)
					histograms.negativeOffset.AppendArr(negativeOffset)
					histograms.flags.AppendArr(flags)
				}
				rows = func() int {
					return histograms.hash.Rows()
				}
			)
			return histograms.Input(), columns, add, rows
		},
		Logger: r.logger,
	}
	return cfg.Do(ctx, dir, r.tables.ExpHistograms, r.client)
}

func (r *metricsRestore) restoreExemplars(ctx context.Context, dir string) error {
	cfg := restoreTable{
		File: "metrics_exemplars",
		NewColumns: func() (proto.Input, Columns, func(), func() int) {
			var (
				name        = new(proto.ColStr).LowCardinality()
				unit        = new(proto.ColStr).LowCardinality()
				description proto.ColStr

				attributes = NewAttributes(colAttrs)
				scope      = NewAttributes(colScope)
				resource   = NewAttributes(colResource)

				timestamp          = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
				filteredAttributes proto.ColBytes
				exemplarTimestamp  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
				value              proto.ColFloat64
				spanID             proto.ColFixedStr8
				traceID            proto.ColFixedStr16

				columns = MergeColumns(
					Columns{
						{Name: "timestamp", Data: timestamp},

						{Name: "filtered_attributes", Data: &filteredAttributes},
						{Name: "exemplar_timestamp", Data: exemplarTimestamp},
						{Name: "value", Data: &value},
						{Name: "span_id", Data: &spanID},
						{Name: "trace_id", Data: &traceID},
					},
					Columns{
						{Name: "name", Data: name},
						{Name: "unit", Data: unit},
						{Name: "description", Data: &description},
					},
					attributes.Columns(),
					scope.Columns(),
					resource.Columns(),
				)

				exemplars = newExemplarColumns()
				add       = func() {
					for i := 0; i < timestamp.Rows(); i++ {
						var (
							timestamp   = timestamp.Row(i)
							name        = name.Row(i)
							unit        = unit.Row(i)
							description = description.Row(i)
							resource    = resource.Row(i)
							scope       = scope.Row(i)
							attributes  = attributes.Row(i)
						)
						hash := r.collectTimeseries(
							timestamp,
							name, unit, description,
							resource, scope, attributes,
						)
						r.collectLabels(name, noMapping, resource, scope, attributes)

						exemplars.hash.Append(hash)
						exemplars.timestamp.Append(timestamp)
						exemplars.filteredAttributes.Append(filteredAttributes.Row(i))
						exemplars.exemplarTimestamp.Append(exemplarTimestamp.Row(i))
					}

					exemplars.value.AppendArr(value)
					exemplars.spanID.AppendArr(spanID)
					exemplars.traceID.AppendArr(traceID)
				}
				rows = func() int {
					return exemplars.hash.Rows()
				}
			)
			return exemplars.Input(), columns, add, rows
		},
		Logger: r.logger,
	}
	return cfg.Do(ctx, dir, r.tables.Exemplars, r.client)
}

func (r *metricsRestore) collectTimeseries(
	timestamp time.Time,
	name, unit, description string,
	res, scope, attributes otelstorage.Attrs,
) [16]byte {
	r.timeseriesMux.Lock()
	defer r.timeseriesMux.Unlock()

	hash := hashTimeseries(
		name,
		res,
		scope,
		attributes,
	)
	if _, ok := r.seenTimeseries[hash]; ok {
		return hash
	}
	r.seenTimeseries[hash] = struct{}{}

	r.timeseries.hash.Append(hash)
	r.timeseries.name.Append(name)
	r.timeseries.unit.Append(unit)
	r.timeseries.description.Append(description)
	r.timeseries.resource.Append(res)
	r.timeseries.scope.Append(scope)
	r.timeseries.attributes.Append(attributes)
	r.timeseries.firstSeen.Append(timestamp)
	r.timeseries.lastSeen.Append(timestamp)
	return hash
}

func (r *metricsRestore) collectLabels(name string, m metricMapping, res, scope, attributes otelstorage.Attrs) {
	r.labelsMux.Lock()
	defer r.labelsMux.Unlock()

	r.labels[[2]string{metricstorage.MetricName, name}] |= 0
	// NOTE: A summary may have not quantiles, but we still need to return the metric name as-is for label values request.
	if originalName := strings.TrimSuffix(name, m.NameSuffix()); m.IsSummary() && originalName != name {
		r.labels[[2]string{metricstorage.MetricName, originalName}] |= 0
	}

	collectAttrs := func(scope labelScope, attrs otelstorage.Attrs) {
		for k, v := range attrs.AsMap().All() {
			if m != noMapping && (k == "le" || k == "quantile") {
				// Special attributes generated by oteldb/Prometheus to map histograms to plain timeseries.
				continue
			}
			pair := [2]string{k, v.AsString()}
			r.labels[pair] |= scope
		}
	}
	collectAttrs(labelScopeResource, res)
	collectAttrs(labelScopeInstrumentation, scope)
	collectAttrs(labelScopeAttribute, attributes)
}
