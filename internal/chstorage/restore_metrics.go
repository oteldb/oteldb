package chstorage

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

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
			return errors.Wrap(err, "restore exp histograms")
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
	w, err := openBackupReader(dir, "metrics_points")
	if err != nil {
		if os.IsNotExist(err) {
			r.logger.Info("No metrics points backup found", zap.String("dir", dir))
			return nil
		}
		return err
	}
	defer func() {
		_ = w.Close()
	}()
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

		block proto.Block
		rd    = proto.NewReader(w)
	)
	for {
		columns.Reset()
		if err := block.DecodeRawBlock(rd, 54451, columns.Result()); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		points := newPointColumns()
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

			points.hash.Append(hash)
		}
		points.timestamp = timestamp
		points.value = value
		points.mapping = mapping
		points.flags = flags

		input := points.Input()
		if err := r.client.Do(ctx, ch.Query{
			Body:  input.Into(r.tables.Points),
			Input: input,
		}); err != nil {
			return errors.Wrap(err, "insert points")
		}
	}
}

func (r *metricsRestore) restoreExpHistograms(ctx context.Context, dir string) error {
	w, err := openBackupReader(dir, "metrics_exp_histograms")
	if err != nil {
		if os.IsNotExist(err) {
			r.logger.Info("No metrics exp_histograms backup found", zap.String("dir", dir))
			return nil
		}
		return err
	}
	defer func() {
		_ = w.Close()
	}()
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

		block proto.Block
		rd    = proto.NewReader(w)
	)
	for {
		columns.Reset()
		if err := block.DecodeRawBlock(rd, 54451, columns.Result()); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		histograms := newExpHistogramColumns()
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
		}
		histograms.timestamp = timestamp
		histograms.count = count
		histograms.sum = sum
		histograms.min = cmin
		histograms.max = cmax
		histograms.scale = scale
		histograms.zerocount = zerocount
		histograms.positiveOffset = positiveOffset
		histograms.positiveBucketCounts = positiveBucketCounts
		histograms.negativeOffset = negativeOffset
		histograms.negativeBucketCounts = negativeBucketCounts
		histograms.flags = flags

		input := histograms.Input()
		if err := r.client.Do(ctx, ch.Query{
			Body:  input.Into(r.tables.ExpHistograms),
			Input: input,
		}); err != nil {
			return errors.Wrap(err, "insert exp histograms")
		}
	}
}

func (r *metricsRestore) restoreExemplars(ctx context.Context, dir string) error {
	w, err := openBackupReader(dir, "metrics_exemplars")
	if err != nil {
		if os.IsNotExist(err) {
			r.logger.Info("No metrics exemplars backup found", zap.String("dir", dir))
			return nil
		}
		return err
	}
	defer func() {
		_ = w.Close()
	}()
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

		block proto.Block
		rd    = proto.NewReader(w)
	)
	for {
		columns.Reset()
		if err := block.DecodeRawBlock(rd, 54451, columns.Result()); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return err
		}

		exemplars := newExemplarColumns()
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
		}
		exemplars.timestamp = timestamp
		exemplars.filteredAttributes = filteredAttributes
		exemplars.exemplarTimestamp = exemplarTimestamp
		exemplars.value = value
		exemplars.spanID = spanID
		exemplars.traceID = traceID

		input := exemplars.Input()
		if err := r.client.Do(ctx, ch.Query{
			Body:  input.Into(r.tables.Exemplars),
			Input: input,
		}); err != nil {
			return errors.Wrap(err, "insert exemplars")
		}
	}
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

	r.labels[[2]string{labels.MetricName, name}] |= 0

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
