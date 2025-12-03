package chstorage

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/promql-engine/execution/exchange"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/parse"
	"github.com/oteldb/promql-engine/logicalplan"
	"github.com/oteldb/promql-engine/query"
	enginestorage "github.com/oteldb/promql-engine/storage"
	promscanners "github.com/oteldb/promql-engine/storage/prometheus"
	"github.com/oteldb/promql-engine/warnings"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
)

type promScanners struct {
	storage *Querier
}

var _ enginestorage.Scanners = (*promScanners)(nil)

func (s *promScanners) Close() error {
	return nil
}

// MetricsScanners returns scanners implementation to use with thanos-io PromQL engine.
func (q *Querier) MetricsScanners() (enginestorage.Scanners, error) {
	return &promScanners{
		storage: q,
	}, nil
}

// NewVectorSelector selects a PromQL vector from the storage.
func (s *promScanners) NewVectorSelector(
	_ context.Context,
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.VectorSelector,
) (model.VectorOperator, error) {
	// Update hints with projection information if available
	if logicalNode.Projection != nil {
		hints.ProjectionLabels = logicalNode.Projection.Labels
		hints.ProjectionInclude = logicalNode.Projection.Include
	}

	pq, err := s.storage.Querier(hints.Start, hints.End)
	if err != nil {
		return nil, errors.Wrap(err, "make querier")
	}
	q := pq.(*promQuerier)

	selector := s.makeFilteredSelector(q, logicalNode.VectorSelector.LabelMatchers, logicalNode.Filters, hints)
	if logicalNode.DecodeNativeHistogramStats {
		selector = newHistogramStatsSelector(selector)
	}
	if canUseSampledPoints(time.Duration(hints.Step)*time.Millisecond, &hints) {
		// HACK(tdakkota): Since we generate the step entirely within ClickHouse, at high step values the
		// 	PromQL engine may consider the metric stale due to a low data point density.
		cpyOpts := *opts
		cpyOpts.LookbackDelta = math.MaxInt64
		cpyOpts.ExtLookbackDelta = math.MaxInt64
		opts = &cpyOpts
	}

	op := promscanners.NewVectorSelector(
		model.NewVectorPool(opts.StepsBatch),
		selector,
		opts,
		logicalNode.Offset,
		logicalNode.BatchSize,
		logicalNode.SelectTimestamp,
		0,
		1,
	)
	op = exchange.NewConcurrent(op, 2, opts)
	return op, nil
}

func (s *promScanners) makeSelector(q storage.Querier, matchers []*labels.Matcher, hints storage.SelectHints) promscanners.SeriesSelector {
	return newSeriesSelector(q, matchers, hints)
}

type seriesSelector struct {
	storage  storage.Querier
	matchers []*labels.Matcher
	hints    storage.SelectHints

	once   sync.Once
	series []promscanners.SignedSeries
}

func newSeriesSelector(querier storage.Querier, matchers []*labels.Matcher, hints storage.SelectHints) *seriesSelector {
	return &seriesSelector{
		storage:  querier,
		matchers: matchers,
		hints:    hints,
	}
}

func (o *seriesSelector) Matchers() []*labels.Matcher {
	return o.matchers
}

func (o *seriesSelector) GetSeries(ctx context.Context, shard, numShards int) ([]promscanners.SignedSeries, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return seriesShard(o.series, shard, numShards), nil
}

func (o *seriesSelector) loadSeries(ctx context.Context) error {
	seriesSet := o.storage.Select(ctx, false, &o.hints, o.matchers...)
	i := 0
	for seriesSet.Next() {
		s := seriesSet.At()
		o.series = append(o.series, promscanners.SignedSeries{
			Series:    s,
			Signature: uint64(i),
		})
		i++
	}

	for _, w := range seriesSet.Warnings() {
		warnings.AddToContext(w, ctx)
	}
	return seriesSet.Err()
}

func (s *promScanners) makeFilteredSelector(q storage.Querier, matchers, filters []*labels.Matcher, hints storage.SelectHints) promscanners.SeriesSelector {
	sub := s.makeSelector(q, matchers, hints)
	return newFilteredSelector(sub, promscanners.NewFilter(filters))
}

type filteredSelector struct {
	selector promscanners.SeriesSelector
	filter   promscanners.Filter

	once   sync.Once
	series []promscanners.SignedSeries
}

func newFilteredSelector(selector promscanners.SeriesSelector, filter promscanners.Filter) promscanners.SeriesSelector {
	return &filteredSelector{
		selector: selector,
		filter:   filter,
	}
}

func (f *filteredSelector) Matchers() []*labels.Matcher {
	return slices.Concat(f.selector.Matchers(), f.filter.Matchers())
}

func (f *filteredSelector) GetSeries(ctx context.Context, shard, numShards int) ([]promscanners.SignedSeries, error) {
	var err error
	f.once.Do(func() { err = f.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return seriesShard(f.series, shard, numShards), nil
}

func seriesShard(series []promscanners.SignedSeries, index, numShards int) []promscanners.SignedSeries {
	start := index * len(series) / numShards
	end := min((index+1)*len(series)/numShards, len(series))

	slice := series[start:end]
	shard := make([]promscanners.SignedSeries, len(slice))
	copy(shard, slice)

	for i := range shard {
		shard[i].Signature = uint64(i)
	}
	return shard
}

func (f *filteredSelector) loadSeries(ctx context.Context) error {
	series, err := f.selector.GetSeries(ctx, 0, 1)
	if err != nil {
		return err
	}

	var i uint64
	f.series = make([]promscanners.SignedSeries, 0, len(series))
	for _, s := range series {
		if f.filter.Matches(s) {
			f.series = append(f.series, promscanners.SignedSeries{
				Series:    s.Series,
				Signature: i,
			})
			i++
		}
	}

	return nil
}

// NewVectorSelector selects a PromQL matrix from the storage.
func (s *promScanners) NewMatrixSelector(
	ctx context.Context,
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.MatrixSelector,
	call logicalplan.FunctionCall,
) (model.VectorOperator, error) {
	arg := 0.0
	arg2 := 0.0
	switch call.Func.Name {
	case "quantile_over_time":
		unwrap, err := logicalplan.UnwrapFloat(call.Args[0])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "quantile_over_time with expression as first argument is not supported")
		}
		arg = unwrap
		if math.IsNaN(unwrap) || unwrap < 0 || unwrap > 1 {
			warnings.AddToContext(annotations.NewInvalidQuantileWarning(unwrap, posrange.PositionRange{}), ctx)
		}
	case "predict_linear":
		unwrap, err := logicalplan.UnwrapFloat(call.Args[1])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "predict_linear with expression as second argument is not supported")
		}
		arg = unwrap
	case "double_exponential_smoothing":
		sf, err := logicalplan.UnwrapFloat(call.Args[1])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "double_exponential_smoothing with expression as second argument is not supported")
		}

		tf, err := logicalplan.UnwrapFloat(call.Args[2])
		if err != nil {
			return nil, errors.Wrapf(parse.ErrNotSupportedExpr, "double_exponential_smoothing with expression as third argument is not supported")
		}

		if sf <= 0 || sf >= 1 || tf <= 0 || tf >= 1 {
			return nil, nil
		}
		arg = sf
		arg2 = tf
	}

	vs := logicalNode.VectorSelector
	if vs.Projection != nil {
		hints.ProjectionLabels = vs.Projection.Labels
		hints.ProjectionInclude = vs.Projection.Include
	}

	q, err := s.storage.Querier(hints.Start, hints.End)
	if err != nil {
		return nil, errors.Wrap(err, "make querier")
	}

	selector := s.makeFilteredSelector(q, vs.LabelMatchers, vs.Filters, hints)
	if logicalNode.VectorSelector.DecodeNativeHistogramStats {
		selector = newHistogramStatsSelector(selector)
	}

	op, err := promscanners.NewMatrixSelector(
		model.NewVectorPool(opts.StepsBatch),
		selector,
		call.Func.Name,
		arg,
		arg2,
		opts,
		logicalNode.Range,
		vs.Offset,
		vs.BatchSize,
		0,
		1,
	)
	if err != nil {
		return nil, err
	}
	op = exchange.NewConcurrent(op, 2, opts)
	return op, nil
}

type histogramStatsSelector struct {
	promscanners.SeriesSelector
}

func newHistogramStatsSelector(seriesSelector promscanners.SeriesSelector) histogramStatsSelector {
	return histogramStatsSelector{SeriesSelector: seriesSelector}
}

func (h histogramStatsSelector) GetSeries(ctx context.Context, shard, numShards int) ([]promscanners.SignedSeries, error) {
	series, err := h.SeriesSelector.GetSeries(ctx, shard, numShards)
	if err != nil {
		return nil, err
	}
	for i := range series {
		series[i].Series = newHistogramStatsSeries(series[i].Series)
	}
	return series, nil
}

type histogramStatsSeries struct {
	storage.Series
}

func newHistogramStatsSeries(series storage.Series) histogramStatsSeries {
	return histogramStatsSeries{Series: series}
}

func (h histogramStatsSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	return promql.NewHistogramStatsIterator(h.Series.Iterator(it))
}
