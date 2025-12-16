package chstorage

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/promql-engine/execution/exchange"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/parse"
	"github.com/oteldb/promql-engine/execution/telemetry"
	"github.com/oteldb/promql-engine/extlabels"
	"github.com/oteldb/promql-engine/logicalplan"
	"github.com/oteldb/promql-engine/query"
	enginestorage "github.com/oteldb/promql-engine/storage"
	promscanners "github.com/oteldb/promql-engine/storage/prometheus"
	"github.com/oteldb/promql-engine/warnings"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/promql"
	"github.com/go-faster/oteldb/internal/xattribute"
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
	return &promScanners{storage: q}, nil
}

// NewVectorSelector selects a PromQL vector from the storage.
func (s *promScanners) NewVectorSelector(
	ctx context.Context,
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.VectorSelector,
) (_ model.VectorOperator, rerr error) {
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

	op := newVectorSelector(
		model.NewVectorPool(opts.StepsBatch),
		q,
		logicalNode.Filters,
		opts,
		metricSelectParams{
			Matchers:        logicalNode.LabelMatchers,
			Step:            opts.Step,
			Start:           time.UnixMilli(hints.Start),
			End:             time.UnixMilli(hints.End),
			Range:           time.Duration(hints.Range) * time.Millisecond,
			LookbackDelta:   opts.LookbackDelta,
			Function:        hints.Func,
			SelectTimestamp: logicalNode.SelectTimestamp,
			GroupBy:         hints.By,
			Grouping:        hints.Grouping,
		},
		logicalNode.Offset,
		logicalNode.BatchSize,
	)
	op = exchange.NewConcurrent(op, 2, opts)
	return op, nil
}

// NewVectorSelector selects a PromQL matrix from the storage.
func (s *promScanners) NewMatrixSelector(
	ctx context.Context,
	opts *query.Options,
	hints storage.SelectHints,
	logicalNode logicalplan.MatrixSelector,
	call logicalplan.FunctionCall,
) (_ model.VectorOperator, rerr error) {
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
		selector = promql.NewHistogramStatsSelector(selector)
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

func (s *promScanners) makeFilteredSelector(q storage.Querier, matchers, filters []*labels.Matcher, hints storage.SelectHints) promscanners.SeriesSelector {
	return promql.NewFilteredSelector(q, matchers, filters, hints)
}

type vectorSelector struct {
	telemetry telemetry.OperatorTelemetry

	querier    *promQuerier
	filter     promscanners.Filter
	once       sync.Once
	vectorPool *model.VectorPool

	series        []labels.Labels
	pointSeries   []*series[pointData]
	expHistSeries []*series[expHistData]

	params metricSelectParams

	numSteps        int
	mint            int64
	maxt            int64
	step            int64
	lookbackDelta   int64
	offset          int64
	seriesBatchSize int64

	currentSeries int64
	currentStep   int64
}

// newVectorSelector creates operator which selects vector of series.
func newVectorSelector(
	pool *model.VectorPool,
	querier *promQuerier,
	filters []*labels.Matcher,
	queryOpts *query.Options,
	params metricSelectParams,
	offset time.Duration,
	batchSize int64,
) model.VectorOperator {
	o := &vectorSelector{
		querier:    querier,
		filter:     promscanners.NewFilter(filters),
		vectorPool: pool,

		params: params,

		mint:            queryOpts.Start.UnixMilli(),
		maxt:            queryOpts.End.UnixMilli(),
		step:            queryOpts.Step.Milliseconds(),
		currentStep:     queryOpts.Start.UnixMilli(),
		lookbackDelta:   queryOpts.LookbackDelta.Milliseconds(),
		offset:          offset.Milliseconds(),
		numSteps:        queryOpts.NumSteps(),
		seriesBatchSize: batchSize,
	}

	// For instant queries, set the step to a positive value
	// so that the operator can terminate.
	if o.step == 0 {
		o.step = 1
		o.params.Step = time.Millisecond
	}

	o.telemetry = telemetry.NewTelemetry(o, queryOpts)
	return telemetry.NewOperator(o.telemetry, o)
}

func (o *vectorSelector) String() string {
	return fmt.Sprintf("[chstorage.vectorSelector] {%v}", o.params.Matchers)
}

func (o *vectorSelector) Explain() (next []model.VectorOperator) {
	return nil
}

func (o *vectorSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *vectorSelector) GetPool() *model.VectorPool {
	return o.vectorPool
}

func (o *vectorSelector) Next(ctx context.Context) ([]model.StepVector, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if o.currentStep > o.maxt {
		return nil, nil
	}

	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	totalSeries := int64(len(o.pointSeries) + len(o.expHistSeries))

	ts := o.currentStep
	vectors := o.vectorPool.GetVectorBatch()
	for currStep := 0; currStep < o.numSteps && ts <= o.maxt; currStep++ {
		vectors = append(vectors, o.vectorPool.GetStepVector(ts))
		ts += o.step
	}

	var currStepSamples int
	// Reset the current timestamp.
	ts = o.currentStep
	fromSeries := o.currentSeries

	for ; o.currentSeries-fromSeries < o.seriesBatchSize && o.currentSeries < totalSeries; o.currentSeries++ {
		if o.currentSeries < int64(len(o.pointSeries)) {
			var (
				series          = o.pointSeries[o.currentSeries]
				seriesTimestamp = ts
			)
			for currStep := 0; currStep < o.numSteps && seriesTimestamp <= o.maxt; currStep++ {
				currStepSamples = 0
				t, v, ok := o.selectPoint(series.ts, series.data.values, seriesTimestamp, o.lookbackDelta, o.offset)
				if o.params.SelectTimestamp {
					v = float64(t) / 1000
				}
				if ok {
					vectors[currStep].AppendSample(o.vectorPool, uint64(o.currentSeries), v)
					currStepSamples++
				}
				o.telemetry.IncrementSamplesAtTimestamp(currStepSamples, seriesTimestamp)
				seriesTimestamp += o.step
			}
		} else {
			var (
				seriesIdx       = o.currentSeries - int64(len(o.pointSeries))
				series          = o.expHistSeries[seriesIdx]
				seriesTimestamp = ts
			)
			for currStep := 0; currStep < o.numSteps && seriesTimestamp <= o.maxt; currStep++ {
				currStepSamples = 0
				_, h, ok, err := o.selectExpHistPoint(series.ts, series.data, seriesTimestamp, o.lookbackDelta, o.offset)
				if err != nil {
					return nil, err
				}
				fh := h.ToFloat(nil)

				if ok {
					vectors[currStep].AppendHistogram(o.vectorPool, uint64(o.currentSeries), fh)
					currStepSamples += telemetry.CalculateHistogramSampleCount(fh)
				}
				o.telemetry.IncrementSamplesAtTimestamp(currStepSamples, seriesTimestamp)
				seriesTimestamp += o.step
			}
		}
	}

	if o.currentSeries == totalSeries {
		o.currentStep += o.step * int64(o.numSteps)
		o.currentSeries = 0
	}
	return vectors, nil
}

func (o *vectorSelector) selectPoint(tss []int64, samples []float64, ts, lookbackDelta, offset int64) (t int64, v float64, _ bool) {
	var (
		refTime = ts - offset
		idx     int
	)
	if !seekIterator(tss, &idx, refTime) {
		// Look for previous sample.
		idx--
		if idx < 0 || idx >= len(tss) {
			return 0, 0, false
		}
		prevT := tss[idx]
		if prevT <= refTime-lookbackDelta {
			return 0, 0, false
		}
	}
	t, v = tss[idx], samples[idx]

	if value.IsStaleNaN(v) {
		return 0, 0, false
	}
	return t, v, true
}

func (o *vectorSelector) selectExpHistPoint(tss []int64, samples expHistData, ts, lookbackDelta, offset int64) (t int64, h histogram.Histogram, _ bool, _ error) {
	var (
		refTime = ts - offset

		idx int
	)
	if !seekIterator(tss, &idx, refTime) {
		// Look for previous sample.
		idx--
		if idx < 0 || idx >= len(tss) {
			return 0, h, false, nil
		}
		prevT := tss[idx]
		if prevT <= refTime-lookbackDelta {
			return 0, h, false, nil
		}
	}

	t = tss[idx]
	h, err := samples.value(idx)
	if err != nil {
		return 0, h, false, err
	}

	if value.IsStaleNaN(h.Sum) {
		return 0, h, false, nil
	}
	return t, h, true, nil
}

func canUseSampledPoints(stepDuration time.Duration, fn string) bool {
	if stepDuration < time.Second {
		return false
	}
	switch fn {
	case "", "count":
		return true
	default:
		return false
	}
}

func (o *vectorSelector) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		ctx, span := o.querier.tracer.Start(ctx, "chstorage.metrics.vectorSelector.loadSeries", trace.WithAttributes(
			attribute.Int64("promql.selector.start", o.params.Start.UnixMilli()),
			attribute.Int64("promql.selector.end", o.params.End.UnixMilli()),
			attribute.Int64("promql.selector.mint", o.mint),
			attribute.Int64("promql.selector.maxt", o.maxt),
			attribute.Int64("promql.selector.step", o.step),
			attribute.Int64("promql.selector.lookback_delta", o.lookbackDelta),
			attribute.Int64("promql.selector.range", o.params.Range.Milliseconds()),
			attribute.String("promql.selector.func", o.params.Function),
			attribute.Bool("promql.selector.select_timestamp", o.params.SelectTimestamp),
			attribute.Bool("promql.selector.group_by", o.params.GroupBy),
			attribute.StringSlice("promql.selector.grouping", o.params.Grouping),
			xattribute.StringerSlice("promql.selector.matchers", o.params.Matchers),
			xattribute.StringerSlice("promql.selector.filter", o.filter.Matchers()),
		))
		defer func() {
			if err != nil {
				span.RecordError(err)
			}
			span.End()
		}()

		r, queryErr := o.querier.querySeriesSingleflight(ctx, true, o.params)
		if queryErr != nil {
			err = queryErr
			return
		}

		o.series = make([]labels.Labels, 0, len(r.points)+len(r.expHist))
		b := labels.NewBuilder(labels.EmptyLabels())

		o.pointSeries = make([]*series[pointData], 0, len(r.points))
		for _, s := range r.points {
			if !o.filter.Matches(s) {
				continue
			}
			o.pointSeries = append(o.pointSeries, s)

			// if we have pushed down a timestamp function into the scan we need to drop
			// the reserved labels (__name__, __type__, __unit__)
			b.Reset(s.labels)
			if o.params.SelectTimestamp {
				b.Del(labels.MetricName)
				b.Del(extlabels.MetricType)
				b.Del(extlabels.MetricUnit)
			}
			o.series = append(o.series, b.Labels())
		}

		o.expHistSeries = make([]*series[expHistData], 0, len(r.expHist))
		for _, s := range r.expHist {
			if !o.filter.Matches(s) {
				continue
			}
			o.expHistSeries = append(o.expHistSeries, s)

			// if we have pushed down a timestamp function into the scan we need to drop
			// the reserved labels (__name__, __type__, __unit__)
			b.Reset(s.labels)
			if o.params.SelectTimestamp {
				b.Del(labels.MetricName)
				b.Del(extlabels.MetricType)
				b.Del(extlabels.MetricUnit)
			}
			o.series = append(o.series, b.Labels())
		}

		numSeries := int64(len(o.series))
		if o.seriesBatchSize == 0 || numSeries < o.seriesBatchSize {
			o.seriesBatchSize = numSeries
		}
		o.vectorPool.SetStepSize(int(o.seriesBatchSize))
	})
	return err
}
