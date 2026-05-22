package chstorage

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/execution/telemetry"
	"github.com/oteldb/promql-engine/extlabels"
	"github.com/oteldb/promql-engine/query"
	promscanners "github.com/oteldb/promql-engine/storage/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/xattribute"
)

const promStaleNaNBits uint64 = 0x7ff0000000000002

type rateSelector struct {
	telemetry telemetry.OperatorTelemetry

	querier *promQuerier
	filter  promscanners.Filter
	once    sync.Once

	series      []labels.Labels
	pointSeries []*series[pointData]

	params metricSelectParams

	numSteps        int
	mint            int64
	maxt            int64
	step            int64
	seriesBatchSize int64

	currentSeries int64
	currentStep   int64
}

func newRateSelector(
	querier *promQuerier,
	filters []*labels.Matcher,
	queryOpts *query.Options,
	params metricSelectParams,
	offset time.Duration,
	batchSize int64,
) model.VectorOperator {
	params.Offset = offset
	o := &rateSelector{
		querier: querier,
		filter:  promscanners.NewFilter(filters),

		params: params,

		mint:            queryOpts.Start.UnixMilli(),
		maxt:            queryOpts.End.UnixMilli(),
		step:            queryOpts.Step.Milliseconds(),
		currentStep:     queryOpts.Start.UnixMilli(),
		numSteps:        queryOpts.NumStepsPerBatch(),
		seriesBatchSize: batchSize,
	}
	if o.step == 0 {
		o.step = 1
	}
	o.telemetry = telemetry.NewTelemetry(o, queryOpts)
	return telemetry.NewOperator(o.telemetry, o)
}

func (o *rateSelector) String() string {
	return fmt.Sprintf("[chstorage.rateSelector] %s({%v}[%s])", o.params.Function, o.params.Matchers, o.params.Range)
}

func (o *rateSelector) Explain() (next []model.VectorOperator) {
	return nil
}

func (o *rateSelector) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *rateSelector) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}
	if o.currentStep > o.maxt {
		return 0, nil
	}
	if err := o.loadSeries(ctx); err != nil {
		return 0, err
	}

	totalSeries := int64(len(o.pointSeries))
	maxSteps := min(o.numSteps, len(buf))
	remainingSeries := int64(len(o.series)) - o.currentSeries
	expectedSamples := int(min(o.seriesBatchSize, remainingSeries))
	if expectedSamples <= 0 {
		expectedSamples = len(o.series)
	}

	n := 0
	ts := o.currentStep
	for currStep := 0; currStep < maxSteps && ts <= o.maxt; currStep++ {
		buf[n].Reset(ts)
		n++
		ts += o.step
	}

	ts = o.currentStep
	fromSeries := o.currentSeries
	for ; o.currentSeries-fromSeries < o.seriesBatchSize && o.currentSeries < totalSeries; o.currentSeries++ {
		var (
			series          = o.pointSeries[o.currentSeries]
			seriesTimestamp = ts
			idx             int
			step            = computeStep(series.ts)
		)
		for currStep := 0; currStep < n && seriesTimestamp <= o.maxt; currStep++ {
			currStepSamples := 0
			if v, ok := selectExactPoint(series.ts, series.data.values, &idx, step, seriesTimestamp); ok {
				buf[currStep].AppendSampleWithSizeHint(uint64(o.currentSeries), v, expectedSamples)
				currStepSamples++
			}
			o.telemetry.IncrementSamplesAtTimestamp(currStepSamples, seriesTimestamp)
			seriesTimestamp += o.step
		}
	}

	if o.currentSeries == totalSeries {
		o.currentStep += o.step * int64(n)
		o.currentSeries = 0
	}
	return n, nil
}

func (o *rateSelector) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		ctx, span := o.querier.tracer.Start(ctx, "chstorage.metrics.rateSelector.loadSeries", trace.WithAttributes(
			attribute.Int64("promql.selector.start", o.params.Start.UnixMilli()),
			attribute.Int64("promql.selector.end", o.params.End.UnixMilli()),
			attribute.Int64("promql.selector.mint", o.mint),
			attribute.Int64("promql.selector.maxt", o.maxt),
			attribute.Int64("promql.selector.step", o.step),
			attribute.Int64("promql.selector.range", o.params.Range.Milliseconds()),
			attribute.Int64("promql.selector.offset", o.params.Offset.Milliseconds()),
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

		o.series = make([]labels.Labels, 0, len(r.points))
		o.pointSeries = make([]*series[pointData], 0, len(r.points))
		var b labels.ScratchBuilder
		for _, s := range r.points {
			if !o.filter.Matches(s) {
				continue
			}
			s.labels = extlabels.DropReserved(s.labels, b)
			o.series = append(o.series, s.labels)
			o.pointSeries = append(o.pointSeries, s)
		}
		numSeries := int64(len(o.series))
		if o.seriesBatchSize == 0 || numSeries < o.seriesBatchSize {
			o.seriesBatchSize = numSeries
		}
	})
	return err
}

func selectExactPoint(tss []int64, samples []float64, idx *int, step, ts int64) (float64, bool) {
	if !seekIterator(tss, idx, step, ts) || *idx >= len(tss) || tss[*idx] != ts {
		return 0, false
	}
	return samples[*idx], true
}

func (p *promQuerier) queryRatePoints(
	ctx context.Context,
	start, end time.Time,
	step, window, offset time.Duration,
	timeseries map[[16]byte]labels.Labels,
) ([]*series[pointData], error) {
	m, err := p.queryRatePointsByHash(ctx, start, end, step, window, offset, timeseries)
	if err != nil {
		return nil, err
	}
	result := make([]*series[pointData], 0, len(m))
	for _, s := range m {
		result = append(result, s)
	}
	return result, nil
}

func (p *promQuerier) queryRatePointsCached(
	ctx context.Context,
	start, end time.Time,
	step, window, offset time.Duration,
	timeseries map[[16]byte]labels.Labels,
) (_ []*series[pointData], rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryRatePointsCached",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
			attribute.Stringer("chstorage.window", window),
			attribute.Stringer("chstorage.offset", offset),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	fetch := func(ctx context.Context, fetchStart, fetchEnd time.Time) (map[[16]byte]*series[pointData], error) {
		return p.queryRatePointsByHashFunc(ctx, fetchStart, fetchEnd, step, window, offset, timeseries)
	}
	resultMap, err := p.fetchAndMergeCache(ctx, span, start, end, step, "rate", timeseries, fetch)
	if err != nil {
		return nil, err
	}

	result := make([]*series[pointData], 0, len(resultMap))
	for _, s := range resultMap {
		result = append(result, s)
	}

	totalPoints := 0
	for _, s := range result {
		totalPoints += len(s.ts)
	}
	span.AddEvent("chstorage.merged_result", trace.WithAttributes(
		attribute.Int("chstorage.merged_series", len(result)),
		attribute.Int("chstorage.merged_points", totalPoints),
	))

	return result, nil
}

func (p *promQuerier) queryRatePointsByHash(
	ctx context.Context,
	start, end time.Time,
	step, window, offset time.Duration,
	timeseries map[[16]byte]labels.Labels,
) (_ map[[16]byte]*series[pointData], rerr error) {
	table := p.tables.Points
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryRatePoints",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
			attribute.Stringer("chstorage.window", window),
			attribute.Stringer("chstorage.offset", offset),
			attribute.String("chstorage.table", table),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	if len(timeseries) == 0 || start.IsZero() || end.IsZero() || window <= 0 {
		return nil, nil
	}

	stepMS := step.Milliseconds()
	if stepMS <= 0 {
		stepMS = window.Milliseconds()
		if stepMS <= 0 {
			stepMS = 1
		}
	}
	windowMS := window.Milliseconds()
	offsetMS := offset.Milliseconds()
	rawStart := start.Add(-offset - window)
	rawEnd := end.Add(-offset)

	var (
		inputTable = "timeseries_hashes"
		inputData  proto.ColFixedStr16

		hash    = proto.NewLowCardinality(&proto.ColFixedStr16{})
		stepTS  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		firstTS = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		lastTS  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		firstV  proto.ColFloat64
		lastV   proto.ColFloat64
		reset   proto.ColFloat64
		samples proto.ColUInt64
	)
	for h := range timeseries {
		inputData.Append(h)
	}

	var (
		firstStepMSIdent = chsql.Ident("first_step_ms")
		lastStepMSIdent  = chsql.Ident("last_step_ms")
		stepMSIdent      = chsql.Ident("step_ms")
		windowMSIdent    = chsql.Ident("window_ms")
		offsetMSIdent    = chsql.Ident("offset_ms")
		pointMSIdent     = chsql.Ident("point_ms")
	)

	var (
		toInt64 = chsql.ToInt64Val[int64]
		tuple   = chsql.ColumnTupleElement
	)

	var (
		pointOffsetMS = chsql.Add(pointMSIdent, offsetMSIdent)
		// offset Expr: greatest(0, point_ms + offset_ms - first_step_ms + step_ms - 1)
		offsetFromStart = chsql.Greatest(
			chsql.ToInt64(chsql.Integer(0)),
			chsql.Sub(
				chsql.Add(pointOffsetMS, chsql.Sub(stepMSIdent, chsql.Integer(1))),
				firstStepMSIdent,
			),
		)
		// first_step_ms + intDiv(offsetFromStart, step_ms) * step_ms
		firstSampleStepMS = chsql.Add(
			firstStepMSIdent,
			chsql.Mul(chsql.IntDiv(offsetFromStart, stepMSIdent), stepMSIdent),
		)
		mapLambda = chsql.Lambda([]string{"i"},
			chsql.Add(firstSampleStepMS, chsql.Mul(chsql.ColumnToInt64("i"), stepMSIdent)),
		)
		rangeExpr = chsql.Range(chsql.ToUInt64(chsql.Add(chsql.IntDiv(windowMSIdent, stepMSIdent), chsql.Integer(1))))

		filterLambda = chsql.Lambda([]string{"s"}, chsql.JoinAnd(
			chsql.Gte(chsql.Ident("s"), firstStepMSIdent),
			chsql.Lte(chsql.Ident("s"), lastStepMSIdent),
			chsql.Lt(chsql.Ident("s"), chsql.Add(pointOffsetMS, windowMSIdent)),
		))

		stepExpr = chsql.ArrayJoin(
			chsql.ArrayFilter(
				filterLambda,
				chsql.ArrayMap(mapLambda, rangeExpr),
			),
		)
	)

	var (
		pairs       = chsql.GroupArray(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")))
		sortedPairs = chsql.ArraySort(
			chsql.Lambda([]string{"x"}, tuple("x", 1)),
			pairs,
		)
		valsExpr = chsql.ArrayMap(
			chsql.Lambda([]string{"x"}, tuple("x", 2)),
			sortedPairs,
		)
	)

	var (
		resetSumLambda = chsql.Lambda([]string{"curr", "prev"},
			chsql.If(
				chsql.Lt(chsql.Ident("curr"), chsql.Ident("prev")),
				chsql.Ident("prev"),
				chsql.ToFloat64(chsql.Integer(0)),
			),
		)
		resetSum = chsql.ArraySum(
			resetSumLambda,
			chsql.ArrayPopFront(chsql.Ident("vals")),
			chsql.ArrayPopBack(chsql.Ident("vals")),
		)
		resetExpr = chsql.If(
			chsql.Gt(chsql.Function("length", chsql.Ident("vals")), chsql.Integer(1)),
			resetSum,
			chsql.ToFloat64(chsql.Integer(0)),
		)
	)

	// Subquery 1: Filter points and map each point to the set of steps it belongs to.
	expanded := chsql.Select(table,
		chsql.Column("hash", nil),
		chsql.Column("timestamp", nil),
		chsql.Column("value", nil),
		chsql.ResultColumn{Name: "step_ms_val", Expr: stepExpr},
	).
		With("first_step_ms", toInt64(start.UnixMilli())).
		With("last_step_ms", toInt64(end.UnixMilli())).
		With("step_ms", toInt64(stepMS)).
		With("window_ms", toInt64(windowMS)).
		With("offset_ms", toInt64(offsetMS)).
		With("point_ms", chsql.ToUnixTimestamp64Milli(chsql.Ident("timestamp"))).
		Where(
			chsql.Gt(chsql.Ident("timestamp"), chsql.DateTime64(rawStart, proto.PrecisionMilli)),
			chsql.Lte(chsql.Ident("timestamp"), chsql.DateTime64(rawEnd, proto.PrecisionMilli)),
			chsql.In(chsql.Ident("hash"), chsql.Ident(inputTable)),
			chsql.NotEq(chsql.ReinterpretAsUInt64(chsql.Ident("value")), chsql.Integer(promStaleNaNBits)),
		)

	// Subquery 2: Group by series and step, then compute rate calculation components (first/last points, reset sum).
	aggregated := chsql.SelectFrom(expanded,
		chsql.Column("hash", nil),
		chsql.Column("step_ms_val", nil),
		chsql.ResultColumn{Name: "first_pair", Expr: chsql.ArgMin(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")), chsql.Ident("timestamp"))},
		chsql.ResultColumn{Name: "last_pair", Expr: chsql.ArgMax(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")), chsql.Ident("timestamp"))},
		chsql.ResultColumn{Name: "vals", Expr: valsExpr},
		chsql.ResultColumn{Name: "samples", Expr: chsql.Function("length", chsql.Ident("vals"))},
		chsql.ResultColumn{Name: "reset_sum", Expr: resetExpr},
	).
		GroupBy(chsql.Ident("hash"), chsql.Ident("step_ms_val")).
		Having(chsql.Gt(chsql.Ident("samples"), chsql.Integer(1)))

	// Final query: Select and map columns to their respective Go types for processing.
	rateQuery := chsql.SelectFrom(aggregated,
		chsql.ResultColumn{Name: "hash", Expr: chsql.Ident("hash"), Data: hash},
		chsql.ResultColumn{Name: "step_ts", Expr: chsql.ToDateTime64(chsql.Div(chsql.Ident("step_ms_val"), chsql.Float(1000.0)), proto.PrecisionMilli), Data: stepTS},
		chsql.ResultColumn{Name: "first_t", Expr: tuple("first_pair", 1), Data: firstTS},
		chsql.ResultColumn{Name: "first_v", Expr: tuple("first_pair", 2), Data: &firstV},
		chsql.ResultColumn{Name: "last_t", Expr: tuple("last_pair", 1), Data: lastTS},
		chsql.ResultColumn{Name: "last_v", Expr: tuple("last_pair", 2), Data: &lastV},
		chsql.ResultColumn{Name: "samples", Expr: chsql.Ident("samples"), Data: &samples},
		chsql.ResultColumn{Name: "reset_sum", Expr: chsql.Ident("reset_sum"), Data: &reset},
	).
		Order(chsql.Ident("hash"), chsql.Asc).
		Order(chsql.Ident("step_ts"), chsql.Asc)

	var (
		set         = map[[16]byte]*series[pointData]{}
		totalPoints int
	)
	if err := p.do(ctx, selectQuery{
		Query:         rateQuery,
		ExternalTable: inputTable,
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: &inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < stepTS.Rows(); i++ {
				var (
					h      = hash.Row(i)
					stepT  = stepTS.Row(i)
					firstT = firstTS.Row(i)
					lastT  = lastTS.Row(i)
				)
				value, ok := extrapolatedRateValue(rateWindow{
					StepTime: stepT.UnixMilli(),
					Range:    window.Milliseconds(),
					Offset:   offset.Milliseconds(),
					FirstT:   firstT.UnixMilli(),
					FirstV:   firstV.Row(i),
					LastT:    lastT.UnixMilli(),
					LastV:    lastV.Row(i),
					ResetSum: reset.Row(i),
					Samples:  int(samples.Row(i)),
				})
				if !ok {
					continue
				}

				s, ok := set[h]
				if !ok {
					lb, ok := timeseries[h]
					if !ok {
						continue
					}
					s = &series[pointData]{
						labels: lb,
					}
					set[h] = s
				}
				s.ts = append(s.ts, stepT.UnixMilli())
				s.data.values = append(s.data.values, value)
				totalPoints++
			}
			return nil
		},

		Type:   "QueryRatePoints",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("rate_points_fetched", trace.WithAttributes(
		attribute.Int("chstorage.total_series", len(set)),
		attribute.Int("chstorage.total_points", totalPoints),
	))

	return set, nil
}

type rateWindow struct {
	StepTime int64
	Range    int64
	Offset   int64

	FirstT int64
	FirstV float64
	LastT  int64
	LastV  float64

	ResetSum float64
	Samples  int
}

func extrapolatedRateValue(w rateWindow) (float64, bool) {
	if w.Samples < 2 || w.Range <= 0 || w.FirstT == w.LastT {
		return 0, false
	}

	rangeStart := w.StepTime - (w.Range + w.Offset)
	rangeEnd := w.StepTime - w.Offset

	resultValue := w.LastV - w.FirstV + w.ResetSum
	durationToStart := float64(w.FirstT-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-w.LastT) / 1000
	sampledInterval := float64(w.LastT-w.FirstT) / 1000
	if sampledInterval <= 0 {
		return 0, false
	}

	averageDurationBetweenSamples := sampledInterval / float64(w.Samples-1)
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}
	if resultValue > 0 && w.FirstV >= 0 {
		durationToZero := sampledInterval * (w.FirstV / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}
	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}

	factor := (sampledInterval + durationToStart + durationToEnd) / sampledInterval
	factor /= float64(w.Range) / 1000
	value := resultValue * factor
	if math.IsNaN(value) {
		return 0, false
	}
	return value, true
}
