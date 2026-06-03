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

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/xattribute"
)

const promStaleNaNBits uint64 = 0x7ff0000000000002

// rateKind identifies which PromQL range-vector function is being offloaded.
type rateKind uint8

const (
	// rateKindRate — rate(): counter, extrapolate, divide by range seconds.
	rateKindRate rateKind = iota
	// rateKindIncrease — increase(): counter, extrapolate, no divide.
	rateKindIncrease
	// rateKindDelta — delta(): gauge, extrapolate, no counter-reset, no divide.
	rateKindDelta
	// rateKindIRate — irate(): counter, last 2 samples, divide by sample dt.
	rateKindIRate
	// rateKindIDelta — idelta(): gauge, last 2 samples, no divide.
	rateKindIDelta
)

// String returns the PromQL function name for this kind.
func (k rateKind) String() string {
	switch k {
	case rateKindRate:
		return "rate"
	case rateKindIncrease:
		return "increase"
	case rateKindDelta:
		return "delta"
	case rateKindIRate:
		return "irate"
	case rateKindIDelta:
		return "idelta"
	default:
		return fmt.Sprintf("rateKind(%d)", int(k))
	}
}

// needsCounterReset reports whether this function requires counter-reset detection.
func (k rateKind) needsCounterReset() bool {
	return k == rateKindRate || k == rateKindIncrease || k == rateKindIRate
}

// isInstant reports whether this function uses only the last two samples (irate/idelta).
func (k rateKind) isInstant() bool {
	return k == rateKindIRate || k == rateKindIDelta
}

// divideBySeconds reports whether the final value should be divided by a time duration.
func (k rateKind) divideBySeconds() bool {
	// rate() divides by range seconds; irate() divides by sample interval seconds.
	return k == rateKindRate || k == rateKindIRate
}

// funcNameToRateKind maps a PromQL function name to a rateKind.
// Returns (kind, true) if the function is supported for rate offloading.
func funcNameToRateKind(name string) (rateKind, bool) {
	switch name {
	case "rate":
		return rateKindRate, true
	case "increase":
		return rateKindIncrease, true
	case "delta":
		return rateKindDelta, true
	case "irate":
		return rateKindIRate, true
	case "idelta":
		return rateKindIDelta, true
	default:
		return 0, false
	}
}

// rateSelector is a VectorOperator that returns pre-computed rate values for
// rate-family functions (rate, increase, delta, irate, idelta).
//
// It works by calling querySeriesSingleflight(ctx, samplePoints=true, ...).
// Because samplePoints is true, querySeries sees a rate-kind Function and
// delegates to queryRatePoints* (the offloaded path), so the returned "points"
// are already the extrapolated results — not raw samples.
// Next() then performs exact-timestamp lookups via selectExactPoint on those
// pre-computed per-step values.
//
// This makes rateSelector look like a "selector" but it is actually a
// specialized operator returning offloaded function results.
type rateSelector struct {
	telemetry telemetry.OperatorTelemetry

	querier *promQuerier
	filter  promscanners.Filter
	once    sync.Once

	series      []labels.Labels
	pointSeries []*series[pointData]

	params metricSelectParams
	kind   rateKind

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
	kind rateKind,
) model.VectorOperator {
	params.Offset = offset
	o := &rateSelector{
		querier: querier,
		filter:  promscanners.NewFilter(filters),

		params: params,
		kind:   kind,

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
	kind rateKind,
) ([]*series[pointData], error) {
	m, err := p.queryRatePointsByHash(ctx, start, end, step, window, offset, timeseries, kind)
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
	kind rateKind,
) (_ []*series[pointData], rerr error) {
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryRatePointsCached",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
			attribute.Stringer("chstorage.window", window),
			attribute.Stringer("chstorage.offset", offset),
			attribute.Stringer("chstorage.kind", kind),
		),
	)
	defer func() {
		if rerr != nil {
			span.RecordError(rerr)
		}
		span.End()
	}()

	fetch := func(ctx context.Context, fetchStart, fetchEnd time.Time) (map[[16]byte]*series[pointData], error) {
		return p.queryRatePointsByHashFunc(ctx, fetchStart, fetchEnd, step, window, offset, timeseries, kind)
	}
	// Use kind.String() as the cache key to prevent collisions between rate/increase/delta.
	resultMap, err := p.fetchAndMergeCache(ctx, span, start, end, step, kind.String(), timeseries, fetch)
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
	kind rateKind,
) (_ map[[16]byte]*series[pointData], rerr error) {
	table := p.tables.Points
	ctx, span := p.tracer.Start(ctx, "chstorage.metrics.queryRatePoints",
		trace.WithAttributes(
			xattribute.UnixNano("chstorage.range.start", start),
			xattribute.UnixNano("chstorage.range.end", end),
			attribute.Stringer("chstorage.step", step),
			attribute.Stringer("chstorage.window", window),
			attribute.Stringer("chstorage.offset", offset),
			attribute.Stringer("chstorage.kind", kind),
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
	)
	for h := range timeseries {
		inputData.Append(h)
	}

	var (
		firstStepMSIdent     = chsql.Ident("first_step_ms")
		lastStepMSIdent      = chsql.Ident("last_step_ms")
		stepMSIdent          = chsql.Ident("step_ms")
		windowMSIdent        = chsql.Ident("window_ms")
		pointOffsetMSIdent   = chsql.Ident("point_offset_ms")
		firstSampleStepIdent = chsql.Ident("first_sample_step_ms")
		numStepsIdent        = chsql.Ident("num_steps")
	)

	toInt64 := chsql.ToInt64Val[int64]

	var (
		mapLambda = chsql.Lambda([]string{"i"},
			chsql.Add(firstSampleStepIdent, chsql.Mul(chsql.ColumnToInt64("i"), stepMSIdent)),
		)
		rangeExpr = chsql.Range(numStepsIdent)

		filterLambda = chsql.Lambda([]string{"s"}, chsql.JoinAnd(
			chsql.Gte(chsql.Ident("s"), firstStepMSIdent),
			chsql.Lte(chsql.Ident("s"), lastStepMSIdent),
			chsql.Lt(chsql.Ident("s"), chsql.Add(pointOffsetMSIdent, windowMSIdent)),
		))

		stepExpr = chsql.ArrayJoin(
			chsql.ArrayFilter(
				filterLambda,
				chsql.ArrayMap(mapLambda, rangeExpr),
			),
		)
	)

	// Subquery 1: Fan out each raw sample into every step bucket whose range window covers it.
	//
	// Each step S covers the range (S-window, S] (adjusted for offset). A single sample near
	// the boundary of two windows belongs to both, so one input row may produce multiple output rows.
	//
	//  time ──────────────────────────────────────────────────────►
	//
	//  steps     │   S1    │   S2    │   S3    │   S4    │
	//            ├─────────┼─────────┼─────────┼─────────┤
	//  S1 window [◄──────window──────]
	//  S2 window          [◄──────window──────]
	//  S3 window                    [◄──────window──────]
	//
	//  sample A ─────────────────────┼  (falls in S2 and S3)
	//  sample B ──────────┼            (falls in S1 and S2)
	//
	//  input row (A) ──► output rows (A, S2), (A, S3)
	//  input row (B) ──► output rows (B, S1), (B, S2)
	//
	// arrayMap generates candidate step timestamps starting from first_sample_step_ms,
	// arrayFilter discards those outside [first_step_ms, last_step_ms] or past the sample,
	// arrayJoin materializes one row per surviving step.
	expanded := chsql.Select(table,
		chsql.Column("hash", nil),
		chsql.Column("timestamp", nil),
		chsql.Column("value", nil),
		chsql.ResultColumn{Name: "step_ms_val", Expr: stepExpr},
	).
		// Query start/end aligned to step boundaries.
		With("first_step_ms", toInt64(start.UnixMilli())).
		With("last_step_ms", toInt64(end.UnixMilli())).
		// Step and range window sizes.
		With("step_ms", toInt64(stepMS)).
		With("window_ms", toInt64(windowMS)).
		// PromQL offset: the query window is shifted back by this amount.
		With("offset_ms", toInt64(offsetMS)).
		// Raw sample timestamp in milliseconds.
		With("point_ms", chsql.ToUnixTimestamp64Milli(chsql.Ident("timestamp"))).
		// point_ms + offset_ms: because offset shifts the query window back by offset_ms,
		// adding it to the sample time is equivalent — a sample at T belongs to step S if
		// S - window_ms <= point_offset_ms < S, which avoids carrying offset through every check.
		With("point_offset_ms", chsql.Add(chsql.Ident("point_ms"), chsql.Ident("offset_ms"))).
		// ceil((point_offset_ms - first_step_ms) / step_ms) * step_ms, clamped to 0:
		// distance in ms from first_step_ms to the earliest step this sample could belong to.
		With("offset_from_start", chsql.Greatest(
			chsql.ToInt64(chsql.Integer(0)),
			chsql.Sub(
				chsql.Add(chsql.Ident("point_offset_ms"), chsql.Sub(chsql.Ident("step_ms"), chsql.Integer(1))),
				chsql.Ident("first_step_ms"),
			),
		)).
		// floor(window_ms / step_ms) + 1: max steps a single sample can fall into.
		With("num_steps", chsql.ToUInt64(chsql.Add(chsql.IntDiv(chsql.Ident("window_ms"), chsql.Ident("step_ms")), chsql.Integer(1)))).
		// first_step_ms + intDiv(offset_from_start, step_ms) * step_ms:
		// timestamp of the earliest step this sample belongs to, snapped to the step grid.
		With("first_sample_step_ms", chsql.Add(
			chsql.Ident("first_step_ms"),
			chsql.Mul(chsql.IntDiv(chsql.Ident("offset_from_start"), chsql.Ident("step_ms")), chsql.Ident("step_ms")),
		)).
		Where(
			chsql.Gt(chsql.Ident("timestamp"), chsql.DateTime64(rawStart, proto.PrecisionMilli)),
			chsql.Lte(chsql.Ident("timestamp"), chsql.DateTime64(rawEnd, proto.PrecisionMilli)),
			chsql.In(chsql.Ident("hash"), chsql.Ident(inputTable)),
			chsql.NotEq(chsql.ReinterpretAsUInt64(chsql.Ident("value")), chsql.Integer(promStaleNaNBits)),
		)

	if kind.isInstant() {
		return p.queryInstantPointsByHash(ctx, span, table, inputTable, &inputData, expanded, timeseries, kind)
	}
	return p.queryExtrapolatedPointsByHash(ctx, span, table, inputTable, &inputData, expanded, window, offset, timeseries, kind)
}

// queryExtrapolatedPointsByHash computes rate(), increase(), or delta() using the
// 3-subquery pipeline. rate and increase use counter-reset detection (prev_value window
// function + reset_sum); delta skips the window function since gauges don't reset.
func (p *promQuerier) queryExtrapolatedPointsByHash(
	ctx context.Context,
	span trace.Span,
	table, inputTable string,
	inputData *proto.ColFixedStr16,
	expanded *chsql.SelectQuery,
	window, offset time.Duration,
	timeseries map[[16]byte]labels.Labels,
	kind rateKind,
) (_ map[[16]byte]*series[pointData], rerr error) {
	tuple := chsql.ColumnTupleElement

	var (
		hash    = proto.NewLowCardinality(&proto.ColFixedStr16{})
		stepTS  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		firstTS = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		lastTS  = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		firstV  proto.ColFloat64
		lastV   proto.ColFloat64
		reset   proto.ColFloat64
		samples proto.ColUInt64
	)

	var aggregated *chsql.SelectQuery
	if kind.needsCounterReset() {
		// Subquery 2: Attach prev_value to each row using a window function.
		//
		// Partitioned by (hash, step_ms_val) and ordered by timestamp, lagInFrame looks one row
		// back within each partition. This is used in subquery 3 to detect counter resets without
		// materializing per-group arrays.
		//
		//  partition (hash=A, step=S2), ordered by timestamp:
		//
		//   timestamp │ value │ prev_value
		//   ──────────┼───────┼───────────
		//      t=10   │  100  │  100   ← first row, default = value itself
		//      t=20   │  150  │  100
		//      t=30   │   20  │  150   ← value < prev_value: reset detected
		//      t=40   │   80  │   20
		withPrev := chsql.SelectFrom(expanded,
			chsql.Column("hash", nil),
			chsql.Column("step_ms_val", nil),
			chsql.Column("timestamp", nil),
			chsql.Column("value", nil),
			chsql.ResultColumn{
				Name: "prev_value",
				Expr: chsql.Over(
					chsql.LagInFrame(chsql.Ident("value"), chsql.Integer(1), chsql.Ident("value")),
					[]chsql.Expr{chsql.Ident("hash"), chsql.Ident("step_ms_val")},
					[]chsql.WindowOrderSpec{chsql.WindowAsc(chsql.Ident("timestamp"))},
				),
			},
		)

		// Subquery 3: Aggregate per (hash, step) into the four scalars needed for rate calculation.
		//
		// Using argMin/argMax for first/last avoids sorting; sumIf accumulates pre-reset values
		// so Go only needs: resultValue = last_v - first_v + reset_sum.
		//
		//  partition (hash=A, step=S2):
		//
		//   timestamp │ value │ prev_value │ contributes to
		//   ──────────┼───────┼────────────┼───────────────────────────────────────────
		//      t=10   │  100  │  100       │ first_pair=(t10,100)
		//      t=20   │  150  │  100       │ (middle)
		//      t=30   │   20  │  150       │ reset_sum += 150  (value 20 < prev 150)
		//      t=40   │   80  │   20       │ last_pair=(t40,80)
		//
		//   → first_pair=(t10,100)  last_pair=(t40,80)  samples=4  reset_sum=150
		//   → resultValue = 80 - 100 + 150 = 130  (true counter delta across the reset)
		aggregated = chsql.SelectFrom(withPrev,
			chsql.Column("hash", nil),
			chsql.Column("step_ms_val", nil),
			chsql.ResultColumn{Name: "first_pair", Expr: chsql.ArgMin(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")), chsql.Ident("timestamp"))},
			chsql.ResultColumn{Name: "last_pair", Expr: chsql.ArgMax(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")), chsql.Ident("timestamp"))},
			chsql.ResultColumn{Name: "samples", Expr: chsql.Count()},
			chsql.ResultColumn{Name: "reset_sum", Expr: chsql.SumIf(chsql.Ident("prev_value"), chsql.Lt(chsql.Ident("value"), chsql.Ident("prev_value")))},
		).
			GroupBy(chsql.Ident("hash"), chsql.Ident("step_ms_val")).
			Having(chsql.Gt(chsql.Ident("samples"), chsql.Integer(1)))
	} else {
		// delta(): no counter-reset detection needed, skip the window function subquery.
		// Aggregate directly from the fan-out (subquery 1): first/last by timestamp, sample count.
		aggregated = chsql.SelectFrom(expanded,
			chsql.Column("hash", nil),
			chsql.Column("step_ms_val", nil),
			chsql.ResultColumn{Name: "first_pair", Expr: chsql.ArgMin(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")), chsql.Ident("timestamp"))},
			chsql.ResultColumn{Name: "last_pair", Expr: chsql.ArgMax(chsql.Tuple(chsql.Ident("timestamp"), chsql.Ident("value")), chsql.Ident("timestamp"))},
			chsql.ResultColumn{Name: "samples", Expr: chsql.Count()},
			// reset_sum is always zero for delta (gauges don't reset).
			chsql.ResultColumn{Name: "reset_sum", Expr: chsql.ToFloat64(chsql.Integer(0))},
		).
			GroupBy(chsql.Ident("hash"), chsql.Ident("step_ms_val")).
			Having(chsql.Gt(chsql.Ident("samples"), chsql.Integer(1)))
	}

	// Final query: Unwrap tuple columns and attach Go column buffers for scanning.
	// Go then calls extrapolatedValue per row to apply Prometheus' extrapolation formula.
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
			{Name: "name", Data: inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < stepTS.Rows(); i++ {
				var (
					h      = hash.Row(i)
					stepT  = stepTS.Row(i)
					firstT = firstTS.Row(i)
					lastT  = lastTS.Row(i)
				)
				value, ok := extrapolatedValue(rateWindow{
					StepTime: stepT.UnixMilli(),
					Range:    window.Milliseconds(),
					Offset:   offset.Milliseconds(),
					FirstT:   firstT.UnixMilli(),
					FirstV:   firstV.Row(i),
					LastT:    lastT.UnixMilli(),
					LastV:    lastV.Row(i),
					ResetSum: reset.Row(i),
					Samples:  int(samples.Row(i)),
				}, kind)
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

// queryInstantPointsByHash computes irate() or idelta() using the last two samples per step.
// It extends subquery 2 with prev_timestamp so aggregation can recover both the last and
// second-to-last (timestamp, value) pairs without sorting.
func (p *promQuerier) queryInstantPointsByHash(
	ctx context.Context,
	span trace.Span,
	table, inputTable string,
	inputData *proto.ColFixedStr16,
	expanded *chsql.SelectQuery,
	timeseries map[[16]byte]labels.Labels,
	kind rateKind,
) (_ map[[16]byte]*series[pointData], rerr error) {
	var (
		hash       = proto.NewLowCardinality(&proto.ColFixedStr16{})
		stepTS     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		lastTS     = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		prevLastTS = new(proto.ColDateTime64).WithPrecision(proto.PrecisionMilli)
		lastV      proto.ColFloat64
		prevLastV  proto.ColFloat64
		samples    proto.ColUInt64
	)

	// Subquery 2 (extended): add prev_timestamp alongside prev_value so that
	// aggregation can recover the second-to-last timestamp without sorting.
	//
	//  partition (hash=A, step=S2), ordered by timestamp:
	//
	//   timestamp │ value │ prev_value │ prev_timestamp
	//   ──────────┼───────┼────────────┼───────────────
	//      t=10   │  100  │  100       │  t=10  ← first row defaults to itself
	//      t=20   │  150  │  100       │  t=10
	//      t=30   │   20  │  150       │  t=20
	//      t=40   │   80  │   20       │  t=30
	//
	//   argMax(prev_value, timestamp) → 20  (prev of last row)
	//   argMax(prev_timestamp, timestamp) → t=30  (timestamp of prev of last row)
	withPrevExt := chsql.SelectFrom(expanded,
		chsql.Column("hash", nil),
		chsql.Column("step_ms_val", nil),
		chsql.Column("timestamp", nil),
		chsql.Column("value", nil),
		chsql.ResultColumn{
			Name: "prev_value",
			Expr: chsql.Over(
				chsql.LagInFrame(chsql.Ident("value"), chsql.Integer(1), chsql.Ident("value")),
				[]chsql.Expr{chsql.Ident("hash"), chsql.Ident("step_ms_val")},
				[]chsql.WindowOrderSpec{chsql.WindowAsc(chsql.Ident("timestamp"))},
			),
		},
		chsql.ResultColumn{
			Name: "prev_timestamp",
			Expr: chsql.Over(
				chsql.LagInFrame(chsql.Ident("timestamp"), chsql.Integer(1), chsql.Ident("timestamp")),
				[]chsql.Expr{chsql.Ident("hash"), chsql.Ident("step_ms_val")},
				[]chsql.WindowOrderSpec{chsql.WindowAsc(chsql.Ident("timestamp"))},
			),
		},
	)

	// Subquery 3: Aggregate per (hash, step) — recover last and second-to-last pairs.
	//
	//  argMax(value, timestamp)           → last_v
	//  max(timestamp)                     → last_t
	//  argMax(prev_value, timestamp)      → prev_last_v  (prev_value of the last row)
	//  argMax(prev_timestamp, timestamp)  → prev_last_t  (timestamp of second-to-last row)
	aggregated := chsql.SelectFrom(withPrevExt,
		chsql.Column("hash", nil),
		chsql.Column("step_ms_val", nil),
		chsql.ResultColumn{Name: "last_v", Expr: chsql.ArgMax(chsql.Ident("value"), chsql.Ident("timestamp"))},
		chsql.ResultColumn{Name: "last_t", Expr: chsql.Max(chsql.Ident("timestamp"))},
		chsql.ResultColumn{Name: "prev_last_v", Expr: chsql.ArgMax(chsql.Ident("prev_value"), chsql.Ident("timestamp"))},
		chsql.ResultColumn{Name: "prev_last_t", Expr: chsql.ArgMax(chsql.Ident("prev_timestamp"), chsql.Ident("timestamp"))},
		chsql.ResultColumn{Name: "samples", Expr: chsql.Count()},
	).
		GroupBy(chsql.Ident("hash"), chsql.Ident("step_ms_val")).
		Having(chsql.Gt(chsql.Ident("samples"), chsql.Integer(1)))

	instantQuery := chsql.SelectFrom(aggregated,
		chsql.ResultColumn{Name: "hash", Expr: chsql.Ident("hash"), Data: hash},
		chsql.ResultColumn{Name: "step_ts", Expr: chsql.ToDateTime64(chsql.Div(chsql.Ident("step_ms_val"), chsql.Float(1000.0)), proto.PrecisionMilli), Data: stepTS},
		chsql.ResultColumn{Name: "last_t", Expr: chsql.Ident("last_t"), Data: lastTS},
		chsql.ResultColumn{Name: "last_v", Expr: chsql.Ident("last_v"), Data: &lastV},
		chsql.ResultColumn{Name: "prev_last_t", Expr: chsql.Ident("prev_last_t"), Data: prevLastTS},
		chsql.ResultColumn{Name: "prev_last_v", Expr: chsql.Ident("prev_last_v"), Data: &prevLastV},
		chsql.ResultColumn{Name: "samples", Expr: chsql.Ident("samples"), Data: &samples},
	).
		Order(chsql.Ident("hash"), chsql.Asc).
		Order(chsql.Ident("step_ts"), chsql.Asc)

	var (
		set         = map[[16]byte]*series[pointData]{}
		totalPoints int
	)
	if err := p.do(ctx, selectQuery{
		Query:         instantQuery,
		ExternalTable: inputTable,
		ExternalData: []proto.InputColumn{
			{Name: "name", Data: inputData},
		},
		OnResult: func(ctx context.Context, block proto.Block) error {
			for i := 0; i < stepTS.Rows(); i++ {
				h := hash.Row(i)
				stepT := stepTS.Row(i)
				value, ok := instantValue(instantWindow{
					LastT:     lastTS.Row(i).UnixMilli(),
					LastV:     lastV.Row(i),
					PrevLastT: prevLastTS.Row(i).UnixMilli(),
					PrevLastV: prevLastV.Row(i),
					Samples:   int(samples.Row(i)),
				}, kind)
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

		Type:   "QueryInstantPoints",
		Signal: "metrics",
		Table:  table,
	}); err != nil {
		return nil, err
	}
	span.AddEvent("instant_points_fetched", trace.WithAttributes(
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

// extrapolatedValue computes the Prometheus extrapolation formula for rate(), increase(), or delta().
func extrapolatedValue(w rateWindow, kind rateKind) (float64, bool) {
	if w.Samples < 2 || w.Range <= 0 || w.FirstT == w.LastT {
		return 0, false
	}

	rangeStart := w.StepTime - (w.Range + w.Offset)
	rangeEnd := w.StepTime - w.Offset

	resultValue := w.LastV - w.FirstV
	if kind.needsCounterReset() {
		resultValue += w.ResetSum
	}
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
	if kind.divideBySeconds() {
		// rate(): divide by range window in seconds.
		factor /= float64(w.Range) / 1000
	}
	value := resultValue * factor
	if math.IsNaN(value) {
		return 0, false
	}
	return value, true
}

type instantWindow struct {
	LastT     int64
	LastV     float64
	PrevLastT int64
	PrevLastV float64
	Samples   int
}

// instantValue computes irate() or idelta() from the last two samples in the step window.
func instantValue(w instantWindow, kind rateKind) (float64, bool) {
	if w.Samples < 2 || w.LastT == w.PrevLastT {
		return 0, false
	}

	delta := w.LastV - w.PrevLastV
	if kind.needsCounterReset() && w.LastV < w.PrevLastV {
		// Counter reset: the true increase since the reset is just the last value.
		delta = w.LastV
	}

	if kind.divideBySeconds() {
		// irate(): divide by the interval between the two samples in seconds.
		dt := float64(w.LastT-w.PrevLastT) / 1000
		if dt <= 0 {
			return 0, false
		}
		value := delta / dt
		if math.IsNaN(value) {
			return 0, false
		}
		return value, true
	}
	return delta, true
}
