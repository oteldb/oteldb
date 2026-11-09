package tempohandler

import (
	"context"
	"time"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/promhandler"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

func (h *TempoAPI) metricsTimeRange(start, end tempoapi.OptTempoTime, since tempoapi.OptPrometheusDuration) (lo, hi time.Time, err error) {
	now := time.Now()
	lo, hi, err = parseQueryTimeRange(now, start, end, since, h.defaultSince)
	if err != nil {
		return lo, hi, err
	}
	if hi.IsZero() {
		hi = now
	}
	if lo.IsZero() {
		lo = hi.Add(-h.defaultSince)
	}
	return lo, hi, nil
}

func (h *TempoAPI) queryMetrics(ctx context.Context, p tempoapi.QueryParams) (*tempoapi.InstantMetrics, error) {
	start, end, err := h.metricsTimeRange(p.Start, p.End, p.Since)
	if err != nil {
		return nil, validationErr(ctx, err, "parse time range")
	}
	if h.engine == nil {
		return nil, executionErr(ctx, &traceqlengine.UnsupportedError{Msg: "TraceQL engine is disabled"}, "query metrics")
	}
	series, err := h.engine.EvalMetrics(ctx, p.Q, traceqlengine.MetricsEvalParams{Start: start, End: end, Instant: true})
	if err != nil {
		return nil, executionErr(ctx, err, "query metrics")
	}
	result := &tempoapi.InstantMetrics{Series: []tempoapi.InstantSeries{}}
	for _, s := range series {
		if len(s.Samples) > 0 {
			result.Series = append(result.Series, tempoapi.InstantSeries{Labels: s.Labels, Value: s.Samples[0].Value})
		}
	}
	return result, nil
}

func (h *TempoAPI) queryRangeMetrics(ctx context.Context, p tempoapi.QueryRangeParams) (*tempoapi.RangeMetrics, error) {
	start, end, err := h.metricsTimeRange(p.Start, p.End, p.Since)
	if err != nil {
		return nil, validationErr(ctx, err, "parse time range")
	}
	step := traceqlengine.DefaultMetricsStep(start, end)
	if value, ok := p.Step.Get(); ok {
		step, err = promhandler.ParseDuration(string(value))
		if err != nil {
			return nil, validationErr(ctx, err, "parse step")
		}
		if step <= 0 {
			return nil, validationErr(ctx, errors.New("step must be positive"), "parse step")
		}
	}
	if value, ok := p.Exemplars.Get(); ok && value < 0 {
		return nil, validationErr(ctx, errors.New("exemplars cannot be negative"), "parse exemplars")
	}
	if h.engine == nil {
		return nil, executionErr(ctx, &traceqlengine.UnsupportedError{Msg: "TraceQL engine is disabled"}, "query range metrics")
	}
	series, err := h.engine.EvalMetrics(ctx, p.Q, traceqlengine.MetricsEvalParams{Start: start, End: end, Step: step})
	if err != nil {
		return nil, executionErr(ctx, err, "query range metrics")
	}
	return &tempoapi.RangeMetrics{Series: series}, nil
}
