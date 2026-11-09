package traceqlengine

import (
	"math"
	"time"
)

// MetricsParamsError reports invalid metrics query parameters.
type MetricsParamsError struct{ Msg string }

func (e *MetricsParamsError) Error() string { return e.Msg }

// MetricsLimitError reports refusal to exceed a metrics evaluation budget.
type MetricsLimitError struct{ Msg string }

func (e *MetricsLimitError) Error() string { return e.Msg }

type metricsLimits struct{ steps, series, samples, spans int }

// MetricsEvalParams defines the requested time range and bucket width.
// Instant queries aggregate the inclusive interval into a single value.
type MetricsEvalParams struct {
	Start, End time.Time
	Step       time.Duration
	Instant    bool
}

type metricsGrid struct {
	start, end int64
	step       int64
	instant    bool
	steps      []int64
}

func newMetricsGrid(p MetricsEvalParams, maxSteps int) (metricsGrid, error) {
	start, end := p.Start.UnixNano(), p.End.UnixNano()
	if p.Start.IsZero() || p.End.IsZero() || start < 0 || end <= start ||
		!time.Unix(0, start).Equal(p.Start) || !time.Unix(0, end).Equal(p.End) {
		return metricsGrid{}, &MetricsParamsError{Msg: "metrics require a valid nonnegative start and a later end within nanosecond timestamp range"}
	}
	g := metricsGrid{start: start, end: end, instant: p.Instant}
	if p.Instant {
		g.step = end - start
		g.steps = []int64{p.End.UnixMilli()}
		return g, nil
	}
	if p.Step < time.Millisecond || p.Step%time.Millisecond != 0 {
		return g, &MetricsParamsError{Msg: "metrics step must be a positive whole number of milliseconds"}
	}
	g.step = int64(p.Step)
	// Tempo aligns range requests and includes an additional initial bucket.
	g.start -= g.start % g.step
	if g.start > g.step {
		g.start -= g.step
	}
	if rem := g.end % g.step; rem != 0 {
		if g.end > math.MaxInt64-(g.step-rem) {
			return g, &MetricsParamsError{Msg: "aligned metrics end overflows timestamp range"}
		}
		g.end += g.step - rem
	}
	count := (g.end - g.start) / g.step
	if count <= 0 || count > int64(maxSteps) {
		return g, &MetricsParamsError{Msg: "metrics grid exceeds the maximum number of steps"}
	}
	g.steps = make([]int64, int(count))
	for i := range g.steps {
		g.steps[i] = (g.start + int64(i+1)*g.step) / int64(time.Millisecond)
	}
	return g, nil
}

func (g metricsGrid) bucket(ts int64) (int, bool) {
	if g.instant {
		return 0, ts >= g.start && ts <= g.end
	}
	if ts <= g.start || ts > g.end {
		return 0, false
	}
	return int((ts - g.start - 1) / g.step), true
}

// DefaultMetricsStep targets roughly 240 buckets using Tempo's granularities.
func DefaultMetricsStep(start, end time.Time) time.Duration {
	delta := end.Sub(start)
	baseline := delta / 240
	if delta < time.Minute {
		return max(50*time.Millisecond, baseline/(50*time.Millisecond)*(50*time.Millisecond))
	}
	for _, granularity := range []time.Duration{time.Hour, 5 * time.Minute, time.Minute, 15 * time.Second, 5 * time.Second, time.Second} {
		if baseline > granularity {
			return baseline / granularity * granularity
		}
	}
	return time.Second
}
