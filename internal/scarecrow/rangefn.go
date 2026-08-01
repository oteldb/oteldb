package scarecrow

import (
	"math"

	"github.com/prometheus/prometheus/model/value"
)

// window is one step's slice of a series' raw samples, the input to every range function. T, V
// and W are index-aligned; W is nil when the series carries no sampling weights.
//
// RangeStart and RangeEnd are the step's window bounds (t-range, t] shifted by any offset, which
// the extrapolating functions need in addition to the samples themselves. EvalMs is the step's
// own, un-shifted evaluation timestamp — predict_linear anchors its regression there rather than
// at RangeEnd, so a query using `offset` still projects forward from the query's own step time.
type window struct {
	T []int64
	V []float64
	W []float64

	RangeStart int64
	RangeEnd   int64
	RangeMs    int64
	EvalMs     int64
}

// Len returns the number of samples in the window.
func (w *window) Len() int { return len(w.T) }

// Weight returns sample i's lossy-sampling scale factor, defaulting to 1.
func (w *window) Weight(i int) float64 {
	if w.W == nil {
		return 1
	}

	return w.W[i]
}

// SumWeights returns the total weight in the window, which is the unbiased estimate of how many
// samples were originally observed.
func (w *window) SumWeights() float64 {
	if w.W == nil {
		return float64(len(w.T))
	}

	total := 0.0
	for _, x := range w.W {
		total += x
	}

	return total
}

// rangeFunc computes one range function over one step's window. ok reports whether the step
// produces a sample at all; PromQL omits a step whose window is too sparse. param carries the
// function's extra scalar argument (quantile_over_time's q, predict_linear's duration) at this
// step; functions that take none simply ignore it.
type rangeFunc func(w *window, param float64) (v float64, ok bool)

// rangeFuncs maps PromQL range-vector function names to their implementations. A name absent
// here is not yet supported and the planner refuses it rather than approximating.
//
// Sampling weights are applied per docs/promql-engine.md §3.5: counting and summing folds scale
// by weight, extremes and last-value folds ignore it, and rate/increase/delta ignore it because
// a cumulative counter's surviving samples still carry correct cumulative values. The M12
// additions below (deriv, predict_linear, quantile_over_time, mad_over_time, ts_of_*) are not
// mentioned there because none of them are candidates for the lossy-sampling weight rule in the
// first place: they either need exact order statistics (quantile_over_time, mad_over_time),
// exact sample timestamps (the ts_of_* family), or a regression that has no defined weighted
// form (deriv, predict_linear) — the same reasoning §3.5 already gives for min/max_over_time.
//
// Only the cumulative rule is specified. oteldb does no delta→cumulative conversion, so rate()
// over a delta-temporality series is undefined before sampling is even considered; see
// https://github.com/oteldb/oteldb/issues/1190.
var rangeFuncs = map[string]rangeFunc{
	"count_over_time":   func(w *window, _ float64) (float64, bool) { return countOverTime(w) },
	"sum_over_time":     func(w *window, _ float64) (float64, bool) { return sumOverTime(w) },
	"avg_over_time":     func(w *window, _ float64) (float64, bool) { return avgOverTime(w) },
	"min_over_time":     func(w *window, _ float64) (float64, bool) { return minOverTime(w) },
	"max_over_time":     func(w *window, _ float64) (float64, bool) { return maxOverTime(w) },
	"last_over_time":    func(w *window, _ float64) (float64, bool) { return lastOverTime(w) },
	"first_over_time":   func(w *window, _ float64) (float64, bool) { return firstOverTime(w) },
	"present_over_time": func(w *window, _ float64) (float64, bool) { return presentOverTime(w) },
	"stddev_over_time":  func(w *window, _ float64) (float64, bool) { return stddevOverTime(w) },
	"stdvar_over_time":  func(w *window, _ float64) (float64, bool) { return stdvarOverTime(w) },
	"rate":              func(w *window, _ float64) (float64, bool) { return extrapolatedRate(w, true, true) },
	"increase":          func(w *window, _ float64) (float64, bool) { return extrapolatedRate(w, true, false) },
	"delta":             func(w *window, _ float64) (float64, bool) { return extrapolatedRate(w, false, false) },
	"irate":             func(w *window, _ float64) (float64, bool) { return instantRate(w, true) },
	"idelta":            func(w *window, _ float64) (float64, bool) { return instantRate(w, false) },
	"changes":           func(w *window, _ float64) (float64, bool) { return changes(w) },
	"resets":            func(w *window, _ float64) (float64, bool) { return resets(w) },
	"deriv":             func(w *window, _ float64) (float64, bool) { return deriv(w) },
	"predict_linear":    predictLinear,
	"quantile_over_time": func(w *window, q float64) (float64, bool) {
		return quantile(q, append([]float64(nil), w.V...)), true
	},
	"mad_over_time":         func(w *window, _ float64) (float64, bool) { return madOverTime(w) },
	"ts_of_first_over_time": func(w *window, _ float64) (float64, bool) { return tsOfFirstOverTime(w) },
	"ts_of_last_over_time":  func(w *window, _ float64) (float64, bool) { return tsOfLastOverTime(w) },
	"ts_of_max_over_time":   func(w *window, _ float64) (float64, bool) { return tsOfExtremeOverTime(w, true) },
	"ts_of_min_over_time":   func(w *window, _ float64) (float64, bool) { return tsOfExtremeOverTime(w, false) },
}

// keepsMetricName lists the range functions that retain __name__. Every other range-vector
// function drops it, as PromQL specifies for functions whose output is no longer the input
// metric.
var keepsMetricName = map[string]bool{
	"last_over_time":  true,
	"first_over_time": true,
}

func countOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	return w.SumWeights(), true
}

func sumOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	sum := 0.0
	for i, v := range w.V {
		sum += v * w.Weight(i)
	}

	return sum, true
}

func avgOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	sum, _ := sumOverTime(w)

	return sum / w.SumWeights(), true
}

func minOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	// NaN loses to any real value, matching upstream's min_over_time.
	m := math.NaN()
	for _, v := range w.V {
		if v < m || math.IsNaN(m) {
			m = v
		}
	}

	return m, true
}

func maxOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	m := math.NaN()
	for _, v := range w.V {
		if v > m || math.IsNaN(m) {
			m = v
		}
	}

	return m, true
}

func lastOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	return w.V[w.Len()-1], true
}

func firstOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	return w.V[0], true
}

func presentOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	return 1, true
}

func stdvarOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	// Welford's method, weighted, for numerical stability over a naive sum-of-squares.
	var count, mean, aux float64

	for i, v := range w.V {
		weight := w.Weight(i)
		count += weight
		delta := v - mean
		mean += delta * weight / count
		aux += weight * delta * (v - mean)
	}

	return aux / count, true
}

func stddevOverTime(w *window) (float64, bool) {
	v, ok := stdvarOverTime(w)
	if !ok {
		return 0, false
	}

	return math.Sqrt(v), true
}

func changes(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	n := 0.0
	prev := w.V[0]

	for _, v := range w.V[1:] {
		if v != prev && !(math.IsNaN(v) && math.IsNaN(prev)) {
			n++
		}

		prev = v
	}

	return n, true
}

func resets(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	n := 0.0
	prev := w.V[0]

	for _, v := range w.V[1:] {
		if v < prev {
			n++
		}

		prev = v
	}

	return n, true
}

// instantRate implements irate and idelta: the change across the last two samples only.
func instantRate(w *window, isRate bool) (float64, bool) {
	if w.Len() < 2 {
		return 0, false
	}

	last, prev := w.Len()-1, w.Len()-2

	resultValue := w.V[last] - w.V[prev]
	if isRate && resultValue < 0 {
		// Counter reset: the post-reset value is the increase.
		resultValue = w.V[last]
	}

	sampledInterval := w.T[last] - w.T[prev]
	if sampledInterval == 0 {
		return 0, false
	}

	if !isRate {
		return resultValue, true
	}

	return resultValue / (float64(sampledInterval) / 1000), true
}

// extrapolatedRate implements rate, increase and delta.
//
// It is a faithful port of Prometheus' promql.extrapolatedRate, deliberately so: the boundary
// extrapolation is the part that makes rate agree with upstream, and the prototype's version
// omitted it entirely (its own comment conceded as much), which skews every window whose
// samples do not reach the edges.
func extrapolatedRate(w *window, isCounter, isRate bool) (float64, bool) {
	if w.Len() < 2 {
		return 0, false
	}

	numSamplesMinusOne := w.Len() - 1
	firstT, lastT := w.T[0], w.T[numSamplesMinusOne]

	resultValue := w.V[numSamplesMinusOne] - w.V[0]

	if isCounter {
		prev := w.V[0]
		for _, cur := range w.V[1:] {
			if cur < prev {
				resultValue += prev
			}

			prev = cur
		}
	}

	// Duration between the first/last samples and the boundary of the range.
	durationToStart := float64(firstT-w.RangeStart) / 1000
	durationToEnd := float64(w.RangeEnd-lastT) / 1000

	sampledInterval := float64(lastT-firstT) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(numSamplesMinusOne)

	// If samples are close enough to a boundary, extrapolate all the way to it; otherwise
	// assume the series starts or ends within the range and extrapolate only by half the
	// average sample spacing.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}

	if isCounter && resultValue > 0 && w.V[0] >= 0 {
		// A counter cannot go negative, so never extrapolate back past its zero point.
		durationToZero := sampledInterval * (w.V[0] / resultValue)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}

	factor := (sampledInterval + durationToStart + durationToEnd) / sampledInterval
	if isRate {
		factor /= float64(w.RangeMs) / 1000
	}

	return resultValue * factor, true
}

// isStale reports whether v is a staleness marker. Range windows exclude these outright rather
// than folding them, matching upstream.
func isStale(v float64) bool { return value.IsStaleNaN(v) }

// linearRegression is a faithful port of upstream's promql.linearRegression: a least-squares fit
// of the window's samples, with x measured in seconds relative to interceptMs so the fitted line
// reads directly at that timestamp. A window whose values never move returns a flat line rather
// than an arbitrary zero-variance division, matching upstream's constant-y short-circuit.
func linearRegression(w *window, interceptMs int64) (slope, intercept float64) {
	var sumX, sumY, sumXY, sumX2 float64

	initY := w.V[0]
	constY := true

	for i, t := range w.T {
		if constY && i > 0 && w.V[i] != initY {
			constY = false
		}

		x := float64(t-interceptMs) / 1000
		sumX += x
		sumY += w.V[i]
		sumXY += x * w.V[i]
		sumX2 += x * x
	}

	if constY {
		if math.IsInf(initY, 0) {
			return math.NaN(), math.NaN()
		}

		return 0, initY
	}

	n := float64(w.Len())
	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n

	slope = covXY / varX
	intercept = sumY/n - slope*sumX/n

	return slope, intercept
}

// deriv is the per-second slope of a window's least-squares line. The intercept anchor (the
// window's own first sample) is arbitrary — only chosen for floating-point accuracy near the
// samples' own timestamps — and does not affect the slope.
func deriv(w *window) (float64, bool) {
	if w.Len() < 2 {
		return 0, false
	}

	slope, _ := linearRegression(w, w.T[0])

	return slope, true
}

// predictLinear projects a window's least-squares line duration seconds past the step's own
// evaluation time (w.EvalMs, not w.RangeEnd — see [window]), which is why the regression is
// anchored there rather than at the window's first sample as [deriv] does.
func predictLinear(w *window, duration float64) (float64, bool) {
	if w.Len() < 2 {
		return 0, false
	}

	slope, intercept := linearRegression(w, w.EvalMs)

	return slope*duration + intercept, true
}

// madOverTime is the median absolute deviation from the median: quantile(0.5, values), then
// quantile(0.5, |value - median|). quantile sorts its argument in place, so each pass gets its
// own slice.
func madOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	median := quantile(0.5, append([]float64(nil), w.V...))

	devs := make([]float64, w.Len())
	for i, v := range w.V {
		devs[i] = math.Abs(v - median)
	}

	return quantile(0.5, devs), true
}

// tsOfFirstOverTime and tsOfLastOverTime report the timestamp, in seconds, of the window's first
// and last sample — the only range functions concerned with *when* a value occurred rather than
// its value.
func tsOfFirstOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	return float64(w.T[0]) / 1000, true
}

func tsOfLastOverTime(w *window) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	return float64(w.T[w.Len()-1]) / 1000, true
}

// tsOfExtremeOverTime reports the timestamp of the window's max (wantMax) or min sample. Ties
// resolve to the *latest* occurrence: upstream's comparator is `>=`/`<=`, not a strict
// inequality, so equal values keep advancing the recorded timestamp as the scan proceeds.
func tsOfExtremeOverTime(w *window, wantMax bool) (float64, bool) {
	if w.Len() == 0 {
		return 0, false
	}

	extreme, ts := w.V[0], w.T[0]

	for i, v := range w.V {
		var take bool
		if wantMax {
			take = v >= extreme || math.IsNaN(extreme)
		} else {
			take = v <= extreme || math.IsNaN(extreme)
		}

		if take {
			extreme, ts = v, w.T[i]
		}
	}

	return float64(ts) / 1000, true
}
