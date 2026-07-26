package scarecrow

import (
	"math"

	"github.com/prometheus/prometheus/model/value"
)

// window is one step's slice of a series' raw samples, the input to every range function. T, V
// and W are index-aligned; W is nil when the series carries no sampling weights.
//
// RangeStart and RangeEnd are the step's window bounds (t-range, t] shifted by any offset, which
// the extrapolating functions need in addition to the samples themselves.
type window struct {
	T []int64
	V []float64
	W []float64

	RangeStart int64
	RangeEnd   int64
	RangeMs    int64
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
// produces a sample at all; PromQL omits a step whose window is too sparse.
type rangeFunc func(w *window) (v float64, ok bool)

// rangeFuncs maps PromQL range-vector function names to their implementations. A name absent
// here is not yet supported and the planner refuses it rather than approximating.
//
// Sampling weights are applied per docs/promql-engine.md §3.5: counting and summing folds scale
// by weight, extremes and last-value folds ignore it, and rate/increase/delta ignore it because
// a cumulative counter's surviving samples still carry correct cumulative values. The
// delta-temporality branch of that matrix is not implemented — the Scanner seam carries no
// temporality yet, so there is nothing to switch on. See §11.
var rangeFuncs = map[string]rangeFunc{
	"count_over_time":   countOverTime,
	"sum_over_time":     sumOverTime,
	"avg_over_time":     avgOverTime,
	"min_over_time":     minOverTime,
	"max_over_time":     maxOverTime,
	"last_over_time":    lastOverTime,
	"present_over_time": presentOverTime,
	"stddev_over_time":  stddevOverTime,
	"stdvar_over_time":  stdvarOverTime,
	"rate":              func(w *window) (float64, bool) { return extrapolatedRate(w, true, true) },
	"increase":          func(w *window) (float64, bool) { return extrapolatedRate(w, true, false) },
	"delta":             func(w *window) (float64, bool) { return extrapolatedRate(w, false, false) },
	"irate":             func(w *window) (float64, bool) { return instantRate(w, true) },
	"idelta":            func(w *window) (float64, bool) { return instantRate(w, false) },
	"changes":           changes,
	"resets":            resets,
}

// keepsMetricName lists the range functions that retain __name__. Every other range-vector
// function drops it, as PromQL specifies for functions whose output is no longer the input
// metric.
var keepsMetricName = map[string]bool{
	"last_over_time": true,
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
