// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logqlmetric

import (
	"cmp"
	"math"
	"slices"
	"time"

	"github.com/oteldb/oteldb/internal/otelstorage"
)

// Note: this file contains stats functions from Prometheus.

// extrapolatedRate is a utility function for rate/increase/delta.
// It calculates the rate (allowing for counter resets if isCounter is true),
// extrapolates if the first/last sample is close to the boundary, and returns
// the result as either per-second (if isRate is true) or overall.
func extrapolatedRate(samples []FPoint, selRange time.Duration, isCounter, isRate bool) float64 {
	if len(samples) < 2 {
		return 0.
	}

	var (
		rangeStart         = timestampMilliseconds(samples[0].Timestamp) - durationMilliseconds(selRange)
		rangeEnd           = timestampMilliseconds(samples[len(samples)-1].Timestamp)
		resultFloat        float64
		firstT, lastT      int64
		numSamplesMinusOne int
	)

	numSamplesMinusOne = len(samples) - 1
	firstT = timestampMilliseconds(samples[0].Timestamp)
	lastT = timestampMilliseconds(samples[numSamplesMinusOne].Timestamp)
	resultFloat = samples[numSamplesMinusOne].Value - samples[0].Value
	if isCounter {
		// Handle counter resets:
		prevValue := samples[0].Value
		for _, currPoint := range samples[1:] {
			if currPoint.Value < prevValue {
				resultFloat += prevValue
			}
			prevValue = currPoint.Value
		}
	}

	// Duration between first/last samples and boundary of range.
	durationToStart := float64(firstT-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastT) / 1000

	sampledInterval := float64(lastT-firstT) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(numSamplesMinusOne)

	// If the first/last samples are close to the boundaries of the range,
	// extrapolate the result. This is as we expect that another sample
	// will exist given the spacing between samples we've seen thus far,
	// with an allowance for noise.
	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}
	if isCounter && resultFloat > 0 && len(samples) > 0 && samples[0].Value >= 0 {
		// Counters cannot be negative. If we have any slope at all
		// (i.e. resultFloat went up), we can extrapolate the zero point
		// of the counter. If the duration to the zero point is shorter
		// than the durationToStart, we take the zero point as the start
		// of the series, thereby avoiding extrapolation to negative
		// counter values.
		// TODO(beorn7): Do this for histograms, too.
		durationToZero := sampledInterval * (samples[0].Value / resultFloat)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}
	extrapolateToInterval += durationToStart

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}
	extrapolateToInterval += durationToEnd

	factor := extrapolateToInterval / sampledInterval
	if isRate {
		factor /= selRange.Seconds()
	}
	resultFloat *= factor

	return resultFloat
}

func timestampMilliseconds(t otelstorage.Timestamp) int64 {
	return t.AsTime().UnixMilli()
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}

// quantile calculates the given quantile of a vector of samples.
//
// The Vector will be sorted.
// If 'values' has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
func quantile(q float64, values []FPoint) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	slices.SortFunc(values, func(a, b FPoint) int {
		if math.IsNaN(a.Value) {
			return -1
		}
		return cmp.Compare(a.Value, b.Value)
	})

	n := float64(len(values))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	lower := values[int(lowerIndex)]
	upper := values[int(upperIndex)]
	return lower.Value*(1-weight) + upper.Value*weight
}
