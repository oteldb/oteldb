// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package chstorage

import (
	"math"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type expHistData struct {
	count                []uint64
	sum                  []proto.Nullable[float64]
	min                  []proto.Nullable[float64]
	max                  []proto.Nullable[float64]
	scale                []int32
	zerocount            []uint64
	positiveOffset       []int32
	positiveBucketCounts [][]uint64
	negativeOffset       []int32
	negativeBucketCounts [][]uint64
	flags                []uint32
}

func (e *expHistData) value(idx int) (h histogram.Histogram, _ error) {
	scale := e.scale[idx]
	if scale < -4 {
		return h, errors.Errorf("cannot convert histogram, scale must be >= -4, was %d", scale)
	}

	var scaleDown int32
	if scale > 8 {
		scaleDown = scale - 8
		scale = 8
	}

	pSpans, pDeltas, err := convertBucketsLayout(e.positiveOffset[idx], e.positiveBucketCounts[idx], scaleDown)
	if err != nil {
		return h, errors.Wrap(err, "convert positive buckets")
	}
	nSpans, nDeltas, err := convertBucketsLayout(e.negativeOffset[idx], e.negativeBucketCounts[idx], scaleDown)
	if err != nil {
		return h, errors.Wrap(err, "convert negative buckets")
	}

	h = histogram.Histogram{
		Schema: scale,

		ZeroCount: e.zerocount[idx],
		// TODO use zero_threshold, if set, see
		// https://github.com/open-telemetry/opentelemetry-proto/pull/441
		ZeroThreshold: defaultZeroThreshold,

		PositiveSpans:   pSpans,
		PositiveBuckets: pDeltas,
		NegativeSpans:   nSpans,
		NegativeBuckets: nDeltas,
	}

	if flags := pmetric.DataPointFlags(e.flags[idx]); flags.NoRecordedValue() {
		h.Sum = math.Float64frombits(value.StaleNaN)
		h.Count = value.StaleNaN
	} else {
		if sum := e.sum[idx]; sum.Set {
			h.Sum = sum.Value
		}
		h.Count = e.count[idx]
	}
	return h, nil
}

// convertBucketsLayout translates OTel Exponential Histogram dense buckets
// representation to Prometheus Native Histogram sparse bucket representation.
//
// The translation logic is taken from the client_golang `histogram.go#makeBuckets`
// function, see `makeBuckets` https://github.com/prometheus/client_golang/blob/main/prometheus/histogram.go
// The bucket indexes conversion was adjusted, since OTel exp. histogram bucket
// index 0 corresponds to the range (1, base] while Prometheus bucket index 0
// to the range (base 1].
//
// scaleDown is the factor by which the buckets are scaled down. In other words 2^scaleDown buckets will be merged into one.
func convertBucketsLayout(offset int32, bucketCounts []uint64, scaleDown int32) (spans []histogram.Span, deltas []int64, err error) {
	numBuckets := len(bucketCounts)
	switch {
	case numBuckets == 0:
		return nil, nil, nil
	case numBuckets > math.MaxInt32:
		return nil, nil, errors.Errorf("too much buckets (%d is overflowing int32)", numBuckets)
	}

	var (
		count     int64
		prevCount int64
	)

	appendDelta := func(count int64) {
		spans[len(spans)-1].Length++
		deltas = append(deltas, count-prevCount)
		prevCount = count
	}

	// The offset is scaled and adjusted by 1 as described above.
	bucketIdx := offset>>scaleDown + 1
	spans = append(spans, histogram.Span{
		Offset: bucketIdx,
		Length: 0,
	})

	for i := 0; i < numBuckets; i++ {
		// The offset is scaled and adjusted by 1 as described above.
		nextBucketIdx := (int32(i)+offset)>>scaleDown + 1
		if bucketIdx == nextBucketIdx { // We have not collected enough buckets to merge yet.
			count += int64(bucketCounts[i])
			continue
		}
		if count == 0 {
			count = int64(bucketCounts[i])
			continue
		}

		gap := nextBucketIdx - bucketIdx - 1
		if gap > 2 {
			// We have to create a new span, because we have found a gap
			// of more than two buckets. The constant 2 is copied from the logic in
			// https://github.com/prometheus/client_golang/blob/27f0506d6ebbb117b6b697d0552ee5be2502c5f2/prometheus/histogram.go#L1296
			spans = append(spans, histogram.Span{
				Offset: gap,
				Length: 0,
			})
		} else {
			// We have found a small gap (or no gap at all).
			// Insert empty buckets as needed.
			for j := int32(0); j < gap; j++ {
				appendDelta(0)
			}
		}
		appendDelta(count)
		count = int64(bucketCounts[i])
		bucketIdx = nextBucketIdx
	}
	// Need to use the last item's index. The offset is scaled and adjusted by 1 as described above.
	gap := (int32(numBuckets)+offset-1)>>scaleDown + 1 - bucketIdx
	if gap > 2 {
		// We have to create a new span, because we have found a gap
		// of more than two buckets. The constant 2 is copied from the logic in
		// https://github.com/prometheus/client_golang/blob/27f0506d6ebbb117b6b697d0552ee5be2502c5f2/prometheus/histogram.go#L1296
		spans = append(spans, histogram.Span{
			Offset: gap,
			Length: 0,
		})
	} else {
		// We have found a small gap (or no gap at all).
		// Insert empty buckets as needed.
		for j := int32(0); j < gap; j++ {
			appendDelta(0)
		}
	}
	appendDelta(count)

	return spans, deltas, nil
}

func (e expHistData) Iterator(ts []int64) chunkenc.Iterator {
	return newExpHistIterator(e, ts)
}

type expHistIterator struct {
	values expHistData

	current histogram.Histogram
	err     error

	ts []int64
	n  int
}

var _ chunkenc.Iterator = (*expHistIterator)(nil)

func newExpHistIterator(values expHistData, ts []int64) *expHistIterator {
	return &expHistIterator{
		values: values,
		ts:     ts,
		n:      -1,
	}
}

// Next advances the iterator by one and returns the type of the value
// at the new position (or ValNone if the iterator is exhausted).
func (p *expHistIterator) Next() chunkenc.ValueType {
	if p.n+1 >= len(p.ts) {
		return chunkenc.ValNone
	}
	p.n++
	p.current, p.err = p.loadValue()
	return chunkenc.ValFloatHistogram
}

// Seek advances the iterator forward to the first sample with a
// timestamp equal or greater than t. If the current sample found by a
// previous `Next` or `Seek` operation already has this property, Seek
// has no effect. If a sample has been found, Seek returns the type of
// its value. Otherwise, it returns ValNone, after which the iterator is
// exhausted.
func (p *expHistIterator) Seek(seek int64) chunkenc.ValueType {
	// Find the closest value.
	if !seekIterator(p.ts, &p.n, seek) {
		return chunkenc.ValNone
	}
	p.current, p.err = p.loadValue()
	return chunkenc.ValHistogram
}

const defaultZeroThreshold = 1e-128

func (p *expHistIterator) loadValue() (histogram.Histogram, error) {
	return p.values.value(p.n)
}

// At returns the current timestamp/value pair if the value is a float.
// Before the iterator has advanced, the behavior is unspecified.
func (p *expHistIterator) At() (t int64, v float64) {
	return 0, 0
}

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts. Before the iterator has advanced,
// the behavior is unspecified.
func (p *expHistIterator) AtHistogram(h *histogram.Histogram) (t int64, _ *histogram.Histogram) {
	t = p.AtT()
	if h == nil {
		h = new(histogram.Histogram)
	}
	*h = p.current
	return t, h
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behavior is unspecified.
func (p *expHistIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behavior is unspecified.
func (p *expHistIterator) AtT() int64 {
	return p.ts[p.n]
}

// Err returns the current error. It should be used only after the
// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
func (p *expHistIterator) Err() error {
	return p.err
}
