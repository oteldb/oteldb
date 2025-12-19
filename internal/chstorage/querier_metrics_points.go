package chstorage

import (
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func seekIterator(ts []int64, n *int, seek int64) bool {
	idx, ok := slices.BinarySearch(ts, seek)
	switch {
	case idx >= len(ts):
		// Outside of the range.
		*n = len(ts)
		return false
	case idx == 0 && !ok:
		// Move to the first point.
		*n = 0
		return true
	default:
		*n = idx
		return true
	}
}

type pointData struct {
	values []float64
}

func (e pointData) Iterator(ts []int64) chunkenc.Iterator {
	return newPointIterator(e.values, ts)
}

type pointIterator struct {
	values []float64
	ts     []int64
	n      int
}

var _ chunkenc.Iterator = (*pointIterator)(nil)

func newPointIterator(values []float64, ts []int64) *pointIterator {
	return &pointIterator{
		values: values,
		ts:     ts,
		n:      -1,
	}
}

// Next advances the iterator by one and returns the type of the value
// at the new position (or ValNone if the iterator is exhausted).
func (p *pointIterator) Next() chunkenc.ValueType {
	if p.n+1 >= len(p.ts) {
		return chunkenc.ValNone
	}
	p.n++
	return chunkenc.ValFloat
}

// Seek advances the iterator forward to the first sample with a
// timestamp equal or greater than t. If the current sample found by a
// previous `Next` or `Seek` operation already has this property, Seek
// has no effect. If a sample has been found, Seek returns the type of
// its value. Otherwise, it returns ValNone, after which the iterator is
// exhausted.
func (p *pointIterator) Seek(seek int64) chunkenc.ValueType {
	// Find the closest value.
	if !seekIterator(p.ts, &p.n, seek) {
		return chunkenc.ValNone
	}
	return chunkenc.ValFloat
}

// At returns the current timestamp/value pair if the value is a float.
// Before the iterator has advanced, the behavior is unspecified.
func (p *pointIterator) At() (t int64, v float64) {
	t = p.AtT()
	v = p.values[p.n]
	return t, v
}

// AtHistogram returns the current timestamp/value pair if the value is
// a histogram with integer counts. Before the iterator has advanced,
// the behavior is unspecified.
func (p *pointIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

// AtFloatHistogram returns the current timestamp/value pair if the
// value is a histogram with floating-point counts. It also works if the
// value is a histogram with integer counts, in which case a
// FloatHistogram copy of the histogram is returned. Before the iterator
// has advanced, the behavior is unspecified.
func (p *pointIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

// AtT returns the current timestamp.
// Before the iterator has advanced, the behavior is unspecified.
func (p *pointIterator) AtT() int64 {
	return p.ts[p.n]
}

// Err returns the current error. It should be used only after the
// iterator is exhausted, i.e. `Next` or `Seek` have returned ValNone.
func (p *pointIterator) Err() error {
	return nil
}
