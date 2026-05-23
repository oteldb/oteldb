package chstorage

import (
	"cmp"
	"slices"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

func seekIterator(ts []int64, n *int, step, seek int64) bool {
	start := 0
	if *n >= 0 {
		if *n < len(ts) && ts[*n] >= seek {
			return true
		}
		start = *n + 1
	}

	if start >= len(ts) {
		*n = len(ts)
		return false
	}

	idx := interpolationSeek(ts[start:], step, seek)
	idx += start
	if idx >= len(ts) {
		*n = len(ts)
		return false
	}
	*n = idx
	return true
}

func interpolationSeek(ts []int64, step, seek int64) int {
	clamp := func(v, lo, hi int) int {
		return max(lo, min(v, hi))
	}

	if len(ts) == 0 {
		return 0
	}
	if seek <= ts[0] {
		return 0
	}
	last := len(ts) - 1
	if seek > ts[last] {
		return len(ts)
	}

	lo, hi := 0, last
	// If we have a step hint, try it first.
	if step > 0 {
		est := int((seek - ts[0]) / step)
		est = clamp(est, 0, last)

		switch cmp.Compare(ts[est], seek) {
		case 0:
			for est > 0 && ts[est-1] >= seek {
				est--
			}
			return est
		case -1:
			lo = est + 1
		default:
			hi = est
		}
	}

	for range 4 {
		if lo >= hi || ts[lo] == ts[hi] {
			break
		}
		span := hi - lo
		seekOffset := seek - ts[lo]
		tsRange := ts[hi] - ts[lo]
		pos := lo + int(int64(span)*seekOffset/tsRange)
		pos = clamp(pos, lo+1, hi)

		switch cmp.Compare(ts[pos], seek) {
		case 0:
			for pos > 0 && ts[pos-1] >= seek {
				pos--
			}
			return pos
		case -1:
			lo = pos + 1
		default:
			hi = pos
		}
	}
	idx, _ := slices.BinarySearch(ts[lo:hi+1], seek)
	return lo + idx
}

func computeStep(ts []int64) int64 {
	if len(ts) < 2 {
		return 0
	}
	return (ts[len(ts)-1] - ts[0]) / int64(len(ts)-1)
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
	step   int64
	n      int
}

var _ chunkenc.Iterator = (*pointIterator)(nil)

func newPointIterator(values []float64, ts []int64) *pointIterator {
	return &pointIterator{
		values: values,
		ts:     ts,
		step:   computeStep(ts),
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
	if !seekIterator(p.ts, &p.n, p.step, seek) {
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
