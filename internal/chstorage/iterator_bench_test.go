package chstorage

import (
	"testing"
)

func BenchmarkPointIterator_Seek(b *testing.B) {
	const (
		numPoints = 10000
		numSteps  = 1000
	)
	ts := make([]int64, numPoints)
	values := make([]float64, numPoints)
	for i := range ts {
		ts[i] = int64(i * 15000) // 15s intervals
		values[i] = float64(i)
	}

	b.ResetTimer()
	for b.Loop() {
		it := newPointIterator(values, ts)
		for j := range numSteps {
			seek := int64(j * 15000 * (numPoints / numSteps))
			it.Seek(seek)
		}
	}
}
