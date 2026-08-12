// Package kernel holds the engine's elementwise float64 loops.
//
// Every kernel takes contiguous slices, has no branches in its loop body, makes no interface
// calls, and handles validity by mask rather than by a per-element conditional. Series-major
// execution (see docs/promql-engine.md §3.2) is what makes this the only shape needed: a
// [github.com/oteldb/oteldb/internal/scarecrow.Column] is contiguous, so unary functions,
// binary operations, and accumulation into a group row are all elementwise over the step axis.
// There is no strided or gathering kernel.
//
// Kernels are isolated behind this narrow API, rather than inlined into operators, so each can
// be benchmarked on its own and so an assembly implementation can be swapped in per
// architecture behind a build tag without touching execution code.
package kernel

import "math"

// AddF64 computes dst[i] += src[i] over the whole slice. It is the accumulation step of every
// sum-shaped aggregation: many series' columns fold into one group row, so the reduction across
// series happens implicitly as columns accumulate.
func AddF64(dst, src []float64) {
	src = src[:len(dst)] // hoist the bounds check out of the loop
	for i := range dst {
		dst[i] += src[i]
	}
}

// AddMaskedF64 computes dst[i] += src[i] for every step whose valid bit is set, and marks those
// steps set in dstValid. Absent steps leave dst untouched, which is what keeps "no sample" and
// "sample with value 0" distinct.
func AddMaskedF64(dst []float64, dstValid []uint64, src []float64, srcValid []uint64) {
	src = src[:len(dst)]
	for w, word := range srcValid {
		if word == 0 {
			continue
		}

		base := w << 6
		for b := range 64 {
			if word&(1<<uint(b)) == 0 {
				continue
			}

			i := base + b
			if i >= len(dst) {
				break
			}

			dst[i] += src[i]
		}

		dstValid[w] |= word
	}
}

// ScaleF64 computes dst[i] *= f over the whole slice.
func ScaleF64(dst []float64, f float64) {
	for i := range dst {
		dst[i] *= f
	}
}

// AbsF64 computes dst[i] = |dst[i]| over the whole slice.
func AbsF64(dst []float64) {
	for i := range dst {
		dst[i] = math.Abs(dst[i])
	}
}

// MinMaskedF64 computes the elementwise minimum of dst and src over set steps, marking them set
// in dstValid. A step present in src but absent in dst takes src's value outright, so the
// accumulator needs no separate "first value seen" pass.
func MinMaskedF64(dst []float64, dstValid []uint64, src []float64, srcValid []uint64) {
	minMax(dst, dstValid, src, srcValid, true)
}

// MaxMaskedF64 is [MinMaskedF64] for the elementwise maximum.
func MaxMaskedF64(dst []float64, dstValid []uint64, src []float64, srcValid []uint64) {
	minMax(dst, dstValid, src, srcValid, false)
}

func minMax(dst []float64, dstValid []uint64, src []float64, srcValid []uint64, wantMin bool) {
	src = src[:len(dst)]
	for w, word := range srcValid {
		if word == 0 {
			continue
		}

		base := w << 6
		for b := range 64 {
			if word&(1<<uint(b)) == 0 {
				continue
			}

			i := base + b
			if i >= len(dst) {
				break
			}

			switch {
			case dstValid[w]&(1<<uint(b)) == 0:
				dst[i] = src[i]
			case wantMin && src[i] < dst[i], !wantMin && src[i] > dst[i]:
				dst[i] = src[i]
			}
		}

		dstValid[w] |= word
	}
}

// CountMaskedF64 computes dst[i]++ for every step whose valid bit is set in srcValid, marking
// those steps set in dstValid. It backs count-shaped aggregations, which need the validity
// bitset but not the values.
func CountMaskedF64(dst []float64, dstValid, srcValid []uint64) {
	for w, word := range srcValid {
		if word == 0 {
			continue
		}

		base := w << 6
		for b := range 64 {
			if word&(1<<uint(b)) == 0 {
				continue
			}

			i := base + b
			if i >= len(dst) {
				break
			}

			dst[i]++
		}

		dstValid[w] |= word
	}
}
