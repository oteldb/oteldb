package kernel

import (
	"math"
	"math/bits"
)

// Small vectors stay in Go to amortize assembly dispatch.
const simdMinLen = 64

// ScaleF64 computes dst[i] *= factor over the whole slice.
func ScaleF64(dst []float64, factor float64) {
	if simdEnabled && len(dst) >= simdMinLen {
		scaleAVX(dst, factor)
		return
	}
	scaleF64(dst, factor)
}

func scaleF64(dst []float64, factor float64) {
	for i := range dst {
		dst[i] *= factor
	}
}

// AbsF64 computes dst[i] = |dst[i]| over the whole slice.
func AbsF64(dst []float64) {
	if simdEnabled && len(dst) >= simdMinLen {
		absAVX(dst)
		return
	}
	absF64(dst)
}

func absF64(dst []float64) {
	for i := range dst {
		dst[i] = math.Abs(dst[i])
	}
}

// AbsDiffF64 computes dst[i] = |src[i] - center|. The slices must have equal
// lengths and be disjoint or start at the same element.
func AbsDiffF64(dst, src []float64, center float64) {
	src = src[:len(dst)]
	if simdEnabled && len(dst) >= simdMinLen {
		absDiffAVX(dst, src, center)
		return
	}
	absDiffF64(dst, src, center)
}

func absDiffF64(dst, src []float64, center float64) {
	for i := range dst {
		dst[i] = math.Abs(src[i] - center)
	}
}

// AbsMaskedF64 applies [AbsF64] to present steps, preserving absent values.
func AbsMaskedF64(dst []float64, valid []uint64) {
	for wi, word := range valid {
		start := wi * 64
		if start >= len(dst) {
			break
		}
		end := min(start+64, len(dst))
		full := ^uint64(0) >> uint(64-(end-start))
		word &= full
		if word == full {
			AbsF64(dst[start:end])
			continue
		}
		for word != 0 {
			bit := bits.TrailingZeros64(word)
			dst[start+bit] = math.Abs(dst[start+bit])
			word &= word - 1
		}
	}
}
