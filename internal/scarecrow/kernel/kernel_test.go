package kernel_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/scarecrow/kernel"
)

// mask builds a bitset over n steps with the given indices set.
func mask(n int, set ...int) []uint64 {
	w := make([]uint64, (n+63)/64)
	for _, i := range set {
		w[i>>6] |= 1 << uint(i&63)
	}

	return w
}

func isSet(w []uint64, i int) bool { return w[i>>6]&(1<<uint(i&63)) != 0 }

func TestAddF64(t *testing.T) {
	t.Parallel()

	dst := []float64{1, 2, 3}
	kernel.AddF64(dst, []float64{10, 20, 30})
	require.Equal(t, []float64{11, 22, 33}, dst)
}

func TestAddMaskedF64(t *testing.T) {
	t.Parallel()

	const n = 70

	dst := make([]float64, n)
	dstValid := mask(n)

	src := make([]float64, n)
	src[0], src[65] = 1.5, 2.5

	kernel.AddMaskedF64(dst, dstValid, src, mask(n, 0, 65))

	require.True(t, isSet(dstValid, 0))
	require.True(t, isSet(dstValid, 65))
	require.InDelta(t, 1.5, dst[0], 0)
	require.InDelta(t, 2.5, dst[65], 0)

	// Absent steps stay absent and untouched, keeping "no sample" distinct from "value 0".
	require.False(t, isSet(dstValid, 1))
	require.InDelta(t, 0.0, dst[1], 0)

	// Accumulating a second series folds into the same row.
	src2 := make([]float64, n)
	src2[0] = 0.5
	kernel.AddMaskedF64(dst, dstValid, src2, mask(n, 0))
	require.InDelta(t, 2.0, dst[0], 0)
}

func TestMinMaxMaskedF64(t *testing.T) {
	t.Parallel()

	const n = 4

	for _, tt := range []struct {
		name string
		fn   func([]float64, []uint64, []float64, []uint64)
		want float64
	}{
		{"min", kernel.MinMaskedF64, 1},
		{"max", kernel.MaxMaskedF64, 5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dst := make([]float64, n)
			dstValid := mask(n)

			// First contributor seeds the accumulator outright — no separate init pass.
			kernel.MinMaskedF64(dst, dstValid, []float64{3, 0, 0, 0}, mask(n, 0))
			require.InDelta(t, 3.0, dst[0], 0)

			tt.fn(dst, dstValid, []float64{tt.want, 0, 0, 0}, mask(n, 0))
			require.InDelta(t, tt.want, dst[0], 0)
		})
	}
}

func TestMinMaskedSeedsFromAbsent(t *testing.T) {
	t.Parallel()

	const n = 2

	dst := []float64{math.Inf(1), math.Inf(1)}
	dstValid := mask(n)

	// A step absent in dst takes src's value even though it is larger than the stale content.
	kernel.MinMaskedF64(dst, dstValid, []float64{7, 0}, mask(n, 0))
	require.InDelta(t, 7.0, dst[0], 0)
	require.True(t, isSet(dstValid, 0))
}

func TestCountMaskedF64(t *testing.T) {
	t.Parallel()

	const n = 70

	dst := make([]float64, n)
	dstValid := mask(n)

	kernel.CountMaskedF64(dst, dstValid, mask(n, 0, 65))
	kernel.CountMaskedF64(dst, dstValid, mask(n, 0))

	require.InDelta(t, 2.0, dst[0], 0)
	require.InDelta(t, 1.0, dst[65], 0)
	require.InDelta(t, 0.0, dst[1], 0)
	require.False(t, isSet(dstValid, 1))
}

func TestScaleAndAbs(t *testing.T) {
	t.Parallel()

	dst := []float64{1, -2, 3}
	kernel.ScaleF64(dst, 2)
	require.Equal(t, []float64{2, -4, 6}, dst)

	kernel.AbsF64(dst)
	require.Equal(t, []float64{2, 4, 6}, dst)
}
