package kernel

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func unaryValues() []float64 {
	return []float64{
		0, math.Copysign(0, -1), 1, -1, .1, math.SmallestNonzeroFloat64,
		-math.SmallestNonzeroFloat64, math.MaxFloat64, -math.MaxFloat64, math.Inf(1), math.Inf(-1),
		math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff0000000000002),
		math.Float64frombits(0x7ff8000000000123), math.Float64frombits(0xfff8000000000456),
	}
}

func requireSameBits(t *testing.T, want, got []float64) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Equal(t, math.Float64bits(want[i]), math.Float64bits(got[i]), "lane %d", i)
	}
}

func TestUnaryKernels(t *testing.T) {
	t.Logf("SIMD enabled: %v", simdEnabled)
	values := unaryValues()
	for _, n := range []int{0, 1, 3, 4, 15, 31, 32, 33, 63, 64, 65, 67, 127, 241, 4096} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			// Offset slices exercise unaligned loads; sentinels catch writes past the tail.
			src := make([]float64, n)
			for i := range src {
				src[i] = values[i%len(values)]
			}
			for _, factor := range values {
				t.Run(fmt.Sprintf("factor=%x", math.Float64bits(factor)), func(t *testing.T) {
					backing := make([]float64, n+2)
					backing[0], backing[n+1] = 123, 456
					got := backing[1 : n+1]
					copy(got, src)
					want := slices.Clone(src)
					scaleF64(want, factor)
					ScaleF64(got, factor)
					requireSameBits(t, want, got)
					require.Equal(t, 123., backing[0])
					require.Equal(t, 456., backing[n+1])

					absDiffF64(want, src, factor)
					AbsDiffF64(got, src, factor)
					requireSameBits(t, want, got)
					copy(got, src)
					AbsDiffF64(got, got, factor)
					requireSameBits(t, want, got)
					require.Equal(t, 123., backing[0])
					require.Equal(t, 456., backing[n+1])
				})
			}
			want, got := slices.Clone(src), slices.Clone(src)
			absF64(want)
			AbsF64(got)
			requireSameBits(t, want, got)
			for _, density := range []int{0, 1, 2, 64} {
				t.Run(fmt.Sprintf("density=%d", density), func(t *testing.T) {
					valid := make([]uint64, (n+63)/64)
					want, got := slices.Clone(src), slices.Clone(src)
					for i := range src {
						if density != 0 && i%density == 0 {
							valid[i>>6] |= 1 << uint(i&63)
							want[i] = math.Abs(want[i])
						}
					}
					if n%64 != 0 {
						valid[len(valid)-1] |= ^uint64(0) << uint(n%64)
					}
					AbsMaskedF64(got, valid)
					requireSameBits(t, want, got)
				})
			}
		})
	}
}

func FuzzUnaryKernels(f *testing.F) {
	for _, v := range unaryValues() {
		f.Add(math.Float64bits(v), uint64(0x7ff8000000000789), uint8(65), uint64(0x5555555555555555))
	}
	f.Fuzz(func(t *testing.T, value, factor uint64, count uint8, mask uint64) {
		n := int(count)
		src := make([]float64, n)
		for i := range src {
			src[i] = math.Float64frombits(value + uint64(i))
		}
		want, got := slices.Clone(src), slices.Clone(src)
		scaleF64(want, math.Float64frombits(factor))
		ScaleF64(got, math.Float64frombits(factor))
		requireSameBits(t, want, got)
		absDiffF64(want, src, math.Float64frombits(factor))
		AbsDiffF64(got, src, math.Float64frombits(factor))
		requireSameBits(t, want, got)
		valid := make([]uint64, (n+63)/64)
		for i := range valid {
			valid[i] = mask
		}
		want, got = slices.Clone(src), slices.Clone(src)
		for i := range src {
			if valid[i>>6]&(1<<uint(i&63)) != 0 {
				want[i] = math.Abs(want[i])
			}
		}
		AbsMaskedF64(got, valid)
		requireSameBits(t, want, got)
	})
}

func BenchmarkUnaryKernels(b *testing.B) {
	for _, n := range []int{1, 16, 63, 64, 65, 241, 4096} {
		for _, op := range []string{"scale", "abs", "absdiff"} {
			b.Run(fmt.Sprintf("%s/n=%d", op, n), func(b *testing.B) {
				src, dst := make([]float64, n), make([]float64, n)
				for i := range src {
					src[i] = -float64(i + 1)
					dst[i] = src[i]
				}
				b.ReportAllocs()
				switch op {
				case "scale":
					for b.Loop() {
						ScaleF64(dst, -1)
					}
				case "abs":
					for b.Loop() {
						AbsF64(dst)
					}
				case "absdiff":
					for b.Loop() {
						AbsDiffF64(dst, src, 1)
					}
				}
			})
		}
	}
}
