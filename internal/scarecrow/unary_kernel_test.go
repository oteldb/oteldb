package scarecrow

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
)

type unaryKernelBenchInput struct{ col Column }

func (*unaryKernelBenchInput) String() string                          { return "UnaryKernelBenchInput" }
func (*unaryKernelBenchInput) Children() []Operator                    { return nil }
func (*unaryKernelBenchInput) Close() error                            { return nil }
func (*unaryKernelBenchInput) Schema(context.Context) (*Schema, error) { return ScalarSchema(), nil }
func (s *unaryKernelBenchInput) Next(context.Context) (*Column, error) { return &s.col, nil }

var unaryKernelSink float64

func TestUnaryKernelBitwiseSemantics(t *testing.T) {
	values := []float64{0, math.Copysign(0, -1), -1, math.Inf(-1), math.Float64frombits(0x7ff0000000000001), math.Float64frombits(0xfff8000000000456)}
	for _, n := range []int{0, 1, 16, 63, 64, 65, 241} {
		for _, density := range []int{1, 2, 64} {
			source := &unaryKernelBenchInput{}
			source.col.Resize(0, n)
			for i := range n {
				source.col.V[i] = values[i%len(values)]
				if i%density == 0 {
					source.col.Set(i, source.col.V[i])
				}
			}
			scalar := newUnaryFn(source, "abs", math.Abs)
			scalar.batch = nil
			want, err := scalar.Next(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			got, err := newUnaryFn(source, "abs", math.Abs).Next(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(want.Valid, got.Valid) {
				t.Fatalf("n=%d density=%d: validity differs", n, density)
			}
			for i := range want.V {
				if math.Float64bits(want.V[i]) != math.Float64bits(got.V[i]) {
					t.Fatalf("n=%d density=%d i=%d: want %x got %x", n, density, i, math.Float64bits(want.V[i]), math.Float64bits(got.V[i]))
				}
			}
		}
	}
}

func BenchmarkUnaryKernel(b *testing.B) {
	for _, n := range []int{1, 16, 241, 4096} {
		for _, density := range []int{1, 2, 64} {
			for _, impl := range []string{"scalar", "batch"} {
				b.Run(fmt.Sprintf("n=%d/density=%d/%s", n, density, impl), func(b *testing.B) {
					source := &unaryKernelBenchInput{}
					source.col.Resize(0, n)
					for i := range n {
						source.col.V[i] = -float64(i + 1)
						if i%density == 0 {
							source.col.Set(i, -float64(i+1))
						}
					}
					base := newUnaryFn(source, "abs", math.Abs)
					if impl == "scalar" {
						base.batch = nil
					}
					_, _ = base.Next(b.Context())
					b.ReportAllocs()
					for b.Loop() {
						col, err := base.Next(b.Context())
						if err != nil {
							b.Fatal(err)
						}
						unaryKernelSink = col.V[0]
					}
				})
			}
		}
	}
}
