package scarecrow

import (
	"context"
	"fmt"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

type binaryBenchInput struct {
	schema      *Schema
	steps, next int
	out         Column
}

func (*binaryBenchInput) String() string                            { return "BinaryBenchInput" }
func (*binaryBenchInput) Children() []Operator                      { return nil }
func (*binaryBenchInput) Close() error                              { return nil }
func (s *binaryBenchInput) Schema(context.Context) (*Schema, error) { return s.schema, nil }
func (s *binaryBenchInput) Next(context.Context) (*Column, error) {
	if s.next == s.schema.Len() {
		return nil, nil
	}
	s.out.Resize(SeriesRef(s.next), s.steps)
	for i := range s.out.V {
		s.out.Set(i, float64(i+s.next+1))
	}
	s.next++
	return &s.out, nil
}

func BenchmarkSharedOperatorBaseline(b *testing.B) {
	const series, steps = 100, 241
	sets := make([]labels.Labels, series)
	for i := range sets {
		sets[i] = labels.FromStrings("svc", fmt.Sprint(i))
	}
	schema := NewSchema(sets)
	ec := &EvalContext{Steps: make([]int64, steps)}
	for _, kind := range []string{"scalar", "vector"} {
		b.Run(kind, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				left := &binaryBenchInput{schema: schema, steps: steps}
				var root Operator
				if kind == "scalar" {
					root = newVectorScalarBinop(left, newNumberLiteral(2, ec), parser.DIV, false, false, ec)
				} else {
					right := &binaryBenchInput{schema: schema, steps: steps}
					root = newVectorBinop(left, right, parser.DIV, &parser.VectorMatching{Card: parser.CardOneToOne}, false, ec)
				}
				if err := resolveSchemas(b.Context(), root); err != nil {
					b.Fatal(err)
				}
				count := 0
				for {
					col, err := root.Next(b.Context())
					if err != nil {
						b.Fatal(err)
					}
					if col == nil {
						break
					}
					count += col.Count()
				}
				if count != series*steps {
					b.Fatal(count)
				}
				if err := root.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
