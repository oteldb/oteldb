package traceqlengine

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/traceql"
)

// ScalarFilter filters Scalars by field expression.
type ScalarFilter struct {
	Eval Evaluater
}

func buildScalarFilter(filter *traceql.ScalarFilter) (Processor, error) {
	eval, err := buildBinaryEvaluater(filter.Left, filter.Op, filter.Right)
	if err != nil {
		return nil, errors.Wrap(err, "build scalar evalauter")
	}
	return &ScalarFilter{Eval: eval}, nil
}

// Process implements Processor.
func (f *ScalarFilter) Process(sets []Spanset) (result []Spanset, _ error) {
	return filterBy(f.Eval, sets), nil
}
