package logqlmetric

import (
	"time"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/otelstorage"
)

type vectorIterator struct {
	// step state
	stepper stepper
	value   float64
}

// Vector creates new vector function step iterator.
func Vector(
	expr *logql.VectorExpr,
	start, end time.Time,
	step time.Duration,
) StepIterator {
	return &vectorIterator{
		stepper: newStepper(start, end, step),
		value:   expr.Value,
	}
}

func (i *vectorIterator) Next(r *Step) bool {
	current, ok := i.stepper.next()
	if !ok {
		return false
	}

	r.Timestamp = otelstorage.NewTimestampFromTime(current)
	r.Samples = append(r.Samples[:0], Sample{
		Data: i.value,
		Set:  logqlabels.EmptyAggregatedLabels(),
	})
	return true
}

func (i *vectorIterator) Err() error {
	return nil
}

func (i *vectorIterator) Close() error {
	return nil
}
