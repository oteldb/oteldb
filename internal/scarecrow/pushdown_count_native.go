package scarecrow

import (
	"context"
	"math"
	"slices"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
)

// ErrCountGridUnsupported retains ordinary aggregate folding for this request.
var ErrCountGridUnsupported = errors.New("count grid unsupported")

// GridSeriesCounter counts present series per step, optionally grouped by one visible label.
// Empty label means ungrouped, under the empty-string key. Zero-only groups are omitted.
// AggregateGrid remains the fallback when CountSeriesGrid returns ErrCountGridUnsupported.
type GridSeriesCounter interface {
	GridAggregateScanner
	CountSeriesGrid(context.Context, WindowGrid, string, []*labels.Matcher) (map[string][]uint64, error)
}

func (o *countSeriesBy) setGridCounts(ctx context.Context, grid WindowGrid, counts map[string][]uint64) error {
	if grid.NumSteps <= 0 || len(counts) > math.MaxInt/grid.NumSteps {
		return errors.New("count grid output dimensions overflow")
	}
	if err := o.ec.charge(len(counts) * grid.NumSteps); err != nil {
		return err
	}
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	slices.Sort(names)
	projected := make([]labels.Labels, len(names))
	o.values = make([]float64, len(names)*grid.NumSteps)
	o.valid = make([]bool, len(o.values))
	for i, name := range names {
		if name == "" {
			projected[i] = labels.EmptyLabels()
		} else {
			projected[i] = labels.FromStrings(o.by, name)
		}
		values := counts[name]
		if len(values) != grid.NumSteps {
			return errors.Errorf("count grid group %q: got %d steps, want %d", name, len(values), grid.NumSteps)
		}
		for step, n := range values {
			if n != 0 {
				o.values[i*grid.NumSteps+step] = float64(n)
				o.valid[i*grid.NumSteps+step] = true
			}
		}
	}
	o.schema = NewSchema(projected)
	return ctx.Err()
}
