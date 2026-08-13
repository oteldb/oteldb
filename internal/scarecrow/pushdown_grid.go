package scarecrow

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"go.opentelemetry.io/otel/attribute"
)

// gridFor derives the [WindowGrid] covering refs, or ok=false when refs are not a multi-step,
// evenly spaced grid a scanner can answer in one call.
//
// Two shapes deliberately decline the grid, and both fall back to the per-window seam rather than
// failing:
//
//   - A single step (every instant query) is a single window, which the per-window call already
//     answers exactly. Synthesizing a one-step grid means inventing a Step the query never had,
//     and storage reads Step as the bucket width it folds samples into — so a made-up Step
//     silently truncates the window. That is not hypothetical: it is what
//     TestScarecrowScannerEngineMatchesFork caught.
//   - The @ modifier pins every step to the same instant, so refs collapse to one repeated
//     timestamp with a zero stride rather than a grid.
func gridFor(refs []int64, width int64) (WindowGrid, bool) {
	if len(refs) < 2 {
		return WindowGrid{}, false
	}

	step := refs[1] - refs[0]
	if step <= 0 {
		return WindowGrid{}, false
	}

	for i := 1; i < len(refs); i++ {
		if refs[i]-refs[i-1] != step {
			return WindowGrid{}, false
		}
	}

	return WindowGrid{Start: refs[0], Step: step, NumSteps: len(refs), Width: width}, true
}

// aggregateGrid runs the grid pushdown and checks the contract the engine then relies on: every
// series carries exactly one aggregate per step, so callers can index Windows by step without
// bounds-checking each one.
func aggregateGrid(
	ctx context.Context, ec *EvalContext, scanner GridAggregateScanner,
	grid WindowGrid, matchers []*labels.Matcher,
) ([]GridAggregate, error) {
	ctx, span := ec.span(ctx, "scarecrow.AggregateGrid",
		attribute.Int("promql.steps", grid.NumSteps),
		attribute.Int64("promql.step_ms", grid.Step),
		attribute.Int64("promql.window_ms", grid.Width),
	)
	defer span.End()

	out, err := scanner.AggregateGrid(ctx, grid, matchers)
	if err != nil {
		span.RecordError(err)

		return nil, errors.Wrap(err, "aggregate grid")
	}

	span.SetAttributes(attribute.Int("promql.series", len(out)))

	// The charge belongs to the caller, not here. A pushdown reads no raw [Samples], so something
	// must charge it or the large queries would be the ones escaping the budget — but the grid is
	// an *intermediate*, and what survives it differs per caller: a `count by` keeps one value per
	// (group, step), where the grid it folded from is per (series, step). Charging the grid here
	// billed a cardinality question for the whole scan it was pushed down to avoid, which is what
	// broke the node-exporter dashboard at 6h. Bounding the intermediate itself is the decode
	// budget's job (oteldb/storage#263), not the sample limit's.

	for i := range out {
		if len(out[i].Windows) != grid.NumSteps {
			return nil, errors.Errorf(
				"series %s: got %d windows, want %d",
				out[i].Labels, len(out[i].Windows), grid.NumSteps,
			)
		}
	}

	return out, nil
}
