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

	// A pushdown reads no raw [Samples], so the selector leaves never charge it. Charge the grid
	// it materialized instead — one folded value per (series, step) — or a pushed-down query
	// would be the one shape that escapes the budget entirely, which is exactly backwards: the
	// pushdown exists because those queries are the large ones.
	if err := ec.charge(len(out) * grid.NumSteps); err != nil {
		return nil, err
	}

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
