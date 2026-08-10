package scarecrow

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
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
	ctx context.Context, scanner GridAggregateScanner, grid WindowGrid, matchers []*labels.Matcher,
) ([]GridAggregate, error) {
	out, err := scanner.AggregateGrid(ctx, grid, matchers)
	if err != nil {
		return nil, errors.Wrap(err, "aggregate grid")
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
