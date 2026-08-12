package scarecrow

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// foldSource supplies the per-series raw samples a [matrixFold] folds over.
//
// It is the seam that makes a subquery and a range selector the same thing to the fold: both
// resolve to a series set and a stream of [Samples]. Only the origin of the samples differs —
// storage for a selector, an inner operator tree for a subquery.
type foldSource interface {
	// Series resolves the source's series set at plan time.
	Series(ctx context.Context) ([]labels.Labels, error)
	// Open begins iterating the source's per-series samples.
	Open(ctx context.Context) (SeriesIterator, error)
	Close() error
}

// scannerSource reads raw samples from storage over a fixed window.
type scannerSource struct {
	scanner  Scanner
	matchers []*labels.Matcher
	mint     int64
	maxt     int64
}

var _ foldSource = (*scannerSource)(nil)

func (s *scannerSource) Series(ctx context.Context) ([]labels.Labels, error) {
	series, err := s.scanner.Series(ctx, s.mint, s.maxt, s.matchers)
	if err != nil {
		return nil, errors.Wrap(err, "enumerate series")
	}

	return series, nil
}

func (s *scannerSource) Open(ctx context.Context) (SeriesIterator, error) {
	it, err := s.scanner.Scan(ctx, s.mint, s.maxt, s.matchers)
	if err != nil {
		return nil, errors.Wrap(err, "scan series")
	}

	return it, nil
}

func (s *scannerSource) Close() error { return nil }

// subquerySource evaluates an inner expression on its own step grid and presents the results as
// raw samples.
//
// A subquery is where series-major execution pays off a second time: the inner operator already
// emits one column per series, and a column *is* that series' samples along the step axis. So
// the conversion is a direct read of the column, one series at a time, with no transpose and no
// materialization of the inner result set. The step-major draft needed a dedicated `Transpose`
// operator here.
type subquerySource struct {
	inner  Operator
	ec     *EvalContext
	schema *Schema
}

var _ foldSource = (*subquerySource)(nil)

func (s *subquerySource) Series(ctx context.Context) ([]labels.Labels, error) {
	schema, err := s.inner.Schema(ctx)
	if err != nil {
		return nil, err
	}

	s.schema = schema

	return schema.Series, nil
}

func (s *subquerySource) Open(context.Context) (SeriesIterator, error) {
	return &subqueryIterator{src: s}, nil
}

func (s *subquerySource) Close() error { return s.inner.Close() }

// subqueryIterator turns the inner operator's columns into per-series samples.
type subqueryIterator struct {
	src *subquerySource
	cur Samples
}

func (it *subqueryIterator) Close() error { return nil }

func (it *subqueryIterator) Next(ctx context.Context) (*Samples, error) {
	col, err := it.src.inner.Next(ctx)
	if err != nil || col == nil {
		return nil, err
	}

	it.cur.Labels = it.src.schema.At(col.Ref)
	it.cur.T = it.cur.T[:0]
	it.cur.V = it.cur.V[:0]
	// An inner result carries no sampling weights: they were consumed by the inner fold.
	it.cur.Weights = nil

	for i, t := range it.src.ec.Steps {
		if !col.IsSet(i) {
			continue
		}

		it.cur.T = append(it.cur.T, t)
		it.cur.V = append(it.cur.V, col.V[i])
	}

	return &it.cur, nil
}

// subqueryGrid returns the inner evaluation grid of a subquery.
//
// The first inner step is the earliest multiple of the subquery step strictly after
// (outerStart - range), so the inner grid is aligned to the step rather than to the outer
// query's start. This mirrors upstream exactly; a differently aligned grid silently shifts
// every value a subquery produces.
func subqueryGrid(outerRefs []int64, rangeMs, stepMs int64) []int64 {
	var (
		lowerBound = outerRefs[0] - rangeMs
		end        = outerRefs[len(outerRefs)-1]
	)

	start := stepMs * (lowerBound / stepMs)
	if start <= lowerBound {
		start += stepMs
	}

	if start > end {
		return nil
	}

	grid := make([]int64, 0, (end-start)/stepMs+1)
	for t := start; t <= end; t += stepMs {
		grid = append(grid, t)
	}

	return grid
}

// subqueryStep returns the subquery's inner step, defaulting to the configured no-step interval
// when the expression omits one (`[5m:]`).
func subqueryStep(e *parser.SubqueryExpr, def time.Duration) int64 {
	if e.Step > 0 {
		return e.Step.Milliseconds()
	}

	return def.Milliseconds()
}
