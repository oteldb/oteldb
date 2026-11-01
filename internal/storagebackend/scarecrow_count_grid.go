package storagebackend

import (
	"context"
	"math"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/storage/query/fetch"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

var _ scarecrow.GridSeriesCounter = (*scarecrowScanner)(nil)

func (s *scarecrowScanner) CountSeriesGrid(ctx context.Context, grid scarecrow.WindowGrid, label string, matchers []*labels.Matcher) (map[string][]uint64, error) {
	const nsPerMs = int64(time.Millisecond)
	if grid.Start < math.MinInt64/nsPerMs || grid.Start > math.MaxInt64/nsPerMs ||
		grid.Step <= 0 || grid.Step > math.MaxInt64/nsPerMs ||
		grid.Width <= 0 || grid.Width > math.MaxInt64/nsPerMs || grid.NumSteps <= 0 {
		return nil, scarecrow.ErrCountGridUnsupported
	}
	steps := int64(grid.NumSteps - 1)
	if steps > math.MaxInt64/grid.Step || grid.Start > math.MaxInt64-steps*grid.Step ||
		grid.Start < math.MinInt64+grid.Width {
		return nil, scarecrow.ErrCountGridUnsupported
	}
	end := grid.Start + steps*grid.Step
	if end > math.MaxInt64/nsPerMs {
		return nil, scarecrow.ErrCountGridUnsupported
	}
	q, err := s.b.queryable().QuerierWithScope(grid.Start-grid.Width+1, end, s.scope)
	if err != nil {
		return nil, err
	}
	defer func() { _ = q.Close() }()
	counter, ok := q.(interface {
		CountSeriesGrid(context.Context, fetch.WindowGrid, string, ...*labels.Matcher) (map[string][]uint64, error)
	})
	if !ok {
		return nil, scarecrow.ErrCountGridUnsupported
	}
	counts, err := counter.CountSeriesGrid(ctx, fetch.WindowGrid{
		Start: grid.Start * nsPerMs, Step: grid.Step * nsPerMs,
		Width: grid.Width * nsPerMs, NumSteps: grid.NumSteps,
	}, label, matchers...)
	if errors.Is(err, fetch.ErrCountGridUnsupported) {
		return nil, scarecrow.ErrCountGridUnsupported
	}
	return counts, err
}
