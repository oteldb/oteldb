package scarecrow

import (
	"sort"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// rangeMerger concatenates the per-chunk results M16's time-chunking produces (see
// [query.execRange]) into one range-query result. Chunks cover disjoint, ascending time
// windows, so joining a series across chunks is concatenation, not merging: each chunk's
// [collectRange] output is already sorted and deduplicated on its own, and a series' identity
// (its label hash) is stable across chunks since schema resolution never depends on values.
type rangeMerger struct {
	// totalSteps upper-bounds how many points any one series can end up with, so a series'
	// backing array is sized once, on first sight, rather than regrown on every later chunk.
	totalSteps int

	byHash map[uint64]*promql.Series
	order  []uint64
}

// add appends one chunk's result. v must be the [parser.Value] a range query produces:
// [promql.Matrix].
func (m *rangeMerger) add(v parser.Value) error {
	mat, ok := v.(promql.Matrix)
	if !ok {
		return errors.Errorf("scarecrow: unexpected range chunk result type %T", v)
	}

	if m.byHash == nil {
		m.byHash = make(map[uint64]*promql.Series, len(mat))
	}

	for i := range mat {
		s := &mat[i]
		h := s.Metric.Hash()

		existing, ok := m.byHash[h]
		if !ok {
			cp := promql.Series{Metric: s.Metric, Floats: append(make([]promql.FPoint, 0, m.totalSteps), s.Floats...)}
			m.byHash[h] = &cp
			m.order = append(m.order, h)

			continue
		}

		existing.Floats = append(existing.Floats, s.Floats...)
	}

	return nil
}

// result returns the merged, sorted matrix. Safe to call on a zero-value [rangeMerger] (a query
// with no steps at all).
func (m *rangeMerger) result() promql.Matrix {
	out := make(promql.Matrix, 0, len(m.order))
	for _, h := range m.order {
		out = append(out, *m.byHash[h])
	}

	sort.Sort(out)

	return out
}
