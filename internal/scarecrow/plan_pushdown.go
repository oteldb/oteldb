package scarecrow

import (
	"github.com/prometheus/prometheus/promql/parser"
)

// The pushdown rules. Each is a planner rewrite guarded by a capability the scanner either has
// or does not: no capability, no rewrite, identical answer. That is the difference from how
// `internal/storagebackend` does this against the Thanos fork, where the same three optimizations
// live in an adapter that intercepts scanner construction and pattern-matches the logical node,
// because there is no plan to rewrite (docs §2.2).
//
// Each returns ok=false to mean "not applicable", never an error: a pushdown that does not apply
// must leave the query planning exactly as it would have without the capability.

// pushDownOverTime rewrites a reducer `*_over_time` over a plain selector into [aggregateOverTime].
func (p *planner) pushDownOverTime(
	fnName string, vs *parser.VectorSelector, ms *parser.MatrixSelector,
) (Operator, bool) {
	scanner, ok := p.scanner.(AggregateScanner)
	if !ok {
		return nil, false
	}

	fold, ok := overTimeFolds[fnName]
	if !ok {
		return nil, false
	}

	return newAggregateOverTime(
		scanner, vs.LabelMatchers, matchersString(vs.LabelMatchers), fnName, fold,
		ms.Range, vs.OriginalOffset, vs.Timestamp, p.ec,
	), true
}

// pushDownCount rewrites `count(selector)` and `count by (label) (selector)` into an index-only
// count.
//
// Restricted to a bare selector on purpose. `count(rate(x[5m]))` counts the series that *produced
// a value*, which is a property of the fold and not of the index, and `count without (…)` and
// multi-label grouping have no single-label counting seam behind them.
func (p *planner) pushDownCount(e *parser.AggregateExpr) (Operator, bool) {
	if e.Op != parser.COUNT || e.Param != nil || e.Without {
		return nil, false
	}

	vs, ok := e.Expr.(*parser.VectorSelector)
	if !ok {
		return nil, false
	}

	label := matchersString(vs.LabelMatchers)

	switch len(e.Grouping) {
	case 0:
		counter, ok := p.scanner.(SeriesCounter)
		if !ok {
			return nil, false
		}

		return newCountSeries(
			counter, vs.LabelMatchers, label, vs.OriginalOffset, vs.Timestamp, p.ec,
		), true

	case 1:
		counter, ok := p.scanner.(GroupedSeriesCounter)
		if !ok {
			return nil, false
		}

		return newCountSeriesBy(
			counter, vs.LabelMatchers, label, e.Grouping[0], vs.OriginalOffset, vs.Timestamp, p.ec,
		), true

	default:
		return nil, false
	}
}
