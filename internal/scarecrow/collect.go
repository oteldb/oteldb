package scarecrow

import (
	"context"
	"sort"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// collectInstant drains the root into an instant-query result. The grid has exactly one step,
// so each column contributes at most one sample.
func collectInstant(
	ctx context.Context, root Operator, schema *Schema, typ parser.ValueType, ec *EvalContext,
) (parser.Value, error) {
	ts := ec.Steps[0]

	if typ == parser.ValueTypeScalar {
		col, err := root.Next(ctx)
		if err != nil {
			return nil, err
		}

		if col == nil || !col.IsSet(0) {
			return promql.Scalar{T: ts, V: 0}, nil
		}

		return promql.Scalar{T: ts, V: col.V[0]}, nil
	}

	var out promql.Vector

	for {
		col, err := root.Next(ctx)
		if err != nil {
			return nil, err
		}

		if col == nil {
			break
		}

		if !col.IsSet(0) {
			continue
		}

		out = append(out, promql.Sample{
			Metric: schema.At(col.Ref),
			T:      ts,
			F:      col.V[0],
		})
	}

	return out, nil
}

// unwrapMatrixSelector reports whether expr is a bare range selector, possibly wrapped by
// parens or step-invariance.
func unwrapMatrixSelector(expr parser.Expr) (*parser.MatrixSelector, bool) {
	for {
		switch e := expr.(type) {
		case *parser.ParenExpr:
			expr = e.Expr
		case *parser.StepInvariantExpr:
			expr = e.Expr
		case *parser.MatrixSelector:
			return e, true
		default:
			return nil, false
		}
	}
}

// collectRawMatrix materializes a bare range selector's samples for an instant query.
//
// This is the one result shape the operator tree cannot produce: no operator emits a range
// vector, by design (§4.3). Rather than bend that rule, the samples are read straight from the
// scanner here, at the result boundary where no operator contract applies.
func collectRawMatrix(
	ctx context.Context, sc Scanner, ms *parser.MatrixSelector, ec *EvalContext,
) (parser.Value, error) {
	vs, ok := ms.VectorSelector.(*parser.VectorSelector)
	if !ok {
		return nil, unsupportedf("matrix selector over %T", ms.VectorSelector)
	}

	if vs.Anchored || vs.Smoothed {
		return nil, unsupportedf("extended range selector")
	}

	refs := refTimes(ec.Steps, vs.OriginalOffset, vs.Timestamp)
	rangeEnd := refs[0]
	rangeStart := rangeEnd - ms.Range.Milliseconds()

	it, err := sc.Scan(ctx, rangeStart, rangeEnd, vs.LabelMatchers)
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()

	// A range selector reports the samples at their own timestamps, shifted back by any offset
	// so the result reads on the query's own timeline.
	offset := vs.OriginalOffset.Milliseconds()

	var out promql.Matrix

	for {
		s, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}

		if s == nil {
			break
		}

		pts := make([]promql.FPoint, 0, len(s.T))
		for i, t := range s.T {
			// Left-open window, and staleness markers are never reported.
			if t <= rangeStart || t > rangeEnd || isStale(s.V[i]) {
				continue
			}

			pts = append(pts, promql.FPoint{T: t + offset, F: s.V[i]})
		}

		if len(pts) == 0 {
			continue
		}

		out = append(out, promql.Series{Metric: s.Labels, Floats: pts})
	}

	sort.Sort(out)

	return out, nil
}

// collectRange drains the root into a range-query result. Each column becomes one series, with
// absent steps omitted rather than emitted as NaN.
func collectRange(ctx context.Context, root Operator, schema *Schema, ec *EvalContext) (parser.Value, error) {
	var out promql.Matrix

	for {
		col, err := root.Next(ctx)
		if err != nil {
			return nil, err
		}

		if col == nil {
			break
		}

		// PromQL omits series that carry no sample anywhere in the range.
		if col.Empty() {
			continue
		}

		pts := make([]promql.FPoint, 0, col.Count())
		for i, t := range ec.Steps {
			if col.IsSet(i) {
				pts = append(pts, promql.FPoint{T: t, F: col.V[i]})
			}
		}

		out = append(out, promql.Series{
			Metric: schema.At(col.Ref),
			Floats: pts,
		})
	}

	sort.Sort(out)

	return out, nil
}
