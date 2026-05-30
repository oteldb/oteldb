package logqlmetric

import (
	"github.com/go-faster/errors"
	"go.uber.org/multierr"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlabels"
	"github.com/oteldb/oteldb/internal/logql/logqlengine/logqlerrors"
)

// BinOp returns new step iterator performing binary operation between two iterators.
func BinOp(
	left, right StepIterator,
	expr *logql.BinOpExpr,
) (StepIterator, error) {
	if m := expr.Modifier; m.Op != "" || len(m.OpLabels) > 0 || m.Group != "" || len(m.Include) > 0 {
		return nil, &logqlerrors.UnsupportedError{Msg: "binary operation modifiers are unsupported yet"}
	}

	switch expr.Op {
	case logql.OpAnd, logql.OpOr, logql.OpUnless:
		merge, err := buildMergeSamplesOp(expr.Op, nopGrouper, nil)
		if err != nil {
			return nil, errors.Wrap(err, "build binary merge operation")
		}

		return &mergeBinOpIterator{
			left:  left,
			right: right,
			merge: merge,
		}, nil
	default:
		op, err := buildSampleBinOp(expr)
		if err != nil {
			return nil, errors.Wrap(err, "build binary sample operation")
		}

		return &binOpIterator{
			left:  left,
			right: right,
			op:    op,
		}, nil
	}
}

type binOpIterator struct {
	left  StepIterator
	right StepIterator
	op    SampleOp
}

func (i *binOpIterator) Next(r *Step) bool {
	var left, right Step
	if !i.left.Next(&left) || !i.right.Next(&right) {
		return false
	}
	r.Samples = r.Samples[:0]
	r.Timestamp = left.Timestamp

	leftSamples := make(map[logqlabels.GroupingKey]Sample, len(left.Samples))
	for _, s := range left.Samples {
		key := s.Set.Key()
		leftSamples[key] = s
	}

	for _, rsample := range right.Samples {
		key := rsample.Set.Key()
		lsample, ok := leftSamples[key]
		if !ok {
			continue
		}

		result, ok := i.op(lsample, rsample)
		if !ok {
			continue
		}

		r.Samples = append(r.Samples, result)
	}

	return true
}

func (i *binOpIterator) Err() error {
	return multierr.Append(
		i.left.Err(),
		i.right.Err(),
	)
}

func (i *binOpIterator) Close() error {
	return multierr.Append(
		i.left.Close(),
		i.right.Close(),
	)
}

type mergeBinOpIterator struct {
	left  StepIterator
	right StepIterator
	merge func(a, b []Sample) []Sample
}

func (i *mergeBinOpIterator) Next(r *Step) bool {
	var left, right Step
	if !i.left.Next(&left) || !i.right.Next(&right) {
		return false
	}
	r.Timestamp = left.Timestamp
	r.Samples = i.merge(left.Samples, right.Samples)
	return true
}

func (i *mergeBinOpIterator) Err() error {
	return multierr.Append(
		i.left.Err(),
		i.right.Err(),
	)
}

func (i *mergeBinOpIterator) Close() error {
	return multierr.Append(
		i.left.Close(),
		i.right.Close(),
	)
}

func buildMergeSamplesOp(op logql.BinOp, grouper grouperFunc, groupLabels []logql.Label) (func(a, b []Sample) []Sample, error) {
	switch op {
	case logql.OpAnd:
		return func(left, right []Sample) (result []Sample) {
			if len(left) == 0 || len(right) == 0 {
				return nil
			}

			rightSamples := samplesSet(right, grouper, groupLabels)
			for _, s := range left {
				key := grouper(s.Set, groupLabels...).Key()
				if _, ok := rightSamples[key]; ok {
					result = append(result, s)
				}
			}
			return result
		}, nil
	case logql.OpOr:
		return func(left, right []Sample) (result []Sample) {
			switch {
			case len(left) == 0:
				return right
			case len(right) == 0:
				return left
			}

			leftSamples := samplesSet(left, grouper, groupLabels)
			result = append(result, left...)
			for _, s := range right {
				key := grouper(s.Set, groupLabels...).Key()
				if _, ok := leftSamples[key]; !ok {
					result = append(result, s)
				}
			}
			return result
		}, nil
	case logql.OpUnless:
		return func(left, right []Sample) (result []Sample) {
			if len(left) == 0 || len(right) == 0 {
				return left
			}

			rightSamples := samplesSet(right, grouper, groupLabels)
			for _, s := range left {
				key := grouper(s.Set, groupLabels...).Key()
				if _, ok := rightSamples[key]; !ok {
					result = append(result, s)
				}
			}
			return result
		}, nil
	default:
		return nil, errors.Errorf("unexpected binary merge operation %q", op)
	}
}

func samplesSet(samples []Sample, grouper grouperFunc, groupLabels []logql.Label) map[logqlabels.GroupingKey]struct{} {
	r := make(map[logqlabels.GroupingKey]struct{}, len(samples))
	for _, s := range samples {
		key := grouper(s.Set, groupLabels...).Key()
		r[key] = struct{}{}
	}
	return r
}

type literalBinOpIterator struct {
	iter  StepIterator
	op    SampleOp
	value float64
	// left whether on which side literal is
	left bool
}

// LiteralBinOp returns new step iterator performing binary operation with literal.
func LiteralBinOp(
	iter StepIterator,
	expr *logql.BinOpExpr,
	value float64,
	left bool,
) (StepIterator, error) {
	op, err := buildSampleBinOp(expr)
	if err != nil {
		return nil, errors.Wrap(err, "build binary sample operation")
	}
	return &literalBinOpIterator{
		iter:  iter,
		op:    op,
		value: value,
		left:  left,
	}, nil
}

func (i *literalBinOpIterator) Next(r *Step) bool {
	if !i.iter.Next(r) {
		return false
	}

	n := 0
	for _, agg := range r.Samples {
		literal := Sample{
			Data: i.value,
			Set:  agg.Set,
		}

		var left, right Sample
		if i.left {
			left, right = literal, agg
		} else {
			left, right = agg, literal
		}

		if val, ok := i.op(left, right); ok {
			r.Samples[n] = val
			n++
		}
	}
	r.Samples = r.Samples[:n]

	return true
}

func (i *literalBinOpIterator) Err() error {
	return i.iter.Err()
}

func (i *literalBinOpIterator) Close() error {
	return i.iter.Close()
}
