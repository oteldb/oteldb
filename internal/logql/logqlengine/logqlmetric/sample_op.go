package logqlmetric

import (
	"math"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/logql"
)

// SampleOp is a binary operation for two samples.
type SampleOp = func(left, right Sample) (Sample, bool)

func buildSampleBinOp(expr *logql.BinOpExpr) (SampleOp, error) {
	filter := expr.Modifier.ReturnBool
	switch expr.Op {
	case logql.OpAdd:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data += right.Data
			return result, true
		}, nil
	case logql.OpSub:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data -= right.Data
			return result, true
		}, nil
	case logql.OpMul:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data *= right.Data
			return result, true
		}, nil
	case logql.OpDiv:
		return func(left, right Sample) (Sample, bool) {
			result := left
			if right.Data != 0 {
				result.Data /= right.Data
			} else {
				result.Data = math.NaN()
			}
			return result, true
		}, nil
	case logql.OpMod:
		return func(left, right Sample) (Sample, bool) {
			result := left
			if right.Data != 0 {
				result.Data = math.Mod(left.Data, right.Data)
			} else {
				result.Data = math.NaN()
			}
			return result, true
		}, nil
	case logql.OpPow:
		return func(left, right Sample) (Sample, bool) {
			result := left
			result.Data = math.Pow(left.Data, right.Data)
			return result, true
		}, nil
	case logql.OpEq:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data, left.Data == right.Data, filter)
			return result, keep
		}, nil
	case logql.OpNotEq:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data, left.Data != right.Data, filter)
			return result, keep
		}, nil
	case logql.OpGt:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data, left.Data > right.Data, filter)
			return result, keep
		}, nil
	case logql.OpGte:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data, left.Data >= right.Data, filter)
			return result, keep
		}, nil
	case logql.OpLt:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data, left.Data < right.Data, filter)
			return result, keep
		}, nil
	case logql.OpLte:
		return func(left, right Sample) (result Sample, keep bool) {
			result = left
			result.Data, keep = boolOp(left.Data, left.Data <= right.Data, filter)
			return result, keep
		}, nil
	default:
		return nil, errors.Errorf("unexpected operation %q", expr.Op)
	}
}

func boolOp(val float64, cmp, returnBool bool) (float64, bool) {
	// TODO(tdakkota): implement via generic comparators?
	//
	// In case if `bool` modifier is present, boolean expression should
	// return 1 as true and 0 as false.
	if returnBool {
		if cmp {
			return 1., true
		}
		return 0., true
	}

	// If `bool` modifier is NOT present, boolean expression should
	// return value as true and no value as false.
	if cmp {
		return val, true
	}
	return 0., false
}
