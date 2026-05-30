package traceqlengine

import (
	"github.com/oteldb/oteldb/internal/traceql"
)

// ReduceExpr evaluates constant expressions and simplifies expression.
func ReduceExpr(expr traceql.TypedExpr) traceql.TypedExpr {
	// FIXME(tdakkota): probably, it's better to create a new expression.
	switch expr := expr.(type) {
	case traceql.FieldExpr:
		return reduceFieldExpr(expr)
	case traceql.ScalarExpr:
		return reduceScalarExpr(expr)
	default:
		return expr
	}
}

func reduceFieldExpr(expr traceql.FieldExpr) traceql.FieldExpr {
	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		expr.Left = reduceFieldExpr(expr.Left)
		expr.Right = reduceFieldExpr(expr.Right)

		left, ok := expr.Left.(*traceql.Static)
		if !ok {
			return expr
		}

		right, ok := expr.Right.(*traceql.Static)
		if !ok {
			return expr
		}

		eval, err := buildBinaryOp(expr.Op, right)
		if err != nil {
			return expr
		}

		result := eval(*left, *right)
		return &result
	case *traceql.UnaryFieldExpr:
		expr.Expr = reduceFieldExpr(expr.Expr)

		sub, ok := expr.Expr.(*traceql.Static)
		if !ok {
			return expr
		}

		switch expr.Op {
		case traceql.OpNot:
			if sub.Type != traceql.TypeBool {
				return expr
			}

			orig := sub.AsBool()
			sub.SetBool(!orig)
			return sub
		case traceql.OpNeg:
			switch sub.Type {
			case traceql.TypeInt:
				orig := sub.AsInt()
				sub.SetInt(-orig)
			case traceql.TypeNumber:
				orig := sub.AsNumber()
				sub.SetNumber(-orig)
			case traceql.TypeDuration:
				orig := sub.AsDuration()
				sub.SetDuration(-orig)
			default:
				return expr
			}
			return sub
		default:
			return expr
		}
	default:
		return expr
	}
}

func reduceScalarExpr(expr traceql.ScalarExpr) traceql.ScalarExpr {
	switch expr := expr.(type) {
	case *traceql.BinaryScalarExpr:
		expr.Left = reduceScalarExpr(expr.Left)
		expr.Right = reduceScalarExpr(expr.Right)

		left, ok := expr.Left.(*traceql.Static)
		if !ok {
			return expr
		}

		right, ok := expr.Right.(*traceql.Static)
		if !ok {
			return expr
		}

		eval, err := buildBinaryOp(expr.Op, right)
		if err != nil {
			return expr
		}

		result := eval(*left, *right)
		return &result
	case *traceql.AggregateScalarExpr:
		if f := expr.Field; f != nil {
			expr.Field = reduceFieldExpr(f)
		}
		return expr
	default:
		return expr
	}
}
