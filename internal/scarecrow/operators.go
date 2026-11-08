package scarecrow

import (
	"context"
	"math"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

// BinaryOp is a numeric operation independent of a query language's AST.
type BinaryOp uint8

const (
	BinaryAdd BinaryOp = iota + 1
	BinarySub
	BinaryMul
	BinaryDiv
	BinaryEqual
	BinaryNotEqual
	BinaryGreater
	BinaryGreaterEqual
	BinaryLess
	BinaryLessEqual
)

func (op BinaryOp) token() (parser.ItemType, error) {
	switch op {
	case BinaryAdd:
		return parser.ADD, nil
	case BinarySub:
		return parser.SUB, nil
	case BinaryMul:
		return parser.MUL, nil
	case BinaryDiv:
		return parser.DIV, nil
	case BinaryEqual:
		return parser.EQLC, nil
	case BinaryNotEqual:
		return parser.NEQ, nil
	case BinaryGreater:
		return parser.GTR, nil
	case BinaryGreaterEqual:
		return parser.GTE, nil
	case BinaryLess:
		return parser.LSS, nil
	case BinaryLessEqual:
		return parser.LTE, nil
	default:
		return 0, errors.Errorf("unknown binary operation %d", op)
	}
}

// BinaryOptions defines language-specific behavior of shared numeric operators.
// The zero value uses PromQL semantics.
type BinaryOptions struct {
	// NaNOnZeroDivision makes division by zero produce NaN instead of infinity.
	NaNOnZeroDivision bool
	// NaNIsAbsent excludes NaN inputs and results, including from != filters.
	NaNIsAbsent bool
	// PreserveMetricNames keeps scalar-operation names and combines vector names.
	PreserveMetricNames bool
	// BroadcastUnlabelled matches an ungrouped vector to every grouped series.
	BroadcastUnlabelled bool
}

func (o BinaryOptions) normalize(out *Column, op parser.ItemType, left, right []float64) {
	for i, value := range out.V {
		if !out.IsSet(i) {
			continue
		}
		if o.NaNIsAbsent && (math.IsNaN(value) || math.IsNaN(left[i]) || math.IsNaN(right[i])) {
			out.Clear(i)
			continue
		}
		if o.NaNOnZeroDivision && op == parser.DIV && right[i] == 0 {
			if o.NaNIsAbsent {
				out.Clear(i)
			} else {
				out.V[i] = math.NaN()
			}
		}
	}
}

// NewScalarBinary applies op to input and a constant. Successful construction
// transfers input ownership to the result; failure leaves it with the caller.
func NewScalarBinary(input Operator, value float64, op BinaryOp, scalarLeft bool, options BinaryOptions, ec *EvalContext) (Operator, error) {
	token, err := op.token()
	if err != nil {
		return nil, err
	}
	if input == nil || ec == nil {
		return nil, errors.New("scalar binary operation requires input and evaluation context")
	}
	out := newVectorScalarBinop(input, newNumberLiteral(value, ec), token, scalarLeft, false, ec)
	out.options = options
	return out, nil
}

// NewVectorBinary combines two vectors by matching labels. The default is
// one-to-one matching excluding __name__. Ownership follows [NewScalarBinary].
func NewVectorBinary(left, right Operator, op BinaryOp, options BinaryOptions, ec *EvalContext) (Operator, error) {
	token, err := op.token()
	if err != nil {
		return nil, err
	}
	if isComparison(token) {
		return nil, errors.New("vector comparisons are not supported by the shared operator builder")
	}
	if left == nil || right == nil || ec == nil {
		return nil, errors.New("vector binary operation requires two inputs and evaluation context")
	}
	out := newVectorBinop(newConcurrent(left), newConcurrent(right), token, &parser.VectorMatching{Card: parser.CardOneToOne}, false, ec)
	out.options = options
	return out, nil
}

// NewTopK selects up to limit highest-valued series at each step.
// Ownership follows [NewScalarBinary]. NaN gaps must be absent in input columns.
func NewTopK(input Operator, limit int, ec *EvalContext) (Operator, error) {
	return newRank(input, limit, parser.TOPK, ec)
}

// NewBottomK selects up to limit lowest-valued series at each step.
// Ownership follows [NewTopK].
func NewBottomK(input Operator, limit int, ec *EvalContext) (Operator, error) {
	return newRank(input, limit, parser.BOTTOMK, ec)
}

func newRank(input Operator, limit int, op parser.ItemType, ec *EvalContext) (Operator, error) {
	if input == nil || ec == nil || limit <= 0 {
		return nil, errors.New("ranking requires input, evaluation context, and a positive limit")
	}
	return newLimitAgg(input, newNumberLiteral(float64(limit), ec), op, nil, false, ec), nil
}

// Prepare resolves schemas before execution. On error the caller must close root.
func Prepare(ctx context.Context, root Operator) error {
	if root == nil {
		return errors.New("missing root operator")
	}
	return resolveSchemas(ctx, root)
}
