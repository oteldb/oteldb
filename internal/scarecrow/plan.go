package scarecrow

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

// ErrUnsupported reports a PromQL construct this engine cannot yet plan. It is returned rather
// than approximated: a wrong answer is worse than a refused one, and the compliance harness
// counts refusals as failures either way.
var ErrUnsupported = errors.New("scarecrow: unsupported expression")

// unsupportedf returns an [ErrUnsupported] annotated with what was rejected.
func unsupportedf(format string, args ...any) error {
	return errors.Wrapf(ErrUnsupported, format, args...)
}

// planner lowers a parsed expression into an operator tree for one chunk.
type planner struct {
	scanner Scanner
	ec      *EvalContext
	// noStepSubqueryInterval is the inner step a subquery written without one (`[5m:]`) uses.
	noStepSubqueryInterval time.Duration
}

// plan builds the physical operator tree for expr and resolves every schema bottom-up, so all
// series identity is frozen before any Next call.
func (p *planner) plan(ctx context.Context, expr parser.Expr) (Operator, error) {
	op, err := p.build(expr)
	if err != nil {
		return nil, err
	}

	// Resolve schemas eagerly, depth-first. Doing it here rather than lazily during execution
	// is what makes series refs stable and order-independent (see [Schema]), and it is also
	// where prefetching starts (see [concurrent.Schema]).
	if err := resolveSchemas(ctx, op); err != nil {
		// Some operators may already have started producing, so unwind rather than leak them.
		_ = op.Close()

		return nil, err
	}

	return op, nil
}

func (p *planner) build(expr parser.Expr) (Operator, error) {
	switch e := expr.(type) {
	case *parser.NumberLiteral:
		return newNumberLiteral(e.Val, p.ec), nil

	case *parser.ParenExpr:
		return p.build(e.Expr)

	case *parser.StepInvariantExpr:
		return p.build(e.Expr)

	case *parser.VectorSelector:
		return p.buildVectorSelector(e)

	case *parser.Call:
		return p.buildCall(e)

	case *parser.AggregateExpr:
		return p.buildAggregate(e)

	case *parser.BinaryExpr:
		return p.buildBinary(e)

	case *parser.UnaryExpr:
		return p.buildUnary(e)

	default:
		return nil, unsupportedf("%T", expr)
	}
}

// aggregateOps are the aggregations that fold incrementally into a per-group row.
var aggregateOps = map[parser.ItemType]bool{
	parser.SUM:    true,
	parser.MIN:    true,
	parser.MAX:    true,
	parser.AVG:    true,
	parser.COUNT:  true,
	parser.GROUP:  true,
	parser.STDDEV: true,
	parser.STDVAR: true,
}

// fullSetOps need the full per-step series set rather than an incremental fold — an exact
// quantile, a selection that only value comparison across the whole set can answer, or (for
// count_values) output labels synthesized from observed values rather than selected from the
// input's own (§4.4).
var fullSetOps = map[parser.ItemType]bool{
	parser.QUANTILE:     true,
	parser.TOPK:         true,
	parser.BOTTOMK:      true,
	parser.LIMITK:       true,
	parser.LIMIT_RATIO:  true,
	parser.COUNT_VALUES: true,
}

func (p *planner) buildAggregate(e *parser.AggregateExpr) (Operator, error) {
	if !aggregateOps[e.Op] && !fullSetOps[e.Op] {
		return nil, unsupportedf("aggregation %s", e.Op)
	}

	if op, ok := p.pushDownCount(e); ok {
		return op, nil
	}

	input, err := p.build(e.Expr)
	if err != nil {
		return nil, err
	}

	if aggregateOps[e.Op] {
		return newAggregate(input, e, p.ec), nil
	}

	if e.Param == nil {
		return nil, unsupportedf("aggregation %s without a parameter", e.Op)
	}

	// count_values' parameter is the output label name, a string literal, not a scalar subtree
	// like every other fullSetOps parameter.
	if e.Op == parser.COUNT_VALUES {
		lit, ok := e.Param.(*parser.StringLiteral)
		if !ok {
			return nil, unsupportedf("count_values with a non-literal label name")
		}

		return newCountValuesAgg(input, lit.Val, e, p.ec)
	}

	param, err := p.build(e.Param)
	if err != nil {
		return nil, err
	}

	if e.Op == parser.QUANTILE {
		return newQuantileAgg(input, param, e, p.ec), nil
	}

	return newLimitAgg(input, param, e, p.ec), nil
}

func (p *planner) buildUnary(e *parser.UnaryExpr) (Operator, error) {
	input, err := p.build(e.Expr)
	if err != nil {
		return nil, err
	}

	if e.Op == parser.ADD {
		return input, nil
	}

	if isScalarExpr(e.Expr) {
		return newScalarBinop(
			newNumberLiteral(-1, p.ec), input, parser.MUL, false, p.ec,
		), nil
	}

	return newNegate(input), nil
}

func (p *planner) buildBinary(e *parser.BinaryExpr) (Operator, error) {
	lhs, err := p.build(e.LHS)
	if err != nil {
		return nil, err
	}

	rhs, err := p.build(e.RHS)
	if err != nil {
		return nil, err
	}

	lScalar, rScalar := isScalarExpr(e.LHS), isScalarExpr(e.RHS)

	switch {
	case lScalar && rScalar:
		return newScalarBinop(lhs, rhs, e.Op, e.ReturnBool, p.ec), nil

	case lScalar:
		return newVectorScalarBinop(rhs, lhs, e.Op, true, e.ReturnBool, p.ec), nil

	case rScalar:
		return newVectorScalarBinop(lhs, rhs, e.Op, false, e.ReturnBool, p.ec), nil
	}

	if e.VectorMatching == nil {
		return nil, unsupportedf("vector-to-vector %s without matching", e.Op)
	}

	// Both sides are independent subtrees that each reach storage, and a vector binop drains one
	// of them before streaming the other. Running them concurrently overlaps that latency, which
	// is the dominant cost of the operation.
	lhs, rhs = newConcurrent(lhs), newConcurrent(rhs)

	switch e.Op {
	case parser.LAND, parser.LOR, parser.LUNLESS:
		return newSetBinop(lhs, rhs, e.Op, e.VectorMatching, p.ec), nil
	}

	return newVectorBinop(lhs, rhs, e.Op, e.VectorMatching, e.ReturnBool, p.ec), nil
}

func (p *planner) buildVectorSelector(e *parser.VectorSelector) (Operator, error) {
	// StartOrEnd has already been resolved into Timestamp by promql.PreprocessExpr.
	return newVectorSelect(p.scanner, e.LabelMatchers, e.OriginalOffset, e.Timestamp, p.ec), nil
}

// buildCall plans a function call. A range-vector function and its matrix selector fuse into a
// single [matrixFold]; instant functions are not yet planned.
func (p *planner) buildCall(e *parser.Call) (Operator, error) {
	if op, ok, err := p.buildInstantCall(e); ok || err != nil {
		return op, err
	}

	fn, ok := rangeFuncs[e.Func.Name]
	if !ok {
		return nil, unsupportedf("function %s", e.Func.Name)
	}

	// quantile_over_time(q, matrix) and predict_linear(matrix, t) are the only range-vector
	// functions with a second, scalar argument, and disagree with each other on which position
	// it's in.
	matrixArg := 0
	paramArg := -1

	switch e.Func.Name {
	case "quantile_over_time":
		matrixArg, paramArg = 1, 0
	case "predict_linear":
		matrixArg, paramArg = 0, 1
	}

	wantArgs := 1
	if paramArg >= 0 {
		wantArgs = 2
	}

	if len(e.Args) != wantArgs {
		return nil, unsupportedf("function %s with %d arguments", e.Func.Name, len(e.Args))
	}

	var param Operator
	if paramArg >= 0 {
		var err error
		if param, err = p.build(e.Args[paramArg]); err != nil {
			return nil, err
		}
	}

	switch arg := e.Args[matrixArg].(type) {
	case *parser.MatrixSelector:
		return p.buildSelectorFold(e.Func.Name, fn, arg, param)

	case *parser.SubqueryExpr:
		return p.buildSubqueryFold(e.Func.Name, fn, arg, param)

	default:
		return nil, unsupportedf("function %s over %T", e.Func.Name, e.Args[matrixArg])
	}
}

func (p *planner) buildSelectorFold(
	fnName string, fn rangeFunc, ms *parser.MatrixSelector, param Operator,
) (Operator, error) {
	vs, ok := ms.VectorSelector.(*parser.VectorSelector)
	if !ok {
		return nil, unsupportedf("matrix selector over %T", ms.VectorSelector)
	}

	if vs.Anchored || vs.Smoothed {
		return nil, unsupportedf("extended range selector")
	}

	// The pushdown-eligible functions never carry a param, so this is unaffected by it.
	if param == nil {
		if op, ok := p.pushDownOverTime(fnName, vs, ms); ok {
			return op, nil
		}
	}

	refs := refTimes(p.ec.Steps, vs.OriginalOffset, vs.Timestamp)
	src := &scannerSource{
		scanner:  p.scanner,
		matchers: vs.LabelMatchers,
		mint:     refs[0] - ms.Range.Milliseconds(),
		maxt:     refs[len(refs)-1],
	}

	return newMatrixFold(
		src, matchersString(vs.LabelMatchers), fnName, fn,
		ms.Range, vs.OriginalOffset, vs.Timestamp, param, p.ec,
	), nil
}

// buildSubqueryFold plans a range function over a subquery. The inner expression is planned
// against its own step grid and its results become the fold's samples.
func (p *planner) buildSubqueryFold(
	fnName string, fn rangeFunc, sq *parser.SubqueryExpr, param Operator,
) (Operator, error) {
	inner, innerEC, err := p.buildSubquery(sq)
	if err != nil {
		return nil, err
	}

	src := &subquerySource{inner: inner, ec: innerEC}

	return newMatrixFold(
		src, sq.Expr.String(), fnName, fn, sq.Range, sq.OriginalOffset, sq.Timestamp, param, p.ec,
	), nil
}

// buildSubquery plans a subquery's inner expression on its own grid, returning the operator and
// the grid it was planned against.
func (p *planner) buildSubquery(sq *parser.SubqueryExpr) (Operator, *EvalContext, error) {
	var (
		refs   = refTimes(p.ec.Steps, sq.OriginalOffset, sq.Timestamp)
		stepMs = subqueryStep(sq, p.noStepSubqueryInterval)
		// grid may be empty — e.g. a subquery whose step exceeds its range can miss every
		// aligned tick inside the window at some outer steps. That is a legitimate "no sample"
		// result, not an error: the fold below runs over zero inner steps and emits nothing.
		grid = subqueryGrid(refs, sq.Range.Milliseconds(), stepMs)
	)

	innerEC := &EvalContext{
		Steps:         grid,
		Interval:      time.Duration(stepMs) * time.Millisecond,
		LookbackDelta: p.ec.LookbackDelta,
	}

	inner, err := (&planner{
		scanner:                p.scanner,
		ec:                     innerEC,
		noStepSubqueryInterval: p.noStepSubqueryInterval,
	}).build(sq.Expr)
	if err != nil {
		return nil, nil, err
	}

	return inner, innerEC, nil
}

// resolveSchemas walks the tree depth-first, forcing each operator to resolve its schema before
// execution begins.
func resolveSchemas(ctx context.Context, op Operator) error {
	for _, c := range op.Children() {
		if err := resolveSchemas(ctx, c); err != nil {
			return err
		}
	}

	if _, err := op.Schema(ctx); err != nil {
		return errors.Wrapf(err, "resolve schema of %s", op)
	}

	return nil
}
