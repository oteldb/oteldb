package traceql

// ExtractMatchers returns [SpanMatcher] list extracted from [Expr].
func ExtractMatchers(expr Expr) (SpansetOp, []SpanMatcher) {
	var e predExtractor
	e.Walk(expr)
	return e.Op, e.Matchers
}

type predExtractor struct {
	Op       SpansetOp // OpAnd, OpOr
	Matchers []SpanMatcher
}

func (p *predExtractor) Walk(expr Expr) {
	p.Op = SpansetOpAnd
	p.walk(expr)
}

func (p *predExtractor) WalkAutocomplete(q autocompleteExpr) {
	p.Op = SpansetOpAnd
	for i := range q.Matchers {
		p.walkField(&q.Matchers[i], false)
	}
}

func (p *predExtractor) walk(expr Expr) {
	switch expr := expr.(type) {
	case *SpansetPipeline:
		for _, stage := range expr.Pipeline {
			p.walkStage(stage)
		}
	case *BinaryExpr:
		p.Op = SpansetOpUnion
		p.walk(expr.Left)
		p.walk(expr.Right)
	case *MetricsAggregation:
		p.walk(expr.Spanset)
	}
}

func (p *predExtractor) walkStage(stage PipelineStage) {
	switch stage := stage.(type) {
	case *BinarySpansetExpr:
		p.Op = SpansetOpUnion
		p.walkStage(stage.Left)
		p.walkStage(stage.Right)
	case *SpansetFilter:
		p.walkField(stage.Expr, false)
	case *ScalarFilter:
		p.walkScalar(stage.Left)
		p.walkScalar(stage.Right)
	case *GroupOperation:
		p.walkField(stage.By, false)
	case *CoalesceOperation:
	case *SelectOperation:
		for _, expr := range stage.Args {
			p.walkField(expr, false)
		}
	}
}

func (p *predExtractor) walkField(expr FieldExpr, negate bool) {
	switch expr := expr.(type) {
	case *BinaryFieldExpr:
		op := expr.Op
		if negate {
			op = negateOp(op)
		}
		if op == OpOr {
			p.Op = SpansetOpUnion
		}

		if expr.Op.IsBoolean() {
			switch left := expr.Left.(type) {
			case *Attribute:
				if right, ok := expr.Right.(*Static); ok {
					p.Matchers = append(p.Matchers, SpanMatcher{
						Attribute: *left,
						Op:        op,
						Static:    *right,
					})
					return
				}
			case *Static:
				if right, ok := expr.Right.(*Attribute); ok {
					p.Matchers = append(p.Matchers, SpanMatcher{
						Attribute: *right,
						Op:        flipOp(op), // Flip operation because attribute on the right side.
						Static:    *left,
					})
					return
				}
			}
		}

		p.walkField(expr.Left, negate)
		p.walkField(expr.Right, negate)
	case *UnaryFieldExpr:
		if expr.Op == OpNot {
			negate = !negate
		}
		p.walkField(expr.Expr, negate)
	case *Attribute:
		p.Matchers = append(p.Matchers, SpanMatcher{
			Attribute: *expr,
		})
	}
}

func (p *predExtractor) walkScalar(expr ScalarExpr) {
	switch expr := expr.(type) {
	case *BinaryScalarExpr:
		p.walkScalar(expr.Left)
		p.walkScalar(expr.Right)
	case *AggregateScalarExpr:
		p.walkField(expr.Field, false)
	}
}

func negateOp(op BinaryOp) BinaryOp {
	switch op {
	case OpAnd:
		return OpOr
	case OpOr:
		return OpAnd
	case OpEq:
		return OpNotEq
	case OpNotEq:
		return OpEq
	case OpRe:
		return OpNotRe
	case OpNotRe:
		return OpRe
	case OpGt:
		return OpLte
	case OpGte:
		return OpLt
	case OpLt:
		return OpGte
	case OpLte:
		return OpGt
	default:
		return op
	}
}

func flipOp(op BinaryOp) BinaryOp {
	switch op {
	case OpGt:
		return OpLt
	case OpGte:
		return OpLte
	case OpLt:
		return OpGt
	case OpLte:
		return OpGte
	default:
		return op
	}
}
