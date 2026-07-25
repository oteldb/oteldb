package traceql

// IsExactSpansetFilter reports whether expr is a bare spanset filter whose predicate is a
// conjunction of comparisons between one attribute and one literal — the one shape for which
// [ExtractMatchers] is lossless.
//
// [ExtractMatchers] flattens an expression to a matcher list, which a storage querier cannot tell
// apart from a differently-shaped query: `{a} | count() > 2` and a structural `{a} >> {b}` both
// reduce to matchers a plain conjunction could also produce. So a querier may only assume "a trace
// holding a span that satisfies every matcher is a result" when this reports true.
//
// It fails closed: anything it does not recognize — a structural operator, a union, a pipeline
// stage (`by`, `select`, `coalesce`, a scalar filter), a negation, an `||`, an arithmetic operand,
// a bare attribute reference, an empty `{}` — is not exact.
func IsExactSpansetFilter(expr Expr) bool {
	pipeline, ok := expr.(*SpansetPipeline)
	if !ok || len(pipeline.Pipeline) != 1 {
		return false
	}
	filter, ok := pipeline.Pipeline[0].(*SpansetFilter)
	if !ok {
		return false
	}
	return isExactFieldExpr(filter.Expr)
}

// isExactFieldExpr reports whether expr is an AND-tree of attribute-to-literal comparisons, i.e.
// whether [predExtractor.walkField] emits exactly one matcher per comparison in it and nothing else.
func isExactFieldExpr(expr FieldExpr) bool {
	binary, ok := expr.(*BinaryFieldExpr)
	if !ok {
		// A bare attribute, a literal, a negation or an arithmetic expression.
		return false
	}
	if binary.Op == OpAnd {
		return isExactFieldExpr(binary.Left) && isExactFieldExpr(binary.Right)
	}
	// OpOr is boolean too, but a union of matchers is not a conjunction.
	if !binary.Op.IsBoolean() || binary.Op == OpOr {
		return false
	}

	// The two operand orders walkField turns into a matcher; anything else it walks into.
	switch left := binary.Left.(type) {
	case *Attribute:
		_, ok := binary.Right.(*Static)
		return ok && !left.Parent
	case *Static:
		right, ok := binary.Right.(*Attribute)
		return ok && !right.Parent
	default:
		return false
	}
}
