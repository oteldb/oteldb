package traceql

// ExtractTraceDurationMatchers returns necessary duration conjuncts from the initial span filter.
// Boolean wrappers, unions and later pipeline stages remain residual predicates.
func ExtractTraceDurationMatchers(expr Expr) []SpanMatcher {
	pipeline, ok := expr.(*SpansetPipeline)
	if !ok || len(pipeline.Pipeline) == 0 {
		return nil
	}
	filter, ok := pipeline.Pipeline[0].(*SpansetFilter)
	if !ok {
		return nil
	}
	var out []SpanMatcher
	var walk func(FieldExpr)
	walk = func(expr FieldExpr) {
		binary, ok := expr.(*BinaryFieldExpr)
		if !ok {
			return
		}
		if binary.Op == OpAnd {
			walk(binary.Left)
			walk(binary.Right)
			return
		}
		switch binary.Op {
		case OpEq, OpNotEq, OpGt, OpGte, OpLt, OpLte:
		default:
			return
		}
		op := binary.Op
		attribute, attrOK := binary.Left.(*Attribute)
		value, valueOK := binary.Right.(*Static)
		if !attrOK || !valueOK {
			attribute, attrOK = binary.Right.(*Attribute)
			value, valueOK = binary.Left.(*Static)
			op = flipOp(op)
		}
		if attrOK && valueOK && attribute.Prop == TraceDuration && !attribute.Parent {
			out = append(out, SpanMatcher{Attribute: *attribute, Op: op, Static: *value})
		}
	}
	walk(filter.Expr)
	return out
}
