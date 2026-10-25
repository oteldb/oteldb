package traceqlengine

import "github.com/oteldb/oteldb/internal/traceql"

// referencedAttributes collects span/resource/instrumentation/event/link
// attribute names referenced by filter, aggregate, group and select
// expressions in expr.
//
// Tempo's search API only ever echoes attributes bound by the query itself
// (filter/select conditions), not the whole span/resource attribute set.
// This is used to match that behavior: propagating every attribute
// regardless of relevance can blow up response size (e.g. large payload
// attributes) and produce a different, unstable set of columns per row,
// which some Tempo API consumers (e.g. Grafana's TraceQL search table) do
// not handle well.
func referencedAttributes(expr traceql.Expr) map[string]struct{} {
	set := make(map[string]struct{})
	collectExprAttrs(expr, func(attr traceql.Attribute) {
		if attr.Prop == traceql.SpanAttribute {
			set[attr.Name] = struct{}{}
		}
	})
	return set
}

func referencedFields(expr traceql.Expr) []traceql.Attribute {
	fields := make([]traceql.Attribute, 0)
	seen := map[traceql.Attribute]struct{}{}
	collectExprAttrs(expr, func(attr traceql.Attribute) {
		if _, ok := seen[attr]; !ok {
			seen[attr] = struct{}{}
			fields = append(fields, attr)
		}
	})
	return fields
}

func collectExprAttrs(expr traceql.Expr, visit func(traceql.Attribute)) {
	switch expr := expr.(type) {
	case *traceql.BinaryExpr:
		collectExprAttrs(expr.Left, visit)
		collectExprAttrs(expr.Right, visit)
	case *traceql.SpansetPipeline:
		for _, stage := range expr.Pipeline {
			collectStageAttrs(stage, visit)
		}
	}
}

func collectStageAttrs(stage traceql.PipelineStage, visit func(traceql.Attribute)) {
	switch stage := stage.(type) {
	case *traceql.BinarySpansetExpr:
		collectStageAttrs(stage.Left, visit)
		collectStageAttrs(stage.Right, visit)
	case *traceql.SpansetFilter:
		collectFieldExprAttrs(stage.Expr, visit)
	case *traceql.ScalarFilter:
		collectScalarExprAttrs(stage.Left, visit)
		collectScalarExprAttrs(stage.Right, visit)
	case *traceql.GroupOperation:
		collectFieldExprAttrs(stage.By, visit)
	case *traceql.SelectOperation:
		for _, arg := range stage.Args {
			collectFieldExprAttrs(arg, visit)
		}
	}
}

func collectFieldExprAttrs(expr traceql.FieldExpr, visit func(traceql.Attribute)) {
	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		collectFieldExprAttrs(expr.Left, visit)
		collectFieldExprAttrs(expr.Right, visit)
	case *traceql.UnaryFieldExpr:
		collectFieldExprAttrs(expr.Expr, visit)
	case *traceql.Attribute:
		visit(*expr)
	}
}

func collectScalarExprAttrs(expr traceql.ScalarExpr, visit func(traceql.Attribute)) {
	switch expr := expr.(type) {
	case *traceql.BinaryScalarExpr:
		collectScalarExprAttrs(expr.Left, visit)
		collectScalarExprAttrs(expr.Right, visit)
	case *traceql.AggregateScalarExpr:
		if expr.Field != nil {
			collectFieldExprAttrs(expr.Field, visit)
		}
	}
}
