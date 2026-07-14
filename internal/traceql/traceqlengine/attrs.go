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
	collectExprAttrs(expr, set)
	return set
}

func collectExprAttrs(expr traceql.Expr, set map[string]struct{}) {
	switch expr := expr.(type) {
	case *traceql.BinaryExpr:
		collectExprAttrs(expr.Left, set)
		collectExprAttrs(expr.Right, set)
	case *traceql.SpansetPipeline:
		for _, stage := range expr.Pipeline {
			collectStageAttrs(stage, set)
		}
	}
}

func collectStageAttrs(stage traceql.PipelineStage, set map[string]struct{}) {
	switch stage := stage.(type) {
	case *traceql.BinarySpansetExpr:
		collectStageAttrs(stage.Left, set)
		collectStageAttrs(stage.Right, set)
	case *traceql.SpansetFilter:
		collectFieldExprAttrs(stage.Expr, set)
	case *traceql.ScalarFilter:
		collectScalarExprAttrs(stage.Left, set)
		collectScalarExprAttrs(stage.Right, set)
	case *traceql.GroupOperation:
		collectFieldExprAttrs(stage.By, set)
	case *traceql.SelectOperation:
		for _, arg := range stage.Args {
			collectFieldExprAttrs(arg, set)
		}
	}
}

func collectFieldExprAttrs(expr traceql.FieldExpr, set map[string]struct{}) {
	switch expr := expr.(type) {
	case *traceql.BinaryFieldExpr:
		collectFieldExprAttrs(expr.Left, set)
		collectFieldExprAttrs(expr.Right, set)
	case *traceql.UnaryFieldExpr:
		collectFieldExprAttrs(expr.Expr, set)
	case *traceql.Attribute:
		if expr.Prop == traceql.SpanAttribute {
			set[expr.Name] = struct{}{}
		}
	}
}

func collectScalarExprAttrs(expr traceql.ScalarExpr, set map[string]struct{}) {
	switch expr := expr.(type) {
	case *traceql.BinaryScalarExpr:
		collectScalarExprAttrs(expr.Left, set)
		collectScalarExprAttrs(expr.Right, set)
	case *traceql.AggregateScalarExpr:
		if expr.Field != nil {
			collectFieldExprAttrs(expr.Field, set)
		}
	}
}
