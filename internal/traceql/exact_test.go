package traceql_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
)

func TestIsExactSpansetFilter(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		// The exact shape: a bare selector whose predicate is a conjunction of comparisons.
		{`{name = "foo"}`, true},
		{`{name != "foo"}`, true},
		{`{name =~ "fo.*"}`, true},
		{`{name !~ "fo.*"}`, true},
		{`{duration > 1ms}`, true},
		{`{duration >= 1ms && duration <= 2ms}`, true},
		{`{status = error}`, true},
		{`{kind = server}`, true},
		{`{span.http.route = "/a"}`, true},
		{`{resource.service.name = "a"}`, true},
		{`{.route = "/a"}`, true},
		{`{span.a = 1 && span.b = 2 && span.c = 3}`, true},
		// A literal on the left is the same comparison with the operands flipped.
		{`{"foo" = name}`, true},
		{`{500 <= span.http.response.status_code}`, true},

		// An empty selector has no matchers at all.
		{`{}`, false},
		{`{true}`, false},
		// A bare attribute reference is an existence-ish predicate the extractor also emits for
		// `by(...)`/`select(...)`, so it is never a filter.
		{`{span.a}`, false},
		{`{span.a = 1 && span.b}`, false},

		// Structural operators: the extractor flattens them to a matcher list a conjunction could
		// also produce.
		{`{name = "a"} >> {name = "b"}`, false},
		{`{name = "a"} > {name = "b"}`, false},
		{`{name = "a"} ~ {name = "b"}`, false},
		{`{name = "a"} && {name = "b"}`, false},
		{`{name = "a"} || {name = "b"}`, false},

		// Unions and negations inside one selector.
		{`{name = "a" || name = "b"}`, false},
		{`{name = "a" && (span.b = 1 || span.c = 2)}`, false},
		{`{!(name = "a")}`, false},
		{`{name = "a" && !(span.b = 1)}`, false},

		// Pipeline stages: everything after the selector is invisible to the extractor.
		{`{name = "a"} | count() > 2`, false},
		{`{name = "a"} | avg(duration) > 1ms`, false},
		{`{name = "a"} | by(span.b)`, false},
		{`{name = "a"} | select(span.b)`, false},

		// Attribute-to-attribute and arithmetic operands are not matchers.
		{`{span.a = span.b}`, false},
		{`{duration > 1ms + 2ms}`, false},
		// `parent.`-scoped attributes are not evaluated by the engine at all.
		{`{parent.span.a = 1}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.want, traceql.IsExactSpansetFilter(expr))
		})
	}
}

// TestIsExactSpansetFilterFailsClosed pins the default for an expression the predicate does not
// recognize: a hand-built AST node it never enumerates must not be exact.
func TestIsExactSpansetFilterFailsClosed(t *testing.T) {
	var attr traceql.Attribute
	attr.Name = "a"

	for _, expr := range []traceql.Expr{
		// Not a pipeline.
		&traceql.BinaryExpr{},
		// A pipeline of no stages, or of a stage that is not a filter.
		&traceql.SpansetPipeline{},
		&traceql.SpansetPipeline{Pipeline: []traceql.PipelineStage{&traceql.GroupOperation{By: &attr}}},
		&traceql.SpansetPipeline{Pipeline: []traceql.PipelineStage{&traceql.CoalesceOperation{}}},
		&traceql.SpansetPipeline{Pipeline: []traceql.PipelineStage{&traceql.ScalarFilter{}}},
		&traceql.SpansetPipeline{Pipeline: []traceql.PipelineStage{&traceql.BinarySpansetExpr{}}},
		// A filter over an expression that is not a comparison.
		&traceql.SpansetPipeline{Pipeline: []traceql.PipelineStage{&traceql.SpansetFilter{Expr: &attr}}},
		&traceql.SpansetPipeline{Pipeline: []traceql.PipelineStage{&traceql.SpansetFilter{Expr: nil}}},
	} {
		require.False(t, traceql.IsExactSpansetFilter(expr))
	}
}
