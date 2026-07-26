package traceql

import (
	"math"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

// allSpans is a `{}` pipeline, the most common spanset of a metrics query.
func allSpans() Expr {
	return &SpansetPipeline{
		Pipeline: []PipelineStage{
			&SpansetFilter{Expr: &Static{Type: TypeBool, Data: 1}},
		},
	}
}

var metricsTests = []TestCase{
	{
		`{} | rate()`,
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
		},
		false,
	},
	{
		`{} | rate() by (resource.service.name)`,
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
			By: []Attribute{
				{Name: "service.name", Scope: ScopeResource},
			},
		},
		false,
	},
	{
		`{ status = error } | count_over_time()`,
		&MetricsAggregation{
			Op: MetricsOpCountOverTime,
			Spanset: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{
						Expr: &BinaryFieldExpr{
							Left:  &Attribute{Prop: SpanStatus},
							Op:    OpEq,
							Right: &Static{Type: TypeSpanStatus, Data: uint64(ptrace.StatusCodeError)},
						},
					},
				},
			},
		},
		false,
	},
	{
		`{} | min_over_time(duration)`,
		&MetricsAggregation{
			Op:      MetricsOpMinOverTime,
			Spanset: allSpans(),
			Field:   new(Attribute{Prop: SpanDuration}),
		},
		false,
	},
	{
		`{} | max_over_time(duration) by (name)`,
		&MetricsAggregation{
			Op:      MetricsOpMaxOverTime,
			Spanset: allSpans(),
			Field:   new(Attribute{Prop: SpanDuration}),
			By:      []Attribute{{Prop: SpanName}},
		},
		false,
	},
	{
		`{} | sum_over_time(span.http.request.size) by (name, resource.service.name)`,
		&MetricsAggregation{
			Op:      MetricsOpSumOverTime,
			Spanset: allSpans(),
			Field:   new(Attribute{Name: "http.request.size", Scope: ScopeSpan}),
			By: []Attribute{
				{Prop: SpanName},
				{Name: "service.name", Scope: ScopeResource},
			},
		},
		false,
	},
	{
		`{} | avg_over_time(.latency)`,
		&MetricsAggregation{
			Op:      MetricsOpAvgOverTime,
			Spanset: allSpans(),
			Field:   new(Attribute{Name: "latency"}),
		},
		false,
	},
	{
		`{} | quantile_over_time(duration, 0.9, 0.99) by (name)`,
		&MetricsAggregation{
			Op:         MetricsOpQuantileOverTime,
			Spanset:    allSpans(),
			Field:      new(Attribute{Prop: SpanDuration}),
			Parameters: []float64{0.9, 0.99},
			By:         []Attribute{{Prop: SpanName}},
		},
		false,
	},
	{
		// Integer quantiles are casted to float.
		`{} | quantile_over_time(duration, 0, 1)`,
		&MetricsAggregation{
			Op:         MetricsOpQuantileOverTime,
			Spanset:    allSpans(),
			Field:      new(Attribute{Prop: SpanDuration}),
			Parameters: []float64{0, 1},
		},
		false,
	},
	{
		`{} | histogram_over_time(duration)`,
		&MetricsAggregation{
			Op:      MetricsOpHistogramOverTime,
			Spanset: allSpans(),
			Field:   new(Attribute{Prop: SpanDuration}),
		},
		false,
	},
	{
		// Aggregation applies to the entire pipeline.
		`{ .a } | by(name) | rate()`,
		&MetricsAggregation{
			Op: MetricsOpRate,
			Spanset: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{Expr: &Attribute{Name: "a"}},
					&GroupOperation{By: &Attribute{Prop: SpanName}},
				},
			},
		},
		false,
	},
	{
		`{ .a } && { .b } | count_over_time()`,
		&MetricsAggregation{
			Op: MetricsOpCountOverTime,
			Spanset: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&BinarySpansetExpr{
						Left:  &SpansetFilter{Expr: &Attribute{Name: "a"}},
						Op:    SpansetOpAnd,
						Right: &SpansetFilter{Expr: &Attribute{Name: "b"}},
					},
				},
			},
		},
		false,
	},

	// Second stage operations.
	{
		`{} | rate() | topk(10)`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
			Stages: []MetricsStage{
				&TopKOperation{Op: MetricsStageOpTopK, Limit: 10},
			},
		},
		false,
	},
	{
		`{} | rate() by (name) | bottomk(5)`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
				By:      []Attribute{{Prop: SpanName}},
			},
			Stages: []MetricsStage{
				&TopKOperation{Op: MetricsStageOpBottomK, Limit: 5},
			},
		},
		false,
	},
	{
		// A filter is not preceded by a pipe.
		`{} | count_over_time() > 100`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpCountOverTime,
				Spanset: allSpans(),
			},
			Stages: []MetricsStage{
				&MetricsFilter{Op: OpGt, Value: &Static{Type: TypeInt, Data: 100}},
			},
		},
		false,
	},
	{
		`{} | quantile_over_time(duration, 0.9) >= 100ms`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:         MetricsOpQuantileOverTime,
				Spanset:    allSpans(),
				Field:      new(Attribute{Prop: SpanDuration}),
				Parameters: []float64{0.9},
			},
			Stages: []MetricsStage{
				&MetricsFilter{
					Op:    OpGte,
					Value: &Static{Type: TypeDuration, Data: uint64(100 * time.Millisecond)},
				},
			},
		},
		false,
	},
	{
		`{} | rate() != 0.5`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
			Stages: []MetricsStage{
				&MetricsFilter{Op: OpNotEq, Value: &Static{Type: TypeNumber, Data: math.Float64bits(0.5)}},
			},
		},
		false,
	},
	{
		// Stages chain in the order they are written.
		`{} | rate() > 10 | topk(3) < 100 | bottomk(2)`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
			Stages: []MetricsStage{
				&MetricsFilter{Op: OpGt, Value: &Static{Type: TypeInt, Data: 10}},
				&TopKOperation{Op: MetricsStageOpTopK, Limit: 3},
				&MetricsFilter{Op: OpLt, Value: &Static{Type: TypeInt, Data: 100}},
				&TopKOperation{Op: MetricsStageOpBottomK, Limit: 2},
			},
		},
		false,
	},

	// compare().
	{
		`{ .a } | compare({ status = error })`,
		&CompareOperation{
			Spanset: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{Expr: &Attribute{Name: "a"}},
				},
			},
			Filter: &SpansetFilter{
				Expr: &BinaryFieldExpr{
					Left:  &Attribute{Prop: SpanStatus},
					Op:    OpEq,
					Right: &Static{Type: TypeSpanStatus, Data: uint64(ptrace.StatusCodeError)},
				},
			},
			TopN: DefaultCompareTopN,
		},
		false,
	},
	{
		`{} | compare({}, 25)`,
		&CompareOperation{
			Spanset: allSpans(),
			Filter:  &SpansetFilter{Expr: &Static{Type: TypeBool, Data: 1}},
			TopN:    25,
		},
		false,
	},
	{
		`{} | compare({}, 25, 1000, 2000)`,
		&CompareOperation{
			Spanset: allSpans(),
			Filter:  &SpansetFilter{Expr: &Static{Type: TypeBool, Data: 1}},
			TopN:    25,
			Start:   1000,
			End:     2000,
		},
		false,
	},
	// compare() takes 0, 1 or 3 arguments.
	{`{} | compare()`, nil, true},
	{`{} | compare({}, 10, 1000)`, nil, true},
	{`{} | compare({}, 10, 1000, 2000, 3000)`, nil, true},
	// TopN must be within [1, MaxCompareTopN].
	{`{} | compare({}, 0)`, nil, true},
	{`{} | compare({}, -1)`, nil, true},
	{`{} | compare({}, 1001)`, nil, true},
	// A value out of int range must not wrap into a valid one.
	{`{} | compare({}, 2147483648)`, nil, true},
	{`{} | compare({}, 4294967297)`, nil, true},
	// Selection window must be a valid range.
	{`{} | compare({}, 10, 2000, 1000)`, nil, true},
	{`{} | compare({}, 10, 0, 2000)`, nil, true},
	{`{} | compare({}, 10, 1000, 0)`, nil, true},
	{`{} | compare({}, 10, -1000, 2000)`, nil, true},
	// compare() supports neither second stage operations nor arithmetic.
	{`{} | compare({}) | topk(10)`, nil, true},
	{`{} | compare({}) > 10`, nil, true},
	{`({} | compare({})) / ({} | rate())`, nil, true},
	{`({} | compare({})) * 2`, nil, true},
	// compare() takes a spanset filter.
	{`{} | compare(.a)`, nil, true},
	{`{} | compare({} | rate())`, nil, true},

	// Metrics arithmetic.
	{
		`({ status = error } | rate()) / ({} | rate())`,
		&MetricsBinaryExpr{
			Left: &MetricsAggregation{
				Op: MetricsOpRate,
				Spanset: &SpansetPipeline{
					Pipeline: []PipelineStage{
						&SpansetFilter{
							Expr: &BinaryFieldExpr{
								Left:  &Attribute{Prop: SpanStatus},
								Op:    OpEq,
								Right: &Static{Type: TypeSpanStatus, Data: uint64(ptrace.StatusCodeError)},
							},
						},
					},
				},
			},
			Op: OpDiv,
			Right: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
		},
		false,
	},
	{
		// A single sub-query is a valid arithmetic expression.
		`({} | rate())`,
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
		},
		false,
	},
	{
		// A constant operand becomes a stage, since it applies to every point.
		`({} | rate()) * 100`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
			Stages: []MetricsStage{
				&MetricsScalarOp{Op: OpMul, Value: 100},
			},
		},
		false,
	},
	{
		`2 - ({} | rate())`,
		&MetricsPipeline{
			Expr: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
			Stages: []MetricsStage{
				&MetricsScalarOp{Op: OpSub, Value: 2, ScalarLeft: true},
			},
		},
		false,
	},
	{
		// Multiplication binds tighter than addition.
		`({} | rate()) + ({} | rate()) * 2`,
		&MetricsBinaryExpr{
			Left: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
			Op: OpAdd,
			Right: &MetricsPipeline{
				Expr: &MetricsAggregation{
					Op:      MetricsOpRate,
					Spanset: allSpans(),
				},
				Stages: []MetricsStage{
					&MetricsScalarOp{Op: OpMul, Value: 2},
				},
			},
		},
		false,
	},
	{
		// Parentheses override precedence.
		`(({} | rate()) + ({} | rate())) / ({} | rate())`,
		&MetricsBinaryExpr{
			Left: &MetricsBinaryExpr{
				Left: &MetricsAggregation{
					Op:      MetricsOpRate,
					Spanset: allSpans(),
				},
				Op: OpAdd,
				Right: &MetricsAggregation{
					Op:      MetricsOpRate,
					Spanset: allSpans(),
				},
			},
			Op: OpDiv,
			Right: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
		},
		false,
	},
	{
		// Second stage operations apply to the arithmetic result.
		`({} | rate()) / ({} | rate()) | topk(10)`,
		&MetricsPipeline{
			Expr: &MetricsBinaryExpr{
				Left: &MetricsAggregation{
					Op:      MetricsOpRate,
					Spanset: allSpans(),
				},
				Op: OpDiv,
				Right: &MetricsAggregation{
					Op:      MetricsOpRate,
					Spanset: allSpans(),
				},
			},
			Stages: []MetricsStage{
				&TopKOperation{Op: MetricsStageOpTopK, Limit: 10},
			},
		},
		false,
	},
	{
		// Second stage operations may also be inside a sub-query.
		`({} | rate() | topk(10)) - ({} | rate())`,
		&MetricsBinaryExpr{
			Left: &MetricsPipeline{
				Expr: &MetricsAggregation{
					Op:      MetricsOpRate,
					Spanset: allSpans(),
				},
				Stages: []MetricsStage{
					&TopKOperation{Op: MetricsStageOpTopK, Limit: 10},
				},
			},
			Op: OpSub,
			Right: &MetricsAggregation{
				Op:      MetricsOpRate,
				Spanset: allSpans(),
			},
		},
		false,
	},
	{
		// A parenthesized spanset expression is not a sub-query.
		`({ .a } && { .b }) | rate()`,
		&MetricsAggregation{
			Op: MetricsOpRate,
			Spanset: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&BinarySpansetExpr{
						Left:  &SpansetFilter{Expr: &Attribute{Name: "a"}},
						Op:    SpansetOpAnd,
						Right: &SpansetFilter{Expr: &Attribute{Name: "b"}},
					},
				},
			},
		},
		false,
	},
	{
		`({ .a }) | rate()`,
		&MetricsAggregation{
			Op: MetricsOpRate,
			Spanset: &SpansetPipeline{
				Pipeline: []PipelineStage{
					&SpansetFilter{Expr: &Attribute{Name: "a"}},
				},
			},
		},
		false,
	},
	{
		// A parenthesized query without a metrics function stays a spanset query.
		`({ .a }) && { .b }`,
		&SpansetPipeline{
			Pipeline: []PipelineStage{
				&BinarySpansetExpr{
					Left:  &SpansetFilter{Expr: &Attribute{Name: "a"}},
					Op:    SpansetOpAnd,
					Right: &SpansetFilter{Expr: &Attribute{Name: "b"}},
				},
			},
		},
		false,
	},

	// Each sub-query must be parenthesized.
	{`{} | rate() + {} | rate()`, nil, true},
	{`({} | rate()) + {} | rate()`, nil, true},
	// A duration cannot be used as a scalar operand.
	{`({} | rate()) * 10s`, nil, true},
	// Constant arithmetic is not folded.
	{`({} | rate()) * (2 * 3)`, nil, true},
	// Only +, -, * and / are supported.
	{`({} | rate()) % ({} | rate())`, nil, true},
	{`({} | rate()) ^ 2`, nil, true},
	// Malformed arithmetic.
	{`({} | rate()) /`, nil, true},
	{`({} | rate()) / ()`, nil, true},
	{`({} | rate()`, nil, true},
	{`({} | rate()) / ({} | rate()`, nil, true},
	{`2 * 2`, nil, true},

	// Operations taking no argument.
	{`{} | rate(duration)`, nil, true},
	{`{} | count_over_time(duration)`, nil, true},
	{`{} | rate(duration, 0.9)`, nil, true},
	// Operations requiring an argument.
	{`{} | min_over_time()`, nil, true},
	{`{} | avg_over_time()`, nil, true},
	{`{} | histogram_over_time()`, nil, true},
	// Only quantile_over_time takes parameters.
	{`{} | sum_over_time(duration, 0.9)`, nil, true},
	{`{} | quantile_over_time(duration)`, nil, true},
	{`{} | quantile_over_time(duration, 1.5)`, nil, true},
	{`{} | quantile_over_time(duration, -1)`, nil, true},
	// Malformed queries.
	{`| rate()`, nil, true},
	{`rate()`, nil, true},
	{`{} | rate`, nil, true},
	{`{} | rate(`, nil, true},
	{`{} | rate() by ()`, nil, true},
	{`{} | rate() by (name`, nil, true},
	{`{} | rate() by (name,)`, nil, true},
	{`{} | rate() | rate()`, nil, true},
	{`{} | rate() by (1)`, nil, true},
	// Second stage without an aggregation.
	{`{} | topk(5)`, nil, true},
	{`topk(5)`, nil, true},
	// Malformed second stage.
	{`{} | rate() | topk()`, nil, true},
	{`{} | rate() | topk(0)`, nil, true},
	{`{} | rate() | topk(-1)`, nil, true},
	{`{} | rate() | topk(1.5)`, nil, true},
	{`{} | rate() | topk(10`, nil, true},
	{`{} | rate() | topk(10) topk(5)`, nil, true},
	{`{} | rate() | count_over_time()`, nil, true},
	{`{} | rate() >`, nil, true},
	{`{} | rate() =~ 10`, nil, true},
	{`{} | rate() > 10 > `, nil, true},
}

func TestParseMetrics(t *testing.T) {
	runParseTests(t, metricsTests)
}
