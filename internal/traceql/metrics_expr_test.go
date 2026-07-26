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
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
			Stages: []MetricsStage{
				&TopKOperation{Op: MetricsStageOpTopK, Limit: 10},
			},
		},
		false,
	},
	{
		`{} | rate() by (name) | bottomk(5)`,
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
			By:      []Attribute{{Prop: SpanName}},
			Stages: []MetricsStage{
				&TopKOperation{Op: MetricsStageOpBottomK, Limit: 5},
			},
		},
		false,
	},
	{
		// A filter is not preceded by a pipe.
		`{} | count_over_time() > 100`,
		&MetricsAggregation{
			Op:      MetricsOpCountOverTime,
			Spanset: allSpans(),
			Stages: []MetricsStage{
				&MetricsFilter{Op: OpGt, Value: &Static{Type: TypeInt, Data: 100}},
			},
		},
		false,
	},
	{
		`{} | quantile_over_time(duration, 0.9) >= 100ms`,
		&MetricsAggregation{
			Op:         MetricsOpQuantileOverTime,
			Spanset:    allSpans(),
			Field:      new(Attribute{Prop: SpanDuration}),
			Parameters: []float64{0.9},
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
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
			Stages: []MetricsStage{
				&MetricsFilter{Op: OpNotEq, Value: &Static{Type: TypeNumber, Data: math.Float64bits(0.5)}},
			},
		},
		false,
	},
	{
		// Stages chain in the order they are written.
		`{} | rate() > 10 | topk(3) < 100 | bottomk(2)`,
		&MetricsAggregation{
			Op:      MetricsOpRate,
			Spanset: allSpans(),
			Stages: []MetricsStage{
				&MetricsFilter{Op: OpGt, Value: &Static{Type: TypeInt, Data: 10}},
				&TopKOperation{Op: MetricsStageOpTopK, Limit: 3},
				&MetricsFilter{Op: OpLt, Value: &Static{Type: TypeInt, Data: 100}},
				&TopKOperation{Op: MetricsStageOpBottomK, Limit: 2},
			},
		},
		false,
	},

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
