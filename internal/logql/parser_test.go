package logql

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/logql/logqlengine/logqlpattern"
)

func ptrTo[T any](v T) *T {
	return &v
}

type TestCase struct {
	input   string
	want    Expr
	wantErr bool
}

var tests = []TestCase{
	{`{}`, &LogExpr{}, false},
	{`({})`, &ParenExpr{X: &LogExpr{}}, false},
	{
		`{foo="bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		"{foo=`bar`}",
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		`{foo = "bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		`{"foo.bar" = "bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo.bar", OpEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		"{`foo.bar` = `bar`}",
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo.bar", OpEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		`{foo!="bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpNotEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		`{foo != "bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpNotEq, "bar", nil},
				},
			},
		},
		false,
	},
	{
		`{foo =~ "bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
				},
			},
		},
		false,
	},
	{
		`{foo !~ "bar"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpNotRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
				},
			},
		},
		false,
	},
	{
		`{foo !~ "bar", foo2 =~ "amongus"}`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"foo", OpNotRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
					{"foo2", OpRe, "amongus", regexp.MustCompile(`^(?:amongus)$`)},
				},
			},
		},
		false,
	},
	{
		`( {foo = "bar"} )`,
		&ParenExpr{
			X: &LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"foo", OpEq, "bar", nil},
					},
				},
			},
		},
		false,
	},
	{
		"{} |= `foo`",
		&LogExpr{
			Pipeline: []PipelineStage{
				&LineFilter{Op: OpEq, By: LineFilterValue{Value: "foo"}},
			},
		},
		false,
	},
	{
		`{} |> "<_>foo" !> "<_>bar"`,
		&LogExpr{
			Pipeline: []PipelineStage{
				&LineFilter{Op: OpPattern, By: LineFilterValue{Value: "<_>foo"}},
				&LineFilter{Op: OpNotPattern, By: LineFilterValue{Value: "<_>bar"}},
			},
		},
		false,
	},
	{
		`{} |= "foo" or "bar" or ip("baz")`,
		&LogExpr{
			Pipeline: []PipelineStage{
				&LineFilter{
					Op: OpEq,
					By: LineFilterValue{Value: "foo"},
					Or: []LineFilterValue{
						{Value: "bar"},
						{Value: "baz", IP: true},
					},
				},
			},
		},
		false,
	},
	{
		`{instance=~"kafka-1",name="kafka"}
				|= "bad"
				|~ "error"
				!= "good"
				!~ "exception"
				|= ip("127.0.0.1")`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"instance", OpRe, "kafka-1", regexp.MustCompile(`^(?:kafka-1)$`)},
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LineFilter{Op: OpEq, By: LineFilterValue{Value: "bad"}},
				&LineFilter{Op: OpRe, By: LineFilterValue{Value: "error", Re: regexp.MustCompile(`error`)}},
				&LineFilter{Op: OpNotEq, By: LineFilterValue{Value: "good"}},
				&LineFilter{Op: OpNotRe, By: LineFilterValue{Value: "exception", Re: regexp.MustCompile(`exception`)}},
				&LineFilter{Op: OpEq, By: LineFilterValue{Value: "127.0.0.1", IP: true}},
			},
		},
		false,
	},
	{
		`( {instance=~"kafka-1",name="kafka"} |= "bad" )`,
		&ParenExpr{
			X: &LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"instance", OpRe, "kafka-1", regexp.MustCompile(`^(?:kafka-1)$`)},
						{"name", OpEq, "kafka", nil},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: OpEq, By: LineFilterValue{Value: "bad"}},
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				|= "bad"
				| logfmt
				| json
				| regexp "(?P<method>\\w+)"
				| pattern "<ip>"
				| unpack
				| line_format "{{ . }}"
				| decolorize`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LineFilter{Op: OpEq, By: LineFilterValue{Value: "bad"}},
				&LogfmtExpressionParser{},
				&JSONExpressionParser{},
				&RegexpLabelParser{
					Regexp: regexp.MustCompile(`(?P<method>\w+)`),
					Mapping: map[int]Label{
						1: "method",
					},
				},
				&PatternLabelParser{Pattern: logqlpattern.MustParse("<ip>", logqlpattern.ExtractorFlags)},
				&UnpackLabelParser{},
				&LineFormat{Template: "{{ . }}"},
				&DecolorizeExpr{},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				|= "bad"
				| json
				| json foo, bar
				| json foo="10", bar="sus"
				| logfmt foo="10", bar="sus"
			`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LineFilter{Op: OpEq, By: LineFilterValue{Value: "bad"}},
				&JSONExpressionParser{},
				&JSONExpressionParser{
					Labels: []Label{
						"foo",
						"bar",
					},
				},
				&JSONExpressionParser{
					Exprs: []LabelExtractionExpr{
						{"foo", "10"},
						{"bar", "sus"},
					},
				},
				&LogfmtExpressionParser{
					Exprs: []LabelExtractionExpr{
						{"foo", "10"},
						{"bar", "sus"},
					},
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				| logfmt --keep-empty
				| logfmt --strict
				| logfmt --strict --keep-empty foo="10"
			`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LogfmtExpressionParser{
					Flags: LogfmtFlagKeepEmpty,
				},
				&LogfmtExpressionParser{
					Flags: LogfmtFlagStrict,
				},
				&LogfmtExpressionParser{
					Exprs: []LabelExtractionExpr{
						{"foo", "10"},
					},
					Flags: LogfmtFlagStrict | LogfmtFlagKeepEmpty,
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				| drop foo
				| drop foo, foo2
				| keep foo=~"bar"
				| keep foo=~"bar", foo2=~"baz"
				| drop foo,foo2=~"bar",foo3
				| keep foo!~"bar",foo2,foo3
			`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&DropLabelsExpr{
					Labels: []Label{"foo"},
				},
				&DropLabelsExpr{
					Labels: []Label{"foo", "foo2"},
				},
				&KeepLabelsExpr{
					Matchers: []LabelMatcher{
						{"foo", OpRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
					},
				},
				&KeepLabelsExpr{
					Matchers: []LabelMatcher{
						{"foo", OpRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
						{"foo2", OpRe, "baz", regexp.MustCompile(`^(?:baz)$`)},
					},
				},
				&DropLabelsExpr{
					Labels: []Label{"foo", "foo3"},
					Matchers: []LabelMatcher{
						{"foo2", OpRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
					},
				},
				&KeepLabelsExpr{
					Labels: []Label{"foo2", "foo3"},
					Matchers: []LabelMatcher{
						{"foo", OpNotRe, "bar", regexp.MustCompile(`^(?:bar)$`)},
					},
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				| label_format foo=bar
				| label_format bar="bar"
				| label_format foo=foo,bar="bar"
			`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFormatExpr{
					Labels: []RenameLabel{
						{To: "foo", From: "bar"},
					},
				},
				&LabelFormatExpr{
					Values: []LabelTemplate{
						{"bar", "bar"},
					},
				},
				&LabelFormatExpr{
					Labels: []RenameLabel{
						{"foo", "foo"},
					},
					Values: []LabelTemplate{
						{"bar", "bar"},
					},
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				| distinct foo
				| distinct foo,bar
				| distinct foo,bar,baz
			`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&DistinctFilter{
					Labels: []Label{"foo"},
				},
				&DistinctFilter{
					Labels: []Label{"foo", "bar"},
				},

				&DistinctFilter{
					Labels: []Label{"foo", "bar", "baz"},
				},
			},
		},
		false,
	},
	{
		`{instance=~"kafka-1",name="kafka"}
				| status == 200
				| (service = "sus1")
				| service = "sus2", request != "GET"
				| service = "sus3" request != "POST"
				| ( (service = "sus4") and request != "PUT" )`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"instance", OpRe, "kafka-1", regexp.MustCompile(`^(?:kafka-1)$`)},
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFilter{
					Pred: &NumberFilter{"status", OpEq, 200},
				},
				&LabelFilter{
					Pred: &LabelPredicateParen{
						X: &LabelMatcher{"service", OpEq, "sus1", nil},
					},
				},
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left:  &LabelMatcher{"service", OpEq, "sus2", nil},
						Op:    OpAnd,
						Right: &LabelMatcher{"request", OpNotEq, "GET", nil},
					},
				},
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left:  &LabelMatcher{"service", OpEq, "sus3", nil},
						Op:    OpAnd,
						Right: &LabelMatcher{"request", OpNotEq, "POST", nil},
					},
				},
				&LabelFilter{
					Pred: &LabelPredicateParen{
						X: &LabelPredicateBinOp{
							Left: &LabelPredicateParen{
								X: &LabelMatcher{"service", OpEq, "sus4", nil},
							},
							Op:    OpAnd,
							Right: &LabelMatcher{"request", OpNotEq, "PUT", nil},
						},
					},
				},
			},
		},
		false,
	},
	{
		`{instance=~"kafka-1",name="kafka"}
				| duration >= 20ms or size == 20kb and method!~"2.."
				| ip = ip("127.0.0.1")
				| ip != ip("127.0.0.255")`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"instance", OpRe, "kafka-1", regexp.MustCompile(`^(?:kafka-1)$`)},
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left: &LabelPredicateBinOp{
							Left:  &DurationFilter{"duration", OpGte, 20 * time.Millisecond},
							Op:    OpOr,
							Right: &BytesFilter{"size", OpEq, 20 * 1000}, // 20kb
						},
						Op:    OpAnd,
						Right: &LabelMatcher{"method", OpNotRe, "2..", regexp.MustCompile(`^(?:2..)$`)},
					},
				},
				&LabelFilter{
					Pred: &IPFilter{"ip", OpEq, "127.0.0.1"},
				},
				&LabelFilter{
					Pred: &IPFilter{"ip", OpNotEq, "127.0.0.255"},
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				| duration >= 20ms or size < 20mb or size <= 20MiB`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left: &LabelPredicateBinOp{
							Left:  &DurationFilter{"duration", OpGte, 20 * time.Millisecond},
							Op:    OpOr,
							Right: &BytesFilter{"size", OpLt, 20 * 1000 * 1000}, // 20MB
						},
						Op:    OpOr,
						Right: &BytesFilter{"size", OpLte, 20 * 1024 * 1024}, // 20MiB
					},
				},
			},
		},
		false,
	},
	// See https://grafana.com/docs/loki/latest/query/log_queries/#label-filter-expression.
	{
		`{name="kafka"}
				| duration >= 20ms or method="GET" and size <= 20KB`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left: &LabelPredicateBinOp{
							Left:  &DurationFilter{"duration", OpGte, 20 * time.Millisecond},
							Op:    OpOr,
							Right: &LabelMatcher{"method", OpEq, "GET", nil},
						},
						Op:    OpAnd,
						Right: &BytesFilter{"size", OpLte, 20 * 1000}, // 20KB
					},
				},
			},
		},
		false,
	},
	{
		`{name="kafka"}
				| duration >= 20ms or (method="GET" and size <= 20KB)`,
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"name", OpEq, "kafka", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left: &DurationFilter{"duration", OpGte, 20 * time.Millisecond},
						Op:   OpOr,
						Right: &LabelPredicateParen{
							X: &LabelPredicateBinOp{
								Left:  &LabelMatcher{"method", OpEq, "GET", nil},
								Op:    OpAnd,
								Right: &BytesFilter{"size", OpLte, 20 * 1000}, // 20KB
							},
						},
					},
				},
			},
		},
		false,
	},
	{
		"{service_name=\"clickhouse\"}" +
			" | label_format log_line_contains_trace_id=`{{ contains \"a3bb0696568ee5069bf8fdc682631ddc\" __line__  }}`" +
			" | log_line_contains_trace_id=\"true\" OR trace_id=\"a3bb0696568ee5069bf8fdc682631ddc\"",
		&LogExpr{
			Sel: Selector{
				Matchers: []LabelMatcher{
					{"service_name", OpEq, "clickhouse", nil},
				},
			},
			Pipeline: []PipelineStage{
				&LabelFormatExpr{
					Values: []LabelTemplate{
						{Label: "log_line_contains_trace_id", Template: `{{ contains "a3bb0696568ee5069bf8fdc682631ddc" __line__  }}`},
					},
				},
				&LabelFilter{
					Pred: &LabelPredicateBinOp{
						Left:  &LabelMatcher{"log_line_contains_trace_id", OpEq, "true", nil},
						Op:    OpOr,
						Right: &LabelMatcher{"trace_id", OpEq, "a3bb0696568ee5069bf8fdc682631ddc", nil},
					},
				},
			},
		},
		false,
	},

	// Metric queries.
	// Range aggregation.
	{
		`count_over_time( ({job="mysql"})[5m] offset 15m)`,
		&RangeAggregationExpr{
			Op: RangeOpCount,
			Range: LogRangeExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"job", OpEq, "mysql", nil},
					},
				},
				Range: 5 * time.Minute,
				Offset: &OffsetExpr{
					Duration: 15 * time.Minute,
				},
			},
		},
		false,
	},
	{
		`avg_over_time({ job = "mysql" }[5h] | unwrap label) without (foo)`,
		&RangeAggregationExpr{
			Op: RangeOpAvg,
			Range: LogRangeExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"job", OpEq, "mysql", nil},
					},
				},
				Unwrap: &UnwrapExpr{
					Label: "label",
				},
				Range: 5 * time.Hour,
			},
			Grouping: &Grouping{
				Labels:  []Label{"foo"},
				Without: true,
			},
		},
		false,
	},
	{
		`quantile_over_time(10, { job = "mysql" }[5h] |= "error" | logfmt | unwrap label) by (bar,foo)`,
		&RangeAggregationExpr{
			Op: RangeOpQuantile,
			Range: LogRangeExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"job", OpEq, "mysql", nil},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: OpEq, By: LineFilterValue{Value: "error"}},
					&LogfmtExpressionParser{},
				},
				Unwrap: &UnwrapExpr{
					Label: "label",
				},
				Range: 5 * time.Hour,
			},
			Parameter: ptrTo(10.0),
			Grouping: &Grouping{
				Labels: []Label{"bar", "foo"},
			},
		},
		false,
	},
	{
		`avg_over_time({}[5h] | unwrap duration)`,
		&RangeAggregationExpr{
			Op: RangeOpAvg,
			Range: LogRangeExpr{
				Unwrap: &UnwrapExpr{
					Label: "duration",
				},
				Range: 5 * time.Hour,
			},
		},
		false,
	},
	{
		`avg_over_time({} | unwrap duration [5h])`,
		&RangeAggregationExpr{
			Op: RangeOpAvg,
			Range: LogRangeExpr{
				Unwrap: &UnwrapExpr{
					Label: "duration",
				},
				Range: 5 * time.Hour,
			},
		},
		false,
	},
	{
		`avg_over_time({ job = "mysql" }[5h] |= "error" | unwrap duration)`,
		&RangeAggregationExpr{
			Op: RangeOpAvg,
			Range: LogRangeExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"job", OpEq, "mysql", nil},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: OpEq, By: LineFilterValue{Value: "error"}},
				},
				Unwrap: &UnwrapExpr{
					Label: "duration",
				},
				Range: 5 * time.Hour,
			},
		},
		false,
	},
	{
		`avg_over_time({ job = "mysql" }[5h] |= "error" | unwrap duration(bytes) | foo="bar")`,
		&RangeAggregationExpr{
			Op: RangeOpAvg,
			Range: LogRangeExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{"job", OpEq, "mysql", nil},
					},
				},
				Pipeline: []PipelineStage{
					&LineFilter{Op: OpEq, By: LineFilterValue{Value: "error"}},
				},
				Unwrap: &UnwrapExpr{
					Op:    "duration",
					Label: "bytes",
					Filters: []LabelMatcher{
						{"foo", OpEq, "bar", nil},
					},
				},
				Range: 5 * time.Hour,
			},
		},
		false,
	},
	// Vector aggregation.
	{
		`topk(1, rate({job="mysql"}[1m])) without ()`,
		&VectorAggregationExpr{
			Op: VectorOpTopk,
			Expr: &RangeAggregationExpr{
				Op: RangeOpRate,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", OpEq, "mysql", nil},
						},
					},
					Range: 1 * time.Minute,
				},
			},
			Parameter: ptrTo(1),
			Grouping: &Grouping{
				Without: true,
			},
		},
		false,
	},
	{
		`sum by (host) (rate({job="mysql"} |= "error" != "timeout" | json | duration > 10s | unwrap label [1m]))`,
		&VectorAggregationExpr{
			Op: VectorOpSum,
			Expr: &RangeAggregationExpr{
				Op: RangeOpRate,
				Range: LogRangeExpr{
					Sel: Selector{
						Matchers: []LabelMatcher{
							{"job", OpEq, "mysql", nil},
						},
					},
					Pipeline: []PipelineStage{
						&LineFilter{Op: OpEq, By: LineFilterValue{Value: "error"}},
						&LineFilter{Op: OpNotEq, By: LineFilterValue{Value: "timeout"}},
						&JSONExpressionParser{},
						&LabelFilter{
							Pred: &DurationFilter{"duration", OpGt, 10 * time.Second},
						},
					},
					Unwrap: &UnwrapExpr{
						Label: "label",
					},
					Range: 1 * time.Minute,
				},
			},
			Grouping: &Grouping{
				Labels: []Label{"host"},
			},
		},
		false,
	},
	{
		`sum(vector(2)*vector(2))`,
		&VectorAggregationExpr{
			Op: VectorOpSum,
			Expr: &BinOpExpr{
				Left:  &VectorExpr{Value: 2},
				Op:    OpMul,
				Right: &VectorExpr{Value: 2},
			},
		},
		false,
	},
	{
		`sum((vector(2)*vector(2)))`,
		&VectorAggregationExpr{
			Op: VectorOpSum,
			Expr: &ParenExpr{
				X: &BinOpExpr{
					Left:  &VectorExpr{Value: 2},
					Op:    OpMul,
					Right: &VectorExpr{Value: 2},
				},
			},
		},
		false,
	},
	// label_replace
	{
		`label_replace(rate({}[5h]), "dst", "replacement", "src", ".*")`,
		&LabelReplaceExpr{
			Expr: &RangeAggregationExpr{
				Op: RangeOpRate,
				Range: LogRangeExpr{
					Range: 5 * time.Hour,
				},
			},
			DstLabel:    "dst",
			Replacement: "replacement",
			SrcLabel:    "src",
			Regex:       ".*",
			Re:          regexp.MustCompile(`^(?:.*)$`),
		},
		false,
	},
	// Literal expression.
	{
		`10.0`,
		&LiteralExpr{
			Value: 10.0,
		},
		false,
	},
	{
		`+10.0`,
		&LiteralExpr{
			Value: 10.0,
		},
		false,
	},
	{
		`-10.0`,
		&LiteralExpr{
			Value: -10.0,
		},
		false,
	},
	// Vector expression.
	{
		`vector (10.0)`,
		&VectorExpr{
			Value: 10.0,
		},
		false,
	},
	// Binary op.
	{
		`vector(10) or vector(10)`,
		&BinOpExpr{
			Left:  &VectorExpr{Value: 10},
			Op:    OpOr,
			Right: &VectorExpr{Value: 10},
		},
		false,
	},
	{
		`(vector(10)) or (vector(10))`,
		&BinOpExpr{
			Left:  &ParenExpr{X: &VectorExpr{Value: 10}},
			Op:    OpOr,
			Right: &ParenExpr{X: &VectorExpr{Value: 10}},
		},
		false,
	},
	{
		`vector(10) and vector(10)`,
		&BinOpExpr{
			Left:  &VectorExpr{Value: 10},
			Op:    OpAnd,
			Right: &VectorExpr{Value: 10},
		},
		false,
	},
	{
		`vector(10) unless vector(10)`,
		&BinOpExpr{
			Left:  &VectorExpr{Value: 10},
			Op:    OpUnless,
			Right: &VectorExpr{Value: 10},
		},
		false,
	},
	{
		`1 == 1`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 1},
			Op:    OpEq,
			Right: &LiteralExpr{Value: 1},
		},
		false,
	},
	{
		`1 != 1`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 1},
			Op:    OpNotEq,
			Right: &LiteralExpr{Value: 1},
		},
		false,
	},
	{
		`1 > 1`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 1},
			Op:    OpGt,
			Right: &LiteralExpr{Value: 1},
		},
		false,
	},
	{
		`1 >= 1`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 1},
			Op:    OpGte,
			Right: &LiteralExpr{Value: 1},
		},
		false,
	},
	{
		`1 < 1`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 1},
			Op:    OpLt,
			Right: &LiteralExpr{Value: 1},
		},
		false,
	},
	{
		`1 <= 1`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 1},
			Op:    OpLte,
			Right: &LiteralExpr{Value: 1},
		},
		false,
	},
	{
		`10.0 + 10.0`,
		&BinOpExpr{
			Left:  &LiteralExpr{Value: 10.0},
			Op:    OpAdd,
			Right: &LiteralExpr{Value: 10.0},
		},
		false,
	},
	{
		`(2+2)*2`,
		&BinOpExpr{
			Left: &ParenExpr{
				X: &BinOpExpr{
					Left:  &LiteralExpr{Value: 2},
					Op:    OpAdd,
					Right: &LiteralExpr{Value: 2},
				},
			},
			Op:    OpMul,
			Right: &LiteralExpr{Value: 2},
		},
		false,
	},
	{
		`2+2*2`,
		&BinOpExpr{
			Left: &LiteralExpr{Value: 2},
			Op:   OpAdd,
			Right: &BinOpExpr{
				Left:  &LiteralExpr{Value: 2},
				Op:    OpMul,
				Right: &LiteralExpr{Value: 2},
			},
		},
		false,
	},
	{
		`vector(2)*vector(3)+vector(4)`,
		&BinOpExpr{
			Left: &BinOpExpr{
				Left:  &VectorExpr{Value: 2},
				Op:    OpMul,
				Right: &VectorExpr{Value: 3},
			},
			Op:    OpAdd,
			Right: &VectorExpr{Value: 4},
		},
		false,
	},
	{
		`vector(2)+vector(3)*vector(4)+vector(5)`,
		&BinOpExpr{
			Left: &BinOpExpr{
				Left: &VectorExpr{Value: 2},
				Op:   OpAdd,
				Right: &BinOpExpr{
					Left:  &VectorExpr{Value: 3},
					Op:    OpMul,
					Right: &VectorExpr{Value: 4},
				},
			},
			Op:    OpAdd,
			Right: &VectorExpr{Value: 5},
		},
		false,
	},
	{
		`vector(0) and bool vector(0)`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				ReturnBool: true,
			},
			Right: &VectorExpr{},
		},
		false,
	},
	{
		`vector(0) and on () vector(0)`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				Op: "on",
			},
			Right: &VectorExpr{},
		},
		false,
	},
	{
		`vector(0) and ignoring (foo) group_left vector(0)`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				Op:       "ignoring",
				OpLabels: []Label{"foo"},
				Group:    "left",
			},
			Right: &VectorExpr{},
		},
		false,
	},
	{
		`vector(0) and ignoring (foo) group_left (vector(0))`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				Op:       "ignoring",
				OpLabels: []Label{"foo"},
				Group:    "left",
			},
			Right: &ParenExpr{X: &VectorExpr{}},
		},
		false,
	},
	{
		`vector(0) and bool ignoring (foo, bar) group_right () vector(0)`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				Op:         "ignoring",
				OpLabels:   []Label{"foo", "bar"},
				Group:      "right",
				ReturnBool: true,
			},
			Right: &VectorExpr{},
		},
		false,
	},
	{
		`vector(0) and ignoring () group_right (foo) vector(0)`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				Op:      "ignoring",
				Group:   "right",
				Include: []Label{"foo"},
			},
			Right: &VectorExpr{},
		},
		false,
	},
	{
		`vector(0) and ignoring () group_right (foo, bar) vector(0)`,
		&BinOpExpr{
			Left: &VectorExpr{},
			Op:   OpAnd,
			Modifier: BinOpModifier{
				Op:      "ignoring",
				Group:   "right",
				Include: []Label{"foo", "bar"},
			},
			Right: &VectorExpr{},
		},
		false,
	},

	// Explain.
	{
		`@explain {foo="bar"}`,
		&ExplainExpr{
			X: &LogExpr{
				Sel: Selector{
					Matchers: []LabelMatcher{
						{Label: "foo", Op: OpEq, Value: "bar"},
					},
				},
			},
		},
		false,
	},
	{
		`@explain sum(vector(2)*vector(2))`,
		&ExplainExpr{
			X: &VectorAggregationExpr{
				Op: VectorOpSum,
				Expr: &BinOpExpr{
					Left:  &VectorExpr{Value: 2},
					Op:    OpMul,
					Right: &VectorExpr{Value: 2},
				},
			},
		},
		false,
	},

	// Invalid syntax.
	{"{", nil, true},
	{"{foo}", nil, true},
	{"{foo =}", nil, true},
	{`{foo == "bar"}`, nil, true},
	{`{foo = "bar\"}`, nil, true},
	{`{foo = bar}`, nil, true},
	{`{foo = "bar"} | addr == ip(`, nil, true},
	{`{"foo\"}`, nil, true},
	{"{`foo`}", nil, true},
	{`label_replace`, nil, true},
	{`label_replace(`, nil, true},
	{`vector`, nil, true},
	{`vector(`, nil, true},
	{`vector(0`, nil, true},
	{`avg`, nil, true},
	{`avg(`, nil, true},
	{`avg_over_time`, nil, true},
	{`avg_over_time(`, nil, true},
	{`avg_over_time({}[5])`, nil, true},
	{`avg_over_time({}[5h] | unwrap "foo")`, nil, true},
	{`avg_over_time({} | unwrap "foo" [5h])`, nil, true},
	{`avg_over_time({}[10h] offset "foo")`, nil, true},
	{`avg_over_time({} | json [10h] offset "foo")`, nil, true},
	{`{foo = "bar"} | label_format foo>`, nil, true},
	{`{foo = "bar"} | "bar"`, nil, true},
	{`{foo = "bar"} | unwrap label`, nil, true},
	{`{foo = "bar"} |= foo`, nil, true},
	{`{foo = "bar"} |= ip("foo"`, nil, true},
	{`{foo = "bar"} |= ip("foo") or ip(`, nil, true},
	{`{foo = "bar"} |= ip("foo") or ip("foo"`, nil, true},
	// Tail expression
	{`{foo = "bar"} |= "foo" {}`, nil, true},
	// Missing identifier.
	{`{foo = "bar"} | json bar,`, nil, true},
	{`{foo = "bar"} | logfmt bar,`, nil, true},
	{`{foo = "bar"} | label_format`, nil, true},
	{`{foo = "bar"} | label_format foo=`, nil, true},
	{`{foo = "bar"} | keep`, nil, true},
	{`{foo = "bar"} | keep foo,`, nil, true},
	{`{foo = "bar"} | keep foo=`, nil, true},
	{`{foo = "bar"} | drop`, nil, true},
	{`{foo = "bar"} | drop foo,`, nil, true},
	{`{foo = "bar"} | drop foo=`, nil, true},
	// Missing expression.
	{`{foo = "bar"} |= "foo" or`, nil, true},
	{`{foo = "bar"} | label="foo",`, nil, true},
	// Missing string value.
	{`{foo = "bar"} | json bar=`, nil, true},
	{`{foo = "bar"} | regexp`, nil, true},
	{`{foo = "bar"} | pattern`, nil, true},
	{`{foo = "bar"} | line_format`, nil, true},
	{`{foo = "bar"} | addr == ip()`, nil, true},
	{`{foo = "bar"} |= ip()`, nil, true},
	{`{foo = "bar"} |= ip("foo") or ip()`, nil, true},
	// Invalid comparison operation.
	{`{foo = "bar"} | addr >= ip("127.0.0.1")`, nil, true},
	{`{foo = "bar"} |~ ip("127.0.0.1")`, nil, true},
	{`{foo = "bar"} | foo == "bar"`, nil, true},
	{`{foo = "bar"} | status = 10`, nil, true},
	{`{foo = "bar"} | status = 10s`, nil, true},
	{`{foo = "bar"} | status = 10b`, nil, true},
	// Unknown flag.
	{`{foo = "bar"} | logfmt --unknown-flag`, nil, true},
	// Invalid logical operation.
	{`1 and vector(1)`, nil, true},
	{`1 or vector(1)`, nil, true},
	{`1 unless vector(1)`, nil, true},
	{`vector(1) and 1`, nil, true},
	{`vector(1) or 1`, nil, true},
	{`vector(1) unless 1`, nil, true},

	// Range is required.
	{`count_over_time({})`, nil, true},
	// Two ranges.
	{`count_over_time({}[5h][5h])`, nil, true},
	// Parameter is required.
	{`quantile_over_time({}[5h])`, nil, true},
	{`topk(rate({job="mysql"}[1m]))`, nil, true},
	{`label_replace()`, nil, true},
	{`label_replace(rate({job="mysql"}[1m]))`, nil, true},
	{`label_replace(rate({job="mysql"}[1m]), "dst")`, nil, true},
	{`label_replace(rate({job="mysql"}[1m]), "dst", "replacement")`, nil, true},
	{`label_replace(rate({job="mysql"}[1m]), "dst", "replacement", "src")`, nil, true},
	// Parameter is not allowed.
	{`avg_over_time(0, {}[5h])`, nil, true},
	{`count_over_time(0, {}[5h] | unwrap label)`, nil, true},
	{`count(1, rate({job="mysql"}[1m]))`, nil, true},
	// Invalid parameter.
	{`topk(0, rate({job="mysql"}[1m]))`, nil, true},
	{`topk(-1, rate({job="mysql"}[1m]))`, nil, true},
	{`topk(1.1, rate({job="mysql"}[1m]))`, nil, true},
	// Grouping is not allowed.
	{`rate({}[5h]) by ()`, nil, true},
	{`sort(rate({}[5h])) by ()`, nil, true},
	// Unwrap expression is required.
	{`avg_over_time({}[5h])`, nil, true},
	// Unwrap expression is not allowed.
	{`bytes_rate({}[5h] | unwrap label)`, nil, true},
	// Label can be changed only once per stage.
	{`{foo = "bar"} | label_format status=foo,status=bar`, nil, true},

	// Invalid regexp.
	//
	{`{foo=~"\\"}`, nil, true},
	{`{} |~ "\\"`, nil, true},
	{`{} |~ ".+" or "\\"`, nil, true},
	{`{} | regexp "\\"`, nil, true},
	{`{} | foo=~"\\"`, nil, true},
	{`label_replace(rate({job="mysql"}[1m]), "dst", "replacement", "src", "\\")`, nil, true},
	// No capture.
	{`{} | regexp "a"`, nil, true},
	{`{} | regexp "a (\\w+)"`, nil, true},
	// Duplicate capture.
	{`{} | regexp "(?P<method>\\w+)(?P<method>\\w+)"`, nil, true},
	// Invalid capture name.
	{`{} | regexp "(?P<0a>\\w+)"`, nil, true},

	// Invalid pattern.
	//
	// No capture.
	{`{} | pattern "a"`, nil, true},
	{`{} | pattern "a <_> a"`, nil, true},
	// Duplicate capture.
	{`{} | pattern "<a> foo <a>"`, nil, true},
	// Consecutive capture.
	{`{} | pattern "<a><b>"`, nil, true},
}

func TestParse(t *testing.T) {
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Input:\n%s", tt.input)
				}
			}()

			got, err := Parse(tt.input, ParseOptions{AllowDots: true})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func FuzzParse(f *testing.F) {
	for _, tt := range tests {
		f.Add(tt.input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		defer func() {
			if r := recover(); r != nil || t.Failed() {
				t.Logf("Input:\n%s", input)
			}

			_, _ = Parse(input, ParseOptions{AllowDots: true})
		}()
	})
}

func TestParseSelector(t *testing.T) {
	tests := []struct {
		input   string
		wantSel Selector
		wantErr bool
	}{
		{`{}`, Selector{}, false},
		{
			`{foo="bar"}`,
			Selector{
				Matchers: []LabelMatcher{
					{Label: "foo", Op: OpEq, Value: "bar"},
				},
			},
			false,
		},

		{``, Selector{}, true},
		{`{} | json`, Selector{}, true},
	}
	for i, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			gotSel, err := ParseSelector(tt.input, ParseOptions{AllowDots: true})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSel, gotSel)
		})
	}
}
