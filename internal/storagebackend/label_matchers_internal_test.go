package storagebackend

import (
	"regexp"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/profileql"
)

func TestPreparedLabelMatchers(t *testing.T) {
	for _, pattern := range []string{`foo|bar`, `(|foo)`, `foo.*bar`, `(?i)foo`, `foo.bar`, `\x{FFFD}`} {
		for _, op := range []logql.BinOp{logql.OpRe, logql.OpNotRe} {
			m := logql.LabelMatcher{Label: "env", Op: op, Value: pattern, Re: regexp.MustCompile("^(?:" + pattern + ")$")}
			selector := prepareSelector([]logql.LabelMatcher{m})[0]
			pipeline := preparePipelineSelector([]logql.LabelMatcher{m})[0]
			fast, err := labels.NewFastRegexMatcher(pattern)
			require.NoError(t, err)
			for _, value := range []string{"", "foo", "bar", "xfooy", "FOO", "foo\nbar", "foo\xffbar", "\xff", "\uFFFD"} {
				negated := op == logql.OpNotRe
				require.Equal(t, m.Re.MatchString(value) != negated, selector.matches(value), "selector %q over %q", pattern, value)
				require.Equal(t, fast.MatchString(value) != negated, pipeline.matches(value), "pipeline %q over %q", pattern, value)
				if selector.matches(value) && len(selector.stringValues) > 0 {
					require.True(t, fetch.InAnyEqual(selector.stringValues, []byte(value)))
				}
				if pipeline.matches(value) && len(pipeline.stringValues) > 0 {
					require.True(t, fetch.InAnyEqual(pipeline.stringValues, []byte(value)))
				}
			}
		}
	}
}

func TestPipelineLabelMatcherExtraction(t *testing.T) {
	m := &logql.LabelMatcher{Label: "env", Op: logql.OpRe, Value: "foo|bar", Re: regexp.MustCompile(`^(?:foo|bar)$`)}
	n := &logql.LabelMatcher{Label: "region", Op: logql.OpNotEq, Value: ""}
	for _, tt := range []struct {
		name     string
		pipeline []logql.PipelineStage
		want     []logql.LabelMatcher
	}{
		{"regexp", []logql.PipelineStage{&logql.LabelFilter{Pred: m}}, []logql.LabelMatcher{*m}},
		{"negation", []logql.PipelineStage{&logql.LabelFilter{Pred: n}}, []logql.LabelMatcher{*n}},
		{"and", []logql.PipelineStage{&logql.LabelFilter{Pred: &logql.LabelPredicateBinOp{Op: logql.OpAnd, Left: m, Right: n}}}, []logql.LabelMatcher{*m, *n}},
		{"or", []logql.PipelineStage{&logql.LabelFilter{Pred: &logql.LabelPredicateBinOp{Op: logql.OpOr, Left: m, Right: n}}}, nil},
		{"parser", []logql.PipelineStage{&logql.JSONExpressionParser{}, &logql.LabelFilter{Pred: m}}, nil},
		{"before_parser", []logql.PipelineStage{&logql.LabelFilter{Pred: m}, &logql.JSONExpressionParser{}, &logql.LabelFilter{Pred: n}}, []logql.LabelMatcher{*m}},
	} {
		t.Run(tt.name, func(t *testing.T) { require.Equal(t, tt.want, labelFilterMatchers(tt.pipeline)) })
	}
}

func TestProfilePreparedRegexpTypedValues(t *testing.T) {
	for _, negate := range []bool{false, true} {
		op := profileql.OpRe
		if negate {
			op = profileql.OpNotRe
		}
		m, err := profileLabelMatcher(profileql.LabelMatcher{Label: "env", Op: op, Value: "foo|42"})
		require.NoError(t, err)
		for _, value := range []signal.Value{
			signal.StringValue([]byte("foo")), signal.BytesValue([]byte("foo")), signal.IntValue(42),
			signal.StringValue([]byte("xfooy")), signal.EmptyValue(), signal.BoolValue(true),
		} {
			want := (valueText(value) == "foo" || valueText(value) == "42") != negate
			require.Equal(t, want, m.Match(value))
			if !negate && value.Kind() == signal.KindStr && m.Match(value) {
				require.True(t, fetch.InAnyEqual(m.StringValues, value.Str()))
			}
		}
		if negate {
			require.Empty(t, m.StringValues)
		} else {
			require.Equal(t, fetch.AnyEqualStrings([]string{"42", "foo"}), m.StringValues)
		}
	}
}
