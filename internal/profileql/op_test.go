package profileql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchOpString(t *testing.T) {
	tests := []struct {
		op   MatchOp
		want string
	}{
		{OpEq, "="},
		{OpNotEq, "!="},
		{OpRe, "=~"},
		{OpNotRe, "!~"},
		{MatchOp(0), "<unknown op 0>"},
		{MatchOp(100), "<unknown op 100>"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.op.String())
	}
}

func TestMatchOpIsRegex(t *testing.T) {
	require.False(t, OpEq.IsRegex())
	require.False(t, OpNotEq.IsRegex())
	require.True(t, OpRe.IsRegex())
	require.True(t, OpNotRe.IsRegex())
}

func TestLabelMatcherString(t *testing.T) {
	tests := []struct {
		m    LabelMatcher
		want string
	}{
		{LabelMatcher{Label: "label", Op: OpEq, Value: "value"}, `label="value"`},
		{LabelMatcher{Label: "label", Op: OpNotEq, Value: "value"}, `label!="value"`},
		{LabelMatcher{Label: "label", Op: OpRe, Value: "^value$"}, `label=~"^value$"`},
		{LabelMatcher{Label: "label", Op: OpNotRe, Value: "^value$"}, `label!~"^value$"`},
		// Values are quoted, escaping inner quotes.
		{LabelMatcher{Label: "msg", Op: OpEq, Value: `a"b`}, `msg="a\"b"`},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, tt.m.String())
	}
}
