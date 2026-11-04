package traceql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractTraceDurationMatchers(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		query     string
		operators []BinaryOp
	}{
		{query: `{}`},
		{`{traceDuration > 1s}`, []BinaryOp{OpGt}},
		{`{1s < traceDuration}`, []BinaryOp{OpGt}},
		{`{traceDuration >= 1s && traceDuration <= 2s}`, []BinaryOp{OpGte, OpLte}},
		{`{name = "target" && traceDuration > 1s}`, []BinaryOp{OpGt}},
		{`{traceDuration > 1s && (name = "target" || true)}`, []BinaryOp{OpGt}},
		{`{traceDuration > 1s && ((traceDuration > 10s) = false)}`, []BinaryOp{OpGt}},
		{query: `{traceDuration > 10s || true}`},
		{query: `{traceDuration > 10s} || {true}`},
		{query: `{(traceDuration > 10s) = false}`},
		{query: `{name = "target" && ((traceDuration > 10s) = false)}`},
		{query: `{!(traceDuration > 10s)}`},
		{query: `{duration > 1s}`},
		{query: `{traceDuration > 1s} || {traceDuration < 100ms}`},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			expr, err := Parse(tt.query)
			require.NoError(t, err)
			matchers := ExtractTraceDurationMatchers(expr)
			operators := make([]BinaryOp, 0, len(matchers))
			for _, matcher := range matchers {
				require.Equal(t, TraceDuration, matcher.Attribute.Prop)
				operators = append(operators, matcher.Op)
			}
			require.Equal(t, len(tt.operators), len(operators))
			if len(operators) > 0 {
				require.Equal(t, tt.operators, operators)
			}
		})
	}
}
