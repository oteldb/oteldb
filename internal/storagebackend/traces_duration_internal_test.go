package storagebackend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
)

func TestDurationRangeHints(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		query                         string
		want, lower, upper, inclusive bool
	}{
		{`{duration = 1s}`, true, true, true, true},
		{`{duration > 1s}`, true, true, false, false},
		{`{duration >= 1s}`, true, true, false, true},
		{`{duration < 1s}`, true, false, true, false},
		{`{duration <= 1s}`, true, false, true, true},
		{query: `{duration != 1s}`},
		{query: `{duration > 1000000000}`},
		{query: `{duration > 1.5}`},
		{query: `{span.duration > 1s}`},
		{query: `{traceDuration > 1s}`},
	} {
		t.Run(tt.query, func(t *testing.T) {
			t.Parallel()
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)
			_, matchers := traceql.ExtractMatchers(expr)
			require.Len(t, matchers, 1)
			filters, pushed := lowerSpanMatcher(matchers[0])
			if !pushed {
				require.False(t, tt.want)
				return
			}
			hint := filters[0].conditions[0].Int64Range
			if !tt.want {
				require.Nil(t, hint)
				return
			}
			require.NotNil(t, hint)
			require.Equal(t, tt.lower, hint.Lower != nil)
			require.Equal(t, tt.upper, hint.Upper != nil)
			if hint.Lower != nil {
				require.Equal(t, int64(time.Second), hint.Lower.Value)
				require.Equal(t, tt.inclusive, hint.Lower.Inclusive)
			}
			if hint.Upper != nil {
				require.Equal(t, int64(time.Second), hint.Upper.Value)
				require.Equal(t, tt.inclusive, hint.Upper.Inclusive)
			}
		})
	}
}
