package storagebackend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

func TestTraceExtentPredicate(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		query string
		want  bool
	}{
		{`{}`, true},
		{`{traceDuration > 2s}`, true},
		{`{traceDuration >= 3s && traceDuration <= 3s}`, true},
		{`{traceDuration > 3s}`, false},
		{`{traceDuration < 2s}`, false},
		{`{traceDuration > 10s || name = "root"}`, true},
		{`{traceDuration < 1s || traceDuration > 10s}`, true},
		{`{name = "root" && traceDuration > 10s}`, false},
		{`{duration > 10s}`, true},
	} {
		t.Run(tt.query, func(t *testing.T) {
			expr, err := traceql.Parse(tt.query)
			require.NoError(t, err)
			matchers := traceql.ExtractTraceDurationMatchers(expr)
			require.Equal(t, tt.want, traceExtentPredicate(matchers)(3*time.Second))
		})
	}
}

func TestNeedsTraceExtents(t *testing.T) {
	t.Parallel()
	expr, err := traceql.Parse(`{traceDuration > 1s}`)
	require.NoError(t, err)
	proven := traceql.ExtractTraceDurationMatchers(expr)
	for _, tt := range []struct {
		name   string
		params traceqlengine.SelectSpansetsParams
		want   bool
	}{
		{"unbounded", traceqlengine.SelectSpansetsParams{}, false},
		{"start", traceqlengine.SelectSpansetsParams{Start: time.Unix(1, 0)}, true},
		{"end", traceqlengine.SelectSpansetsParams{End: time.Unix(2, 0)}, true},
		{"minimum", traceqlengine.SelectSpansetsParams{MinDuration: time.Second}, true},
		{"maximum", traceqlengine.SelectSpansetsParams{MaxDuration: time.Second}, true},
		{"duration_intrinsic", traceqlengine.SelectSpansetsParams{TraceDurationMatchers: proven}, true},
	} {
		t.Run(tt.name, func(t *testing.T) { t.Parallel(); require.Equal(t, tt.want, needsTraceExtents(tt.params)) })
	}
}
