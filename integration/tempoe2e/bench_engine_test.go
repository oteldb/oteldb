package tempoe2e_test

import (
	"context"
	"testing"

	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"

	"github.com/stretchr/testify/require"
)

func BenchmarkTraceQL(b *testing.B) {
	ctx := context.Background()

	set, err := readBatchSet()
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	var result *tempoapi.Traces
	for i := 0; i < b.N; i++ {
		result, err = set.Engine.Eval(ctx, `{ .http.method = "POST" && .http.status_code = 200 }`, traceqlengine.EvalParams{Limit: 20})
		if err != nil {
			b.Fatal(err)
		}
	}

	if result == nil || len(result.Traces) < 1 {
		b.Fatal("empty result")
	}
}
