package traceqlengine

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func TestAggregator(t *testing.T) {
	generateSpan := func(val float64) tracestorage.Span {
		attrs := pcommon.NewMap()
		attrs.PutDouble("aggregate_me", val)

		return tracestorage.Span{
			Name:       "spanName",
			Start:      1700000001_000000000,
			End:        1700000003_000000000,
			Kind:       int32(ptrace.SpanKindServer),
			StatusCode: int32(ptrace.StatusCodeOk),
			Attrs:      otelstorage.Attrs(attrs),
		}
	}

	tests := []struct {
		input string
		vals  []float64
		want  bool
	}{
		// Count.
		{
			`count() = 3`,
			[]float64{1., 2., 3.},
			true,
		},
		{
			`count() > 10`,
			[]float64{1., 2., 3.},
			false,
		},
		// Maximum.
		{
			`max(.aggregate_me) = 1.`,
			[]float64{1.},
			true,
		},
		{
			`max(.aggregate_me) = 3.`,
			[]float64{1., 2., 3.},
			true,
		},
		// Minimum.
		{
			`min(.aggregate_me) = 1.`,
			[]float64{1.},
			true,
		},
		{
			`min(.aggregate_me) = 1.`,
			[]float64{3., 2., 1.},
			true,
		},
		// Average.
		{
			`avg(.aggregate_me) = 1.`,
			[]float64{1.},
			true,
		},
		{
			`avg(.aggregate_me) = 2.`,
			[]float64{1., 2., 3.},
			true,
		},
		{
			`avg(.aggregate_me) = 2.`,
			[]float64{3., 2., 1.},
			true,
		},
		// Sum.
		{
			`sum(.aggregate_me) = 1.`,
			[]float64{1.},
			true,
		},
		{
			`sum(.aggregate_me) = 6.`,
			[]float64{1., 2., 3.},
			true,
		},
		{
			`sum(.aggregate_me) = 6.`,
			[]float64{3., 2., 1.},
			true,
		},
		{
			`sum(.aggregate_me) = 3*2`,
			[]float64{3., 2., 1.},
			true,
		},
		// Complex.
		{
			`sum(1) = count()`,
			[]float64{3., 2., 1.},
			true,
		},
		{
			`sum(1+1) = count()*2`,
			[]float64{3., 2., 1.},
			true,
		},
		{
			`sum(2^2) = 4*count()`,
			[]float64{3., 2., 1.},
			true,
		},
		{
			`sum(.aggregate_me) / count() = avg(.aggregate_me)`,
			[]float64{3., 2., 1.},
			true,
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("Test%d", i+1), func(t *testing.T) {
			defer func() {
				if t.Failed() {
					t.Logf("Query: \n%s", tt.input)
				}
			}()
			a := require.New(t)

			root, err := traceql.Parse(tt.input)
			a.NoError(err)

			a.IsType((*traceql.SpansetPipeline)(nil), root)
			pipeline := root.(*traceql.SpansetPipeline).Pipeline

			a.IsType((*traceql.ScalarFilter)(nil), pipeline[0])
			filter := pipeline[0].(*traceql.ScalarFilter)

			e, err := buildBinaryEvaluater(filter.Left, filter.Op, filter.Right)
			a.NoError(err)

			var spans []tracestorage.Span
			for _, val := range tt.vals {
				spans = append(spans, generateSpan(val))
			}

			ectx := EvaluateCtx{
				Set: Spanset{
					RootSpanName:    "rootName",
					RootServiceName: "rootServiceName",
					TraceDuration:   time.Minute,
					Spans:           spans,
				},
			}
			a.NotEmpty(t, ectx.Set.Spans, "input cannot be empty")

			got := e.Eval(ectx.Set.Spans[0], ectx)
			a.Equal(traceql.TypeBool, got.Type)
			a.Equal(tt.want, got.AsBool())
		})
	}
}
