package traceqlengine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/scarecrow"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

type groupingMetricsQuerier struct {
	MemoryQuerier
	spans []tracestorage.Span
}

func (*groupingMetricsQuerier) WithQueryBudget(ctx context.Context) context.Context { return ctx }
func (q *groupingMetricsQuerier) SelectMetricsSpans(context.Context, SelectSpansetsParams) (iterators.Iterator[tracestorage.Span], error) {
	return iterators.Slice(q.spans), nil
}

func (*groupingMetricsQuerier) SelectMetricsSpansets(context.Context, SelectSpansetsParams) (iterators.Iterator[Trace], error) {
	return iterators.Empty[Trace](), nil
}

func TestMetricsGroupingLabelsRemainIndependent(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_010, 0)
	q := &groupingMetricsQuerier{}
	for i := range 3 {
		attrs := otelstorage.NewAttrs()
		switch i {
		case 0:
			attrs.AsMap().PutStr("a", "one")
			attrs.AsMap().PutStr("b", "present")
		case 1:
			attrs.AsMap().PutStr("a", "two")
		}
		q.spans = append(q.spans, tracestorage.Span{Start: otelstorage.Timestamp(base.Add(time.Second).UnixNano()), Attrs: attrs})
	}
	expr, err := traceql.Parse(`{} | count_over_time() by(span.a,span.b)`)
	require.NoError(t, err)
	grid, err := newMetricsGrid(MetricsEvalParams{Start: base, End: base.Add(15 * time.Second), Step: 15 * time.Second}, 100)
	require.NoError(t, err)
	source := &spanMetricsSource{querier: q, grid: grid, limits: metricsLimits{series: 10, spans: 10}}
	root, err := source.Aggregation(t.Context(), expr.(*traceql.MetricsAggregation), scarecrow.NewEvalContext(grid.steps, 100))
	require.NoError(t, err)
	defer func() { require.NoError(t, root.Close()) }()
	schema, err := root.Schema(t.Context())
	require.NoError(t, err)
	require.Len(t, schema.Series, 3)
	first, err := metricsLabels(schema.At(0))
	require.NoError(t, err)
	require.Len(t, first, 2)
	require.Equal(t, "one", first[0].Value.StringValue.StringValue)
	require.Equal(t, "present", first[1].Value.StringValue.StringValue)
	second, err := metricsLabels(schema.At(1))
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, "span.a", second[0].Key)
	require.Equal(t, "two", second[0].Value.StringValue.StringValue)
	missing, err := metricsLabels(schema.At(2))
	require.NoError(t, err)
	require.Empty(t, missing)
}

func BenchmarkMetricsGrouping(b *testing.B) {
	const count = 1000
	base := time.Unix(1_700_000_010, 0)
	q := &groupingMetricsQuerier{}
	for i := range count {
		attrs := otelstorage.NewAttrs()
		attrs.AsMap().PutStr("host", fmt.Sprintf("host-%d", i))
		attrs.AsMap().PutStr("region", "region")
		if i%2 == 0 {
			attrs.AsMap().PutStr("zone", "zone")
		}
		q.spans = append(q.spans, tracestorage.Span{Start: otelstorage.Timestamp(base.Add(time.Second).UnixNano()), Attrs: attrs})
	}
	grid, err := newMetricsGrid(MetricsEvalParams{Start: base, End: base.Add(15 * time.Second), Step: 15 * time.Second}, 100)
	if err != nil {
		b.Fatal(err)
	}
	for _, fields := range []string{"span.host", "span.host,span.region,span.zone"} {
		expr, err := traceql.Parse(`{} | count_over_time() by(` + fields + `)`)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fields, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				source := &spanMetricsSource{querier: q, grid: grid, limits: metricsLimits{series: count + 1, spans: count + 1}}
				root, err := source.Aggregation(b.Context(), expr.(*traceql.MetricsAggregation), scarecrow.NewEvalContext(grid.steps, 10000))
				if err != nil {
					b.Fatal(err)
				}
				schema, err := root.Schema(b.Context())
				if err != nil {
					b.Fatal(err)
				}
				if schema.Len() != count {
					b.Fatal(schema.Len())
				}
				if err := root.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
