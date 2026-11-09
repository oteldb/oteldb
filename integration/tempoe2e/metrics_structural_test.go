package tempoe2e_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

func TestStorageBackendMetricsStructural(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_010, 0).UTC()
	td := ptrace.NewTraces()
	for i, service := range []string{"gateway", "checkout"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", service)
		span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetTraceID(pcommon.TraceID{15: 1})
		span.SetSpanID(pcommon.SpanID{7: byte(i + 1)})
		start := base.Add(-time.Minute)
		span.SetName("parent")
		if i > 0 {
			start = base.Add(time.Second)
			span.SetParentSpanID(pcommon.SpanID{7: 1})
			span.SetName("child")
		}
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(start.Add(time.Millisecond)))
	}
	require.NoError(t, db.ConsumeTraces(t.Context(), td))
	q := db.Traces()
	client := setupDB(t, integration.TraceProvider(t), q, q)
	for _, query := range []string{
		`{name="parent"} >> {name="child"} | count_over_time() by(rootServiceName)`,
		`{name="child" && trace:duration>1m} | count_over_time() by(rootServiceName)`,
	} {
		t.Run(query, func(t *testing.T) {
			result, err := client.QueryRange(t.Context(), tempoapi.QueryRangeParams{Q: query, Start: tempoTime(base), End: tempoTime(base.Add(15 * time.Second)), Step: tempoapi.NewOptPrometheusDuration("15s")})
			require.NoError(t, err)
			require.Len(t, result.Series, 1)
			series := result.Series[0]
			require.Len(t, series.Labels, 1)
			require.Equal(t, "rootServiceName", series.Labels[0].Key)
			require.Equal(t, "gateway", series.Labels[0].Value.StringValue.StringValue)
			require.Len(t, series.Samples, 2)
			require.Equal(t, 0.0, series.Samples[0].Value.Value)
			require.Equal(t, 1.0, series.Samples[1].Value.Value)
		})
	}
}

func TestStorageBackendMetricsTypedGrouping(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_010, 0).UTC()
	td := ptrace.NewTraces()
	spans := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	for i := range 3 {
		span := spans.AppendEmpty()
		span.SetTraceID(pcommon.TraceID{15: byte(i + 1)})
		span.SetSpanID(pcommon.SpanID{7: 1})
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Second)))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(2 * time.Second)))
		switch i {
		case 0:
			span.Attributes().PutInt("value", 1)
		case 1:
			span.Attributes().PutStr("value", "1")
		}
	}
	require.NoError(t, db.ConsumeTraces(t.Context(), td))
	q := db.Traces()
	client := setupDB(t, integration.TraceProvider(t), q, q)
	result, err := client.Query(t.Context(), tempoapi.QueryParams{Q: `{} | count_over_time() by(span.value)`, Start: tempoTime(base), End: tempoTime(base.Add(15 * time.Second))})
	require.NoError(t, err)
	require.Len(t, result.Series, 3)
	missing, integer, text := 0, 0, 0
	for _, series := range result.Series {
		require.Equal(t, 1.0, series.Value.Value)
		if len(series.Labels) == 0 {
			missing++
			continue
		}
		require.Len(t, series.Labels, 1)
		label := series.Labels[0]
		require.Equal(t, "span.value", label.Key)
		switch label.Value.Type {
		case tempoapi.IntValueAnyValue:
			require.Equal(t, int64(1), label.Value.IntValue.IntValue)
			integer++
		case tempoapi.StringValueAnyValue:
			require.Equal(t, "1", label.Value.StringValue.StringValue)
			text++
		default:
			t.Fatalf("unexpected grouping type %v", label.Value.Type)
		}
	}
	require.Equal(t, 1, missing)
	require.Equal(t, 1, integer)
	require.Equal(t, 1, text)
}

func TestStorageBackendMetricsLimits(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_010, 0).UTC()
	require.NoError(t, db.ConsumeTraces(t.Context(), metricsTraceFixture(base)))
	q := db.Traces()
	for _, tt := range []struct {
		name, query string
		options     traceqlengine.Options
	}{
		{"groups", `{} | count_over_time() by(resource.service.name)`, traceqlengine.Options{MetricsMaxSeries: 1}},
		{"samples", `{} | count_over_time()`, traceqlengine.Options{MetricsMaxSamples: 1}},
		{"scanned spans", `{} | count_over_time()`, traceqlengine.Options{MetricsMaxSpans: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := setupDB(t, integration.TraceProvider(t), q, q, tt.options)
			_, err := client.QueryRange(t.Context(), tempoapi.QueryRangeParams{Q: tt.query, Start: tempoTime(base), End: tempoTime(base.Add(time.Minute)), Step: tempoapi.NewOptPrometheusDuration("15s")})
			var apiErr *tempoapi.ErrorStatusCode
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, http.StatusUnprocessableEntity, apiErr.StatusCode)
		})
	}
}
