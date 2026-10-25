package tempoe2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

func TestStorageBackendTagSearchFilters(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	backend := storagebackend.New(store)
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "tag-search-test")
	rs.Resource().Attributes().PutStr("tag", "resource-value")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().Attributes().PutStr("tag", "scope-value")
	base := time.Unix(1_700_000_000, 0).UTC()
	for trace := range 2 {
		for i := range 2 {
			s := ss.Spans().AppendEmpty()
			s.SetTraceID(pcommon.TraceID{15: byte(trace + 1)})
			s.SetSpanID(pcommon.SpanID{7: byte(i + 1)})
			if i > 0 {
				s.SetParentSpanID(pcommon.SpanID{7: 1})
			}
			s.SetName("target")
			s.SetStartTimestamp(pcommon.Timestamp(base.Add(time.Duration(trace)*time.Second + time.Duration(i)*time.Millisecond).UnixNano()))
			s.SetEndTimestamp(s.StartTimestamp() + pcommon.Timestamp(10*time.Millisecond))
			s.Attributes().PutStr("tag", "span-value")
			s.Attributes().PutStr("duration", "attribute-duration")
			s.Attributes().PutInt("number", 42)
		}
	}
	require.NoError(t, backend.ConsumeTraces(ctx, td))
	querier := backend.Traces()
	client := setupDB(t, integration.TraceProvider(t), querier, querier)
	for _, tt := range []struct {
		name  string
		tags  string
		min   time.Duration
		max   time.Duration
		count int
	}{
		{name: "intrinsic", tags: `name=target`, count: 1},
		{name: "span_precedence", tags: `name=target tag=span-value`, count: 1},
		{name: "resource_shadowed", tags: `name=target tag=resource-value`},
		{name: "scope_shadowed", tags: `name=target tag=scope-value`},
		{name: "typed_attribute", tags: `name=target number=42`, count: 1},
		{name: "fixed_column_collision", tags: `name=target duration=attribute-duration`, count: 1},
		{name: "no_match", tags: `name=missing`},
		{name: "inclusive_duration", tags: `name=target`, min: 10 * time.Millisecond, max: 10 * time.Millisecond, count: 1},
		{name: "duration_no_match", tags: `name=target`, min: 11 * time.Millisecond},
		{name: "duration_maximum", tags: `name=target`, max: 9 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.Search(ctx, tempoapi.SearchParams{
				Tags: tempoapi.NewOptString(tt.tags), Limit: tempoapi.NewOptInt(1),
				MinDuration: tempoapi.NewOptDuration(tt.min), MaxDuration: tempoapi.NewOptDuration(tt.max),
			})
			require.NoError(t, err)
			require.Len(t, got.Traces, tt.count)
			for _, trace := range got.Traces {
				require.Equal(t, "target", trace.RootTraceName.Value)
				require.Equal(t, "tag-search-test", trace.RootServiceName.Value)
				require.Len(t, trace.SpanSet.Value.Spans, 2, "a trace limit must retain every matching span of that trace")
			}
		})
	}
}
