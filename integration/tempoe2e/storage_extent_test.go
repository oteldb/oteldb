package tempoe2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tempoapi"
)

func TestStorageBackendTraceExtent(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	for i, service := range []string{"frontend", "payments"} {
		td := ptrace.NewTraces()
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", service)
		ss := rs.ScopeSpans().AppendEmpty()
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID{15: 1})
		s.SetSpanID(pcommon.SpanID{7: byte(i + 1)})
		s.SetName("root")
		if i > 0 {
			s.SetParentSpanID(pcommon.SpanID{7: 1})
			s.SetName("late")
		}
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Duration(i) * 2 * time.Second)))
		s.SetEndTimestamp(s.StartTimestamp() + pcommon.Timestamp(time.Second))
		require.NoError(t, db.ConsumeTraces(t.Context(), td))
		if i == 0 {
			require.NoError(t, store.Admin().Flush(t.Context(), "default", signal.Trace))
		}
	}
	q := db.Traces()
	client := setupDB(t, integration.TraceProvider(t), q, q)
	for _, tt := range []struct {
		name, query      string
		minimum, maximum time.Duration
		count            int
	}{
		{"trace_intrinsic", `{traceDuration > 2s}`, 0, 0, 1},
		{"minimum", `{}`, 2 * time.Second, 0, 1},
		{"maximum", `{}`, 0, 2 * time.Second, 0},
		{"inclusive", `{}`, 3 * time.Second, 3 * time.Second, 1},
		{"span_duration", `{duration > 2s}`, 0, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := client.Search(t.Context(), tempoapi.SearchParams{
				Q: tempoapi.NewOptString(tt.query), MinDuration: tempoapi.NewOptDuration(tt.minimum),
				MaxDuration: tempoapi.NewOptDuration(tt.maximum), Limit: tempoapi.NewOptInt(20),
			})
			require.NoError(t, err)
			require.Len(t, got.Traces, tt.count)
			if tt.count > 0 {
				require.Equal(t, 3000, got.Traces[0].DurationMs.Value)
				require.Len(t, got.Traces[0].SpanSet.Value.Spans, 2)
			}
		})
	}
}
