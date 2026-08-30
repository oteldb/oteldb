package storagebackend_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// TestBackendTraceByIDResource pins that a trace spanning several services keeps each span under
// its own resource on the by-id read, whether the spans are still in the head, flushed to separate
// parts, or merged into one — the unbounded-window equality fetch must not collapse stream identity.
func TestBackendTraceByIDResource(t *testing.T) {
	for _, mode := range []string{"head", "flushed", "compacted"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			store, err := storage.InMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close(ctx) })
			b := storagebackend.New(store)

			ts := time.Now().Truncate(time.Second)
			traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

			want := map[string]string{}
			svcs := []string{"alpha", "beta", "gamma"}
			for round := range 3 {
				td := ptrace.NewTraces()
				for i, svc := range svcs {
					rs := td.ResourceSpans().AppendEmpty()
					rs.Resource().Attributes().PutStr("service.name", svc)
					ss := rs.ScopeSpans().AppendEmpty()
					for k := range 2 {
						sp := ss.Spans().AppendEmpty()
						sp.SetTraceID(traceID)
						sp.SetSpanID(pcommon.SpanID([8]byte{byte(round + 1), byte(i + 1), byte(k + 1)}))
						name := fmt.Sprintf("%s.%d.%d", svc, round, k)
						sp.SetName(name)
						want[name] = svc
						at := ts.Add(time.Duration(round*10+i*2+k) * time.Second)
						sp.SetStartTimestamp(pcommon.Timestamp(at.UnixNano()))
						sp.SetEndTimestamp(pcommon.Timestamp(at.Add(time.Second).UnixNano()))
					}
				}
				require.NoError(t, b.ConsumeTraces(ctx, td))
				if mode != "head" {
					require.NoError(t, store.Admin().Flush(ctx, "default", signal.Trace))
				}
			}

			if mode == "compacted" {
				require.NoError(t, store.Admin().Compact(ctx, "default", signal.Trace))
			}

			it, err := b.Traces().TraceByID(ctx, otelstorage.TraceID(traceID), tracestorage.TraceByIDOptions{})
			require.NoError(t, err)
			got := map[string]string{}
			var span tracestorage.Span
			for it.Next(&span) {
				v, ok := span.ResourceAttrs.AsMap().Get("service.name")
				require.True(t, ok, "span %s has no service.name", span.Name)
				got[span.Name] = v.Str()
			}
			require.NoError(t, it.Err())
			require.Equal(t, want, got)
		})
	}
}
