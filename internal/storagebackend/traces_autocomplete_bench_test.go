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

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func BenchmarkTraceFilteredAutocomplete(b *testing.B) {
	store, err := storage.InMemory()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	td := ptrace.NewTraces()
	for i := range 20 {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", fmt.Sprintf("svc-%d", i))
		spans := rs.ScopeSpans().AppendEmpty().Spans()
		for n := range 200 {
			span := spans.AppendEmpty()
			span.SetTraceID(pcommon.TraceID{15: byte(i + 1)})
			span.SetSpanID(pcommon.SpanID{7: byte(n + 1)})
			span.SetName("work")
			span.SetStartTimestamp(pcommon.Timestamp(base.UnixNano()))
			span.SetEndTimestamp(pcommon.Timestamp(base.Add(time.Second).UnixNano()))
		}
	}
	require.NoError(b, db.ConsumeTraces(b.Context(), td))
	for _, tt := range []struct{ name, query string }{
		{"identity", `{resource.service.name =~ "^(svc-0|svc-1)$"}`},
		{"span_columns", `{resource.service.name =~ "^(svc-0|svc-1)$" && name != ""}`},
	} {
		b.Run(tt.name, func(b *testing.B) {
			query := traceql.ParseAutocomplete(tt.query)
			b.ReportAllocs()
			for b.Loop() {
				it, err := db.Traces().TagValues(b.Context(), traceql.Attribute{Name: "service.name", Scope: traceql.ScopeResource},
					tracestorage.TagValuesOptions{AutocompleteQuery: query})
				if err != nil {
					b.Fatal(err)
				}
				var tag tracestorage.Tag
				count := 0
				for it.Next(&tag) {
					if tag.Value != "svc-0" && tag.Value != "svc-1" {
						b.Fatal("unexpected service", tag.Value)
					}
					count++
				}
				if err := it.Err(); err != nil || count != 2 {
					b.Fatal("tag values", count, err)
				}
				if err := it.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
