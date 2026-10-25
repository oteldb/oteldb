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
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

type traceTagsSource struct {
	storagebackend.Source
	rows     int
	requests []fetch.Request
}

func (s *traceTagsSource) TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return traceTagsFetch{source: s, inner: s.Source.TraceFetcher(tenants...)}
}

type traceTagsFetch struct {
	source *traceTagsSource
	inner  fetch.Fetcher
}

func (f traceTagsFetch) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	f.source.requests = append(f.source.requests, r)
	it, err := f.inner.Fetch(ctx, r)
	if err != nil {
		return nil, err
	}
	return &traceTagsIterator{source: f.source, Iterator: it}, nil
}

type traceTagsIterator struct {
	source *traceTagsSource
	fetch.Iterator
}

func (i *traceTagsIterator) Next(ctx context.Context) (*fetch.Batch, error) {
	batch, err := i.Iterator.Next(ctx)
	if err == nil {
		i.source.rows += len(batch.Timestamps)
	}
	return batch, err
}

func tagSearchFixture(tb testing.TB, flushed bool) (*storage.Storage, tracestorage.SearchTagsOptions) {
	tb.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close(ctx)) })
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("tag", "resource-value")
	rs.Resource().Attributes().PutStr("resource-only", "resource-value")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().Attributes().PutStr("tag", "scope-value")
	ss.Scope().Attributes().PutStr("scope-only", "scope-value")
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := range 100 {
		s := ss.Spans().AppendEmpty()
		s.SetTraceID(pcommon.TraceID{15: byte(i + 1)})
		s.SetSpanID(pcommon.SpanID{7: 1})
		name := "other"
		if i%10 == 0 {
			name = "target"
		}
		if i == 99 {
			name = ""
		}
		s.SetName(name)
		s.SetStartTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		s.SetEndTimestamp(s.StartTimestamp() + pcommon.Timestamp(time.Duration(i+1)*time.Millisecond))
		s.Attributes().PutStr("tag", "span-value")
		s.Attributes().PutStr("duration", "attribute-duration")
		s.Attributes().PutStr("name", "attribute-name")
		s.Attributes().PutInt("number", 42)
		e := s.Events().AppendEmpty()
		e.SetName("unused")
		e.Attributes().PutStr("payload", fmt.Sprint(i))
	}
	require.NoError(tb, storagebackend.New(store).ConsumeTraces(ctx, td))
	if flushed {
		require.NoError(tb, store.Admin().Flush(ctx, "default", signal.Trace))
	}
	return store, tracestorage.SearchTagsOptions{Start: base.Add(-time.Second), End: base.Add(time.Second)}
}

func TestTraceTagSearchFiltersBeforeMaterialization(t *testing.T) {
	t.Parallel()
	for _, flushed := range []bool{false, true} {
		name := "head"
		if flushed {
			name = "parts"
		}
		t.Run(name, func(t *testing.T) {
			store, base := tagSearchFixture(t, flushed)
			for _, tt := range []struct {
				name       string
				tags       map[string]string
				min, max   time.Duration
				want, rows int
			}{
				{name: "name", tags: map[string]string{"name": "target"}, want: 10, rows: 10},
				{name: "duration", min: 98 * time.Millisecond, max: 99 * time.Millisecond, want: 2, rows: 2},
				{name: "both", tags: map[string]string{"name": "target"}, min: 21 * time.Millisecond, max: 41 * time.Millisecond, want: 3, rows: 3},
				{name: "no_match", tags: map[string]string{"name": "missing"}, rows: 0},
				{name: "empty_name", tags: map[string]string{"name": ""}, rows: 0},
				{name: "duration_no_match", min: 101 * time.Millisecond, rows: 0},
				{name: "disabled_bounds", min: -time.Second, max: -time.Second, want: 100, rows: 100},
				{name: "span_precedence", tags: map[string]string{"tag": "span-value"}, want: 100, rows: 100},
				{name: "resource_shadowed", tags: map[string]string{"tag": "resource-value"}, rows: 100},
				{name: "scope_shadowed", tags: map[string]string{"tag": "scope-value"}, rows: 100},
				{name: "resource_only", tags: map[string]string{"resource-only": "resource-value"}, want: 100, rows: 100},
				{name: "scope_only", tags: map[string]string{"scope-only": "scope-value"}, want: 100, rows: 100},
				{name: "typed_value", tags: map[string]string{"number": "42"}, want: 100, rows: 100},
				{name: "fixed_column_collision", tags: map[string]string{"duration": "attribute-duration"}, want: 100, rows: 100},
			} {
				t.Run(tt.name, func(t *testing.T) {
					src := &traceTagsSource{Source: store}
					opts := base
					opts.MinDuration = tt.min
					opts.MaxDuration = tt.max
					it, err := storagebackend.NewQuery(src).Traces().SearchTags(t.Context(), tt.tags, opts)
					require.NoError(t, err)
					count := 0
					require.NoError(t, iterators.ForEach(it, func(tracestorage.Span) error { count++; return nil }))
					require.Equal(t, tt.want, count)
					require.Equal(t, tt.rows, src.rows, "only native-filter survivors may be materialized")
					for _, request := range src.requests {
						require.Zero(t, request.Limit, "a trace-count limit cannot become a span-row limit")
					}
				})
			}
		})
	}
}

func BenchmarkTraceTagSearch(b *testing.B) {
	store, opts := tagSearchFixture(b, true)
	querier := storagebackend.New(store).Traces()
	opts.MinDuration = 21 * time.Millisecond
	opts.MaxDuration = 41 * time.Millisecond
	b.ReportAllocs()
	for b.Loop() {
		it, err := querier.SearchTags(context.Background(), map[string]string{"name": "target"}, opts)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		if err := iterators.ForEach(it, func(tracestorage.Span) error { count++; return nil }); err != nil {
			b.Fatal(err)
		}
		if count != 3 {
			b.Fatal(count)
		}
	}
}

func TestTraceTagSearchMatchesGoldenCorpus(t *testing.T) {
	t.Parallel()
	f := traceqlNewFixture(t, storagebackend.WithTraceQLPushdown(false))
	it, err := f.querier.SearchTags(t.Context(), map[string]string{
		"name": "checkout.process", "http.route": traceqlSelectedRoute,
	}, tracestorage.SearchTagsOptions{
		Start: f.start, End: f.end,
		MinDuration: 55 * time.Millisecond, MaxDuration: 71 * time.Millisecond,
	})
	require.NoError(t, err)
	count := 0
	require.NoError(t, iterators.ForEach(it, func(span tracestorage.Span) error {
		require.Equal(t, "checkout.process", span.Name)
		count++
		return nil
	}))
	require.Equal(t, traceqlRouteTraces, count)
}
