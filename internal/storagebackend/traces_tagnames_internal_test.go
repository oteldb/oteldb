package storagebackend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// legacyTagNames is the scan-based TagNames this package used to ship, kept verbatim as the oracle
// the enumeration-based one is diffed against: the whole point of the rewrite is that it answers
// exactly the same set without materializing a span.
func (q *TraceQuerier) legacyTagNames(ctx context.Context, opts tracestorage.TagNamesOptions) ([]tracestorage.TagName, error) {
	spans, err := q.scanSpans(ctx, opts.Start, opts.End, nil)
	if err != nil {
		return nil, err
	}

	seen := map[tracestorage.TagName]struct{}{}
	for _, span := range spans {
		forEachSpanTag(span, func(scope traceql.AttributeScope, name, _ string) {
			if opts.Scope != traceql.ScopeNone && opts.Scope != scope {
				return
			}
			seen[tracestorage.TagName{Scope: scope, Name: name}] = struct{}{}
		})
	}

	out := make([]tracestorage.TagName, 0, len(seen))
	for tn := range seen {
		out = append(out, tn)
	}
	return out, nil
}

// tagNamesFixture ingests two services, each with resource, instrumentation-scope and span
// attributes of its own, plus a set instrumentation scope name/version — the intrinsics that must not
// leak into the tag-name list even though the key dictionary reports them.
func tagNamesFixture(t *testing.T) (b *Backend, start, end time.Time) {
	t.Helper()

	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b = New(store)
	ts := time.Now().Truncate(time.Second)

	td := ptrace.NewTraces()

	for i, svc := range []string{"frontend", "cart"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", svc)
		rs.Resource().Attributes().PutStr("deployment.environment", "prod")
		rs.Resource().Attributes().PutStr("resource.only."+svc, "x")

		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("otelhttp")
		ss.Scope().SetVersion("1.2.3")
		ss.Scope().Attributes().PutStr("library.tier", "http")
		ss.Scope().Attributes().PutStr("scope.only."+svc, "x")

		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(i + 1)}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(i + 1)}))
		sp.SetName(svc + ".handle")
		sp.Attributes().PutStr("http.method", "GET")
		sp.Attributes().PutStr("span.only."+svc, "x")
		sp.SetStartTimestamp(pcommon.Timestamp(ts.UnixNano()))
		sp.SetEndTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	}

	require.NoError(t, b.ConsumeTraces(ctx, td))

	return b, ts.Add(-time.Minute), ts.Add(time.Minute)
}

// TestTraceTagNamesMatchesScan is the differential test for the rewrite: for every scope the API can
// ask about, the enumeration-based answer must equal the scan-based one it replaced.
func TestTraceTagNamesMatchesScan(t *testing.T) {
	t.Parallel()

	b, start, end := tagNamesFixture(t)
	q := b.Traces()
	ctx := context.Background()

	for _, scope := range []traceql.AttributeScope{
		traceql.ScopeNone,
		traceql.ScopeResource,
		traceql.ScopeSpan,
		traceql.ScopeInstrumentation,
	} {
		t.Run(scope.String(), func(t *testing.T) {
			opts := tracestorage.TagNamesOptions{Scope: scope, Start: start, End: end}

			want, err := q.legacyTagNames(ctx, opts)
			require.NoError(t, err)
			require.NotEmpty(t, want, "the oracle must actually find something")

			got, err := q.TagNames(ctx, opts)
			require.NoError(t, err)

			assert.ElementsMatch(t, want, got)
		})
	}
}

// The scoped names the fixture pins, spelled out so a regression names the attribute it lost.
func TestTraceTagNamesPerScope(t *testing.T) {
	t.Parallel()

	b, start, end := tagNamesFixture(t)
	ctx := context.Background()

	names := func(scope traceql.AttributeScope) []string {
		t.Helper()

		got, err := b.Traces().TagNames(ctx, tracestorage.TagNamesOptions{Scope: scope, Start: start, End: end})
		require.NoError(t, err)

		out := make([]string, 0, len(got))
		for _, tn := range got {
			require.Equal(t, scope, tn.Scope)
			out = append(out, tn.Name)
		}

		return out
	}

	assert.ElementsMatch(t,
		[]string{"service.name", "deployment.environment", "resource.only.frontend", "resource.only.cart"},
		names(traceql.ScopeResource))
	assert.ElementsMatch(t,
		[]string{"library.tier", "scope.only.frontend", "scope.only.cart"},
		names(traceql.ScopeInstrumentation))
	assert.ElementsMatch(t,
		[]string{"http.method", "span.only.frontend", "span.only.cart"},
		names(traceql.ScopeSpan))
}

// The instrumentation scope name/version are intrinsics, not attributes. TraceKeys reports them as
// scope-scoped keys, so answering the identity half from it instead of from TraceSeries would invent
// tag names the scan never returned.
func TestTraceTagNamesExcludesScopeIntrinsics(t *testing.T) {
	t.Parallel()

	b, start, end := tagNamesFixture(t)

	got, err := b.Traces().TagNames(context.Background(), tracestorage.TagNamesOptions{Start: start, End: end})
	require.NoError(t, err)

	for _, tn := range got {
		assert.NotEqual(t, "otel.scope.name", tn.Name)
		assert.NotEqual(t, "otel.scope.version", tn.Name)
	}
}

// noFetchSource fails the test if the traces read seam is opened at all: TagNames must be answered
// from the key and stream enumerations, never by materializing spans.
type noFetchSource struct {
	Source

	t *testing.T
}

func (s noFetchSource) TraceFetcher(...signal.TenantID) fetch.Fetcher {
	s.t.Helper()
	s.t.Error("TagNames materialized spans: it opened the traces fetcher")

	return emptyFetcher{}
}

type emptyFetcher struct{}

func (emptyFetcher) Fetch(context.Context, fetch.Request) (fetch.Iterator, error) {
	return fetch.NewSliceIterator(nil), nil
}

// The regression this whole change exists for (oteldb#1335): the fetcher is never opened.
func TestTraceTagNamesDoesNotScan(t *testing.T) {
	t.Parallel()

	b, start, end := tagNamesFixture(t)
	guarded := NewQuery(noFetchSource{Source: b.src, t: t})

	for _, scope := range []traceql.AttributeScope{
		traceql.ScopeNone,
		traceql.ScopeResource,
		traceql.ScopeSpan,
		traceql.ScopeInstrumentation,
	} {
		got, err := guarded.Traces().TagNames(context.Background(),
			tracestorage.TagNamesOptions{Scope: scope, Start: start, End: end})
		require.NoError(t, err)
		assert.NotEmpty(t, got, "scope %s still enumerates names", scope)
	}
}
