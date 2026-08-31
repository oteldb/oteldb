package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

// tagValueFixture ingests two services, each with its own scope, span attributes and status message,
// so every routing branch of TagValues has something distinct to find.
func tagValueFixture(t *testing.T) *storagebackend.Backend {
	t.Helper()

	ctx := context.Background()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)
	ts := time.Now().Truncate(time.Second)

	td := ptrace.NewTraces()

	for i, svc := range []string{"frontend", "cart"} {
		rs := td.ResourceSpans().AppendEmpty()
		rs.Resource().Attributes().PutStr("service.name", svc)
		rs.Resource().Attributes().PutStr("deployment.environment", "prod")

		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("otelhttp")
		ss.Scope().SetVersion("1.2.3")
		ss.Scope().Attributes().PutStr("library.tier", "http")

		sp := ss.Spans().AppendEmpty()
		sp.SetTraceID(pcommon.TraceID([16]byte{byte(i + 1)}))
		sp.SetSpanID(pcommon.SpanID([8]byte{byte(i + 1)}))
		sp.SetName(svc + ".handle")
		sp.Attributes().PutStr("http.method", "GET")
		sp.Status().SetMessage("boom-" + svc)
		sp.SetStartTimestamp(pcommon.Timestamp(ts.UnixNano()))
		sp.SetEndTimestamp(pcommon.Timestamp(ts.Add(time.Second).UnixNano()))
	}

	require.NoError(t, b.ConsumeTraces(ctx, td))

	return b
}

func tagValues(t *testing.T, b *storagebackend.Backend, attr traceql.Attribute) []string {
	t.Helper()

	iter, err := b.Traces().TagValues(context.Background(), attr, tracestorage.TagValuesOptions{})
	require.NoError(t, err)

	t.Cleanup(func() { _ = iter.Close() })

	var out []string

	require.NoError(t, iterators.ForEach(iter, func(tag tracestorage.Tag) error {
		out = append(out, tag.Value)

		return nil
	}))

	return out
}

// The reported bug: service.name is a resource attribute, so it used to fall through to a scan of
// every span in the window to produce a handful of strings.
func TestTraceTagValuesResourceScope(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.Equal(t, []string{"cart", "frontend"},
		tagValues(t, b, traceql.Attribute{Name: "service.name", Scope: traceql.ScopeResource}))
	assert.Equal(t, []string{"prod"},
		tagValues(t, b, traceql.Attribute{Name: "deployment.environment", Scope: traceql.ScopeResource}))
}

func TestTraceTagValuesInstrumentationScope(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.Equal(t, []string{"http"},
		tagValues(t, b, traceql.Attribute{Name: "library.tier", Scope: traceql.ScopeInstrumentation}))
}

// The shape Grafana's autocomplete actually sends: no scope at all. It must union the per-record
// attribute dictionary with the stream identities, because dropping either half silently hides
// values that exist.
func TestTraceTagValuesUnscopedUnionsBothHalves(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.Equal(t, []string{"cart", "frontend"},
		tagValues(t, b, traceql.Attribute{Name: "service.name"}), "the resource half")
	assert.Equal(t, []string{"GET"},
		tagValues(t, b, traceql.Attribute{Name: "http.method"}), "the span half")
}

func TestTraceTagValuesSpanScopeUnchanged(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.Equal(t, []string{"GET"},
		tagValues(t, b, traceql.Attribute{Name: "http.method", Scope: traceql.ScopeSpan}))
	assert.Empty(t, tagValues(t, b, traceql.Attribute{Name: "service.name", Scope: traceql.ScopeSpan}),
		"a resource attribute is not a span attribute")
}

func TestTraceTagValuesStatusMessage(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.ElementsMatch(t, []string{"boom-cart", "boom-frontend"},
		tagValues(t, b, traceql.Attribute{Prop: traceql.SpanStatusMessage}),
		"status message has its own column, so it needs no scan")
}

func TestTraceTagValuesInstrumentationIntrinsics(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.Equal(t, []string{"otelhttp"},
		tagValues(t, b, traceql.Attribute{Prop: traceql.InstrumentationName}))
	assert.Equal(t, []string{"1.2.3"},
		tagValues(t, b, traceql.Attribute{Prop: traceql.InstrumentationVersion}))
}

// These used to scan the whole window and then return nothing, because the generic fallback only
// visits attribute maps and an intrinsic's Name is empty. Two of them are also unbounded
// cardinality: enumerating trace ids would build a set the size of the window.
func TestTraceTagValuesUnenumerableIntrinsics(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	for _, prop := range []traceql.SpanProperty{
		traceql.SpanDuration, traceql.SpanChildCount, traceql.SpanParent, traceql.TraceDuration,
		traceql.NestedSetLeft, traceql.NestedSetRight, traceql.NestedSetParent,
		traceql.EventTimeSinceStart,
		traceql.SpanID, traceql.ParentID, traceql.TraceID,
		traceql.LinkTraceID, traceql.LinkSpanID,
	} {
		assert.Empty(t, tagValues(t, b, traceql.Attribute{Prop: prop}),
			"prop %v yields no autocomplete values", prop)
	}
}

// The intrinsics and scopes that still work exactly as before.
func TestTraceTagValuesEnumerableIntrinsicsUnchanged(t *testing.T) {
	t.Parallel()

	b := tagValueFixture(t)

	assert.ElementsMatch(t, []string{"cart.handle", "frontend.handle"},
		tagValues(t, b, traceql.Attribute{Prop: traceql.SpanName}))
	assert.ElementsMatch(t, []string{"unset", "ok", "error"},
		tagValues(t, b, traceql.Attribute{Prop: traceql.SpanStatus}))
	assert.ElementsMatch(t, []string{"frontend", "cart"},
		tagValues(t, b, traceql.Attribute{Prop: traceql.RootServiceName}))
}
