package scarecrow_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/oteldb/oteldb/internal/scarecrow"
)

// recordSpans runs qs as a range query against a recording tracer and returns the spans it
// produced.
func recordSpans(
	t *testing.T, opts scarecrow.Opts, store storage.Queryable, qs string,
	start, end time.Time,
) tracetest.SpanStubs {
	const step = time.Minute

	t.Helper()

	rec := tracetest.NewSpanRecorder()
	opts.TracerProvider = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	q, err := scarecrow.NewEngine(opts).NewRangeQuery(context.Background(), store, nil, qs, start, end, step)
	require.NoError(t, err)

	defer q.Close()

	require.NoError(t, q.Exec(context.Background()).Err)

	return tracetest.SpanStubsFromReadOnlySpans(rec.Ended())
}

// spansNamed returns the recorded spans with the given name.
func spansNamed(spans tracetest.SpanStubs, name string) []tracetest.SpanStub {
	var out []tracetest.SpanStub

	for _, s := range spans {
		if s.Name == name {
			out = append(out, s)
		}
	}

	return out
}

// attrInt reads an int attribute off a span, failing if it is absent.
func attrInt(t *testing.T, s tracetest.SpanStub, key string) int64 {
	t.Helper()

	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			require.Equal(t, attribute.INT64, kv.Value.Type(), "attribute %s", key)

			return kv.Value.AsInt64()
		}
	}

	t.Fatalf("span %q has no attribute %q", s.Name, key)

	return 0
}

// TestTracingDescribesTheQuery pins the spans a plain range query emits. The root has to carry
// enough to identify the query without the operator spans, because that is all a sampled trace
// may keep.
func TestTracingDescribesTheQuery(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	spans := recordSpans(t, scarecrow.Opts{}, store,
		`sum by (job) (http_requests)`, time.Unix(0, 0), time.Unix(600, 0))

	root := spansNamed(spans, "scarecrow.Exec")
	require.Len(t, root, 1)
	require.EqualValues(t, 11, attrInt(t, root[0], "promql.steps"))

	require.Len(t, spansNamed(spans, "scarecrow.Plan"), 1)
	require.NotEmpty(t, spansNamed(spans, "scarecrow.Series"), "the selector must be visible")

	// Unchunked queries must not emit a chunk span wrapping their only chunk.
	require.Empty(t, spansNamed(spans, "scarecrow.Chunk"))
}

// TestTracingShowsPerWindowCallCount is the regression guard for the pathology that motivated
// this instrumentation: a pushdown issuing one storage call per step, which read as a cheap
// index lookup and was ~240x slower than no pushdown at all on a 241-step grid.
//
// It asserts the trace makes the call *count* visible, not merely the total duration — a single
// slow span looks like slow storage, whereas "241 calls" names the bug outright.
func TestTracingShowsPerWindowCallCount(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	var scanner *pushdownScanner

	// perWindowScanner drops the grid capability, forcing the per-step path.
	spans := recordSpans(t, scarecrow.Opts{
		NewScanner: func(q storage.Queryable) scarecrow.Scanner {
			scanner = &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}

			return perWindowScanner{s: scanner}
		},
	}, store, `count by (job) (http_requests)`, time.Unix(0, 0), time.Unix(600, 0))

	perWindow := spansNamed(spans, "scarecrow.CountSeriesBy.PerWindow")
	require.Len(t, perWindow, 1)

	// 11 steps, 11 storage calls — the number the trace has to surface.
	require.EqualValues(t, 11, attrInt(t, perWindow[0], "promql.calls"))
	require.EqualValues(t, 11, scanner.groupCount.Load())

	require.Empty(t, spansNamed(spans, "scarecrow.AggregateGrid"))
}

// TestTracingShowsGridPushdown is the other half: with the capability present the same query
// makes exactly one storage call, and the trace says so.
func TestTracingShowsGridPushdown(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	spans := recordSpans(t, scarecrow.Opts{
		NewScanner: func(q storage.Queryable) scarecrow.Scanner {
			return &pushdownScanner{Scanner: scarecrow.NewQueryableScanner(q)}
		},
	}, store, `count by (job) (http_requests)`, time.Unix(0, 0), time.Unix(600, 0))

	grid := spansNamed(spans, "scarecrow.AggregateGrid")
	require.Len(t, grid, 1, "one grid call for the whole range")
	require.EqualValues(t, 11, attrInt(t, grid[0], "promql.steps"))

	require.Empty(t, spansNamed(spans, "scarecrow.CountSeriesBy.PerWindow"))
}

// TestTracingSpansChunks checks the M16 chunk spans appear, and that each reports its own step
// count — the trace is where an unexpectedly chunked query becomes visible.
func TestTracingSpansChunks(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	spans := recordSpans(t, scarecrow.Opts{ChunkSteps: 4}, store,
		`sum by (job) (http_requests)`, time.Unix(0, 0), time.Unix(600, 0))

	chunks := spansNamed(spans, "scarecrow.Chunk")
	require.Len(t, chunks, 3, "11 steps at 4 per chunk")

	total := int64(0)
	for _, c := range chunks {
		require.EqualValues(t, 3, attrInt(t, c, "promql.chunks"))
		total += attrInt(t, c, "promql.steps")
	}

	require.EqualValues(t, 11, total, "chunks must cover every step exactly once")
}

// TestTracingIsOptional guards the nil-tracer path: an EvalContext built without a tracer, which
// every test double and embedder does, must not panic.
func TestTracingIsOptional(t *testing.T) {
	t.Parallel()

	store := promqltest.LoadedStorage(t, pushdownData)
	defer func() { require.NoError(t, store.Close()) }()

	e := scarecrow.NewEngine(scarecrow.Opts{})

	q, err := e.NewInstantQuery(context.Background(), store, nil, `sum(http_requests)`, time.Unix(60, 0))
	require.NoError(t, err)
	defer q.Close()

	require.NoError(t, q.Exec(context.Background()).Err)
}
