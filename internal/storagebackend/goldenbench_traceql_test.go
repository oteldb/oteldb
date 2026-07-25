package storagebackend_test

// Golden TraceQL benchmarks — the stable set used to detect github.com/oteldb/storage performance
// regressions from the embedder's side. They are deliberately:
//
//   - Deterministic: one fixed synthetic trace corpus (no RNG), built once and reused read-only, so
//     run-to-run variance is the machine, not the data.
//   - Flushed: the corpus is flushed and compacted into an immutable part before any query runs, so
//     the benchmarks exercise the part-scan path (decode + filter), not the in-memory head.
//   - Comparable: b.SetBytes carries the LOGICAL (uncompressed) footprint of the spans a query must
//     scan, so MB/s is a real scan speed rather than a function of the codec's ratio.
//
// Keep the sub-benchmark names stable — they are the CI baseline, i.e. an API. Changing the corpus
// resets the historical baseline, so only do it deliberately.
//
// Two families live here:
//
//   - The unprefixed cases run the real TraceQL engine end-to-end (parse → SelectSpansets → filter →
//     tempoapi result). [storagebackend.TraceQuerier.SelectSpansets] lowers what it can of the
//     query's matchers to storage filters and materializes only the candidate traces; a case whose
//     predicate has no per-span column form (`{}`, the root intrinsics) still costs a full window
//     scan plus a full span materialization.
//   - The pushdown/… cases lower the same predicates to the storage fetch contract directly, without
//     the engine. They are the floor the end-to-end cases are measured against, and they isolate the
//     record engine's condition memoization (per-distinct-value for narrow int columns such as kind
//     and status_code, per-dictionary-entry for byte and attribute columns).

import (
	"context"
	"encoding/binary"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

const (
	// traceqlTraces is the number of traces in the golden corpus.
	traceqlTraces = 500
	// traceqlSpansPerTrace is the fixed span-tree size of every trace.
	traceqlSpansPerTrace = 8
	// traceqlRoutes is the http.route cardinality; a route filter selects traceqlTraces/traceqlRoutes
	// traces, i.e. a high-selectivity attribute lookup.
	traceqlRoutes = 64
	// traceqlErrorEvery makes every Nth trace fail, so `status = error` selects 1/N of the corpus.
	traceqlErrorEvery = 10
	// traceqlLimit is deliberately >= traceqlTraces, so no case is ever cut short by the engine's
	// post-filter limit and every want below is a true match count.
	traceqlLimit = traceqlTraces
	// traceqlSelectedRoute is the route the attribute-filter case selects.
	traceqlSelectedRoute = "/route/7"
	// traceqlRouteTraces is how many traces carry traceqlSelectedRoute (i%64 == 7 over 500 traces).
	traceqlRouteTraces = 8
	// traceqlComboRoute is the route used by the combined attribute+status case: it is the only
	// route that co-occurs with a failing trace (i%64 == 0 && i%10 == 0 ⇒ i ∈ {0, 320}).
	traceqlComboRoute = "/route/0"
	// traceqlComboSpans is the surviving span count of that combination: 2 traces × 2 failing spans.
	traceqlComboSpans = 4
)

// traceqlTenant is the tenant every ingested batch routes to (storagebackend.New has no tenant
// callback).
const traceqlTenant signal.TenantID = "default"

// traceqlStart is the fixed wall-clock origin of the corpus. A constant (not time.Now) keeps the
// encoded timestamps — and therefore the part layout — identical between runs.
var traceqlStart = time.Unix(1_600_000_000, 0).UTC()

// traceqlMethods is the http.request.method cycle; a method varies per (trace, span) so a method
// filter is neither trivially true nor trivially false per trace.
var traceqlMethods = [...]string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}

// traceqlSpanSpec is one node of the fixed span tree shared by every trace.
type traceqlSpanSpec struct {
	service string
	name    string
	kind    ptrace.SpanKind
	parent  int // index into traceqlShape, -1 for the root
	durMs   int
	fails   bool // carries StatusCodeError on a failing trace
}

// traceqlShape is the canonical trace: a frontend root fanning out to cart and checkout, with
// checkout calling payments — four services, a spread of kinds, and a two-level descendant chain so
// the structural operator has something to walk. The root's duration is far above every child's, so
// a duration comparison isolates roots.
var traceqlShape = [traceqlSpansPerTrace]traceqlSpanSpec{
	{service: "frontend", name: "GET /api/checkout", kind: ptrace.SpanKindServer, parent: -1, durMs: 200},
	{service: "frontend", name: "authorize", kind: ptrace.SpanKindInternal, parent: 0, durMs: 5},
	{service: "cart", name: "GET /cart", kind: ptrace.SpanKindClient, parent: 0, durMs: 20},
	{service: "cart", name: "cart.load", kind: ptrace.SpanKindServer, parent: 2, durMs: 15},
	{service: "checkout", name: "POST /checkout", kind: ptrace.SpanKindClient, parent: 0, durMs: 60},
	{service: "checkout", name: "checkout.process", kind: ptrace.SpanKindServer, parent: 4, durMs: 55},
	{service: "payments", name: "POST /pay", kind: ptrace.SpanKindClient, parent: 5, durMs: 40, fails: true},
	{service: "payments", name: "payments.charge", kind: ptrace.SpanKindServer, parent: 6, durMs: 35, fails: true},
}

// traceqlServices lists the corpus' resources in a stable order; each becomes one storage stream.
var traceqlServices = [...]string{"frontend", "cart", "checkout", "payments"}

// traceqlTraceID derives trace i's id: a fixed prefix plus the big-endian index, so ids are distinct,
// non-zero and reproducible.
func traceqlTraceID(i int) pcommon.TraceID {
	var id [16]byte
	binary.BigEndian.PutUint64(id[:8], 0x0705_0300_0000_0000)
	binary.BigEndian.PutUint64(id[8:], uint64(i)+1)

	return pcommon.TraceID(id)
}

// traceqlSpanID derives the id of span j of trace i.
func traceqlSpanID(i, j int) pcommon.SpanID {
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], uint64(i)<<8|uint64(j)+1)

	return pcommon.SpanID(id)
}

// traceqlCorpus builds the canonical trace set and returns it with its logical (uncompressed)
// footprint: the bytes a full scan must decode per span — ids, timestamps, the narrow int columns,
// the name, and the attribute key/value payload. Fully deterministic (no RNG).
func traceqlCorpus() (td ptrace.Traces, logical int64) {
	td = ptrace.NewTraces()

	spans := make(map[string]ptrace.SpanSlice, len(traceqlServices))
	for _, svc := range traceqlServices {
		rs := td.ResourceSpans().AppendEmpty()
		attrs := rs.Resource().Attributes()
		attrs.PutStr("service.name", svc)
		attrs.PutStr("deployment.environment", "production")
		attrs.PutStr("host.name", "host-"+svc)

		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("oteldb/goldenbench")
		ss.Scope().SetVersion("v1")
		spans[svc] = ss.Spans()
	}

	for i := range traceqlTraces {
		var (
			traceID = traceqlTraceID(i)
			base    = traceqlStart.Add(time.Duration(i) * time.Millisecond)
			route   = "/route/" + strconv.Itoa(i%traceqlRoutes)
			failing = i%traceqlErrorEvery == 0
		)

		for j, spec := range &traceqlShape {
			s := spans[spec.service].AppendEmpty()
			s.SetTraceID(traceID)
			s.SetSpanID(traceqlSpanID(i, j))
			if spec.parent >= 0 {
				s.SetParentSpanID(traceqlSpanID(i, spec.parent))
			}
			s.SetName(spec.name)
			s.SetKind(spec.kind)

			start := base.Add(time.Duration(j) * time.Millisecond)
			// The i%17 jitter is deterministic but keeps durations from being a single repeated value.
			dur := time.Duration(spec.durMs)*time.Millisecond + time.Duration(i%17)*time.Millisecond
			s.SetStartTimestamp(pcommon.Timestamp(start.UnixNano()))
			s.SetEndTimestamp(pcommon.Timestamp(start.Add(dur).UnixNano()))

			status := int64(200)
			if failing && spec.fails {
				s.Status().SetCode(ptrace.StatusCodeError)
				s.Status().SetMessage("payment declined by upstream")
				status = 500
			}

			method := traceqlMethods[(i+j)%len(traceqlMethods)]
			a := s.Attributes()
			a.PutStr("http.request.method", method)
			a.PutInt("http.response.status_code", status)
			a.PutStr("http.route", route)

			// 16 (trace id) + 8 (span id) + 8 (parent) + 8 (ts) + 8 (duration) + 8 (kind) + 8 (status).
			logical += 64
			logical += int64(len(spec.name) + len(method) + len(route) + 8)
			logical += int64(len("http.request.method") + len("http.response.status_code") + len("http.route"))
		}
	}

	return td, logical
}

// traceqlFixture is the shared, read-only benchmark fixture: a flushed+compacted store, the oteldb
// backend over it, a TraceQL engine, and the corpus' bounds.
type traceqlFixture struct {
	store   *storage.Storage
	backend *storagebackend.Backend
	querier *storagebackend.TraceQuerier
	engine  *traceqlengine.Engine

	logical    int64 // uncompressed bytes of the whole corpus
	perTrace   int64 // uncompressed bytes of one trace
	start, end time.Time
}

// traceqlNewFixture ingests the canonical corpus into a memory-backed store and flushes + compacts
// it, so every query below reads immutable parts rather than the head. opts configure the backend
// (the equivalence test builds a second one with the pushdown disabled).
func traceqlNewFixture(b testing.TB, opts ...storagebackend.Option) *traceqlFixture {
	b.Helper()

	ctx := context.Background()

	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(b, err)
	b.Cleanup(func() { _ = store.Close(ctx) })

	be := storagebackend.New(store, opts...)

	td, logical := traceqlCorpus()
	require.NoError(b, be.ConsumeTraces(ctx, td))

	admin := store.Admin()
	require.NoError(b, admin.Flush(ctx, traceqlTenant, signal.Trace))
	require.NoError(b, admin.Compact(ctx, traceqlTenant, signal.Trace))

	querier := be.Traces()

	return &traceqlFixture{
		store:    store,
		backend:  be,
		querier:  querier,
		engine:   traceqlengine.NewEngine(querier, traceqlengine.Options{}),
		logical:  logical,
		perTrace: logical / traceqlTraces,
		// Widen the window past the corpus: the engine drops any trace not fully inside it.
		start: traceqlStart.Add(-time.Minute),
		end:   traceqlStart.Add(time.Duration(traceqlTraces+traceqlSpansPerTrace)*time.Millisecond + time.Minute),
	}
}

// traceqlEvalParams is the fixed evaluation window shared by every TraceQL case.
func (f *traceqlFixture) evalParams() traceqlengine.EvalParams {
	return traceqlengine.EvalParams{Start: f.start, End: f.end, Limit: traceqlLimit}
}

// traceqlEval runs one TraceQL query and returns the number of matched traces.
func traceqlEval(b *testing.B, f *traceqlFixture, query string) int {
	b.Helper()

	res, err := f.engine.Eval(context.Background(), query, f.evalParams())
	if err != nil {
		b.Fatal(err)
	}

	return len(res.Traces)
}

// traceqlCase describes one end-to-end TraceQL sub-benchmark.
type traceqlCase struct {
	name  string
	query string
	// want is the exact number of traces the query must return. Asserting the count (not just
	// non-emptiness) keeps a silently-degenerate query from ever looking fast.
	want int
}

// traceqlCases is the golden query set. Every case is a distinct storage-relevant predicate shape:
// intrinsics over the narrow int columns (kind, status), the byte columns (name), the serialized
// attribute blob, a numeric comparison, a structural walk, and the unfiltered scan as the ceiling.
var traceqlCases = []traceqlCase{
	{name: "scan_all", query: `{}`, want: traceqlTraces},
	{name: "by_service", query: `{resource.service.name = "payments"}`, want: traceqlTraces},
	{name: "by_name", query: `{name = "checkout.process"}`, want: traceqlTraces},
	{name: "attr_route", query: `{span.http.route = "` + traceqlSelectedRoute + `"}`, want: traceqlRouteTraces},
	{name: "attr_status_code", query: `{span.http.response.status_code = 500}`, want: traceqlTraces / traceqlErrorEvery},
	{name: "status_error", query: `{status = error}`, want: traceqlTraces / traceqlErrorEvery},
	{name: "kind_server", query: `{kind = server}`, want: traceqlTraces},
	{name: "duration_gt", query: `{duration > 150ms}`, want: traceqlTraces},
	{
		name:  "attr_and_status",
		query: `{span.http.response.status_code = 500 && status = error}`,
		want:  traceqlTraces / traceqlErrorEvery,
	},
	// Two constraints shape this one. Both sides must match in *every* trace, because the engine's
	// descendant operator indexes a[0]/b[0] unconditionally and panics on a trace where only one
	// side matches — so a `>> {status = error}` shape would crash on the 90% of traces that do not
	// fail. And the ancestor walk only knows the parent links of the spans in left ∪ right, so the
	// chain must not pass through a span neither side selected; cart hangs directly off the
	// frontend root, payments does not.
	{
		name:  "descendant",
		query: `{resource.service.name = "frontend"} >> {resource.service.name = "cart"}`,
		want:  traceqlTraces,
	},
	// The relational family. Every operand is spelled from traceqlShape rather than a literal, so a
	// change to the corpus shape cannot silently turn one of these into an always-false query.
	//
	// rootName/rootServiceName are spanset-level intrinsics: the engine resolves the trace's root
	// once per trace (engine.go picks the parentless span) and every span of that trace then compares
	// against the same constant, so these are whole-corpus scans, not selectors.
	{
		name:  "root_name",
		query: `{rootName = "` + traceqlRootName + `"}`,
		want:  traceqlTraces,
	},
	{
		name:  "root_service_name",
		query: `{rootServiceName = "` + traceqlRootService + `"}`,
		want:  traceqlTraces,
	},
	// The same both-sides-must-match constraint as `descendant` applies to `~` and `>`: the engine
	// indexes a[0]/b[0] after checking only that *both* sides are empty, so a pair where one side
	// misses on some trace panics. authorize and GET /cart are both direct children of the root, and
	// checkout.process is the child of POST /checkout, in every single trace.
	{
		name:  "sibling",
		query: `{name = "` + traceqlSiblingLeft + `"} ~ {name = "` + traceqlSiblingRight + `"}`,
		want:  traceqlTraces,
	},
	{
		name:  "child",
		query: `{name = "` + traceqlChildParent + `"} > {name = "` + traceqlChildName + `"}`,
		want:  traceqlTraces,
	},
}

// The operands of the relational cases, taken from traceqlShape so they track the corpus.
//
// There is deliberately no `parent.<attr>` case: `parent.` scoped predicates parse (traceql.Attribute
// carries a Parent bool) but traceqlengine cannot build them — buildAttributeEvaluater's default
// branch does `if attr.Parent { break }` on a TODO, so the query fails with
// `unsupported attribute "parent.span.…"` before it ever reaches a span.
var (
	traceqlRootName    = traceqlShape[0].name // the parentless span's name
	traceqlRootService = traceqlShape[0].service
	// Two direct children of the root, hence siblings in every trace.
	traceqlSiblingLeft  = traceqlShape[1].name
	traceqlSiblingRight = traceqlShape[2].name
	// A parent/child pair present in every trace.
	traceqlChildParent = traceqlShape[4].name
	traceqlChildName   = traceqlShape[5].name
)

// traceqlPushdownCase describes one storage-level sub-benchmark: the same predicate lowered to a
// [fetch.Condition] instead of being applied by the TraceQL engine after materialization.
type traceqlPushdownCase struct {
	name string
	// conds builds the request's conditions.
	conds func() []fetch.Condition
	// want is the exact number of surviving spans.
	want int
}

// traceqlPushdownCases isolate the record engine's condition evaluation. status_code and kind are
// narrow int columns (per-distinct-value memo); name is a dictionary byte column and route/method
// are attribute lookups inside the serialized attrs blob (per-dictionary-entry memo).
var traceqlPushdownCases = []traceqlPushdownCase{
	{
		name:  "status_code",
		conds: func() []fetch.Condition { return []fetch.Condition{traceqlIntEq(sigtrace.ColStatusCode, 2)} },
		// Both payments spans of every failing trace.
		want: traceqlTraces / traceqlErrorEvery * 2,
	},
	{
		name: "kind",
		conds: func() []fetch.Condition {
			return []fetch.Condition{traceqlIntEq(sigtrace.ColKind, int64(ptrace.SpanKindServer))}
		},
		want: traceqlTraces * 4, // four server spans per trace
	},
	{
		name:  "name",
		conds: func() []fetch.Condition { return []fetch.Condition{traceqlStrEq(sigtrace.ColName, "checkout.process")} },
		want:  traceqlTraces,
	},
	{
		name:  "attr_route",
		conds: func() []fetch.Condition { return []fetch.Condition{traceqlStrEq("http.route", traceqlSelectedRoute)} },
		want:  traceqlRouteTraces * traceqlSpansPerTrace,
	},
	{
		name: "attr_status_code",
		conds: func() []fetch.Condition {
			return []fetch.Condition{traceqlIntEq("http.response.status_code", 500)}
		},
		want: traceqlTraces / traceqlErrorEvery * 2,
	},
	{
		name: "attr_and_status",
		conds: func() []fetch.Condition {
			return []fetch.Condition{
				traceqlStrEq("http.route", traceqlComboRoute),
				traceqlIntEq(sigtrace.ColStatusCode, 2),
			}
		},
		want: traceqlComboSpans,
	},
}

// traceqlIntEq builds an exact integer condition over a column (or attribute key).
func traceqlIntEq(column string, want int64) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return v.Kind() == signal.KindInt && v.Int() == want },
	}
}

// traceqlStrEq builds an exact string condition over a column (or attribute key).
func traceqlStrEq(column, want string) fetch.Condition {
	return fetch.Condition{
		Column: column,
		Match:  func(v signal.Value) bool { return v.Kind() == signal.KindStr && string(v.Str()) == want },
	}
}

// traceqlFetchRows runs one filtered fetch over the whole corpus window and returns the surviving
// row count.
func traceqlFetchRows(b *testing.B, f *traceqlFixture, conds []fetch.Condition) int {
	b.Helper()

	ctx := context.Background()
	it, err := f.store.TraceFetcher(traceqlTenant).Fetch(ctx, fetch.Request{
		Tenant:        traceqlTenant,
		Signal:        signal.Trace,
		Start:         f.start.UnixNano(),
		End:           f.end.UnixNano(),
		Conditions:    conds,
		AllConditions: true,
	})
	if err != nil {
		b.Fatal(err)
	}

	batches, err := fetch.Drain(ctx, it)
	if err != nil {
		b.Fatal(err)
	}

	rows := 0
	for _, batch := range batches {
		rows += len(batch.Timestamps)
	}

	return rows
}

// BenchmarkGoldenTraceQL is the definitive TraceQL-over-storage set.
//
//	by_trace_id      — the high-selectivity trace-by-id lookup (equality bloom + SIMD equality scan)
//	scan_all         — the unfiltered window scan: the ceiling every other case is measured against
//	by_service       — a resource-identity selector (one stream out of four)
//	by_name          — an intrinsic over the dictionary-encoded name column
//	attr_route       — a high-selectivity span-attribute lookup
//	attr_status_code — an int span-attribute lookup
//	status_error     — the status intrinsic (narrow int column)
//	kind_server      — the kind intrinsic (narrow int column)
//	duration_gt      — a numeric comparison outside the int memo's domain
//	attr_and_status  — a selective attribute filter ANDed with a status filter
//	descendant       — the structural walk (frontend root >> payments descendant)
//
// The relational family, all engine-side (SelectSpansets pushes nothing down):
//
//	root_name         — the rootName intrinsic: a per-trace constant resolved from the parentless span
//	root_service_name — the rootServiceName intrinsic, resolved from that span's resource
//	sibling           — the `~` operator over two direct children of the root
//	child             — the `>` operator over a parent/child pair present in every trace
//
//	pushdown/…       — the same predicates lowered to the storage fetch contract
func BenchmarkGoldenTraceQL(b *testing.B) {
	f := traceqlNewFixture(b)

	b.Run("by_trace_id", func(b *testing.B) { benchTraceQLByTraceID(b, f) })

	for _, tc := range traceqlCases {
		b.Run(tc.name, func(b *testing.B) {
			if got := traceqlEval(b, f, tc.query); got != tc.want {
				b.Fatalf("%s matched %d traces, want %d", tc.query, got, tc.want)
			}

			b.SetBytes(f.logical)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				traceqlEval(b, f, tc.query)
			}
		})
	}

	for _, tc := range traceqlPushdownCases {
		b.Run("pushdown/"+tc.name, func(b *testing.B) {
			conds := tc.conds()
			if got := traceqlFetchRows(b, f, conds); got != tc.want {
				b.Fatalf("pushdown/%s matched %d spans, want %d", tc.name, got, tc.want)
			}

			b.SetBytes(f.logical)
			b.ReportAllocs()
			b.ResetTimer()

			for range b.N {
				traceqlFetchRows(b, f, conds)
			}
		})
	}
}

// benchTraceQLByTraceID measures the trace-by-id lookup: a single equality condition on the trace_id
// column, pruned by its per-part equality bloom. It is the one trace read path that storage filters
// itself, so it is the most direct signal for the equality fast path.
func benchTraceQLByTraceID(b *testing.B, f *traceqlFixture) {
	ctx := context.Background()
	id := otelstorage.TraceID(traceqlTraceID(traceqlTraces / 2))

	lookup := func() int {
		it, err := f.querier.TraceByID(ctx, id, tracestorage.TraceByIDOptions{Start: f.start, End: f.end})
		if err != nil {
			b.Fatal(err)
		}

		var (
			span tracestorage.Span
			n    int
		)
		for it.Next(&span) {
			n++
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
		if err := it.Close(); err != nil {
			b.Fatal(err)
		}

		return n
	}

	if got := lookup(); got != traceqlSpansPerTrace {
		b.Fatalf("trace by id returned %d spans, want %d", got, traceqlSpansPerTrace)
	}

	b.SetBytes(f.perTrace)
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		lookup()
	}
}
