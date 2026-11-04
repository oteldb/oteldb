package storagebackend_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/backend/file"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

func TestTraceExtentRetainedSpans(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"head", "mixed", "flushed", "reopened", "merged", "routed"} {
		t.Run(stage, func(t *testing.T) {
			be := backend.Memory()
			if stage == "reopened" {
				var err error
				be, err = file.New(t.TempDir())
				require.NoError(t, err)
			}
			store, err := storage.Open(t.Context(), storage.Options{}, storage.WithBackend(be))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
			base := time.Unix(1_700_000_000, 0).UTC()
			var memory traceqlengine.MemoryQuerier
			ingest := func(late bool) {
				t.Helper()
				td := ptrace.NewTraces()
				rs := td.ResourceSpans().AppendEmpty()
				service := "frontend"
				if late {
					service = "payments"
				}
				rs.Resource().Attributes().PutStr("service.name", service)
				ss := rs.ScopeSpans().AppendEmpty()
				for i, offsets := range [][2]time.Duration{{0, 2 * time.Second}, {4 * time.Second, 4100 * time.Millisecond}, {-3 * time.Second, time.Second}, {6 * time.Second, 12 * time.Second}} {
					index := 0
					if late {
						index = 1
					}
					s := ss.Spans().AppendEmpty()
					s.SetTraceID(pcommon.TraceID{15: byte(i + 1)})
					s.SetSpanID(pcommon.SpanID{7: byte(index + 1)})
					s.SetName("root")
					if late {
						s.SetParentSpanID(pcommon.SpanID{7: 1})
						s.SetName("child")
					}
					duration := time.Second
					if i == 1 {
						duration = 200 * time.Millisecond
					}
					s.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(offsets[index])))
					s.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(offsets[index] + duration)))
					s.Attributes().PutStr("payload", "attribute")
					s.Events().AppendEmpty().SetName("event")
					memory.Add(tracestorage.NewSpanFromOTEL(uuid.Nil, rs.Resource(), ss.Scope(), s))
				}
				require.NoError(t, storagebackend.New(store).ConsumeTraces(t.Context(), td))
			}
			ingest(false)
			if stage != "head" {
				require.NoError(t, store.Admin().Flush(t.Context(), "default", signal.Trace))
			}
			ingest(true)
			if stage != "head" && stage != "mixed" {
				require.NoError(t, store.Admin().Flush(t.Context(), "default", signal.Trace))
			}
			if stage == "merged" {
				require.NoError(t, store.Admin().CompactNow(t.Context(), "default", signal.Trace))
			}
			if stage == "reopened" {
				require.NoError(t, store.Close(t.Context()))
				store, err = storage.Open(t.Context(), storage.Options{}, storage.WithBackend(be))
				require.NoError(t, err)
			}
			var src storagebackend.Source = store
			if stage == "routed" {
				server := httptest.NewServer(cluster.NewReadHandler(func(ctx context.Context, r fetch.Request) ([]*fetch.Batch, error) {
					it, err := store.TraceFetcher(r.Tenant).Fetch(ctx, r)
					if err != nil {
						return nil, err
					}
					return fetch.Drain(ctx, it)
				}))
				t.Cleanup(server.Close)
				src = extentRoutedSource{Source: store, fetcher: fetch.Filter(cluster.NewRemoteFetcher(signal.Trace, strings.TrimPrefix(server.URL, "http://"), server.Client()))}
			}
			reference := traceqlengine.NewEngine(&memory, traceqlengine.Options{})
			for _, tt := range []struct {
				name, query string
				min, max    time.Duration
				count       int
			}{
				{"bare", `{}`, 0, 0, 2},
				{"gap", `{traceDuration > 2s}`, 0, 0, 1},
				{"inclusive", `{traceDuration >= 3s && traceDuration <= 3s}`, 0, 0, 1},
				{"upper", `{traceDuration < 1s}`, 0, 0, 1},
				{"exclusive", `{traceDuration > 3s}`, 0, 0, 0},
				{"minimum", `{}`, 2 * time.Second, 0, 1},
				{"maximum", `{}`, 0, time.Second, 1},
				{"bounded", `{}`, 3 * time.Second, 3 * time.Second, 1},
				{"constant_field_union", `{traceDuration > 10s || true}`, 0, 0, 2},
				{"constant_spanset_union", `{traceDuration > 10s} || {true}`, 0, 0, 2},
				{"boolean_wrapper", `{(traceDuration > 10s) = false}`, 0, 0, 2},
				{"profiled_constant_union", `{name = "root" && (traceDuration > 10s || true)}`, 0, 0, 2},
				{"profiled_boolean_wrapper", `{name = "root" && ((traceDuration > 10s) = false)}`, 0, 0, 2},
				{"mixed_union", `{traceDuration > 2s || name = "root"}`, 0, 0, 2},
				{"extent_union", `{traceDuration > 2s || traceDuration < 1s}`, 0, 0, 2},
				{"span_conjunction", `{name = "child" && traceDuration > 2s}`, 0, 0, 1},
			} {
				t.Run(tt.name, func(t *testing.T) {
					params := traceqlengine.EvalParams{Start: base, End: base.Add(10 * time.Second), MinDuration: tt.min, MaxDuration: tt.max, Limit: 20}
					source := &traceProjectionSource{Source: src}
					engine := traceqlengine.NewEngine(storagebackend.NewQuery(source).Traces(), traceqlengine.Options{})
					got, err := engine.Eval(t.Context(), tt.query, params)
					require.NoError(t, err)
					want, err := reference.Eval(t.Context(), tt.query, params)
					require.NoError(t, err)
					require.Len(t, got.Traces, tt.count)
					require.Equal(t, want, got)
					require.Len(t, source.requests, 2, "candidate resolution precedes one retained-trace materialization")
					for _, request := range source.requests {
						require.NotEqual(t, []string{sigtrace.ColTraceID, sigtrace.ColDuration}, request.Projection, "duration must not add a separate timing scan")
					}
					last := source.requests[len(source.requests)-1]
					require.Equal(t, int64(-1<<63), last.Start)
					require.Equal(t, int64(1<<63-1), last.End)
					if tt.name == "gap" {
						for i := byte(1); i <= 4; i++ {
							id := pcommon.TraceID{15: i}
							require.True(t, last.Conditions[0].Match(signal.StringValue(id[:])), "duration is evaluated after candidate materialization")
						}
					}
				})
			}
		})
	}
}

type extentRoutedSource struct {
	storagebackend.Source
	fetcher fetch.Fetcher
}

func (s extentRoutedSource) TraceFetcher(...signal.TenantID) fetch.Fetcher { return s.fetcher }

func TestTraceExtentUnboundedBareNoPrepass(t *testing.T) {
	t.Parallel()
	store, params := projectionTraceFixture(t)
	params.Start, params.End = time.Time{}, time.Time{}
	source := &traceProjectionSource{Source: store}
	engine := traceqlengine.NewEngine(storagebackend.NewQuery(source).Traces(), traceqlengine.Options{})
	got, err := engine.Eval(t.Context(), `{}`, params)
	require.NoError(t, err)
	require.Len(t, got.Traces, 2)
	require.Len(t, source.requests, 1)
}

func TestTraceExtentDirectSecondaryBounds(t *testing.T) {
	t.Parallel()
	store, window := projectionTraceFixture(t)
	base := window.Start.Add(time.Second)
	late := ptrace.NewTraces()
	rs := late.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "late")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(pcommon.TraceID{15: 1})
	span.SetSpanID(pcommon.SpanID{7: 3})
	span.SetParentSpanID(pcommon.SpanID{7: 1})
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(2 * time.Second)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(3 * time.Second)))
	require.NoError(t, storagebackend.New(store).ConsumeTraces(t.Context(), late))
	for _, tt := range []struct {
		name     string
		params   traceqlengine.SelectSpansetsParams
		id       byte
		requests int
	}{
		{"containment", traceqlengine.SelectSpansetsParams{Start: window.Start, End: window.End}, 2, 2},
		{"minimum", traceqlengine.SelectSpansetsParams{MinDuration: 2 * time.Second}, 1, 1},
		{"maximum", traceqlengine.SelectSpansetsParams{MaxDuration: time.Second}, 2, 1},
		{"inclusive", traceqlengine.SelectSpansetsParams{MinDuration: 3 * time.Second, MaxDuration: 3 * time.Second}, 1, 1},
		{"none", traceqlengine.SelectSpansetsParams{MaxDuration: time.Millisecond}, 0, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := &traceProjectionSource{Source: store}
			querier := storagebackend.NewQuery(source).Traces()
			iterator, err := querier.SelectSpansets(t.Context(), tt.params)
			require.NoError(t, err)
			defer func() { require.NoError(t, iterator.Close()) }()
			var trace traceqlengine.Trace
			if tt.id != 0 {
				require.True(t, iterator.Next(&trace))
				require.Equal(t, tt.id, trace.TraceID[15])
			}
			require.False(t, iterator.Next(&trace))
			require.NoError(t, iterator.Err())
			require.Len(t, source.requests, tt.requests)
		})
	}
}
