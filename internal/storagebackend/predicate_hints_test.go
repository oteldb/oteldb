package storagebackend_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

type (
	hintPeerRead struct {
		request fetch.Request
		rows    int
	}
	hintPeerSource struct {
		storagebackend.Source
		log, trace fetch.Fetcher
		mu         sync.Mutex
		reads      []hintPeerRead
	}
)

func (s *hintPeerSource) LogFetcher(...signal.TenantID) fetch.Fetcher   { return s.log }
func (s *hintPeerSource) TraceFetcher(...signal.TenantID) fetch.Fetcher { return s.trace }
func (s *hintPeerSource) snapshot() []hintPeerRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads)
}

func newHintPeer(tb testing.TB, store *storage.Storage, capture bool) *hintPeerSource {
	tb.Helper()
	src := &hintPeerSource{Source: store}
	srv := httptest.NewServer(cluster.NewReadHandler(func(ctx context.Context, r fetch.Request) ([]*fetch.Batch, error) {
		input := store.LogFetcher(r.Tenant)
		if r.Signal == signal.Trace {
			input = store.TraceFetcher(r.Tenant)
		}
		it, err := input.Fetch(ctx, r)
		if err != nil {
			return nil, err
		}
		batches, err := fetch.Drain(ctx, it)
		if err != nil {
			return nil, err
		}
		if capture {
			read := hintPeerRead{request: r}
			for _, batch := range batches {
				read.rows += len(batch.Timestamps)
			}
			src.mu.Lock()
			src.reads = append(src.reads, read)
			src.mu.Unlock()
		}
		return batches, nil
	}))
	tb.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	src.log = fetch.Filter(cluster.NewRemoteFetcher(signal.Log, addr, srv.Client()))
	src.trace = fetch.Filter(cluster.NewRemoteFetcher(signal.Trace, addr, srv.Client()))
	return src
}

func hintLogFixture(tb testing.TB) (*storage.Storage, logqlengine.EvalParams) {
	tb.Helper()
	ctx := context.Background()
	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, store.Close(ctx)) })
	ld := plog.NewLogs()
	rs := ld.ResourceLogs().AppendEmpty()
	rs.Resource().Attributes().PutStr("job", "api")
	records := rs.ScopeLogs().AppendEmpty().LogRecords()
	base := time.Unix(1_700_000_000, 0).UTC()
	for i := range 1000 {
		r := records.AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		body := "noise"
		if i == 0 {
			body = "foo"
		}
		if i == 1 {
			body = "bar"
		}
		r.Body().SetStr(body)
		r.Attributes().PutStr("payload", strings.Repeat("x", 1024))
	}
	require.NoError(tb, storagebackend.New(store).ConsumeLogs(ctx, ld))
	require.NoError(tb, store.Admin().Flush(ctx, "default", signal.Log))
	return store, logqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(2 * time.Second), Limit: 1000}
}

func TestFiniteLineHintsReachPeer(t *testing.T) {
	t.Parallel()
	store, params := hintLogFixture(t)
	src := newHintPeer(t, store, true)
	engine, err := logqlengine.NewEngine(storagebackend.NewQuery(src).Logs(), logqlengine.Options{Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}})
	require.NoError(t, err)
	query, err := engine.NewQuery(t.Context(), `{job="api"} |~ "^(foo|bar)$"`)
	require.NoError(t, err)
	got, err := query.Eval(t.Context(), params)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"line:foo", "line:bar"}, normalize(t, got))
	reads := src.snapshot()
	require.Len(t, reads, 1)
	require.Equal(t, 2, reads[0].rows, "the owner must filter finite alternatives before returning record payloads")
	require.Len(t, reads[0].request.Conditions, 1)
	require.Equal(t, fetch.AnyEqualStrings([]string{"bar", "foo"}), reads[0].request.Conditions[0].AnyEqual)
}

func TestTraceHintsReachPeer(t *testing.T) {
	t.Parallel()
	f := traceqlNewFixture(t)
	for _, tt := range []struct {
		name, query                    string
		firstRows, secondRows, setSize int
	}{
		{name: "resolved_ids", query: `{span.http.route = "/route/7"}`, firstRows: traceqlRouteTraces * traceqlSpansPerTrace, secondRows: traceqlRouteTraces * traceqlSpansPerTrace, setSize: traceqlRouteTraces},
		{name: "literal_message", query: `{statusMessage = "payment declined by upstream"}`, firstRows: 100, secondRows: 400, setSize: 50},
		{name: "finite_message", query: `{statusMessage =~ "^(payment declined by upstream|not present)$"}`, firstRows: 100, secondRows: 400, setSize: 50},
	} {
		t.Run(tt.name, func(t *testing.T) {
			src := newHintPeer(t, f.store, true)
			engine := traceqlengine.NewEngine(storagebackend.NewQuery(src).Traces(), traceqlengine.Options{})
			got, err := engine.Eval(t.Context(), tt.query, f.evalParams())
			require.NoError(t, err)
			want, err := f.engine.Eval(t.Context(), tt.query, f.evalParams())
			require.NoError(t, err)
			require.Equal(t, want, got)
			require.NotEmpty(t, got.Traces)
			reads := src.snapshot()
			require.Len(t, reads, 2)
			require.Equal(t, tt.firstRows, reads[0].rows)
			require.Equal(t, tt.secondRows, reads[1].rows, "the owner must apply the resolved trace set before returning spans")
			require.Len(t, reads[1].request.Conditions, 1)
			require.Len(t, reads[1].request.Conditions[0].AnyEqual, tt.setSize)
			for _, id := range reads[1].request.Conditions[0].AnyEqual {
				require.Len(t, id, 16)
			}
		})
	}
}

func BenchmarkExactPredicateHints(b *testing.B) {
	b.Run("log_finite_regexp", func(b *testing.B) {
		store, params := hintLogFixture(b)
		engine, err := logqlengine.NewEngine(storagebackend.New(store).Logs(), logqlengine.Options{Optimizers: []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}}})
		require.NoError(b, err)
		query, err := engine.NewQuery(b.Context(), `{job="api"} |~ "^(foo|bar)$"`)
		require.NoError(b, err)
		b.ReportAllocs()
		for b.Loop() {
			result, err := query.Eval(b.Context(), params)
			if err != nil {
				b.Fatal(err)
			}
			if logqlResultCount(result) != 2 {
				b.Fatal("unexpected result count")
			}
		}
	})
	b.Run("trace_ids_rpc", func(b *testing.B) {
		f := traceqlNewFixture(b, storagebackend.WithTraceQLPushdown(true))
		src := newHintPeer(b, f.store, false)
		engine := traceqlengine.NewEngine(storagebackend.NewQuery(src).Traces(), traceqlengine.Options{})
		b.ReportAllocs()
		for b.Loop() {
			result, err := engine.Eval(b.Context(), fmt.Sprintf(`{span.http.route = %q}`, traceqlSelectedRoute), f.evalParams())
			if err != nil {
				b.Fatal(err)
			}
			if len(result.Traces) != traceqlRouteTraces {
				b.Fatal("unexpected result count")
			}
		}
	})
}
