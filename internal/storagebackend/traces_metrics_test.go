package storagebackend_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"
	sigtrace "github.com/oteldb/storage/signal/trace"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

type (
	metricTraceRead struct {
		projection []string
		budget     *readbudget.Budget
		remaining  int64
	}
	metricTraceSpy struct {
		storagebackend.Source
		reads []metricTraceRead
	}
)

func (s *metricTraceSpy) TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return metricTraceFetcher{Fetcher: s.Source.TraceFetcher(tenants...), spy: s}
}

type metricTraceFetcher struct {
	fetch.Fetcher
	spy *metricTraceSpy
}

func (f metricTraceFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	budget := readbudget.From(ctx)
	var remaining int64
	if budget != nil {
		remaining = budget.Remaining()
	}
	f.spy.reads = append(f.spy.reads, metricTraceRead{projection: slices.Clone(r.Projection), budget: budget, remaining: remaining})
	return f.Fetcher.Fetch(ctx, r)
}

func TestTraceMetricsProjectedReadsShareBudget(t *testing.T) {
	t.Parallel()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	base := time.Unix(1_700_000_010, 0).UTC()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "checkout")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := range 2 {
		s := spans.AppendEmpty()
		s.SetTraceID(pcommon.TraceID{15: 1})
		s.SetSpanID(pcommon.SpanID{7: byte(i + 1)})
		s.SetStartTimestamp(pcommon.NewTimestampFromTime(base.Add(time.Second)))
		s.SetEndTimestamp(pcommon.NewTimestampFromTime(base.Add(2 * time.Second)))
		s.Attributes().PutStr("unused", "a span attribute that counts must not decode")
		if i == 0 {
			s.Status().SetCode(ptrace.StatusCodeError)
		}
	}
	require.NoError(t, storagebackend.New(store).ConsumeTraces(t.Context(), td))
	require.NoError(t, store.Admin().Flush(t.Context(), "default", signal.Trace))
	spy := &metricTraceSpy{Source: store}
	q := storagebackend.NewQuery(spy).Traces()
	engine := traceqlengine.NewEngine(q, traceqlengine.Options{})
	result, err := engine.EvalMetrics(t.Context(), `({status=error} | rate() by(resource.service.name)) / ({} | rate())`, traceqlengine.MetricsEvalParams{Start: base, End: base.Add(15 * time.Second), Step: 15 * time.Second})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Len(t, result[0].Samples, 1)
	require.Equal(t, .5, result[0].Samples[0].Value.Value)
	require.Len(t, spy.reads, 2)
	for i, read := range spy.reads {
		require.NotNil(t, read.budget)
		require.Same(t, spy.reads[0].budget, read.budget, "operand %d", i)
		require.Contains(t, read.projection, sigtrace.ColTraceID)
		for _, column := range []string{sigtrace.ColAttrs, sigtrace.ColEvents, sigtrace.ColLinks, sigtrace.ColName, sigtrace.ColDuration} {
			require.NotContains(t, read.projection, column)
		}
		require.Equal(t, spy.reads[0].remaining, read.budget.Remaining(), "query retained native buffers")
	}
	require.Contains(t, spy.reads[0].projection, sigtrace.ColStatusCode)
	require.NotContains(t, spy.reads[1].projection, sigtrace.ColStatusCode)
}
