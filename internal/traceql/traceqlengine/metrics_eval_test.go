package traceqlengine

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/iterators"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/tracestorage"
)

type cancelMetricsQuerier struct {
	MemoryQuerier
	cancel context.CancelFunc
	closed bool
}

func (*cancelMetricsQuerier) WithQueryBudget(ctx context.Context) context.Context { return ctx }
func (q *cancelMetricsQuerier) SelectMetricsSpans(context.Context, SelectSpansetsParams) (iterators.Iterator[tracestorage.Span], error) {
	return &cancelSpanInput{q: q}, nil
}

func (*cancelMetricsQuerier) SelectMetricsSpansets(context.Context, SelectSpansetsParams) (iterators.Iterator[Trace], error) {
	return iterators.Empty[Trace](), nil
}

type cancelSpanInput struct {
	q    *cancelMetricsQuerier
	next bool
}

func (i *cancelSpanInput) Next(span *tracestorage.Span) bool {
	if i.next {
		return false
	}
	i.next = true
	*span = tracestorage.Span{Start: otelstorage.Timestamp(1_700_000_011_000_000_000)}
	i.q.cancel()
	return true
}
func (*cancelSpanInput) Err() error     { return context.Canceled }
func (i *cancelSpanInput) Close() error { i.q.closed = true; return nil }

func TestMetricsCancellationClosesRead(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	q := &cancelMetricsQuerier{cancel: cancel}
	engine := NewEngine(q, Options{})
	base := time.Unix(1_700_000_010, 0)
	_, err := engine.EvalMetrics(ctx, `{} | rate()`, MetricsEvalParams{Start: base, End: base.Add(15 * time.Second), Step: 15 * time.Second})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, q.closed)
}

func TestMetricsTraceMetadataFirstRoot(t *testing.T) {
	t.Parallel()
	first := tracestorage.Span{Start: 1, End: 2}
	first.ResourceAttrs = otelstorage.Attrs{}
	second := tracestorage.Span{Start: 3, End: 4, Name: "second root"}
	set := metricsTraceSet(Trace{Spans: []tracestorage.Span{first, second}})
	require.Empty(t, set.RootSpanName)
	require.True(t, time.Unix(0, 1).Equal(set.Start))
	require.Equal(t, 3*time.Nanosecond, set.TraceDuration)
}

func TestMetricsUnsupportedBackend(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_700_000_010, 0)
	engine := NewEngine(&MemoryQuerier{}, Options{})
	_, err := engine.EvalMetrics(t.Context(), `{} | rate()`, MetricsEvalParams{Start: base, End: base.Add(time.Minute), Step: 15 * time.Second})
	var unsupported *UnsupportedError
	require.ErrorAs(t, err, &unsupported)
}

func TestMetricsUnsupportedOperandDoesNotRead(t *testing.T) {
	t.Parallel()
	reads := 0
	q := &cancelMetricsQuerier{cancel: func() { reads++ }}
	engine := NewEngine(q, Options{})
	base := time.Unix(1_700_000_010, 0)
	_, err := engine.EvalMetrics(t.Context(), `({} | rate()) / ({} | avg_over_time(duration))`, MetricsEvalParams{Start: base, End: base.Add(time.Minute), Step: 15 * time.Second})
	var unsupported *UnsupportedError
	require.ErrorAs(t, err, &unsupported)
	require.Zero(t, reads)
}
