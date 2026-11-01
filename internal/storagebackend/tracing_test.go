package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/logql"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestLogQueryTracing(t *testing.T) {
	failure := errors.New("read refused")
	for _, tt := range []struct {
		name                        string
		keysErr, fetchErr, drainErr error
	}{
		{name: "success"},
		{name: "metadata fallback", keysErr: failure},
		{name: "fetch failure", fetchErr: failure},
		{name: "drain failure", drainErr: failure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			t.Cleanup(func() { require.NoError(t, tp.Shutdown(context.Background())) })
			ctx, parent := tp.Tracer("test").Start(t.Context(), "query")
			src := &tracedLogSource{t: t, keysErr: tt.keysErr, fetchErr: tt.fetchErr, drainErr: tt.drainErr}
			b := storagebackend.NewQuery(src, storagebackend.WithTracerProvider(tp))
			node, err := b.Logs().Query(ctx, []logql.LabelMatcher{{Label: "host_name", Op: logql.OpEq, Value: "test"}})
			require.NoError(t, err)
			start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			it, err := node.EvalPipeline(ctx, logqlengine.EvalParams{Start: start, End: start.Add(time.Minute), Limit: 5})
			if tt.fetchErr != nil || tt.drainErr != nil {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
				require.NoError(t, it.Close())
			}
			parent.End()

			spans := map[string]sdktrace.ReadOnlySpan{}
			for _, span := range recorder.Ended() {
				require.Equal(t, parent.SpanContext().TraceID(), span.SpanContext().TraceID())
				spans[span.Name()] = span
			}
			parents := map[string]string{
				"storagebackend.logs.eval":           "query",
				"storagebackend.logs.fetch":          "storagebackend.logs.eval",
				"storagebackend.logs.resolveFilters": "storagebackend.logs.fetch",
				"storagebackend.logs.keys":           "storagebackend.logs.resolveFilters",
				"storagebackend.logs.read":           "storagebackend.logs.fetch",
			}
			if tt.fetchErr == nil {
				parents["storagebackend.logs.drain"] = "storagebackend.logs.fetch"
				require.True(t, src.closed)
			}
			if err == nil {
				parents["storagebackend.logs.materialize"] = "storagebackend.logs.eval"
				parents["storagebackend.logs.sort"] = "storagebackend.logs.eval"
			}
			for child, parent := range parents {
				require.Contains(t, spans, child)
				require.Equal(t, spans[parent].SpanContext().SpanID(), spans[child].Parent().SpanID())
			}
			require.Equal(t, spans["storagebackend.logs.keys"].SpanContext().SpanID(), src.keysSpanID)
			require.Equal(t, spans["storagebackend.logs.read"].SpanContext().SpanID(), src.fetchSpanID)
			if tt.fetchErr == nil {
				require.Equal(t, spans["storagebackend.logs.drain"].SpanContext().SpanID(), src.drainSpanID)
			}
			if tt.keysErr != nil {
				require.Equal(t, codes.Error, spans["storagebackend.logs.keys"].Status().Code)
				require.Zero(t, src.request.Limit)
			} else {
				require.Equal(t, 5, src.request.Limit)
			}
			if err != nil {
				require.Equal(t, codes.Error, spans["storagebackend.logs.eval"].Status().Code)
				require.Equal(t, codes.Error, spans["storagebackend.logs.fetch"].Status().Code)
			} else {
				require.Equal(t, codes.Unset, spans["storagebackend.logs.eval"].Status().Code)
			}
		})
	}
}

type tracedLogSource struct {
	storagebackend.Source
	t                                    *testing.T
	keysErr, fetchErr, drainErr          error
	request                              fetch.Request
	closed                               bool
	keysSpanID, fetchSpanID, drainSpanID trace.SpanID
}

func (s *tracedLogSource) WithQueryBudget(ctx context.Context) context.Context { return ctx }

func (s *tracedLogSource) LogKeys(ctx context.Context, _ signal.TenantID, _, _ int64) ([]storage.KeyInfo, error) {
	require.True(s.t, trace.SpanFromContext(ctx).IsRecording())
	s.keysSpanID = trace.SpanContextFromContext(ctx).SpanID()
	return []storage.KeyInfo{{Key: []byte("host.name"), Scope: storage.KeyScopeResource}}, s.keysErr
}

func (s *tracedLogSource) LogFetcher(...signal.TenantID) fetch.Fetcher { return s }

func (s *tracedLogSource) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	require.True(s.t, trace.SpanFromContext(ctx).IsRecording())
	s.fetchSpanID = trace.SpanContextFromContext(ctx).SpanID()
	s.request = r
	if s.fetchErr != nil {
		return nil, s.fetchErr
	}
	return s, nil
}

func (s *tracedLogSource) Next(ctx context.Context) (*fetch.Batch, error) {
	require.True(s.t, trace.SpanFromContext(ctx).IsRecording())
	s.drainSpanID = trace.SpanContextFromContext(ctx).SpanID()
	if s.drainErr != nil {
		return nil, s.drainErr
	}
	return fetch.NewSliceIterator(nil).Next(ctx)
}

func (s *tracedLogSource) Close() error { s.closed = true; return nil }
