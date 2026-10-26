package storagebackend_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/storagebackend"
	"github.com/oteldb/oteldb/internal/traceql/traceqlengine"
)

func TestTraceFiniteAttributeHintsPreserveTypes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.Open(ctx, storage.Options{}, storage.WithBackend(backend.Memory()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	base := time.Unix(1_700_000_000, 0).UTC()
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "api")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i, value := range []any{"foo", []byte("foo"), []byte("bar"), int64(42), nil, map[string]any{"a": "foo"}, ""} {
		s := spans.AppendEmpty()
		s.SetTraceID(pcommon.TraceID{byte(i + 1)})
		s.SetSpanID(pcommon.SpanID{byte(i + 1)})
		s.SetName("test")
		s.SetStartTimestamp(pcommon.Timestamp(base.UnixNano()))
		s.SetEndTimestamp(pcommon.Timestamp(base.Add(time.Millisecond).UnixNano()))
		if value != nil {
			require.NoError(t, s.Attributes().PutEmpty("route").FromRaw(value))
		}
	}
	require.NoError(t, storagebackend.New(store).ConsumeTraces(ctx, td))
	params := traceqlengine.EvalParams{Start: base.Add(-time.Second), End: base.Add(time.Second), Limit: 100}
	plain := traceqlengine.NewEngine(storagebackend.New(store, storagebackend.WithTraceQLPushdown(false)).Traces(), traceqlengine.Options{})
	for _, phase := range []string{"head", "parts"} {
		if phase == "parts" {
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Trace))
		}
		for _, remote := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/remote=%t", phase, remote), func(t *testing.T) {
				var src storagebackend.Source = store
				if remote {
					src = newHintPeer(t, store, false)
				}
				engine := traceqlengine.NewEngine(storagebackend.NewQuery(src).Traces(), traceqlengine.Options{})
				for _, tt := range []struct {
					query string
					want  int
				}{
					{`{span.route =~ "^(foo|bar)$"}`, 3},
					{`{span.route =~ "^bar$"}`, 1},
					{`{span.route =~ "^42$"}`, 0},
					{`{span.route =~ "^(|foo)$"}`, 3},
					{`{span.route !~ "^(foo|bar)$"}`, 1},
				} {
					t.Run(tt.query, func(t *testing.T) {
						want, err := plain.Eval(ctx, tt.query, params)
						require.NoError(t, err)
						require.Len(t, want.Traces, tt.want)
						got, err := engine.Eval(ctx, tt.query, params)
						require.NoError(t, err)
						require.Equal(t, want, got)
					})
				}
			})
		}
	}
}
