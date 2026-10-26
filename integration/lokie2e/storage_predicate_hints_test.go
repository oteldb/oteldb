package lokie2e_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

func TestStorageFiniteRegexpHints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	backend := storagebackend.New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	ld := plog.NewLogs()
	rs := ld.ResourceLogs().AppendEmpty()
	rs.Resource().Attributes().PutStr("job", "api")
	records := rs.ScopeLogs().AppendEmpty().LogRecords()
	for i, body := range []string{"foo", "bar", "xfoo", "barx", "FOO", "", "foobar"} {
		r := records.AppendEmpty()
		r.SetTimestamp(pcommon.Timestamp(base.Add(time.Duration(i) * time.Millisecond).UnixNano()))
		r.Body().SetStr(body)
	}
	require.NoError(t, backend.ConsumeLogs(ctx, ld))
	require.NoError(t, store.Admin().Flush(ctx, "default", signal.Log))
	querier := backend.Logs()
	provider := integration.TraceProvider(t)
	optimized := setupDBWithOptimizers(t, provider, querier, querier, []logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}})
	reference := setupDBWithOptimizers(t, provider, querier, querier, nil)
	for _, tt := range []struct {
		name, pipeline string
		count          int
	}{
		{name: "finite", pipeline: `|~ "^(foo|bar)$"`, count: 2},
		{name: "empty", pipeline: `|~ "^$"`, count: 1},
		{name: "substring", pipeline: `|~ "foo|bar"`, count: 5},
		{name: "partial_anchor", pipeline: `|~ "^foo|bar$"`, count: 3},
		{name: "folded", pipeline: `|~ "(?i)^(foo|bar)$"`, count: 3},
		{name: "transformed", pipeline: `| line_format "foo" |~ "^(foo|bar)$"`, count: 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			params := lokiapi.QueryRangeParams{
				Query: `{job="api"} ` + tt.pipeline,
				Start: lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(base.Add(-time.Second)))),
				End:   lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(base.Add(time.Second)))),
				Limit: lokiapi.NewOptInt(100),
			}
			got, err := optimized.QueryRange(ctx, params)
			require.NoError(t, err)
			want, err := reference.QueryRange(ctx, params)
			require.NoError(t, err)
			require.Equal(t, want.Data, got.Data)
			streams, ok := got.Data.GetStreamsResult()
			require.True(t, ok)
			count := 0
			for _, stream := range streams.Result {
				count += len(stream.Values)
			}
			require.Equal(t, tt.count, count)
		})
	}
}
