package lokie2e_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/logql/logqlengine"
	"github.com/oteldb/oteldb/internal/lokiapi"
	"github.com/oteldb/oteldb/internal/otelstorage"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

type matcherSeriesSource struct {
	storagebackend.Source
	remote func(context.Context, signal.TenantID, []fetch.Matcher, int64, int64) ([]signal.Series, error)
	mu     sync.Mutex
	rows   []int
}

func (s *matcherSeriesSource) LogSeries(ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64) ([]signal.Series, error) {
	return s.remote(ctx, tenant, matchers, start, end)
}

func TestStorageLabelMatcherEndpoints(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	db := storagebackend.New(store)
	base := time.Unix(1_700_000_000, 0).UTC()
	ld := plog.NewLogs()
	for _, id := range []string{"foo", "bar", "subword", "number", "bytes", "empty", "missing", "map"} {
		rl := ld.ResourceLogs().AppendEmpty()
		attrs := rl.Resource().Attributes()
		attrs.PutStr("job", "api")
		attrs.PutStr("id", id)
		switch id {
		case "foo", "bar":
			attrs.PutStr("route", id)
		case "subword":
			attrs.PutStr("route", "xfooy")
		case "number":
			attrs.PutInt("route", 7)
		case "bytes":
			attrs.PutEmptyBytes("route").FromRaw([]byte("foo"))
		case "empty":
			attrs.PutStr("route", "")
		case "map":
			attrs.PutEmptyMap("route").PutStr("x", "y")
		}
		rec := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		rec.SetTimestamp(pcommon.Timestamp(base.UnixNano()))
		rec.Body().SetStr(id)
	}
	require.NoError(t, db.ConsumeLogs(ctx, ld))
	start := lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(base.Add(-time.Second))))
	end := lokiapi.NewOptLokiTime(asLokiTime(otelstorage.NewTimestampFromTime(base.Add(time.Second))))
	for _, phase := range []string{"head", "parts"} {
		if phase == "parts" {
			require.NoError(t, store.Admin().Flush(ctx, "default", signal.Log))
		}
		t.Run(phase, func(t *testing.T) {
			src := &matcherSeriesSource{Source: store}
			server := httptest.NewServer(cluster.SeriesHandler(func(ctx context.Context, sig signal.Signal, tenant string, lo, hi int64, matchers []fetch.Matcher) ([]signal.Series, error) {
				require.Equal(t, signal.Log, sig)
				series, err := store.LogSeries(ctx, signal.TenantID(tenant), matchers, lo, hi)
				src.mu.Lock()
				src.rows = append(src.rows, len(series))
				src.mu.Unlock()
				return series, err
			}))
			t.Cleanup(server.Close)
			addr := strings.TrimPrefix(server.URL, "http://")
			src.remote = func(ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, lo, hi int64) ([]signal.Series, error) {
				series, err := cluster.FetchSeries(ctx, server.Client(), addr, signal.Log, string(tenant), lo, hi,
					fetch.EqualitySpecs(matchers), cluster.WithStringMatchers(fetch.StringSpecs(matchers)))
				return fetch.FilterSeries(series, matchers), err
			}
			querier := storagebackend.NewQuery(src).Logs()
			client := setupDBWithOptimizers(t, integration.TraceProvider(t), querier, querier,
				[]logqlengine.Optimizer{&storagebackend.LogQLOptimizer{}})
			for _, tt := range []struct {
				selector string
				ids      []string
			}{
				{`{job="api",route=~"foo|bar"}`, []string{"foo", "bar"}},
				{`{job="api",route=~"foo|"}`, []string{"foo", "empty", "missing"}},
				{`{job="api",route="Zm9v"}`, []string{"bytes"}},
				{`{job="api",route=~"7"}`, []string{"number"}},
				{`{job="api",route!~"foo|bar"}`, []string{"subword", "number", "bytes", "empty", "missing", "map"}},
			} {
				t.Run(tt.selector, func(t *testing.T) {
					series, err := client.Series(ctx, lokiapi.SeriesParams{Start: start, End: end, Match: []string{tt.selector}})
					require.NoError(t, err)
					var ids []string
					for _, item := range series.Data {
						ids = append(ids, item["id"])
					}
					require.ElementsMatch(t, tt.ids, ids)
					values, err := client.LabelValues(ctx, lokiapi.LabelValuesParams{Name: "id", Start: start, End: end, Query: lokiapi.NewOptString(tt.selector)})
					require.NoError(t, err)
					require.ElementsMatch(t, tt.ids, values.Data)
				})
			}
			for _, tt := range []struct {
				pipeline string
				ids      []string
			}{
				{`| route =~ "foo|bar"`, []string{"foo", "bar"}},
				{`| route =~ "foo|"`, []string{"foo", "empty", "missing"}},
				{`| route !~ "foo|bar"`, []string{"subword", "number", "bytes", "empty", "missing", "map"}},
				{`| route =~ "foo|bar" and id != "bar"`, []string{"foo"}},
				{`| label_format route="foo" | route =~ "foo|bar"`, []string{"foo", "bar", "subword", "number", "bytes", "empty", "missing", "map"}},
			} {
				t.Run(tt.pipeline, func(t *testing.T) {
					result, err := client.QueryRange(ctx, lokiapi.QueryRangeParams{Query: `{job="api"} ` + tt.pipeline, Start: start, End: end, Limit: lokiapi.NewOptInt(100)})
					require.NoError(t, err)
					streams, ok := result.Data.GetStreamsResult()
					require.True(t, ok)
					var ids []string
					for _, stream := range streams.Result {
						for _, value := range stream.Values {
							ids = append(ids, value.V)
						}
					}
					require.ElementsMatch(t, tt.ids, ids)
				})
			}
			src.mu.Lock()
			rows := append([]int(nil), src.rows...)
			src.mu.Unlock()
			require.GreaterOrEqual(t, len(rows), 2)
			require.Less(t, rows[0], 8, "the owner must prune finite string alternatives before returning identities")
		})
	}
}
