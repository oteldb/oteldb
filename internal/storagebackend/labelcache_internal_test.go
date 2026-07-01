package storagebackend

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
)

// TestBackendLabelCacheSharedAcrossQueries is a white-box check that the Backend-lifetime label
// cache (b.labels) interns each series' projection once and reuses it on later queries, instead of
// rebuilding it per query. It guards the fix for the per-query labelCache churn (#1118).
func TestBackendLabelCacheSharedAcrossQueries(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := New(store)
	require.Zero(t, b.labels.Len(), "cache starts empty")

	ts := time.Now().Truncate(time.Second)
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	sm := rm.ScopeMetrics().AppendEmpty()
	for _, route := range []string{"a", "b", "c"} {
		m := sm.Metrics().AppendEmpty()
		m.SetName("test_metric")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		dp.SetDoubleValue(1)
		dp.Attributes().PutStr("route", route)
	}
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	matcher, err := labels.NewMatcher(labels.MatchEqual, "__name__", "test_metric")
	require.NoError(t, err)

	// selectAll drives one query through a fresh querier (fresh fetcher) sharing b.labels.
	selectAll := func() int {
		q, err := b.Querier(ts.Add(-time.Hour).UnixMilli(), ts.Add(time.Hour).UnixMilli())
		require.NoError(t, err)
		defer func() { _ = q.Close() }()

		ss := q.Select(ctx, true, nil, matcher)
		n := 0
		for ss.Next() {
			ss.At().Labels() // force projection through the cache.
			n++
		}
		require.NoError(t, ss.Err())

		return n
	}

	require.Equal(t, 3, selectAll(), "three series match")
	require.Equal(t, 3, b.labels.Len(), "each series interned once")

	require.Equal(t, 3, selectAll(), "second query, same series")
	require.Equal(t, 3, b.labels.Len(), "no rebuild: cache reused across queries")
}
