package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestBackendMetricsRoundtrip ingests OTLP gauge metrics through the storage backend's
// ingestion sink and queries them back through oteldb's PromQL engine, exercising the full
// adapter (ConsumeMetrics → WriteMetrics, Querier + MetricsScanners → Fetcher).
func TestBackendMetricsRoundtrip(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	ts := time.Now().Truncate(time.Second)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(42)
	dp.Attributes().PutStr("foo", "bar")

	require.NoError(t, b.ConsumeMetrics(ctx, md))

	eng, err := promql.New(b, promql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	q, err := eng.NewInstantQuery(ctx, b, nil, `test_metric`, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, vec, 1, "expected one series")
	require.Equal(t, float64(42), vec[0].F)
	require.Equal(t, "test_metric", vec[0].Metric.Get("__name__"))
	require.Equal(t, "bar", vec[0].Metric.Get("foo"))
}

// TestBackendEmpty verifies a query over an empty engine returns no series rather than
// erroring, so the metrics API is usable before any data is ingested.
func TestBackendEmpty(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	eng, err := promql.New(b, promql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	q, err := eng.NewInstantQuery(ctx, b, nil, `absent_metric`, time.Now())
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)
	require.Empty(t, vec)
}
