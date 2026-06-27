package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/promapi"
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

// TestBackendLabelMetadataUnbounded ingests a metric and lists label names/values over the
// unbounded window the label API uses when start/end are omitted (promapi.MinTime/MaxTime). Their
// millisecond magnitude overflows the storage querier's ms→ns conversion unless clamped, which used
// to make /api/v1/labels and the metric browser return nothing despite data being present.
func TestBackendLabelMetadataUnbounded(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	ts := time.Now().Truncate(time.Second)
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "test")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(1)
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	q, err := b.Querier(promapi.MinTime.UnixMilli(), promapi.MaxTime.UnixMilli())
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	names, _, err := q.LabelNames(ctx, nil)
	require.NoError(t, err)
	require.Contains(t, names, "__name__")
	require.Contains(t, names, "service.name")

	vals, _, err := q.LabelValues(ctx, "__name__", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"test_metric"}, vals)
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
