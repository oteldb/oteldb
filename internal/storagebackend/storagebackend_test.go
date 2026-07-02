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

// TestBackendCountByPushdown pins the count by (label) pushdown end to end: ingest series across
// three cpu groups (plus one series without the label), then check the pushdown-shaped query
// against hand-computed groups AND against the generic aggregate-over-Select path — `m + 0` turns
// the inner expression into a Binary, which the pushdown rejects, so both evaluation paths run on
// identical data and must agree exactly.
func TestBackendCountByPushdown(t *testing.T) {
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

	addPoint := func(attrs map[string]string) {
		m := sm.Metrics().AppendEmpty()
		m.SetName("cpu_metric")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
		dp.SetDoubleValue(1)
		for k, v := range attrs {
			dp.Attributes().PutStr(k, v)
		}
	}

	// cpu=0: two series (mode idle/user); cpu=1: one; cpu=2: one; one series without cpu.
	addPoint(map[string]string{"cpu": "0", "mode": "idle"})
	addPoint(map[string]string{"cpu": "0", "mode": "user"})
	addPoint(map[string]string{"cpu": "1", "mode": "idle"})
	addPoint(map[string]string{"cpu": "2", "mode": "idle"})
	addPoint(map[string]string{"mode": "steal"})

	require.NoError(t, b.ConsumeMetrics(ctx, md))

	eng, err := promql.New(b, promql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	instant := func(expr string) map[string]float64 {
		t.Helper()

		q, err := eng.NewInstantQuery(ctx, b, nil, expr, ts)
		require.NoError(t, err)
		t.Cleanup(q.Close)

		res := q.Exec(ctx)
		require.NoError(t, res.Err)

		vec, err := res.Vector()
		require.NoError(t, err)

		out := make(map[string]float64, len(vec))
		for _, s := range vec {
			out[s.Metric.Get("cpu")] = s.F
		}

		return out
	}

	want := map[string]float64{"0": 2, "1": 1, "2": 1, "": 1}

	pushed := instant(`count by (cpu) (cpu_metric)`)
	require.Equal(t, want, pushed, "pushdown groups must match hand-computed counts")

	// Differential oracle: `cpu_metric + 0` is a Binary expression, which the pushdown rejects, so
	// this runs the generic aggregate-over-Select path over the same data.
	generic := instant(`count by (cpu) (cpu_metric + 0)`)
	require.Equal(t, generic, pushed, "pushdown and generic evaluation must agree")

	// The enclosing distinct-values count composes through the generic outer aggregation.
	distinct := instant(`count(count by (cpu) (cpu_metric))`)
	require.Equal(t, map[string]float64{"": 4}, distinct, "4 distinct groups (3 cpu values + absent)")

	distinctGeneric := instant(`count(count by (cpu) (cpu_metric + 0))`)
	require.Equal(t, distinctGeneric, distinct)
}
