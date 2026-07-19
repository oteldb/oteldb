package ch2storagebackende2e_test

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/backend"

	"github.com/oteldb/oteldb/integration"
	"github.com/oteldb/oteldb/internal/ch2storagebackend"
	"github.com/oteldb/oteldb/internal/chstorage"
	oteldbpromql "github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// buildMetrics constructs one resource carrying a gauge, an explicit-bucket histogram, and an
// exponential histogram, each with a single datapoint at base.
func buildMetrics(base time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "metricService")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("ch2storagebackend_test")

	ts := pcommon.NewTimestampFromTime(base)

	// Gauge.
	{
		m := sm.Metrics().AppendEmpty()
		m.SetName("test_gauge")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetDoubleValue(42)
		dp.Attributes().PutStr("host", "a")
	}

	// Explicit-bucket histogram: bounds [1,5,10], per-bucket counts [1,2,3,4] (total 10), sum 100.
	{
		m := sm.Metrics().AppendEmpty()
		m.SetName("test_hist")
		h := m.SetEmptyHistogram()
		h.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := h.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetCount(10)
		dp.SetSum(100)
		dp.ExplicitBounds().FromRaw([]float64{1, 5, 10})
		dp.BucketCounts().FromRaw([]uint64{1, 2, 3, 4})
	}

	// Exponential histogram.
	{
		m := sm.Metrics().AppendEmpty()
		m.SetName("test_exp")
		eh := m.SetEmptyExponentialHistogram()
		eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp := eh.DataPoints().AppendEmpty()
		dp.SetTimestamp(ts)
		dp.SetScale(0)
		dp.SetCount(6)
		dp.SetSum(30)
		dp.SetZeroCount(0)
		dp.Positive().SetOffset(0)
		dp.Positive().BucketCounts().FromRaw([]uint64{1, 2, 3})
	}

	return md
}

func TestMigrateMetrics(t *testing.T) {
	integration.Skip(t)
	var (
		ctx      = t.Context()
		provider = integration.TraceProvider(t)
	)

	_, client, tables := integration.SetupCH(t, integration.SetupCHOptions{
		Name:           "ch2storagebackend-metrics",
		TablePrefix:    "ch2sbm",
		TracerProvider: provider,
	})

	inserter, err := chstorage.NewInserter(client, chstorage.InserterOptions{
		Tables:         tables,
		TracerProvider: provider,
	})
	require.NoError(t, err)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, inserter.ConsumeMetrics(ctx, buildMetrics(base)))

	store, err := storage.Open(ctx, storage.Options{},
		storage.WithBackend(backend.Memory()),
		storage.WithDurability(storage.DurabilityEphemeral),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close(ctx)
	})
	back := storagebackend.New(store)

	m := ch2storagebackend.NewMigrator(client, tables, back, integration.Logger(t))
	stats, err := m.MigrateMetrics(ctx, 0, 1000)
	require.NoError(t, err)
	// Number points: gauge(1) + hist _count/_sum/_bucket(le=1,5,10,+Inf) = 1 + 1 + 1 + 4 = 7.
	require.Equal(t, 7, stats.Points)
	require.Equal(t, 1, stats.ExpHistograms)

	engine, err := oteldbpromql.New(back, oteldbpromql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	query := func(q string) float64 {
		t.Helper()
		iq, err := engine.NewInstantQuery(ctx, back, promql.NewPrometheusQueryOpts(false, 0), q, base)
		require.NoError(t, err)
		defer iq.Close()
		res := iq.Exec(ctx)
		require.NoError(t, res.Err)
		vec, ok := res.Value.(promql.Vector)
		require.Truef(t, ok, "query %q: not a vector", q)
		require.Lenf(t, vec, 1, "query %q: expected exactly one sample", q)
		return vec[0].F
	}

	// Gauge round-trips.
	require.Equal(t, float64(42), query(`test_gauge`))

	// Histogram components round-trip verbatim (name suffixes + le labels preserved). Cumulative
	// bucket counts: le="1"=1, le="5"=1+2=3, le="10"=1+2+3=6, le="+Inf"=1+2+3+4=10 (the overflow
	// bucket), matching the datapoint count.
	require.Equal(t, float64(10), query(`test_hist_count`))
	require.Equal(t, float64(100), query(`test_hist_sum`))
	require.Equal(t, float64(1), query(`test_hist_bucket{le="1"}`))
	require.Equal(t, float64(3), query(`test_hist_bucket{le="5"}`))
	require.Equal(t, float64(6), query(`test_hist_bucket{le="10"}`))
	require.Equal(t, float64(10), query(`test_hist_bucket{le="+Inf"}`))

	// Exp histogram is reconstructed natively, then decomposed by storagebackend into _count/_sum.
	require.Equal(t, float64(6), query(`test_exp_count`))
	require.Equal(t, float64(30), query(`test_exp_sum`))
}
