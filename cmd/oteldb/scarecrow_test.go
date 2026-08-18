package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/oteldb/storage"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// TestPrometheusConfigScarecrowYAML checks the flag parses from the documented YAML shape via the
// real config loader.
func TestPrometheusConfigScarecrowYAML(t *testing.T) {
	const data = `
prometheus:
  enable_scarecrow_engine: true
`
	f, err := os.CreateTemp("", "oteldb.yml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(f.Name()) }()
	_, err = f.WriteString(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	cfg, err := loadConfig(f.Name())
	require.NoError(t, err)

	require.True(t, cfg.Prometheus.EnableScarecrowEngine)
}

// TestNewScarecrowEngineNativeScanner checks that a storage-backend querier gets the native
// Scanner rather than falling back to scarecrow's generic storage.Queryable adapter: a query
// evaluates correctly and returns the ingested sample.
func TestNewScarecrowEngineNativeScanner(t *testing.T) {
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
	m.SetName("scarecrow_flag_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(7)
	dp.Attributes().PutStr("foo", "bar")
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	app := &App{lg: zap.NewNop()}
	cfg := PrometheusConfig{EnableScarecrowEngine: true}
	cfg.SetDefaults()

	engine := app.newScarecrowEngine(b, cfg)

	q, err := engine.NewInstantQuery(ctx, b, nil, `scarecrow_flag_metric`, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, vec, 1)
	require.Equal(t, float64(7), vec[0].F)
	require.Equal(t, "bar", vec[0].Metric.Get("foo"))
}

// metricQuerierWrapper embeds a metricQuerier behind a distinct concrete type, so
// newScarecrowEngine's `q.(*storagebackend.Backend)` type assertion fails even though every method
// call is forwarded to a real, fully working backend underneath.
type metricQuerierWrapper struct{ metricQuerier }

// TestNewScarecrowEngineFallsBackOverOtherQuerier checks that a querier which isn't
// *storagebackend.Backend still gets a working (if generically-adapted) engine rather than a nil
// Scanner or a panic — exercising scarecrow's queryableScanner fallback end to end.
func TestNewScarecrowEngineFallsBackOverOtherQuerier(t *testing.T) {
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
	m.SetName("scarecrow_fallback_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(9)
	dp.Attributes().PutStr("foo", "baz")
	require.NoError(t, b.ConsumeMetrics(ctx, md))

	app := &App{lg: zap.NewNop()}
	cfg := PrometheusConfig{EnableScarecrowEngine: true}
	cfg.SetDefaults()

	wrapped := metricQuerierWrapper{b}
	engine := app.newScarecrowEngine(wrapped, cfg)
	require.NotNil(t, engine)

	q, err := engine.NewInstantQuery(ctx, wrapped, nil, `scarecrow_fallback_metric`, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)
	require.Len(t, vec, 1)
	require.Equal(t, float64(9), vec[0].F)
}
