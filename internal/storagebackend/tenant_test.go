package storagebackend_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/multitenancy"
	"github.com/oteldb/oteldb/internal/promql"
	"github.com/oteldb/oteldb/internal/storagebackend"
)

// routedStore returns an in-memory engine whose write routing follows *route, so a test can put
// byte-identical telemetry in different tenants — which is how header-routed ingest behaves, and
// the only way two tenants come to hold the same series identity.
func routedStore(t *testing.T) (*storage.Storage, *signal.TenantID) {
	t.Helper()

	route := new(signal.TenantID)

	store, err := storage.InMemory(storage.WithTenant(
		func(signal.Resource, signal.Scope) signal.TenantID { return *route },
	))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	return store, route
}

// gauge builds a one-point OTLP gauge batch.
func gauge(name string, value float64, ts time.Time) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(name)
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.Timestamp(ts.UnixNano()))
	dp.SetDoubleValue(value)

	return md
}

// instantValues evaluates expr through oteldb's PromQL engine over b and returns the sample values.
func instantValues(ctx context.Context, t *testing.T, b *storagebackend.Backend, expr string, ts time.Time) []float64 {
	t.Helper()

	eng, err := promql.New(b, promql.EngineOpts{
		MaxSamples:    1_000_000,
		Timeout:       time.Minute,
		LookbackDelta: 5 * time.Minute,
	})
	require.NoError(t, err)

	q, err := eng.NewInstantQuery(ctx, b, nil, expr, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	vec, err := res.Vector()
	require.NoError(t, err)

	out := make([]float64, 0, len(vec))
	for _, s := range vec {
		out = append(out, s.F)
	}

	return out
}

// TestBackendTenantIsolation proves a request authorized for one tenant cannot read another's data.
func TestBackendTenantIsolation(t *testing.T) {
	ctx := context.Background()
	store, route := routedStore(t)
	b := storagebackend.New(store, storagebackend.WithTenancy())

	ts := time.Now().Truncate(time.Second)

	*route = "acme"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("shared_metric", 1, ts)))

	*route = "globex"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("shared_metric", 2, ts)))
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("globex_only", 3, ts)))

	acme := multitenancy.WithTenant(ctx, "acme")
	globex := multitenancy.WithTenant(ctx, "globex")

	require.Equal(t, []float64{1}, instantValues(acme, t, b, `shared_metric`, ts),
		"acme must see only its own value of the shared metric")
	require.Equal(t, []float64{2}, instantValues(globex, t, b, `shared_metric`, ts))

	require.Empty(t, instantValues(acme, t, b, `globex_only`, ts),
		"acme must not see a metric only globex wrote")
	require.Equal(t, []float64{3}, instantValues(globex, t, b, `globex_only`, ts))
}

// TestBackendTenantIsolationLabels proves the metadata paths — label names and values — are
// tenant-scoped too, not just sample selection.
func TestBackendTenantIsolationLabels(t *testing.T) {
	ctx := context.Background()
	store, route := routedStore(t)
	b := storagebackend.New(store, storagebackend.WithTenancy())

	ts := time.Now().Truncate(time.Second)

	*route = "acme"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("acme_metric", 1, ts)))

	*route = "globex"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("globex_metric", 2, ts)))

	for _, tt := range []struct {
		tenant string
		want   string
		absent string
	}{
		{"acme", "acme_metric", "globex_metric"},
		{"globex", "globex_metric", "acme_metric"},
	} {
		t.Run(tt.tenant, func(t *testing.T) {
			q, err := b.Querier(ts.Add(-time.Hour).UnixMilli(), ts.Add(time.Hour).UnixMilli())
			require.NoError(t, err)
			t.Cleanup(func() { _ = q.Close() })

			values, _, err := q.LabelValues(multitenancy.WithTenant(ctx, tt.tenant), "__name__", nil)
			require.NoError(t, err)
			require.Contains(t, values, tt.want)
			require.NotContains(t, values, tt.absent)
		})
	}
}

// TestBackendTenancyRequiresTenant pins the fail-closed behavior: with tenancy on, a read whose
// context carries no tenant is refused rather than served from the default tenant, so a route that
// bypassed the tenancy middleware reads nothing.
func TestBackendTenancyRequiresTenant(t *testing.T) {
	ctx := context.Background()
	store, route := routedStore(t)
	b := storagebackend.New(store, storagebackend.WithTenancy())

	ts := time.Now().Truncate(time.Second)
	*route = "acme"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("acme_metric", 1, ts)))

	q, err := b.Querier(ts.Add(-time.Hour).UnixMilli(), ts.Add(time.Hour).UnixMilli())
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })

	_, _, err = q.LabelNames(ctx, nil)
	require.ErrorIs(t, err, storagebackend.ErrNoTenant)
}

// TestBackendWithoutTenancyIgnoresContextTenant pins that a deployment which has not opted in is
// unchanged: reads serve its single tenant, and a tenant riding the context — which nothing puts
// there without the middleware — does not redirect them.
func TestBackendWithoutTenancyIgnoresContextTenant(t *testing.T) {
	ctx := context.Background()

	store, err := storage.InMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	b := storagebackend.New(store)

	ts := time.Now().Truncate(time.Second)
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("test_metric", 7, ts)))

	require.Equal(t, []float64{7}, instantValues(ctx, t, b, `test_metric`, ts))
	require.Equal(t, []float64{7},
		instantValues(multitenancy.WithTenant(ctx, "acme"), t, b, `test_metric`, ts),
		"without WithTenancy the backend stays pinned to its single tenant")
}

// TestBackendLabelCacheNoCrossTenantLeak proves the Backend-lifetime label cache — shared by every
// tenant and keyed only by content-addressed series id — cannot carry one tenant's series into
// another's result.
//
// Both tenants below write byte-identical series identities, so they collide on one cache entry.
// Querying acme first populates it; globex must then still see only its own sample, and acme must
// not gain globex's.
func TestBackendLabelCacheNoCrossTenantLeak(t *testing.T) {
	ctx := context.Background()
	store, route := routedStore(t)
	b := storagebackend.New(store, storagebackend.WithTenancy())

	ts := time.Now().Truncate(time.Second)

	*route = "acme"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("collide", 10, ts)))

	*route = "globex"
	require.NoError(t, b.ConsumeMetrics(ctx, gauge("collide", 20, ts)))

	acme := multitenancy.WithTenant(ctx, "acme")
	globex := multitenancy.WithTenant(ctx, "globex")

	require.Equal(t, []float64{10}, instantValues(acme, t, b, `collide`, ts))
	require.Equal(t, []float64{20}, instantValues(globex, t, b, `collide`, ts),
		"a warm label-cache entry from acme must not carry acme's data into globex's result")
	require.Equal(t, []float64{10}, instantValues(acme, t, b, `collide`, ts),
		"nor globex's back into acme's")
}
