package adminhandler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/adminapi"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
)

type fakeEngine struct {
	stats      storage.StoreStats
	efficiency []storage.TenantEfficiency
	maintains  int
}

func (f *fakeEngine) Inspect() storage.StoreStats { return f.stats }

func (f *fakeEngine) EfficiencyStats(context.Context) ([]storage.TenantEfficiency, error) {
	return f.efficiency, nil
}

func testServer(t *testing.T, opts Options) *httptest.Server {
	t.Helper()
	srv, err := adminapi.NewServer(NewAdminAPI(opts))
	require.NoError(t, err)
	h := httpmiddleware.Wrap(srv, UIMiddleware())
	return httptest.NewServer(h)
}

func get(t *testing.T, base, path string, dst any) {
	t.Helper()
	resp, err := http.Get(base + path)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, path)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(dst))
}

func TestAdmin_InfoRuntimeUI(t *testing.T) {
	ts := testServer(t, Options{
		Info:              BuildInfo{Version: "v1.2.3", Commit: "deadbeef", GoVersion: "go1.26"},
		StartTime:         time.Now().Add(-90 * time.Second),
		StorageBackend:    "file",
		Engine:            &fakeEngine{},
		ClickHouseEnabled: false,
		Signals: []adminapi.SignalInfo{
			{Signal: adminapi.SignalMetrics, Backend: "storage", Queryable: true, Bind: adminapi.NewOptString(":9090")},
		},
	})
	defer ts.Close()

	var info adminapi.InstanceInfo
	get(t, ts.URL, "/api/v1/info", &info)
	assert.Equal(t, "v1.2.3", info.Version)
	assert.True(t, info.StorageEnabled)
	assert.Equal(t, "file", info.StorageBackend.Value)
	assert.GreaterOrEqual(t, info.UptimeSeconds, 89.0)

	var rt adminapi.RuntimeStats
	get(t, ts.URL, "/api/v1/runtime", &rt)
	assert.Positive(t, rt.Goroutines)
	assert.Positive(t, rt.NumCPU)

	// UI is served for the root path.
	resp, err := http.Get(ts.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
}

func TestAdmin_EngineStorage(t *testing.T) {
	eng := &fakeEngine{stats: storage.StoreStats{
		Tenants: []storage.TenantStats{{
			Tenant:    signal.TenantID("default"),
			Admission: storage.AdmissionStats{Accepted: 100, RejectedOOO: 2},
			Signals: []storage.SignalStats{{
				Signal:          signal.Metric,
				Series:          10,
				HeadItems:       5,
				HeadBytes:       2048,
				Parts:           3,
				MinTimeUnixNano: time.Unix(1700000000, 0).UnixNano(),
				MaxTimeUnixNano: time.Unix(1700003600, 0).UnixNano(),
				WAL:             true,
				WALSegments:     1,
				WALBytes:        512,
			}},
		}},
		Caches: storage.CacheStats{Decode: engine.DecodeCacheStats{Hits: 80, Misses: 20, Bytes: 4096, Items: 7}},
		Maintenance: storage.MaintenanceStats{
			Cycles:                 12,
			LastCycleStartUnixNano: time.Unix(1700003600, 0).UnixNano(),
			LastCycleDurationNano:  int64(1500 * time.Millisecond),
			LastCycleTasks:         4,
		},
		Cluster: &storage.ClusterStats{
			Self:    "node-1",
			Members: []storage.MemberStats{{ID: "node-1", Addr: ":9000"}},
			Owned:   []string{"default/0"},
			PartSync: &storage.PartSyncStats{
				Passes: 42, Mirrored: 3, Copied: 10, CopiedBytes: 8192, Pruned: 1, Errors: 0,
				LastSyncUnixNano: time.Unix(1700003600, 0).UnixNano(),
			},
			EC: &storage.ECStats{Converted: 5, PrunedStagedParts: 5, Reconstructs: 2},
		},
	}}
	ts := testServer(t, Options{Engine: eng, StorageBackend: "file"})
	defer ts.Close()

	var st adminapi.StorageStats
	get(t, ts.URL, "/api/v1/storage", &st)
	require.True(t, st.StorageEnabled)
	require.True(t, st.Engine.Set)
	e := st.Engine.Value
	require.Len(t, e.Tenants, 1)
	tn := e.Tenants[0]
	assert.Equal(t, "default", tn.Tenant)
	assert.Equal(t, int64(10), tn.TotalSeries)
	assert.Equal(t, int64(3), tn.TotalParts)
	assert.Equal(t, int64(100), tn.Admission.Accepted)
	require.Len(t, tn.Signals, 1)
	assert.Equal(t, adminapi.SignalMetrics, tn.Signals[0].Signal)
	assert.True(t, tn.Signals[0].MinTime.Set)
	assert.Equal(t, int64(80), e.Caches.DecodeCache.Hits)
	assert.Equal(t, int64(12), e.Maintenance.Cycles)
	assert.True(t, e.Maintenance.LastCycleStart.Set)
	assert.InDelta(t, 1.5, e.Maintenance.LastCycleDurationSeconds, 1e-9)
	assert.Equal(t, int64(4), e.Maintenance.LastCycleTasks)
	require.True(t, e.Cluster.Set)
	c := e.Cluster.Value
	assert.Equal(t, "node-1", c.Self)
	require.True(t, c.PartSync.Set)
	assert.Equal(t, int64(42), c.PartSync.Value.Passes)
	assert.True(t, c.PartSync.Value.LastSync.Set)
	require.True(t, c.Ec.Set)
	assert.Equal(t, int64(5), c.Ec.Value.Converted)
	assert.Equal(t, int64(2), c.Ec.Value.Reconstructs)
	assert.False(t, st.Clickhouse.Set)
}

func TestAdmin_Efficiency(t *testing.T) {
	eng := &fakeEngine{efficiency: []storage.TenantEfficiency{{
		Tenant: signal.TenantID("default"),
		Signals: []storage.SignalEfficiency{
			{
				Signal: signal.Metric, Series: 10, Parts: 3, Points: 1000,
				StoredBytes: 2000, BytesPerPoint: 2, LogicalBytes: 16000, CompressionRatio: 8,
			},
			{
				Signal: signal.Log, Series: 4, Parts: 1, Points: 100,
				StoredBytes: 5000, BytesPerPoint: 50,
			},
		},
	}}}
	ts := testServer(t, Options{Engine: eng})
	defer ts.Close()

	var eff adminapi.EfficiencyStats
	get(t, ts.URL, "/api/v1/storage/efficiency", &eff)
	require.True(t, eff.StorageEnabled)
	require.Len(t, eff.Tenants, 1)
	require.Len(t, eff.Tenants[0].Signals, 2)
	metric := eff.Tenants[0].Signals[0]
	assert.Equal(t, adminapi.SignalMetrics, metric.Signal)
	assert.Equal(t, int64(16000), metric.LogicalBytes.Value)
	assert.InDelta(t, 8, metric.CompressionRatio.Value, 1e-9)
	logs := eff.Tenants[0].Signals[1]
	assert.Equal(t, adminapi.SignalLogs, logs.Signal)
	assert.False(t, logs.LogicalBytes.Set)
	assert.False(t, logs.CompressionRatio.Set)
	assert.InDelta(t, 50, logs.BytesPerPoint, 1e-9)

	// Disabled engine reports empty stats instead of an error.
	ts2 := testServer(t, Options{})
	defer ts2.Close()
	get(t, ts2.URL, "/api/v1/storage/efficiency", &eff)
	assert.False(t, eff.StorageEnabled)
	assert.Empty(t, eff.Tenants)
}

func TestAdmin_Actions(t *testing.T) {
	eng := &fakeEngine{}
	ts := testServer(t, Options{
		Engine:   eng,
		Maintain: func(context.Context) error { eng.maintains++; return nil },
	})
	defer ts.Close()

	for _, action := range []string{"gc", "free-os-memory", "storage-maintain"} {
		resp, err := http.Post(ts.URL+"/api/v1/actions/"+action, "", nil)
		require.NoError(t, err)
		var res adminapi.ActionResult
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&res))
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, action)
		assert.True(t, res.Ok, action)
	}
	assert.Equal(t, 1, eng.maintains)
}
