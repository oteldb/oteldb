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
	stats     storage.StoreStats
	maintains int
}

func (f *fakeEngine) Inspect() storage.StoreStats { return f.stats }

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
	assert.False(t, st.Clickhouse.Set)
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
