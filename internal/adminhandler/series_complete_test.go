package adminhandler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/adminapi"
)

func TestAdminSeriesCountCompleteness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		signals  []storage.SignalStats
		complete bool
		total    int64
	}{
		{name: "empty", complete: true},
		{name: "complete", signals: []storage.SignalStats{
			{Signal: signal.Metric, Series: 10, SeriesComplete: true},
			{Signal: signal.Log, Series: 5, SeriesComplete: true},
		}, complete: true, total: 15},
		{name: "preserved metric count", signals: []storage.SignalStats{
			{Signal: signal.Metric, Series: 10, SeriesComplete: false},
			{Signal: signal.Log, Series: 5, SeriesComplete: true},
		}, total: 15},
		{name: "unknown zero", signals: []storage.SignalStats{
			{Signal: signal.Metric, SeriesComplete: false},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine := &fakeEngine{stats: storage.StoreStats{Tenants: []storage.TenantStats{{Tenant: "default", Signals: tc.signals}}}}
			server := testServer(t, Options{Engine: engine, StorageBackend: "file"})
			t.Cleanup(server.Close)
			var response adminapi.StorageStats
			get(t, server.URL, "/api/v1/storage", &response)
			require.True(t, response.Engine.Set)
			require.Len(t, response.Engine.Value.Tenants, 1)
			tenant := response.Engine.Value.Tenants[0]
			require.Equal(t, tc.total, tenant.TotalSeries)
			require.True(t, tenant.TotalSeriesComplete.Set, "false completeness must be present on the wire")
			require.Equal(t, tc.complete, tenant.TotalSeriesComplete.Value)
			require.Len(t, tenant.Signals, len(tc.signals))
			for i, signalStats := range tenant.Signals {
				require.True(t, signalStats.SeriesComplete.Set)
				require.Equal(t, tc.signals[i].SeriesComplete, signalStats.SeriesComplete.Value)
				require.Equal(t, tc.signals[i].Series, signalStats.Series)
			}
		})
	}
}
