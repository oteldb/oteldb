package adminhandler

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/adminapi"
)

func TestAdminClusterStateHTTP(t *testing.T) {
	t.Parallel()
	for _, unhealthy := range []bool{false, true} {
		name := "healthy"
		if unhealthy {
			name = "incomplete"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			engine := &fakeEngine{stats: storage.StoreStats{
				Cluster: &storage.ClusterStats{
					Self: "node-a", PrivateBackend: unhealthy, SelfAbsent: unhealthy, Fenced: unhealthy,
					Rejoins: 3, FenceDeadlineUnixNano: 123, Owned: []string{"t/_s0"},
					Members:       []storage.MemberStats{{ID: "a", Addr: "node-a"}},
					Placements:    []storage.ShardPlacement{{Shard: "t/_s0", Owners: []string{"a", "b"}}},
					LastRebalance: []storage.RebalanceMove{{Shard: "t/_s0", Added: []string{"b"}, Removed: []string{"c"}}},
				},
				Tenants: []storage.TenantStats{{Tenant: "t/_s0", Signals: []storage.SignalStats{{
					Signal: signal.Metric, HasReadGap: unhealthy, ReadGapAfterUnixNano: math.MinInt64,
					OutOfSpace: unhealthy, WantedParts: 2, Holes: 1, LostParts: math.MaxUint64,
					IndexFenced: unhealthy, IndexLoadError: "index unreadable", TenureUnestablished: unhealthy,
				}}}},
			}}
			server := testServer(t, Options{Engine: engine})
			t.Cleanup(server.Close)
			var got adminapi.StorageStats
			get(t, server.URL, "/api/v1/storage", &got)
			require.Nil(t, got.NodeReports)
			require.True(t, got.Engine.IsSet())
			cluster := got.Engine.Value.Cluster.Value
			require.Equal(t, adminapi.NewOptBool(unhealthy), cluster.PrivateBackend)
			require.Equal(t, adminapi.NewOptBool(unhealthy), cluster.SelfAbsent)
			require.Equal(t, adminapi.NewOptBool(unhealthy), cluster.Fenced)
			require.Equal(t, adminapi.NewOptInt64(3), cluster.Rejoins)
			require.Equal(t, adminapi.NewOptInt64(123), cluster.FenceDeadlineUnixNano)
			require.Equal(t, []adminapi.ShardPlacement{{Shard: "t/_s0", Owners: []string{"a", "b"}}}, cluster.Placements)
			require.Equal(t, []adminapi.ClusterRebalanceMove{{Shard: "t/_s0", Added: []string{"b"}, Removed: []string{"c"}}}, cluster.LastRebalance)
			reported := got.Engine.Value.Tenants[0].Signals[0]
			require.Equal(t, adminapi.NewOptBool(unhealthy), reported.HasReadGap)
			require.Equal(t, unhealthy, reported.ReadGapAfterUnixNano.IsSet())
			if unhealthy {
				require.EqualValues(t, math.MinInt64, reported.ReadGapAfterUnixNano.Value)
			}
			require.Equal(t, adminapi.NewOptBool(unhealthy), reported.OutOfSpace)
			require.Equal(t, adminapi.NewOptInt64(2), reported.WantedParts)
			require.Equal(t, adminapi.NewOptInt64(1), reported.Holes)
			require.Equal(t, adminapi.NewOptUint64(math.MaxUint64), reported.LostParts)
			require.Equal(t, adminapi.NewOptBool(unhealthy), reported.IndexFenced)
			require.Equal(t, adminapi.NewOptString("index unreadable"), reported.IndexLoadError)
			require.Equal(t, adminapi.NewOptBool(unhealthy), reported.TenureUnestablished)
		})
	}
}
