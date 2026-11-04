package clusteradmin

import (
	"math"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/adminapi"
)

func TestStorageNodeReportsPreserveObservations(t *testing.T) {
	t.Parallel()
	makeStats := func(legacy, fenced bool, owner string) *adminapi.StorageStats {
		cluster := adminapi.ClusterStats{Self: owner, Members: []adminapi.ClusterMember{{ID: owner}}, Owned: []string{"t/_s0"}}
		signal := adminapi.EngineSignalStats{Signal: adminapi.SignalMetrics}
		if !legacy {
			cluster.Fenced = adminapi.NewOptBool(fenced)
			cluster.PrivateBackend = adminapi.NewOptBool(fenced)
			cluster.Placements = []adminapi.ShardPlacement{{Shard: "t/_s0", Owners: []string{owner}}}
			cluster.LastRebalance = []adminapi.ClusterRebalanceMove{}
			signal.HasReadGap = adminapi.NewOptBool(fenced)
			signal.ReadGapAfterUnixNano = adminapi.NewOptInt64(math.MinInt64)
			signal.LostParts = adminapi.NewOptUint64(7)
		}
		return &adminapi.StorageStats{StorageEnabled: true, Engine: adminapi.NewOptEngineStats(adminapi.EngineStats{
			Cluster: adminapi.NewOptClusterStats(cluster),
			Tenants: []adminapi.TenantStats{{Tenant: "t/_s0", Signals: []adminapi.EngineSignalStats{signal}}},
		})}
	}
	nodes := map[string]*fakeNode{
		"a":      {storage: makeStats(false, true, "a")},
		"b":      {storage: makeStats(false, false, "b")},
		"legacy": {storage: makeStats(true, false, "old")},
		"down":   {err: errors.New("offline")},
	}
	a := newTestAggregator(t, 2, nodes)
	got, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{})
	require.NoError(t, err)
	require.Len(t, got.NodeReports, 4)
	for _, report := range got.NodeReports {
		if report.Node == "down" {
			require.Equal(t, adminapi.ClusterNodeStateUnreachable, report.Status)
			require.False(t, report.Engine.IsSet())
			require.Contains(t, report.Error.Value, "offline")
			continue
		}
		require.Equal(t, adminapi.ClusterNodeStateOk, report.Status)
		require.Equal(t, nodes[report.Node].storage.Engine, report.Engine)
	}
	cluster := got.Engine.Value.Cluster.Value
	require.False(t, cluster.Fenced.IsSet())
	require.False(t, cluster.PrivateBackend.IsSet())
	require.Nil(t, cluster.Placements)
	require.Nil(t, cluster.LastRebalance)
	signal := got.Engine.Value.Tenants[0].Signals[0]
	require.False(t, signal.HasReadGap.IsSet())
	require.False(t, signal.ReadGapAfterUnixNano.IsSet())
	require.False(t, signal.LostParts.IsSet())

	for _, node := range []string{"a", "b", "legacy"} {
		forwarded, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{Node: adminapi.NewOptString(node)})
		require.NoError(t, err)
		require.Equal(t, nodes[node].storage, forwarded)
	}
}

func TestStorageNodeReportsAllUnreachable(t *testing.T) {
	t.Parallel()
	a := newTestAggregator(t, 2, map[string]*fakeNode{"down": {err: errors.New("offline")}})
	got, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{})
	require.NoError(t, err)
	require.False(t, got.Engine.IsSet())
	require.Len(t, got.NodeReports, 1)
	require.Equal(t, adminapi.ClusterNodeStateUnreachable, got.NodeReports[0].Status)
	require.False(t, got.NodeReports[0].Engine.IsSet())
}
