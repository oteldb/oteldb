package clusteradmin

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage/cluster/etcd"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// TestGetHealth pins that the cluster's health is its nodes': one component per member, an
// unreachable node reported unhealthy rather than dropped, and the overall verdict degraded only
// while something still answers.
func TestGetHealth(t *testing.T) {
	t.Parallel()

	healthy := &adminapi.HealthReport{Status: adminapi.HealthStatusHealthy}
	sick := &adminapi.HealthReport{
		Status:     adminapi.HealthStatusDegraded,
		Components: []adminapi.ComponentHealth{{Name: "clickhouse", Status: adminapi.HealthStatusUnhealthy}},
	}

	for _, tt := range []struct {
		name  string
		nodes map[string]*fakeNode
		want  adminapi.HealthStatus
	}{
		{
			name:  "every node healthy",
			nodes: map[string]*fakeNode{"a": {health: healthy}, "b": {health: healthy}},
			want:  adminapi.HealthStatusHealthy,
		},
		{
			name:  "one node down",
			nodes: map[string]*fakeNode{"a": {health: healthy}, "b": {err: errors.New("down")}},
			want:  adminapi.HealthStatusDegraded,
		},
		{
			name:  "one node degraded",
			nodes: map[string]*fakeNode{"a": {health: healthy}, "b": {health: sick}},
			want:  adminapi.HealthStatusDegraded,
		},
		{
			name:  "every node down",
			nodes: map[string]*fakeNode{"a": {err: errors.New("down")}, "b": {err: errors.New("down")}},
			want:  adminapi.HealthStatusUnhealthy,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newTestAggregator(t, 2, tt.nodes).GetHealth(t.Context())
			require.NoError(t, err)

			assert.Equal(t, tt.want, got.Status)
			assert.Len(t, got.Components, len(tt.nodes), "an absent node is reported, not dropped")
		})
	}
}

// TestGetHealthEmptyRing pins that a cluster with no members is unhealthy: an empty aggregate is
// not a healthy cluster holding nothing.
func TestGetHealthEmptyRing(t *testing.T) {
	t.Parallel()

	a, err := New(Options{Peers: staticPeers{}})
	require.NoError(t, err)

	got, err := a.GetHealth(t.Context())
	require.NoError(t, err)

	assert.Equal(t, adminapi.HealthStatusUnhealthy, got.Status)
	assert.Empty(t, got.Components)
}

// TestGetRuntimeSums pins that runtime counters are summed over the nodes that answered, and that a
// memory limit is only reported when every one of them has one — a partial sum would read as
// headroom the unlimited nodes do not have.
func TestGetRuntimeSums(t *testing.T) {
	t.Parallel()

	limited := &adminapi.RuntimeStats{
		Goroutines: 10, NumCPU: 4, Gomaxprocs: 4, HeapAllocBytes: 100, GcCount: 7,
		MemLimitBytes: adminapi.NewOptInt64(1 << 30),
	}
	unlimited := &adminapi.RuntimeStats{Goroutines: 5, NumCPU: 2, Gomaxprocs: 2, HeapAllocBytes: 50, GcCount: 3}

	t.Run("all limited", func(t *testing.T) {
		t.Parallel()

		got, err := newTestAggregator(t, 2, map[string]*fakeNode{
			"a": {runtime: limited}, "b": {runtime: limited},
		}).GetRuntime(t.Context())
		require.NoError(t, err)

		assert.Equal(t, int64(20), got.Goroutines)
		assert.Equal(t, 8, got.NumCPU)
		assert.Equal(t, int64(200), got.HeapAllocBytes)
		assert.Equal(t, int64(14), got.GcCount)
		assert.Equal(t, int64(2<<30), got.MemLimitBytes.Or(0))
	})

	t.Run("mixed limits", func(t *testing.T) {
		t.Parallel()

		got, err := newTestAggregator(t, 2, map[string]*fakeNode{
			"a": {runtime: limited}, "b": {runtime: unlimited},
		}).GetRuntime(t.Context())
		require.NoError(t, err)

		assert.Equal(t, int64(15), got.Goroutines)
		assert.False(t, got.MemLimitBytes.Set)
	})

	t.Run("one node down", func(t *testing.T) {
		t.Parallel()

		got, err := newTestAggregator(t, 2, map[string]*fakeNode{
			"a": {runtime: unlimited}, "b": {err: errors.New("down")},
		}).GetRuntime(t.Context())
		require.NoError(t, err)

		assert.Equal(t, int64(5), got.Goroutines, "a missing node subtracts its share")
	})
}

// TestGetInfoUnion pins that a capability is enabled when any node has it, that the signal list is
// the union of what the members serve, and that a storage backend only names the cluster's when
// every node agrees on it.
func TestGetInfoUnion(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, 2, map[string]*fakeNode{
		"a": {info: &adminapi.InstanceInfo{
			StorageEnabled: true,
			StorageBackend: adminapi.NewOptString("file"),
			Signals: []adminapi.SignalInfo{
				{Signal: adminapi.SignalTraces, Backend: "none"},
				{Signal: adminapi.SignalMetrics, Backend: "storage", Queryable: true, Bind: adminapi.NewOptString(":9090")},
			},
		}},
		"b": {info: &adminapi.InstanceInfo{
			ClickhouseEnabled: true,
			StorageBackend:    adminapi.NewOptString("s3"),
			Signals: []adminapi.SignalInfo{
				{Signal: adminapi.SignalTraces, Backend: "storage", Queryable: true, Bind: adminapi.NewOptString(":3200")},
			},
		}},
	})

	got, err := a.GetInfo(t.Context())
	require.NoError(t, err)

	assert.True(t, got.StorageEnabled)
	assert.True(t, got.ClickhouseEnabled)
	assert.False(t, got.StorageBackend.Set, "a mixed backend describes no single cluster")

	require.Len(t, got.Signals, 2)
	assert.Equal(t, adminapi.SignalMetrics, got.Signals[0].Signal)
	assert.Equal(t, adminapi.SignalTraces, got.Signals[1].Signal)
	assert.Equal(t, "storage", got.Signals[1].Backend, "\"none\" yields to a node that names a backend")
	assert.True(t, got.Signals[1].Queryable)
	assert.Equal(t, ":3200", got.Signals[1].Bind.Or(""))
}

// TestGetEfficiencySums pins that the per-node schema is summed, not deduplicated — it has nowhere
// to say otherwise — and that the derived ratios are recomputed from the totals rather than
// averaged, which would weight a node holding one part like one holding a thousand.
func TestGetEfficiencySums(t *testing.T) {
	t.Parallel()

	node := func(stored, points, logical int64) *fakeNode {
		return &fakeNode{efficiency: &adminapi.EfficiencyStats{
			StorageEnabled: true,
			Tenants: []adminapi.TenantEfficiency{{
				Tenant: "default",
				Signals: []adminapi.SignalEfficiency{{
					Signal: adminapi.SignalMetrics, Series: 1, Parts: 1,
					Points: points, StoredBytes: stored, BytesPerPoint: 99,
					LogicalBytes: adminapi.NewOptInt64(logical),
				}},
			}},
		}}
	}

	got, err := newTestAggregator(t, 2, map[string]*fakeNode{
		"a": node(100, 10, 400),
		"b": node(300, 30, 1200),
	}).GetEfficiency(t.Context(), adminapi.GetEfficiencyParams{})
	require.NoError(t, err)

	require.Len(t, got.Tenants, 1)
	require.Len(t, got.Tenants[0].Signals, 1)

	sig := got.Tenants[0].Signals[0]
	assert.Equal(t, int64(400), sig.StoredBytes)
	assert.Equal(t, int64(40), sig.Points)
	assert.Equal(t, int64(2), sig.Parts)
	assert.InDelta(t, 10.0, sig.BytesPerPoint, 1e-9)
	assert.Equal(t, int64(1600), sig.LogicalBytes.Or(0))
	assert.InDelta(t, 4.0, sig.CompressionRatio.Or(0), 1e-9)
	assert.Empty(t, sig.PartsDetail, "a part belongs to the node holding it, not to the cluster")
}

// fakeMembership is a fixed ring view.
type fakeMembership []etcd.Member

func (m fakeMembership) Members() []etcd.Member { return m }

// TestRingPeersEndpoints pins how a ring member's peer address becomes its admin API URL: the host
// is kept and the cluster RPC port is replaced by the configured admin port.
func TestRingPeersEndpoints(t *testing.T) {
	t.Parallel()

	peers, err := NewRingPeers(RingPeersOptions{
		Members: fakeMembership{
			{ID: "oteldb-1", Addr: "oteldb-1:7946"},
			{ID: "oteldb-2", Addr: "10.20.0.5:7946"},
			{ID: "oteldb-3"},
		},
		Scheme: "http",
		Port:   8090,
	})
	require.NoError(t, err)

	got, err := peers.Peers()
	require.NoError(t, err)

	require.Len(t, got, 3)
	assert.Equal(t, "http://oteldb-1:8090", got[0].Addr)
	assert.Equal(t, "http://10.20.0.5:8090", got[1].Addr, "an address without a port is used as the host")
	assert.Equal(t, "http://oteldb-3:8090", got[2].Addr, "a member with no address falls back to its id")
}

// TestRingPeersReusesClients pins that a node keeps its client across calls: an ogen client owns a
// connection pool, and rebuilding it per request would reconnect to every node on every poll.
func TestRingPeersReusesClients(t *testing.T) {
	t.Parallel()

	members := fakeMembership{{ID: "oteldb-1", Addr: "oteldb-1:7946"}}
	peers, err := NewRingPeers(RingPeersOptions{Members: members, Scheme: "http", Port: 8090})
	require.NoError(t, err)

	first, err := peers.Peers()
	require.NoError(t, err)
	second, err := peers.Peers()
	require.NoError(t, err)

	require.Len(t, first, 1)
	require.Len(t, second, 1)
	assert.Same(t, first[0].Client, second[0].Client)
}

// TestRingPeersRejectsBadConfig pins that an unusable endpoint template fails at construction, not
// on the first request with every node unreachable.
func TestRingPeersRejectsBadConfig(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		scheme string
		port   int
	}{
		{"no scheme", "", 8090},
		{"unsupported scheme", "grpc", 8090},
		{"port out of range", "http", 0},
		{"port too large", "http", 70000},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewRingPeers(RingPeersOptions{
				Members: fakeMembership{}, Scheme: tt.scheme, Port: tt.port,
			})
			require.Error(t, err)
		})
	}
}
