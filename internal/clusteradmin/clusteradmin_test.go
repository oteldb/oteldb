package clusteradmin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// fakeNode is one member node's admin API, either answering with canned values or refusing.
type fakeNode struct {
	adminapi.UnimplementedHandler

	info       *adminapi.InstanceInfo
	health     *adminapi.HealthReport
	runtime    *adminapi.RuntimeStats
	storage    *adminapi.StorageStats
	efficiency *adminapi.EfficiencyStats

	// err makes every call fail, standing in for an unreachable or broken node.
	err error
	// wantParts records whether the last efficiency call asked for part identities.
	wantParts bool
}

// NewError renders f.err the way a real node does, so a test can assert the aggregator surfaces
// the node's own message rather than a transport-level one.
func (f *fakeNode) NewError(_ context.Context, err error) *adminapi.ErrorStatusCode {
	return &adminapi.ErrorStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response:   adminapi.Error{ErrorMessage: err.Error()},
	}
}

func (f *fakeNode) GetInfo(context.Context) (*adminapi.InstanceInfo, error) {
	return f.info, f.err
}

func (f *fakeNode) GetHealth(context.Context) (*adminapi.HealthReport, error) {
	return f.health, f.err
}

func (f *fakeNode) GetRuntime(context.Context) (*adminapi.RuntimeStats, error) {
	return f.runtime, f.err
}

func (f *fakeNode) GetStorage(context.Context) (*adminapi.StorageStats, error) {
	return f.storage, f.err
}

func (f *fakeNode) GetEfficiency(
	_ context.Context, params adminapi.GetEfficiencyParams,
) (*adminapi.EfficiencyStats, error) {
	f.wantParts = params.Parts.Or(false)

	return f.efficiency, f.err
}

// staticPeers is a fixed peer list, standing in for a live ring view.
type staticPeers struct {
	peers []Peer
	err   error
}

func (s staticPeers) Peers() ([]Peer, error) { return s.peers, s.err }

// newTestAggregator serves each node over HTTP and points an aggregator at them, so the fan-out
// exercises the real ogen client and encoding rather than an in-process call.
func newTestAggregator(t *testing.T, rf int, nodes map[string]*fakeNode) *Aggregator {
	t.Helper()

	var peers []Peer
	for name, node := range nodes {
		srv, err := adminapi.NewServer(node)
		require.NoError(t, err)

		ts := httptest.NewServer(srv)
		t.Cleanup(ts.Close)

		client, err := adminapi.NewClient(ts.URL)
		require.NoError(t, err)

		peers = append(peers, Peer{Node: name, Addr: ts.URL, Client: client})
	}

	a, err := New(Options{
		Peers:             staticPeers{peers: peers},
		ReplicationFactor: rf,
		Timeout:           10 * time.Second,
	})
	require.NoError(t, err)

	return a
}

// part builds one part's efficiency entry.
func part(id string, bytes, rows int64) adminapi.PartEfficiency {
	return adminapi.PartEfficiency{ID: id, Bytes: bytes, Rows: rows, Series: 1}
}

// efficiency builds a single-tenant, metrics-only efficiency report from a part list.
func efficiency(parts ...adminapi.PartEfficiency) *adminapi.EfficiencyStats {
	se := adminapi.SignalEfficiency{Signal: adminapi.SignalMetrics, Series: 1, PartsDetail: parts}
	for _, p := range parts {
		se.Parts++
		se.Points += p.Rows
		se.StoredBytes += p.Bytes
	}

	return &adminapi.EfficiencyStats{
		StorageEnabled: true,
		Tenants: []adminapi.TenantEfficiency{{
			Tenant:  "default",
			Signals: []adminapi.SignalEfficiency{se},
		}},
	}
}

// TestGetClusterStorageDedupe pins the whole point of the endpoint: summing per-node bytes counts a
// mirrored part once per replica, and the logical figures must count it once — by part id, never by
// dividing the physical total by the replication factor, which is wrong exactly when replication is
// incomplete.
func TestGetClusterStorageDedupe(t *testing.T) {
	t.Parallel()

	mirrored := []adminapi.PartEfficiency{part("default/metrics/a", 100, 10), part("default/metrics/b", 200, 20)}

	for _, tt := range []struct {
		name  string
		rf    int
		nodes map[string]*fakeNode

		wantComplete  bool
		wantLogical   int64
		wantPhysical  int64
		wantLogParts  int64
		wantPhysParts int64
		wantLogPoints int64
		wantNodes     int
	}{
		{
			name: "fully replicated",
			rf:   2,
			nodes: map[string]*fakeNode{
				"oteldb-1": {efficiency: efficiency(mirrored...)},
				"oteldb-2": {efficiency: efficiency(mirrored...)},
				"oteldb-3": {efficiency: &adminapi.EfficiencyStats{StorageEnabled: true}},
			},
			wantComplete: true,
			wantLogical:  300, wantPhysical: 600,
			wantLogParts: 2, wantPhysParts: 4, wantLogPoints: 30, wantNodes: 2,
		},
		{
			// A rebalance in flight: one part has a second copy, the other does not. physical/rf is
			// 250 here, which is not the 300 bytes the cluster actually holds.
			name: "under-replicated",
			rf:   2,
			nodes: map[string]*fakeNode{
				"oteldb-1": {efficiency: efficiency(mirrored...)},
				"oteldb-2": {efficiency: efficiency(part("default/metrics/a", 100, 10))},
			},
			wantComplete: true,
			wantLogical:  300, wantPhysical: 400,
			wantLogParts: 2, wantPhysParts: 3, wantLogPoints: 30, wantNodes: 2,
		},
		{
			// Distinct parts on every node: nothing to deduplicate, so the two readings agree.
			name: "no replication",
			rf:   1,
			nodes: map[string]*fakeNode{
				"oteldb-1": {efficiency: efficiency(part("default/metrics/a", 100, 10))},
				"oteldb-2": {efficiency: efficiency(part("default/metrics/b", 200, 20))},
			},
			wantComplete: true,
			wantLogical:  300, wantPhysical: 300,
			wantLogParts: 2, wantPhysParts: 2, wantLogPoints: 30, wantNodes: 2,
		},
		{
			// A node reporting parts without their identities cannot be deduplicated, so it counts
			// physically while the report stops claiming completeness.
			name: "node without part identities",
			rf:   2,
			nodes: map[string]*fakeNode{
				"oteldb-1": {efficiency: efficiency(mirrored...)},
				"oteldb-2": {efficiency: stripParts(efficiency(mirrored...))},
			},
			wantComplete: false,
			wantLogical:  300, wantPhysical: 600,
			wantLogParts: 2, wantPhysParts: 4, wantLogPoints: 30, wantNodes: 2,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newTestAggregator(t, tt.rf, tt.nodes).GetClusterStorage(t.Context())
			require.NoError(t, err)

			assert.Equal(t, tt.rf, got.ReplicationFactor)
			assert.Equal(t, tt.wantComplete, got.Complete)
			assert.Len(t, got.Nodes, len(tt.nodes))

			require.Len(t, got.Tenants, 1)
			require.Len(t, got.Tenants[0].Signals, 1)

			sig := got.Tenants[0].Signals[0]
			assert.Equal(t, "default", got.Tenants[0].Tenant)
			assert.Equal(t, adminapi.SignalMetrics, sig.Signal)
			assert.Equal(t, tt.wantLogical, sig.LogicalBytes)
			assert.Equal(t, tt.wantPhysical, sig.PhysicalBytes)
			assert.Equal(t, tt.wantLogParts, sig.LogicalParts)
			assert.Equal(t, tt.wantPhysParts, sig.PhysicalParts)
			assert.Equal(t, tt.wantLogPoints, sig.LogicalPoints)
			assert.Equal(t, tt.rf, sig.ReplicationFactor)
			assert.Len(t, sig.Nodes, tt.wantNodes)
		})
	}
}

// stripParts drops the part identities from a report, standing in for a node that does not send
// them.
func stripParts(s *adminapi.EfficiencyStats) *adminapi.EfficiencyStats {
	for i := range s.Tenants {
		for j := range s.Tenants[i].Signals {
			s.Tenants[i].Signals[j].PartsDetail = nil
		}
	}

	return s
}

// TestGetClusterStorageAsksForParts pins that the aggregator requests part identities: without them
// every logical figure silently collapses to the physical one.
func TestGetClusterStorageAsksForParts(t *testing.T) {
	t.Parallel()

	node := &fakeNode{efficiency: efficiency(part("default/metrics/a", 100, 10))}

	_, err := newTestAggregator(t, 1, map[string]*fakeNode{"oteldb-1": node}).GetClusterStorage(t.Context())
	require.NoError(t, err)

	assert.True(t, node.wantParts)
}

// TestGetClusterStoragePartialFailure pins the degradation contract: an unreachable node subtracts
// its share instead of failing the response, is named in the per-node status, and makes the logical
// figures an explicit lower bound rather than a silently smaller number.
func TestGetClusterStoragePartialFailure(t *testing.T) {
	t.Parallel()

	only := part("default/metrics/only-on-two", 500, 50)

	a := newTestAggregator(t, 2, map[string]*fakeNode{
		"oteldb-1": {efficiency: efficiency(part("default/metrics/a", 100, 10))},
		"oteldb-2": {efficiency: efficiency(only), err: errors.New("boom")},
	})

	got, err := a.GetClusterStorage(t.Context())
	require.NoError(t, err)

	assert.False(t, got.Complete, "a missing node makes the deduplicated totals a lower bound")

	byNode := map[string]adminapi.ClusterNodeStatus{}
	for _, n := range got.Nodes {
		byNode[n.Node] = n
	}
	require.Len(t, byNode, 2)
	assert.Equal(t, adminapi.ClusterNodeStateOk, byNode["oteldb-1"].Status)
	assert.Equal(t, adminapi.ClusterNodeStateUnreachable, byNode["oteldb-2"].Status)
	assert.Contains(t, byNode["oteldb-2"].Error.Or(""), "boom")

	require.Len(t, got.Tenants, 1)
	sig := got.Tenants[0].Signals[0]
	assert.Equal(t, int64(100), sig.LogicalBytes, "the absent node's part is missing from the union")
	assert.Equal(t, int64(100), sig.PhysicalBytes)
	assert.Len(t, sig.Nodes, 1)
}

// TestGetClusterStorageEveryNodeDown pins that a total outage is still an answer: an admin page has
// to render the fact that nothing responded.
func TestGetClusterStorageEveryNodeDown(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, 2, map[string]*fakeNode{
		"oteldb-1": {err: errors.New("down")},
		"oteldb-2": {err: errors.New("down")},
	})

	got, err := a.GetClusterStorage(t.Context())
	require.NoError(t, err)

	assert.False(t, got.Complete)
	assert.Empty(t, got.Tenants)
	assert.Len(t, got.Nodes, 2)
}

// TestGetClusterStorageSortsSignals pins a stable ordering, so a dashboard polling the endpoint does
// not see rows swap places between refreshes.
func TestGetClusterStorageSortsSignals(t *testing.T) {
	t.Parallel()

	mixed := &adminapi.EfficiencyStats{
		StorageEnabled: true,
		Tenants: []adminapi.TenantEfficiency{
			{Tenant: "zeta", Signals: []adminapi.SignalEfficiency{{Signal: adminapi.SignalTraces}}},
			{Tenant: "alpha", Signals: []adminapi.SignalEfficiency{
				{Signal: adminapi.SignalTraces},
				{Signal: adminapi.SignalLogs},
			}},
		},
	}

	got, err := newTestAggregator(t, 1, map[string]*fakeNode{
		"oteldb-1": {efficiency: mixed},
	}).GetClusterStorage(t.Context())
	require.NoError(t, err)

	require.Len(t, got.Tenants, 2)
	assert.Equal(t, "alpha", got.Tenants[0].Tenant)
	assert.Equal(t, "zeta", got.Tenants[1].Tenant)
	require.Len(t, got.Tenants[0].Signals, 2)
	assert.Equal(t, adminapi.SignalLogs, got.Tenants[0].Signals[0].Signal)
	assert.Equal(t, adminapi.SignalTraces, got.Tenants[0].Signals[1].Signal)
}

// TestPeerResolutionFailure pins that a broken membership view is an error rather than an empty
// cluster, which would read as a healthy cluster holding nothing.
func TestPeerResolutionFailure(t *testing.T) {
	t.Parallel()

	a, err := New(Options{Peers: staticPeers{err: errors.New("etcd is gone")}})
	require.NoError(t, err)

	_, err = a.GetClusterStorage(t.Context())
	require.ErrorContains(t, err, "etcd is gone")
}

// TestNewRequiresPeers pins that an aggregator with nothing to aggregate is refused at construction
// rather than started to serve empty reports.
func TestNewRequiresPeers(t *testing.T) {
	t.Parallel()

	_, err := New(Options{})
	require.Error(t, err)
}

// TestWritesAreNotAggregated pins the read-only scope: fanning a mutating action across a cluster
// needs a partial-failure contract this API does not have, so it is refused rather than half-run.
func TestWritesAreNotAggregated(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, 1, map[string]*fakeNode{"oteldb-1": {}})

	_, err := a.RunAction(t.Context(), adminapi.RunActionParams{Action: adminapi.ActionNameGc})
	require.Error(t, err)

	_, err = a.GetStreamCosts(t.Context(), adminapi.GetStreamCostsParams{})
	require.Error(t, err)
}
