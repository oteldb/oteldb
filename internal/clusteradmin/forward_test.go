package clusteradmin

import (
	"testing"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// twoNodes is a cluster whose members report different figures, so an answer that aggregated them
// cannot be mistaken for one that addressed a single node.
func twoNodes() map[string]*fakeNode {
	return map[string]*fakeNode{
		"a": {
			info:    &adminapi.InstanceInfo{StorageEnabled: true},
			health:  &adminapi.HealthReport{Status: adminapi.HealthStatusHealthy},
			runtime: &adminapi.RuntimeStats{Goroutines: 10, NumCPU: 4, HeapAllocBytes: 100},
			storage: &adminapi.StorageStats{StorageEnabled: true},
			efficiency: &adminapi.EfficiencyStats{
				StorageEnabled: true,
				Tenants: []adminapi.TenantEfficiency{{
					Tenant: "default",
					Signals: []adminapi.SignalEfficiency{{
						Signal: adminapi.SignalMetrics, Parts: 1, Points: 10, StoredBytes: 100,
						PartsDetail: []adminapi.PartEfficiency{part("default/metrics/a", 100, 10)},
					}},
				}},
			},
		},
		"b": {
			info:    &adminapi.InstanceInfo{StorageEnabled: true},
			health:  &adminapi.HealthReport{Status: adminapi.HealthStatusHealthy},
			runtime: &adminapi.RuntimeStats{Goroutines: 5, NumCPU: 2, HeapAllocBytes: 50},
			storage: &adminapi.StorageStats{StorageEnabled: true},
			efficiency: &adminapi.EfficiencyStats{
				StorageEnabled: true,
				Tenants: []adminapi.TenantEfficiency{{
					Tenant: "default",
					Signals: []adminapi.SignalEfficiency{{
						Signal: adminapi.SignalMetrics, Parts: 1, Points: 30, StoredBytes: 300,
						PartsDetail: []adminapi.PartEfficiency{part("default/metrics/b", 300, 30)},
					}},
				}},
			},
		},
	}
}

// TestForwardAddressesOneNode pins the point of the node parameter: the answer is that member's
// own, not the cluster's fold of every member's.
func TestForwardAddressesOneNode(t *testing.T) {
	t.Parallel()

	t.Run("runtime", func(t *testing.T) {
		t.Parallel()

		a := newTestAggregator(t, 2, twoNodes())

		got, err := a.GetRuntime(t.Context(), adminapi.GetRuntimeParams{Node: adminapi.NewOptString("b")})
		require.NoError(t, err)

		assert.Equal(t, int64(5), got.Goroutines, "the named node's own counter, not the sum")
		assert.Equal(t, 2, got.NumCPU)
	})

	t.Run("health", func(t *testing.T) {
		t.Parallel()

		nodes := twoNodes()
		nodes["b"] = &fakeNode{err: errors.New("down")}
		a := newTestAggregator(t, 2, nodes)

		got, err := a.GetHealth(t.Context(), adminapi.GetHealthParams{Node: adminapi.NewOptString("a")})
		require.NoError(t, err)

		assert.Equal(t, adminapi.HealthStatusHealthy, got.Status)
		assert.Empty(t, got.Components,
			"a node's own report, not one component per member")

		// A node that cannot answer fails the request rather than subtracting its share: a request
		// that named a node has no useful partial form.
		_, err = a.GetHealth(t.Context(), adminapi.GetHealthParams{Node: adminapi.NewOptString("b")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "node b")
	})

	t.Run("efficiency keeps the node's parts", func(t *testing.T) {
		t.Parallel()

		a := newTestAggregator(t, 2, twoNodes())

		got, err := a.GetEfficiency(t.Context(), adminapi.GetEfficiencyParams{
			Node:  adminapi.NewOptString("b"),
			Parts: adminapi.NewOptBool(true),
		})
		require.NoError(t, err)

		require.Len(t, got.Tenants, 1)
		require.Len(t, got.Tenants[0].Signals, 1)

		sig := got.Tenants[0].Signals[0]
		assert.Equal(t, int64(300), sig.StoredBytes, "one node's bytes, not the cluster's")
		require.Len(t, sig.PartsDetail, 1, "one node's parts are exactly what it has to say")
		assert.Equal(t, "default/metrics/b", sig.PartsDetail[0].ID)
	})

	t.Run("storage", func(t *testing.T) {
		t.Parallel()

		nodes := twoNodes()
		nodes["b"].storage = &adminapi.StorageStats{ClickhouseEnabled: true}
		a := newTestAggregator(t, 2, nodes)

		got, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{Node: adminapi.NewOptString("b")})
		require.NoError(t, err)

		assert.False(t, got.StorageEnabled, "the named node's own configuration, not the union")
		assert.True(t, got.ClickhouseEnabled)
	})
}

// TestForwardPerNodeOnlyOperations pins the reason the parameter exists. Both operations stay
// refused cluster-wide, and both answer once a node is named: attribution then decodes one node's
// parts, and an action addressed to one member has no partial failure to contract for.
func TestForwardPerNodeOnlyOperations(t *testing.T) {
	t.Parallel()

	t.Run("stream costs", func(t *testing.T) {
		t.Parallel()

		a := newTestAggregator(t, 2, twoNodes())
		params := adminapi.GetStreamCostsParams{
			Signal:  adminapi.RecordSignalLogs,
			GroupBy: adminapi.NewOptString("service.name"),
		}

		_, err := a.GetStreamCosts(t.Context(), params)
		require.Error(t, err, "unaddressed, attribution would decode each replicated part once per replica")

		params.Node = adminapi.NewOptString("a")
		got, err := a.GetStreamCosts(t.Context(), params)
		require.NoError(t, err)
		assert.Equal(t, adminapi.RecordSignalLogs, got.Signal)
		assert.Equal(t, "service.name", got.GroupBy.Or(""), "the rest of the query is forwarded intact")
	})

	t.Run("actions", func(t *testing.T) {
		t.Parallel()

		nodes := twoNodes()
		a := newTestAggregator(t, 2, nodes)
		params := adminapi.RunActionParams{Action: adminapi.ActionNameGc}

		_, err := a.RunAction(t.Context(), params)
		require.Error(t, err, "an action that half-succeeds across a cluster has no contract here")

		params.Node = adminapi.NewOptString("b")
		got, err := a.RunAction(t.Context(), params)
		require.NoError(t, err)
		assert.Equal(t, adminapi.ActionNameGc, got.Action)
		assert.True(t, got.Ok)

		assert.Empty(t, nodes["a"].ran, "only the named node is mutated")
		assert.Equal(t, []adminapi.ActionName{adminapi.ActionNameGc}, nodes["b"].ran)
	})
}

// TestForwardUnknownNode pins that an id no member answers to is named as such, listing the members
// that do exist: the client picked from a list that may have changed under it.
func TestForwardUnknownNode(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, 2, twoNodes())

	_, err := a.GetRuntime(t.Context(), adminapi.GetRuntimeParams{Node: adminapi.NewOptString("c")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `node "c" is not a member`)
	assert.Contains(t, err.Error(), "a, b")
}

// TestGetClusterNodes pins the membership query behind the selector: every member is listed whether
// it answers or not, since a node that cannot answer is exactly what an operator needs to see
// before addressing it.
func TestGetClusterNodes(t *testing.T) {
	t.Parallel()

	nodes := twoNodes()
	nodes["b"] = &fakeNode{err: errors.New("down")}

	got, err := newTestAggregator(t, 2, nodes).GetClusterNodes(t.Context())
	require.NoError(t, err)

	require.Len(t, got.Nodes, 2)
	assert.Equal(t, "a", got.Nodes[0].Node)
	assert.Equal(t, adminapi.ClusterNodeStateOk, got.Nodes[0].Status)
	assert.NotEmpty(t, got.Nodes[0].Addr.Or(""), "the selector needs the endpoint it would reach")

	assert.Equal(t, "b", got.Nodes[1].Node)
	assert.Equal(t, adminapi.ClusterNodeStateUnreachable, got.Nodes[1].Status)
	assert.Contains(t, got.Nodes[1].Error.Or(""), "down")
}

// encodeJSON renders a response the way the server does, so an assertion about it is an assertion
// about the bytes a client receives.
func encodeJSON(t *testing.T, v interface{ Encode(*jx.Encoder) }) string {
	t.Helper()

	var e jx.Encoder
	v.Encode(&e)

	return e.String()
}

// TestUnaddressedResponsesUnchanged pins the regression the node parameter risks: a request that
// names no node must answer exactly as it did before the parameter existed. The expectations are
// written out rather than derived, so a change in the aggregation shows up here as a diff instead
// of being recomputed by the test along with the code.
func TestUnaddressedResponsesUnchanged(t *testing.T) {
	t.Parallel()

	a := newTestAggregator(t, 2, twoNodes())

	t.Run("runtime", func(t *testing.T) {
		t.Parallel()

		got, err := a.GetRuntime(t.Context(), adminapi.GetRuntimeParams{})
		require.NoError(t, err)

		assert.JSONEq(t, `{
			"goroutines": 15, "num_cpu": 6, "gomaxprocs": 0,
			"heap_alloc_bytes": 150, "heap_inuse_bytes": 0, "heap_sys_bytes": 0,
			"stack_inuse_bytes": 0, "gc_count": 0, "next_gc_bytes": 0
		}`, encodeJSON(t, got))
	})

	t.Run("health", func(t *testing.T) {
		t.Parallel()

		got, err := a.GetHealth(t.Context(), adminapi.GetHealthParams{})
		require.NoError(t, err)

		require.Len(t, got.Components, 2)
		// Durations and addresses are per-run, and pinning them would pin the test host instead of
		// the aggregation.
		for i := range got.Components {
			got.Components[i].Addr.Reset()
		}

		assert.JSONEq(t, `{
			"status": "healthy",
			"components": [
				{"name": "a", "status": "healthy"},
				{"name": "b", "status": "healthy"}
			]
		}`, encodeJSON(t, got))
	})

	t.Run("storage", func(t *testing.T) {
		t.Parallel()

		got, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{})
		require.NoError(t, err)

		assert.JSONEq(t, `{"storage_enabled": true, "clickhouse_enabled": false}`, encodeJSON(t, got))
	})

	t.Run("efficiency", func(t *testing.T) {
		t.Parallel()

		got, err := a.GetEfficiency(t.Context(), adminapi.GetEfficiencyParams{})
		require.NoError(t, err)

		assert.JSONEq(t, `{
			"storage_enabled": true,
			"tenants": [{
				"tenant": "default",
				"signals": [{
					"signal": "metrics", "series": 0, "parts": 2, "points": 40,
					"stored_bytes": 400, "bytes_per_point": 10
				}]
			}]
		}`, encodeJSON(t, got), "parts stay dropped from the aggregate")
	})
}
