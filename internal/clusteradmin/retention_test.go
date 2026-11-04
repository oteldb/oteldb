package clusteradmin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/adminapi"
)

func TestRetentionAggregation(t *testing.T) {
	t.Parallel()
	old := adminapi.EngineSignalStats{Signal: adminapi.SignalLogs, RetainFrom: adminapi.NewOptDateTime(time.Unix(100, 0)), RetentionBoundBy: adminapi.NewOptRetentionBound(adminapi.RetentionBoundAge), RetentionLagSeconds: adminapi.NewOptFloat64(80), RetentionPolicy: adminapi.NewOptSignalRetentionPolicy(adminapi.SignalRetentionPolicy{MaxAgeSeconds: 100})}
	latest := adminapi.EngineSignalStats{Signal: adminapi.SignalLogs, RetainFrom: adminapi.NewOptDateTime(time.Unix(200, 0)), RetentionBoundBy: adminapi.NewOptRetentionBound(adminapi.RetentionBoundSignalBytes), RetentionLagSeconds: adminapi.NewOptFloat64(20), RetentionPolicy: adminapi.NewOptSignalRetentionPolicy(adminapi.SignalRetentionPolicy{MaxBytes: 200})}
	replica := adminapi.EngineSignalStats{Signal: adminapi.SignalLogs, RetentionBoundBy: adminapi.NewOptRetentionBound(adminapi.RetentionBoundNone), RetentionLagSeconds: adminapi.NewOptFloat64(999), RetentionPolicy: adminapi.NewOptSignalRetentionPolicy(adminapi.SignalRetentionPolicy{MaxAgeSeconds: 999})}
	for _, tc := range []struct {
		name    string
		reports []adminapi.EngineSignalStats
	}{
		{"replica first", []adminapi.EngineSignalStats{replica, old, latest}},
		{"replica last", []adminapi.EngineSignalStats{latest, old, replica}},
		{"latest first", []adminapi.EngineSignalStats{latest, replica, old}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			into := adminapi.TenantStats{}
			for _, s := range tc.reports {
				addTenantStats(&into, adminapi.TenantStats{Signals: []adminapi.EngineSignalStats{s}})
			}
			require.Len(t, into.Signals, 1)
			got := into.Signals[0]
			assert.Equal(t, latest.RetainFrom, got.RetainFrom)
			assert.Equal(t, latest.RetentionBoundBy, got.RetentionBoundBy)
			assert.Equal(t, latest.RetentionPolicy, got.RetentionPolicy)
			assert.EqualValues(t, 80, got.RetentionLagSeconds.Or(0))
		})
	}
}

func TestRetentionAdmissionConsensus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		modes        []adminapi.OptBool
		known, value bool
	}{
		{name: "all enabled", modes: []adminapi.OptBool{adminapi.NewOptBool(true), adminapi.NewOptBool(true)}, known: true, value: true},
		{name: "all disabled", modes: []adminapi.OptBool{adminapi.NewOptBool(false), adminapi.NewOptBool(false)}, known: true},
		{name: "mixed", modes: []adminapi.OptBool{adminapi.NewOptBool(true), adminapi.NewOptBool(false)}},
		{name: "missing old node", modes: []adminapi.OptBool{{}, adminapi.NewOptBool(true)}},
		{name: "missing last", modes: []adminapi.OptBool{adminapi.NewOptBool(true), {}}},
		{name: "mixed three nodes", modes: []adminapi.OptBool{adminapi.NewOptBool(true), adminapi.NewOptBool(false), adminapi.NewOptBool(true)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var state retentionAdmissionState
			for _, m := range tc.modes {
				state.add(m)
			}
			got := state.result()
			assert.Equal(t, tc.known, got.IsSet())
			if tc.known {
				assert.Equal(t, tc.value, got.Value)
			}
		})
	}
}

func TestRetentionRejectionCounts(t *testing.T) {
	t.Parallel()
	var into adminapi.TenantStats
	for _, n := range []adminapi.OptInt64{{}, adminapi.NewOptInt64(3), adminapi.NewOptInt64(4), {}} {
		addTenantStats(&into, adminapi.TenantStats{Admission: adminapi.AdmissionStats{RejectedRetention: n}})
	}
	assert.EqualValues(t, 7, into.Admission.RejectedRetention.Or(0))
	var legacy adminapi.TenantStats
	addTenantStats(&legacy, adminapi.TenantStats{})
	assert.False(t, legacy.Admission.RejectedRetention.IsSet())
}

func TestGetStorageRetentionConsensus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		second adminapi.OptBool
		known  bool
	}{
		{name: "uniform", second: adminapi.NewOptBool(true), known: true},
		{name: "old peer"},
		{name: "different config", second: adminapi.NewOptBool(false)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			makeStats := func(mode adminapi.OptBool, cutoff int64, bound adminapi.RetentionBound, count int64) *adminapi.StorageStats {
				e := adminapi.EngineStats{RejectOutOfRetention: mode, Tenants: []adminapi.TenantStats{{Tenant: "t", Admission: adminapi.AdmissionStats{RejectedRetention: adminapi.NewOptInt64(count)}, Signals: []adminapi.EngineSignalStats{{Signal: adminapi.SignalLogs, RetainFrom: adminapi.NewOptDateTime(time.Unix(cutoff, 0).UTC()), RetentionBoundBy: adminapi.NewOptRetentionBound(bound)}}}}}
				return &adminapi.StorageStats{StorageEnabled: true, Engine: adminapi.NewOptEngineStats(e)}
			}
			a := newTestAggregator(t, 2, map[string]*fakeNode{
				"a": {storage: makeStats(adminapi.NewOptBool(true), 100, adminapi.RetentionBoundAge, 3)},
				"b": {storage: makeStats(tc.second, 200, adminapi.RetentionBoundPooledBytes, 4)},
			})
			got, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{})
			require.NoError(t, err)
			require.True(t, got.Engine.IsSet())
			e := got.Engine.Value
			assert.Equal(t, tc.known, e.RejectOutOfRetention.IsSet())
			assert.EqualValues(t, 7, e.Tenants[0].Admission.RejectedRetention.Or(0))
			assert.Equal(t, time.Unix(200, 0).UTC(), e.Tenants[0].Signals[0].RetainFrom.Value)
			assert.Equal(t, adminapi.RetentionBoundPooledBytes, e.Tenants[0].Signals[0].RetentionBoundBy.Value)
		})
	}
}

func TestEqualRetentionCutoffTie(t *testing.T) {
	t.Parallel()
	a := adminapi.EngineSignalStats{Signal: adminapi.SignalLogs, RetainFrom: adminapi.NewOptDateTime(time.Unix(100, 0)), RetentionBoundBy: adminapi.NewOptRetentionBound(adminapi.RetentionBoundAge)}
	b := a
	b.RetentionBoundBy = adminapi.NewOptRetentionBound(adminapi.RetentionBoundPooledBytes)
	for _, tc := range []struct {
		name    string
		reports []adminapi.EngineSignalStats
	}{{"age first", []adminapi.EngineSignalStats{a, b}}, {"age last", []adminapi.EngineSignalStats{b, a}}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var into adminapi.TenantStats
			for _, report := range tc.reports {
				addTenantStats(&into, adminapi.TenantStats{Signals: []adminapi.EngineSignalStats{report}})
			}
			require.Len(t, into.Signals, 1)
			assert.Equal(t, adminapi.RetentionBoundAge, into.Signals[0].RetentionBoundBy.Value)
		})
	}
}

func TestGetStorageLegacyRetentionFieldsAbsent(t *testing.T) {
	t.Parallel()
	legacy := &adminapi.StorageStats{StorageEnabled: true, Engine: adminapi.NewOptEngineStats(adminapi.EngineStats{Tenants: []adminapi.TenantStats{{Tenant: "t", Signals: []adminapi.EngineSignalStats{{Signal: adminapi.SignalLogs}}}}})}
	modern := &adminapi.StorageStats{StorageEnabled: true, Engine: adminapi.NewOptEngineStats(adminapi.EngineStats{RejectOutOfRetention: adminapi.NewOptBool(false), Tenants: []adminapi.TenantStats{{Tenant: "t", Admission: adminapi.AdmissionStats{RejectedRetention: adminapi.NewOptInt64(3)}, Signals: []adminapi.EngineSignalStats{{Signal: adminapi.SignalLogs, RetainFrom: adminapi.NewOptDateTime(time.Unix(100, 0).UTC()), RetentionBoundBy: adminapi.NewOptRetentionBound(adminapi.RetentionBoundAge)}}}}})}
	a := newTestAggregator(t, 2, map[string]*fakeNode{"old": {storage: legacy}, "new": {storage: modern}})
	got, err := a.GetStorage(t.Context(), adminapi.GetStorageParams{})
	require.NoError(t, err)
	require.True(t, got.Engine.IsSet())
	e := got.Engine.Value
	assert.False(t, e.RejectOutOfRetention.IsSet())
	require.Len(t, e.Tenants, 1)
	assert.EqualValues(t, 3, e.Tenants[0].Admission.RejectedRetention.Or(0))
	require.Len(t, e.Tenants[0].Signals, 1)
	assert.Equal(t, time.Unix(100, 0).UTC(), e.Tenants[0].Signals[0].RetainFrom.Value)
}
