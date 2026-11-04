package adminhandler

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"

	"github.com/oteldb/oteldb/internal/adminapi"
)

func TestMapRetentionStats(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		stats  storage.SignalStats
		cutoff bool
		lag    float64
	}{
		{name: "no target", stats: storage.SignalStats{Signal: signal.Metric, Parts: 1, MinTimeUnixNano: 1}},
		{name: "empty", stats: storage.SignalStats{Signal: signal.Metric, RetainFromUnixNano: 3e9, RetentionBoundBy: storage.RetentionBoundAge}, cutoff: true},
		{name: "converged", stats: storage.SignalStats{Signal: signal.Metric, Parts: 1, MinTimeUnixNano: 4e9, RetainFromUnixNano: 3e9, RetentionBoundBy: storage.RetentionBoundAge}, cutoff: true},
		{name: "epoch zero", stats: storage.SignalStats{Signal: signal.Log, Parts: 1, RetainFromUnixNano: 3e9, RetentionBoundBy: storage.RetentionBoundSignalBytes}, cutoff: true, lag: 3},
		{name: "negative", stats: storage.SignalStats{Signal: signal.Trace, Parts: 1, MinTimeUnixNano: -2e9, RetainFromUnixNano: 3e9, RetentionBoundBy: storage.RetentionBoundPooledBytes}, cutoff: true, lag: 5},
		{name: "overflow-safe", stats: storage.SignalStats{Signal: signal.Trace, Parts: 1, MinTimeUnixNano: math.MinInt64, RetainFromUnixNano: math.MaxInt64, RetentionBoundBy: storage.RetentionBoundAge}, cutoff: true, lag: float64(math.MaxInt64)/1e9 - float64(math.MinInt64)/1e9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := mapSignalStats(tc.stats)
			assert.Equal(t, tc.cutoff, got.RetainFrom.IsSet())
			assert.Equal(t, tc.cutoff, got.RetentionLagSeconds.IsSet())
			assert.InDelta(t, tc.lag, got.RetentionLagSeconds.Or(0), 1e-9)
			assert.Equal(t, adminapi.RetentionBound(tc.stats.RetentionBoundBy.String()), got.RetentionBoundBy.Or(""))
			if tc.cutoff {
				assert.Equal(t, time.Unix(0, tc.stats.RetainFromUnixNano).UTC(), got.RetainFrom.Or(time.Time{}))
			}
		})
	}
}

type policyEngine struct {
	fakeEngine
	policy        tenant.Retention
	known, reject bool
	tenants       []signal.TenantID
}

func (p *policyEngine) RetentionPolicy(t signal.TenantID) (tenant.Retention, bool) {
	p.tenants = append(p.tenants, t)
	return p.policy, p.known
}

func (p *policyEngine) RejectOutOfRetention() bool { return p.reject }

func TestAdminConfiguredRetention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		known, reject bool
		policy        tenant.Retention
	}{
		{name: "known overrides", known: true, reject: true, policy: tenant.Retention{MaxAge: 24 * time.Hour, MaxBytes: 1000, PerSignal: map[signal.Signal]tenant.SignalRetention{signal.Log: {MaxAge: time.Hour, MaxBytes: 400}}}},
		{name: "known unbounded", known: true},
		{name: "unknown custom source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			engine := &policyEngine{known: tc.known, reject: tc.reject, policy: tc.policy}
			engine.stats = storage.StoreStats{Tenants: []storage.TenantStats{{Tenant: "customer", Admission: storage.AdmissionStats{RejectedRetention: 7}, Signals: []storage.SignalStats{{Signal: signal.Log}, {Signal: signal.Metric}}}}}
			srv := testServer(t, Options{Engine: engine})
			t.Cleanup(srv.Close)
			var result adminapi.StorageStats
			get(t, srv.URL, "/api/v1/storage", &result)
			require.True(t, result.Engine.IsSet())
			got := result.Engine.Value
			assert.Equal(t, tc.known, got.RejectOutOfRetention.IsSet())
			if tc.known {
				assert.Equal(t, tc.reject, got.RejectOutOfRetention.Value)
			}
			require.Len(t, got.Tenants, 1)
			assert.EqualValues(t, 7, got.Tenants[0].Admission.RejectedRetention.Or(0))
			for _, s := range got.Tenants[0].Signals {
				assert.Equal(t, tc.known, s.RetentionPolicy.IsSet())
				if !tc.known {
					continue
				}
				sig, err := engineSignal(s.Signal)
				require.NoError(t, err)
				assert.Equal(t, tc.policy.AgeFor(sig).Seconds(), s.RetentionPolicy.Value.MaxAgeSeconds)
				assert.Equal(t, tc.policy.PerSignal[sig].MaxBytes, s.RetentionPolicy.Value.MaxBytes)
				assert.Equal(t, tc.policy.MaxBytes, s.RetentionPolicy.Value.PooledMaxBytes)
			}
			assert.Contains(t, engine.tenants, signal.TenantID("customer"))
		})
	}
}
