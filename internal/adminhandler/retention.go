package adminhandler

import (
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"

	"github.com/oteldb/oteldb/internal/adminapi"
)

// RetentionPolicyProvider exposes configured retention when the adapter knows its resolver.
type RetentionPolicyProvider interface {
	RetentionPolicy(signal.TenantID) (tenant.Retention, bool)
	RejectOutOfRetention() bool
}

func mapRetentionPolicies(engine *adminapi.EngineStats, provider RetentionPolicyProvider) {
	if _, known := provider.RetentionPolicy(""); known {
		engine.RejectOutOfRetention = adminapi.NewOptBool(provider.RejectOutOfRetention())
	}
	for i := range engine.Tenants {
		t := &engine.Tenants[i]
		policy, known := provider.RetentionPolicy(signal.TenantID(t.Tenant))
		if !known {
			continue
		}
		for j := range t.Signals {
			stats := &t.Signals[j]
			sig, err := engineSignal(stats.Signal)
			if err != nil {
				continue
			}
			stats.RetentionPolicy = adminapi.NewOptSignalRetentionPolicy(adminapi.SignalRetentionPolicy{
				MaxAgeSeconds:  max(policy.AgeFor(sig).Seconds(), 0),
				MaxBytes:       max(policy.PerSignal[sig].MaxBytes, 0),
				PooledMaxBytes: max(policy.MaxBytes, 0),
			})
		}
	}
}
