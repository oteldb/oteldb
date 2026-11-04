package storagebackend

import (
	"maps"
	"slices"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

func tenancyResolver(cfg *PolicyConfig, overrides map[string]*PolicyConfig) (tenant.Resolver, error) {
	if cfg.empty() && len(overrides) == 0 {
		return nil, nil
	}
	base := mergePolicyConfig(cfg, nil)
	fallback, err := base.policy()
	if err != nil {
		return nil, errors.Wrap(err, "default policy")
	}
	policies := make(map[signal.TenantID]tenant.Policy, len(overrides))
	for _, id := range slices.Sorted(maps.Keys(overrides)) {
		if id == "" {
			return nil, errors.New("tenant \"\": tenant ID must not be empty")
		}
		merged := mergePolicyConfig(cfg, overrides[id])
		policy, err := merged.policy()
		if err != nil {
			return nil, errors.Wrapf(err, "tenant %q", id)
		}
		policies[signal.TenantID(id)] = policy
	}
	return tenant.ResolverFunc(func(id signal.TenantID) tenant.Policy {
		if policy, ok := policies[id]; ok {
			return policy
		}
		return fallback
	}), nil
}

func mergePolicyConfig(base, override *PolicyConfig) PolicyConfig {
	var merged PolicyConfig
	if base != nil {
		merged = *base
	}
	if override == nil {
		return merged
	}
	if override.Retention != nil {
		merged.Retention = override.Retention
	}
	if override.Limits != nil {
		merged.Limits = override.Limits
	}
	if override.Precision != nil {
		merged.Precision = override.Precision
	}
	if override.Downsample != nil {
		merged.Downsample = override.Downsample
	}
	if override.Recompress != nil {
		merged.Recompress = override.Recompress
	}
	if override.EC != nil {
		merged.EC = override.EC
	}
	return merged
}

func logTenantPolicies(cfg Config, resolver tenant.Resolver, lg *zap.Logger) {
	logPolicy := func(id string, policy PolicyConfig, resolved tenant.Policy) {
		lg := lg.With(zap.String("tenant", id))
		lg.Info("Applying embedded storage tenancy policy",
			zap.Int("precision_tiers", len(policy.Precision)),
			zap.Int("downsample_tiers", len(policy.Downsample)),
			zap.Bool("recompress", policy.Recompress != nil), zap.Bool("ec", policy.EC != nil),
			zap.Duration("retention_max_age", resolved.Retention.MaxAge),
			zap.Int("retention_signal_budgets", retentionSignalBudgets(policy.Retention)),
			zap.Bool("limits", policy.Limits != nil),
		)
		warnECInert(cfg.Cluster, &policy, lg)
		logRetentionPolicy(lg, resolved.Retention, cfg.RejectOutOfRetention)
		warnRetentionPolicy(lg, &policy, resolved.Retention)
	}
	base := mergePolicyConfig(cfg.Policy, nil)
	fallback, _ := base.policy() // Validated before opening the engine.
	logPolicy("*", base, fallback)
	for _, id := range slices.Sorted(maps.Keys(cfg.Tenants)) {
		logPolicy(id, mergePolicyConfig(cfg.Policy, cfg.Tenants[id]), resolver.Resolve(signal.TenantID(id)))
	}
}
