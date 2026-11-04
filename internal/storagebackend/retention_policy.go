package storagebackend

import (
	"maps"
	"time"

	"go.uber.org/zap"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"
)

// RetentionPolicy returns the effective configured policy when its resolver is known.
func (b *Backend) RetentionPolicy(id signal.TenantID) (tenant.Retention, bool) {
	if b.policyResolver == nil {
		return tenant.Retention{}, false
	}
	id = signal.TenantID(cluster.TenantOfShard(id).String())
	policy := b.policyResolver.Resolve(id).Retention
	policy.PerSignal = maps.Clone(policy.PerSignal)
	return policy, true
}

// RejectOutOfRetention reports whether expired-write admission is configured.
func (b *Backend) RejectOutOfRetention() bool { return b.rejectOutOfRetention }

func logRetentionPolicy(lg *zap.Logger, r tenant.Retention, reject bool) {
	for sig := signal.Metric; sig <= signal.Exemplar; sig++ {
		lg.Info("Effective embedded storage retention",
			zap.Stringer("signal", sig), zap.Duration("max_age", r.AgeFor(sig)),
			zap.Int64("max_bytes", r.PerSignal[sig].MaxBytes), zap.Int64("pooled_max_bytes", r.MaxBytes),
			zap.Bool("reject_out_of_retention", reject),
		)
	}
}

func warnRetentionPolicy(lg *zap.Logger, cfg *PolicyConfig, r tenant.Retention) {
	traceAge, exemplarAge := r.AgeFor(signal.Trace), r.AgeFor(signal.Exemplar)
	if traceAge > 0 && (exemplarAge <= 0 || exemplarAge > traceAge) {
		lg.Warn("Exemplar retention outlives trace retention", zap.Duration("exemplar_max_age", exemplarAge), zap.Duration("trace_max_age", traceAge))
	}
	metricAge := r.AgeFor(signal.Metric)
	if cfg == nil || metricAge <= 0 {
		return
	}
	for _, t := range cfg.Precision {
		if t.Bits > 0 && t.Bits < 64 && t.After >= metricAge {
			warnRetentionTier(lg, "precision", t.After, metricAge)
		}
	}
	for _, t := range cfg.Downsample {
		if t.Interval > 0 && t.After >= metricAge {
			warnRetentionTier(lg, "downsample", t.After, metricAge)
		}
	}
	if t := cfg.Recompress; t != nil && t.After >= metricAge {
		warnRetentionTier(lg, "recompress", t.After, metricAge)
	}
	if t := cfg.EC; t != nil && t.After >= metricAge {
		warnRetentionTier(lg, "ec", t.After, metricAge)
	}
}

func warnRetentionTier(lg *zap.Logger, tier string, after, age time.Duration) {
	lg.Warn("Storage policy tier starts at or after metric retention expires", zap.String("tier", tier), zap.Duration("after", after), zap.Duration("metric_max_age", age))
}
