package storagebackend

import (
	"time"

	"github.com/go-faster/errors"
	"go.uber.org/zap"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster/ec"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/tenant"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// PolicyConfig configures the per-tenant storage policy applied to the embedded engine's
// background merges: age-tiered lossy float precision, downsampling, cold-data recompression, and
// cold-data erasure coding.
// Unset tenant override blocks inherit the corresponding default policy blocks.
type PolicyConfig struct {
	// Precision is the age-tiered lossy float-compression policy: each tier re-encodes, at merge,
	// the value column of parts older than After to retain only Bits significant mantissa bits, so
	// recent data stays lossless and only old data trades accuracy for size. Empty ⇒ lossless.
	Precision []PrecisionTierConfig `json:"precision" yaml:"precision"`
	// Downsample is the age-tiered merge-time rollup: each tier replaces samples older than After
	// with one representative per Interval-wide bucket, the bucket combined by Agg. Empty ⇒ raw.
	Downsample []DownsampleTierConfig `json:"downsample" yaml:"downsample"`
	// Recompress rewrites fully-cold parts (older than After) at a higher Zstandard level than the
	// size ladder picks, trading merge CPU for storage. It is decode-transparent. Nil ⇒ no archival
	// tier, which does not mean uncompressed: a merge always compresses at a level its part's size
	// earns.
	Recompress *RecompressConfig `json:"recompress" yaml:"recompress"`
	// EC erasure-codes fully-cold parts (older than After) across Data+Parity cluster nodes instead
	// of storing RF full copies. Like Recompress it is an age tier, so recent data stays full-copy
	// for fast local reads. Nil ⇒ full-copy replication.
	EC *ECConfig `json:"ec" yaml:"ec"`
	// Retention bounds readable age and eventually cleans up stored data by age and bytes.
	// Nil retains forever.
	Retention *RetentionConfig `json:"retention" yaml:"retention"`
	// Limits shed over-budget writes; admission statistics report the rejected records.
	// The Collector sink currently acknowledges these writes without OTLP partial success. Nil is unlimited.
	Limits *LimitsConfig `json:"limits" yaml:"limits"`
}

// PrecisionTierConfig is one age band of the lossy float-precision policy.
type PrecisionTierConfig struct {
	// After is the age past which this tier applies (relative to now at merge time).
	After time.Duration `json:"after" yaml:"after"`
	// Bits is the significant mantissa bits retained (1..63). 0 or ≥64 ⇒ lossless (ignored).
	Bits uint8 `json:"bits" yaml:"bits"`
}

// DownsampleTierConfig is one age band of the downsampling policy.
type DownsampleTierConfig struct {
	// After is the age past which this tier applies (relative to now at merge time).
	After time.Duration `json:"after" yaml:"after"`
	// Interval is the rollup bucket width. Zero ⇒ the tier is disabled.
	Interval time.Duration `json:"interval" yaml:"interval"`
	// Agg combines a bucket's samples: "last" (default), "first", "min", "max", "sum", "avg",
	// "count". Empty ⇒ "last".
	Agg string `json:"agg" yaml:"agg"`
}

// RecompressConfig configures the cold-data archival tier. It sits above the size-graduated level
// every merge already applies, so it is worth setting only for a level that ladder will not reach.
type RecompressConfig struct {
	// After is the age past which a fully-cold part is recompressed at merge. Zero is invalid here
	// (a Recompress block is present only to enable it); use a positive age.
	After time.Duration `json:"after" yaml:"after"`
	// Level is the Zstandard level (1 fastest … 19 best ratio). Zero ⇒
	// [tenant.DefaultRecompressLevel]. Levels past ~9 buy single-digit percent for roughly an order
	// of magnitude more CPU, which competes with merge and retention.
	Level int `json:"level" yaml:"level"`
}

// ECConfig configures the cold-data erasure-coding tier: a flushed part older than After is
// re-encoded, at merge, into Data + Parity Reed-Solomon shards spread one per cluster node, in
// place of RF full copies. It stores (Data+Parity)/Data of the logical bytes and survives Parity
// node losses — {4,2} is 1.5x for two tolerated losses, against 3x for RF=3 — paying a
// reconstruct-on-read cost only for parts that have converted.
//
// It applies to shared-nothing deployments only (cluster mode with private_backend): on a shared
// object store the store owns durability, and an EC policy there does nothing. Objects below
// [ec.FullCopyFloor] stay full-copy on every owner, since k+m shards of a tiny object cost more
// than they save.
type ECConfig struct {
	// Data is the number of data shards (k); any Data shards reconstruct the object. Must be ≥ 1.
	Data int `json:"data" yaml:"data"`
	// Parity is the number of parity shards (m); the scheme tolerates Parity node losses. Must be
	// ≥ 1, and Data+Parity at most 256.
	//
	// Data+Parity is also the tenant's owner count under EC — the replication factor, cluster-wide
	// or per-tenant, is ignored. Placement spreads the shards over the rack/server/disk hierarchy,
	// and is rack-safe (one rack failure costs at most Parity shards) only with at least
	// ceil((Data+Parity)/Parity) racks; below that the engine converts anyway and warns.
	Parity int `json:"parity" yaml:"parity"`
	// After is the age past which a fully-cold part is erasure-coded, mirroring
	// [RecompressConfig.After] and riding the same background-merge engine. Zero ⇒ erasure-code
	// every part, accepting the reconstruct-on-read cost everywhere for the cheapest storage.
	After time.Duration `json:"after" yaml:"after"`
}

// RetentionConfig configures per-signal age and stored-byte retention.
type RetentionConfig struct {
	// MaxAge is the default age. Zero retains forever unless a signal overrides it.
	MaxAge time.Duration `json:"max_age" yaml:"max_age"`
	// MaxBytes is the pooled per-node stored-byte bound. Zero is unlimited.
	MaxBytes xbytes.Bytes `json:"max_bytes" yaml:"max_bytes"`
	// Signals overrides age and adds byte bounds for individual signals. Zero fields inherit.
	Signals map[string]SignalRetentionConfig `json:"signals" yaml:"signals"`
	// MaxBytesPerSignal rejects the removed key, including empty and null values.
	MaxBytesPerSignal removedSignalBudgets `json:"max_bytes_per_signal,omitempty" yaml:"max_bytes_per_signal,omitempty"`
}

// SignalRetentionConfig bounds one signal independently.
type SignalRetentionConfig struct {
	MaxAge   time.Duration `json:"max_age" yaml:"max_age"`
	MaxBytes xbytes.Bytes  `json:"max_bytes" yaml:"max_bytes"`
}

// LimitsConfig bounds per-tenant ingestion by shedding over-budget writes.
// Rejections appear in storage admission statistics, not the Collector response. Zero is unlimited.
type LimitsConfig struct {
	// IngestBytesPerSecond caps the ingest rate (a token bucket bursting to one second of budget).
	IngestBytesPerSecond xbytes.Bytes `json:"ingest_bytes_per_second" yaml:"ingest_bytes_per_second"`
	// MaxInFlightBytes caps the unflushed in-flight bytes buffered before backpressure sheds.
	MaxInFlightBytes xbytes.Bytes `json:"max_in_flight_bytes" yaml:"max_in_flight_bytes"`
	// MaxSeries is the hard active-series cardinality ceiling: a sample minting a new series past
	// it is shed (or routed to overflow, see MaxSeriesSoft). Existing series are unaffected.
	MaxSeries int64 `json:"max_series" yaml:"max_series"`
	// MaxSeriesSoft, when 0 < MaxSeriesSoft <= MaxSeries, is a soft cardinality budget: past it a
	// new series' samples go to a synthetic per-metric overflow series instead of being shed, until
	// MaxSeries is reached. Metrics only.
	MaxSeriesSoft int64 `json:"max_series_soft" yaml:"max_series_soft"`
	// MaxPartSize caps a flushed part's approximate uncompressed size, so the head's rows land
	// promptly. It is structural: fixed when the tenant's engine is created. It does not bound
	// merged parts — see MaxMergePartSize.
	MaxPartSize xbytes.Bytes `json:"max_part_size" yaml:"max_part_size"`
	// MaxMergePartSize caps a merged part's size on disk, in compressed bytes rather than
	// MaxPartSize's uncompressed estimate. Zero derives it from the backend's free space, which is
	// the default and lets part size track the deployment. The library never seals on a negative,
	// but a size cannot be written negative, so that opt-out is not expressible here.
	//
	// Merges are sized separately from flushes because they answer a different question: a flush is
	// bounded so rows land promptly, a merge so part *count* stays low. Under a byte constant the
	// span a part covers shrinks as active series grow, so a fixed-range query opens proportionally
	// more parts the larger the tenant gets.
	MaxMergePartSize xbytes.Bytes `json:"max_merge_part_size" yaml:"max_merge_part_size"`
}

// retentionSignalNames lists the accepted signal keys, for the error a typo produces.
const retentionSignalNames = `"metric", "log", "trace", "profile", "exemplar"`

// retentionPerSignal converts named signal bounds to the library's policy.
func retentionPerSignal(cfg map[string]SignalRetentionConfig) (map[signal.Signal]tenant.SignalRetention, error) {
	if len(cfg) == 0 {
		return nil, nil
	}
	bounds := make(map[signal.Signal]tenant.SignalRetention, len(cfg))
	for name, c := range cfg {
		sig, err := signal.ParseSignal(name)
		if err != nil {
			return nil, errors.Wrapf(err, "signals: expected one of %s", retentionSignalNames)
		}
		bounds[sig] = tenant.SignalRetention{MaxAge: c.MaxAge, MaxBytes: int64(c.MaxBytes)}
	}
	return bounds, nil
}

// retentionSignalBudgets reports how many signals carry their own byte budget. A zero entry is
// inert in the library, so it is not counted: the startup log reports what is enforced.
func retentionSignalBudgets(cfg *RetentionConfig) int {
	if cfg == nil {
		return 0
	}

	var n int
	for _, b := range cfg.Signals {
		if b.MaxBytes > 0 {
			n++
		}
	}
	return n
}

// warnECInert warns when an EC policy is configured but cannot take effect, because the engine
// gates erasure coding on shared-nothing cluster mode as well: without it every part stays
// full-copy and nothing reports why.
//
// It warns rather than refuses, matching the shared-nothing diagnostic in storage#373: EC on a
// shared store is a redundant setting, not a broken one — the store owns durability there, so the
// deployment is still correct, only paying for a policy it does not get. Refusing would also turn
// a config that is valid on its own into a startup failure decided by an unrelated section.
func warnECInert(cluster *ClusterConfig, policy *PolicyConfig, lg *zap.Logger) {
	if policy == nil || policy.EC == nil {
		return
	}

	clustered := cluster != nil && len(cluster.Etcd) > 0
	if clustered && cluster.PrivateBackend {
		return
	}

	lg.Warn("storage policy sets erasure coding, but it cannot apply",
		zap.Bool("clustered", clustered),
		zap.Bool("private_backend", clustered && cluster.PrivateBackend),
		zap.String("effect", "every part stays full-copy; the policy costs and saves nothing"),
		zap.String("action", "erasure coding needs cluster mode with private_backend set, i.e. a "+
			"shared-nothing deployment; on a shared object store the store owns durability and the "+
			"policy should be dropped"),
	)
}

// empty reports whether the policy configures nothing, in which case no resolver is installed.
func (cfg *PolicyConfig) empty() bool {
	return cfg == nil || (len(cfg.Precision) == 0 &&
		len(cfg.Downsample) == 0 &&
		cfg.Recompress == nil &&
		cfg.EC == nil &&
		cfg.Retention == nil &&
		cfg.Limits == nil)
}

// tenancyOption builds the storage tenancy option for a default policy.
func tenancyOption(cfg *PolicyConfig) (storage.Option, error) {
	resolver, err := tenancyResolver(cfg, nil)
	if err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, nil
	}
	return storage.WithTenancy(resolver), nil
}

// policy translates the config into a [tenant.Policy]. It validates the downsample aggregation
// names so a typo is a startup error rather than a silently-ignored tier.
func (cfg *PolicyConfig) policy() (tenant.Policy, error) {
	var p tenant.Policy

	for _, t := range cfg.Precision {
		p.Precision.Tiers = append(p.Precision.Tiers, tenant.PrecisionTier{
			After: t.After,
			Bits:  t.Bits,
		})
	}

	for i, t := range cfg.Downsample {
		agg := signal.AggLast
		if t.Agg != "" {
			parsed, err := signal.ParseAggregation(t.Agg)
			if err != nil {
				return tenant.Policy{}, errors.Wrapf(err, "downsample tier %d", i)
			}
			agg = parsed
		}
		p.Downsample.Tiers = append(p.Downsample.Tiers, tenant.DownsampleTier{
			After:    t.After,
			Interval: t.Interval,
			Agg:      agg,
		})
	}

	if r := cfg.Recompress; r != nil {
		p.Recompress = tenant.Recompress{After: r.After, Level: r.Level}
	}

	if e := cfg.EC; e != nil {
		// An invalid scheme is silently swallowed downstream (Storage.ecSchemeFor drops to
		// full-copy when Validate fails), so validate here to make it a startup error instead.
		if err := (ec.Scheme{Data: e.Data, Parity: e.Parity}).Validate(); err != nil {
			return tenant.Policy{}, errors.Wrap(err, "ec")
		}
		if e.After < 0 {
			return tenant.Policy{}, errors.New("ec: after must not be negative")
		}
		p.Durability.EC = &tenant.ECScheme{
			Data:   e.Data,
			Parity: e.Parity,
			After:  e.After,
		}
	}

	if r := cfg.Retention; r != nil {
		if len(r.MaxBytesPerSignal) > 0 {
			return tenant.Policy{}, removedSignalBudgetError()
		}
		perSignal, err := retentionPerSignal(r.Signals)
		if err != nil {
			return tenant.Policy{}, errors.Wrap(err, "retention")
		}
		p.Retention = tenant.Retention{MaxAge: r.MaxAge, MaxBytes: int64(r.MaxBytes), PerSignal: perSignal}
		if err := p.Retention.Validate(); err != nil {
			return tenant.Policy{}, errors.Wrap(err, "retention")
		}
	}

	if l := cfg.Limits; l != nil {
		if err := checkBytes(
			namedBytes{"ingest_bytes_per_second", l.IngestBytesPerSecond},
			namedBytes{"max_in_flight_bytes", l.MaxInFlightBytes},
			namedBytes{"max_part_size", l.MaxPartSize},
			namedBytes{"max_merge_part_size", l.MaxMergePartSize},
		); err != nil {
			return tenant.Policy{}, errors.Wrap(err, "limits")
		}
		if l.MaxSeriesSoft > 0 && l.MaxSeries > 0 && l.MaxSeriesSoft > l.MaxSeries {
			return tenant.Policy{}, errors.Errorf("limits: max_series_soft (%d) must not exceed max_series (%d)",
				l.MaxSeriesSoft, l.MaxSeries)
		}
		p.Limits = tenant.Limits{
			IngestBytesPerSecond: int64(l.IngestBytesPerSecond),
			MaxInFlightBytes:     int64(l.MaxInFlightBytes),
			MaxSeries:            l.MaxSeries,
			MaxSeriesSoft:        l.MaxSeriesSoft,
			MaxPartSize:          int64(l.MaxPartSize),
			MaxMergePartSize:     int64(l.MaxMergePartSize),
		}
	}

	return p, nil
}
