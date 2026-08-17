package storagebackend

import (
	"time"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// Config configures the embedded storage engine (used when
// a signal is served by the embedded engine).
type Config struct {
	// Backend is the engine backend: "memory" (default, ephemeral), "file", or "s3".
	Backend string `json:"backend" yaml:"backend"`
	// Dir is the data directory for the file backend (parts and WAL).
	Dir string `json:"dir" yaml:"dir"`
	// WALDir is the local directory for the write-ahead log when the backend is the (stateless) "s3"
	// object store, so unflushed head data survives a restart. Empty ⇒ no WAL (recent, unflushed
	// writes are lost on an unclean restart). Ignored for the "file" backend, which keeps its WAL
	// alongside the parts in Dir.
	WALDir string `json:"wal_dir" yaml:"wal_dir"`
	// S3 configures the "s3" object-store backend. Required (with a non-empty Bucket) when Backend is
	// "s3"; ignored otherwise.
	S3 *S3Config `json:"s3" yaml:"s3"`
	// FlushInterval is the max age of unflushed head data before it is flushed to a part.
	// Zero uses the engine default. Ignored for the ephemeral memory backend.
	FlushInterval time.Duration `json:"flush_interval" yaml:"flush_interval"`
	// LogQueryParallelism enables concurrent materialization of LogQL query results across up to
	// this many workers. Zero or one (default) keeps the sequential path. Opt-in.
	LogQueryParallelism int `json:"log_query_parallelism" yaml:"log_query_parallelism"`
	// ReadCacheBytes sizes the in-memory LRU object cache over the backend (the object-store read
	// cache for the cold tier). The storage library keeps this opt-in; oteldb flips the polarity and
	// enables it by default, sized from available RAM. Set to 0 to disable. No effect for the
	// ephemeral memory backend.
	ReadCacheBytes *xbytes.Bytes `json:"read_cache_bytes" yaml:"read_cache_bytes"`
	// DecodeCacheBytes sizes the per-tenant LRU cache of decoded part columns (plus concurrent
	// prefetch of a fetch's parts). Enabled by default, sized from available RAM; set to 0 to
	// disable.
	DecodeCacheBytes *xbytes.Bytes `json:"decode_cache_bytes" yaml:"decode_cache_bytes"`
	// DecodeMemoryBytes caps the total in-flight decoded column bytes across concurrent metric
	// queries (decode admission control, shared across tenants): a query reserves its estimated
	// decode footprint before reading parts and blocks while the budget is exhausted, so query
	// concurrency cannot drive the live heap past the process memory limit. Enabled by default,
	// fitted to the detected memory limit minus the caches and headroom; set to 0 to disable.
	DecodeMemoryBytes *xbytes.Bytes `json:"decode_memory_bytes" yaml:"decode_memory_bytes"`
	// MergeMemoryBytes caps the memory all concurrent merges together may hold, and through that the
	// size a merged part reaches before it is sealed. It is the write-side counterpart of
	// DecodeMemoryBytes: on a backend that takes objects whole a merge holds its output part encoded
	// in RAM, so free space alone cannot bound it. Unset ⇒ the library default (a share of the Go
	// memory limit); negative ⇒ unbounded.
	//
	// Unlike the caches this is a pass-through: oteldb adds no default of its own, because the
	// library already derives one from the same memory limit oteldb would read.
	MergeMemoryBytes *xbytes.Bytes `json:"merge_memory_bytes" yaml:"merge_memory_bytes"`
	// AggregateStats writes a per-series aggregate sidecar (count/sum/min/max) alongside each
	// metric part so range-covering aggregates — and the *_over_time pushdown — can be answered
	// without decoding. Enabled by default; set to false to disable.
	AggregateStats *bool `json:"aggregate_stats" yaml:"aggregate_stats"`
	// Policy configures the per-tenant merge-time storage policy: age-tiered lossy float precision,
	// downsampling, and cold-data recompression. Empty ⇒ the library default (lossless, no rollup).
	Policy *PolicyConfig `json:"policy" yaml:"policy"`
	// Cluster, when set with a non-empty Etcd endpoint list, joins this node to a storage cluster:
	// nodes coordinate through etcd, form a rendezvous-hash ring, and replicate writes across each
	// other's local backends. Unset (or empty Etcd) ⇒ a single-node engine.
	Cluster *ClusterConfig `json:"cluster" yaml:"cluster"`
}

// ClusterConfig configures the embedded storage engine's distribution layer (the shared-
// nothing cluster: each node keeps its shards on its own local backend and replicates them to RF
// peers). It maps onto storage's cluster.Config.
type ClusterConfig struct {
	// Etcd is the etcd endpoint list used for membership coordination. Enables cluster mode.
	Etcd []string `json:"etcd" yaml:"etcd"`
	// ID is this node's stable ring identity. Empty ⇒ the OS hostname.
	ID string `json:"id" yaml:"id"`
	// Zone is this node's failure domain (replicas are spread across zones). Optional.
	Zone string `json:"zone" yaml:"zone"`
	// Addr is the host:port peers use to reach this node's replication server. Empty ⇒
	// "<ID or hostname>:<Port>".
	Addr string `json:"addr" yaml:"addr"`
	// Port is the replication-server port used when Addr is derived from the hostname. Zero ⇒ 7946.
	Port int `json:"port" yaml:"port"`
	// RF is the replication factor (replicas per write). Zero ⇒ the storage default (3).
	RF int `json:"rf" yaml:"rf"`
	// ShardsPerTenant splits each tenant's metric series across this many independently-placed
	// shards. Zero or one ⇒ a single shard per tenant.
	ShardsPerTenant int `json:"shards_per_tenant" yaml:"shards_per_tenant"`
	// Root is the etcd key prefix for this cluster's state. Empty ⇒ "/oteldb".
	Root string `json:"root" yaml:"root"`
	// PrivateBackend declares this node's backend private to it (a local disk, not a shared object
	// store), so peers cannot read the parts it flushes. The cluster then replicates flushed parts
	// node-to-node: replicas mirror their owner's objects over the parts endpoints instead of
	// loading them from a shared store, and an owner backfills from its peers before compacting.
	// False (the default) keeps the shared-store model. Not inferable from the backend type — an
	// S3 bucket shared by every node and a per-node local disk are both legal backends.
	PrivateBackend bool `json:"private_backend" yaml:"private_backend"`
}

// S3Config configures the embedded storage engine's "s3" backend: an S3-compatible object
// store as the durable, stateless tier (the read path is reconstructed from objects). It maps onto
// storage's backend/s3.
type S3Config struct {
	// Bucket is the S3 bucket holding the data. Required for the s3 backend.
	Bucket string `json:"bucket" yaml:"bucket"`
	// Prefix is an optional root key prefix (e.g. "oteldb/") so several datasets can share one
	// bucket. Empty ⇒ keys live at the bucket root.
	Prefix string `json:"prefix" yaml:"prefix"`
	// Region is the AWS region. Empty ⇒ resolved from the environment/credential chain.
	Region string `json:"region" yaml:"region"`
	// Endpoint overrides the S3 endpoint URL for S3-compatible stores (e.g. MinIO,
	// "http://minio:9000"). Empty ⇒ the AWS default endpoint for the region.
	Endpoint string `json:"endpoint" yaml:"endpoint"`
	// ForcePathStyle addresses objects as endpoint/bucket/key instead of the virtual-host style.
	// Required by most S3-compatible stores (MinIO, Ceph).
	ForcePathStyle bool `json:"force_path_style" yaml:"force_path_style"`
	// AccessKeyID and SecretAccessKey are static credentials. When both are empty, the default AWS
	// credential chain (environment, shared config, IAM role) is used instead.
	AccessKeyID     string `json:"access_key_id" yaml:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key" yaml:"secret_access_key"`
	// SessionToken is an optional token for temporary static credentials.
	SessionToken string `json:"session_token" yaml:"session_token"`
	// Retry selects the resilience profile for an unreliable endpoint (per-attempt timeouts, bounded
	// retries, hedged GETs): "" or "none" (the AWS SDK's own retryer only), "default", or "lossy".
	Retry string `json:"retry" yaml:"retry"`
}

// SetDefaults fills in the defaults for unset fields.
func (cfg *Config) SetDefaults() {
	if cfg.Backend == "" {
		cfg.Backend = "memory"
	}
}
