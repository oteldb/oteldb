// Package storagebackend adapts the embeddable github.com/oteldb/storage engine to oteldb's
// query and ingestion interfaces, so any signal can be served from the native Go storage
// engine instead of ClickHouse.
//
// All four signals are wired over a single shared *storage.Storage instance. [Backend]
// implements the metrics seam directly (Prometheus storage.Queryable + ExemplarQueryable, the
// native Scarecrow Scanner, metricstorage.MetadataQuerier) and the ingestion sinks for
// every signal (ConsumeMetrics/ConsumeTraces/ConsumeLogs/ConsumeProfiles). Because the logs and
// profiles read interfaces declare colliding method names, each non-metric signal's query
// interface is implemented by a small wrapper obtained via [Backend.Logs], [Backend.Traces],
// and [Backend.Profiles] (see signals.go).
package storagebackend

import (
	"context"
	"math"
	"time"

	"github.com/go-faster/errors"
	promstorage "github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/pdata/pmetric"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/otlp/pdataconv"
	storagepromql "github.com/oteldb/storage/query/promql"
	"github.com/oteldb/storage/signal"
	"github.com/oteldb/storage/signal/metric"

	"github.com/oteldb/oteldb/internal/metricstorage"
)

// Backend adapts a *storage.Storage to oteldb's metric query and ingestion interfaces.
// The zero value is not usable; construct with [New].
type Backend struct {
	// src is the read seam every query API is served from. It is the engine itself on a storage
	// node, and a routed view of the cluster on a stateless query node.
	src Source
	// store is the local engine, set only when this Backend was built over one. Ingestion,
	// maintenance and the statistics views need engine-local state, so they are the only methods
	// that reach for it — and they refuse with [ErrNoEngine] when it is nil.
	store  *storage.Storage
	tenant signal.TenantID
	// logParallelism is the max number of workers used to materialize log query results across the
	// fetched record set. <= 1 keeps the sequential path (the default). See [WithLogParallelism].
	logParallelism int
	// overTimePushdown routes *_over_time queries through the aggregate-sidecar pushdown
	// ([storage.Storage.AggregateMetricsNamed]) instead of a raw fetch-and-fold. True by default;
	// see [WithOverTimePushdown].
	overTimePushdown bool
	// traceQLPushdown lowers a TraceQL query's span matchers to storage filters and resolves the
	// candidate traces before materializing spans, instead of scanning the whole window. True by
	// default; see [WithTraceQLPushdown].
	traceQLPushdown bool
	// labels interns series→Prometheus-label projections for the Backend's lifetime. Each query takes
	// a fresh fetcher (to observe the latest head) but shares this cache, so the pointer-rich
	// labels.Labels set is built once per series and reused across queries instead of rebuilt and
	// re-scanned by GC every query. The cache is keyed by content-addressed series id, so an entry is
	// valid for the life of the series; it is bounded by resident cardinality.
	labels *storagepromql.LabelCache
	// dropped counts records the OTLP conversion refused. Nil unless [WithMeterProvider] was given.
	dropped otelmetric.Int64Counter
}

// Option configures a [Backend].
type Option func(*Backend)

// WithLogParallelism enables concurrent materialization of LogQL query results across up to n
// workers. The fetched record set is split into contiguous chunks built in parallel and merged in
// order, so the result is identical to the sequential path regardless of scheduling. Opt-in: n <= 1
// (the default) keeps the sequential path. Effective only above an internal record-count threshold.
func WithLogParallelism(n int) Option {
	return func(b *Backend) { b.logParallelism = n }
}

// WithOverTimePushdown toggles the *_over_time aggregate pushdown. It is on by default
// (the sidecar path is faster and correct); passing false restores the raw matrix-selector path
// (useful as a fallback or for differential testing).
func WithOverTimePushdown(enabled bool) Option {
	return func(b *Backend) { b.overTimePushdown = enabled }
}

// WithTraceQLPushdown toggles the TraceQL span-matcher pushdown. It is on by default (the filtered
// candidate-trace scan is faster and returns the same traces); passing false restores the plain full
// window scan, for differential testing or as a fallback.
func WithTraceQLPushdown(enabled bool) Option {
	return func(b *Backend) { b.traceQLPushdown = enabled }
}

// New returns a Backend over store. The ingest side has no tenant callback, so every batch routes
// to the "default" tenant; the empty tenant id here normalizes to "default" on the read side,
// keeping reads and writes on the same tenant (which also makes cluster reads owner-aware).
func New(store *storage.Storage, opts ...Option) *Backend {
	b := NewQuery(store, opts...)
	b.store = store
	return b
}

// NewQuery returns a read-only Backend over src. It serves the same query APIs as [New], but every
// method that needs engine-local state — the ingestion sinks, maintenance, and the statistics views
// — refuses with [ErrNoEngine]. It is what a stateless query node is built from.
func NewQuery(src Source, opts ...Option) *Backend {
	b := &Backend{
		src:              src,
		overTimePushdown: true,
		traceQLPushdown:  true,
		labels:           storagepromql.NewLabelCache(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Store returns the local storage engine, or nil when this Backend was built with [NewQuery] over a
// bare [Source] and has none. It is the escape hatch for the few tools that need the engine itself
// rather than a query API — the backup driver enumerates tenants and reads through the raw fetch
// seam, which no query interface exposes.
func (b *Backend) Store() *storage.Storage { return b.store }

// Inspect returns an in-memory snapshot of engine statistics (tenants, per-signal series/parts/head
// and WAL state, caches, and cluster membership when clustered). It performs no backend I/O and is
// safe to poll at a seconds cadence; it is the admin panel's primary storage view.
// A query-only backend has no engine to inspect and returns the zero snapshot.
func (b *Backend) Inspect() storage.StoreStats {
	if b.store == nil {
		return storage.StoreStats{}
	}
	return b.store.Inspect()
}

// EfficiencyStats returns the per-tenant, per-signal capacity/efficiency breakdown (stored bytes,
// bytes per point, compression ratios). Unlike Inspect it performs backend I/O (per-part object
// sizes) — poll it at dashboard cadence, not per request.
func (b *Backend) EfficiencyStats(ctx context.Context) ([]storage.TenantEfficiency, error) {
	if b.store == nil {
		return nil, ErrNoEngine
	}
	return b.store.EfficiencyStats(ctx)
}

// PartsDetailed lists a (tenant, signal)'s flushed parts with their identity and on-backend size.
// Like EfficiencyStats it reads object sizes from the backend, so it is a drill-down rather than a
// poll. It returns nil (no error) when the tenant has no engine for the signal.
func (b *Backend) PartsDetailed(
	ctx context.Context, tenant signal.TenantID, sig signal.Signal,
) ([]storage.PartDetail, error) {
	if b.store == nil {
		return nil, ErrNoEngine
	}
	return b.store.PartsDetailed(ctx, tenant, sig)
}

// MaintainNow runs one full maintenance cycle immediately (flush + merge + retention across every
// owned tenant and signal), i.e. the background maintenance loop's body on demand.
func (b *Backend) MaintainNow(ctx context.Context) error {
	if b.store == nil {
		return ErrNoEngine
	}
	return b.store.Admin().MaintainNow(ctx)
}

// StreamCosts attributes a record signal's flushed parts to streams — or, with opts.GroupBy, to a
// stream label's values. An empty tenant selects the backend's own.
//
// It is the heaviest call the storage library exposes: every accounted byte column of every live
// part is read and decoded once. Serve it on operator demand, never on a poll, and narrow it with
// opts.Columns when only one column is in question. Metrics are rejected by the library — their
// samples carry no per-record columns to attribute.
func (b *Backend) StreamCosts(
	ctx context.Context, tenant signal.TenantID, sig signal.Signal, opts storage.StreamCostOptions,
) ([]storage.StreamCost, error) {
	if b.store == nil {
		return nil, ErrNoEngine
	}
	if tenant == "" {
		tenant = b.tenant
	}
	return b.store.StreamCosts(ctx, tenant, sig, opts)
}

// CompactNow forces one compaction of every (tenant, signal) this node holds, overriding the merge
// selector's heuristic. It is the escape from the fixed point a maintenance cycle cannot break by
// itself: parts remain mergeable (MergeBacklog > 0) but no run of them qualifies
// (MergeCandidates == 0), so every cycle selects nothing and the part count never falls.
//
// Only the selection is overridden — the seal threshold and the merge memory bound still apply, so
// this reads and holds no more than a background merge. One pass compacts one group per signal;
// call it again to make further progress. Shards this node is not the compaction owner of are
// skipped rather than failing the whole pass.
func (b *Backend) CompactNow(ctx context.Context) error {
	if b.store == nil {
		return ErrNoEngine
	}
	admin := b.store.Admin()
	for _, t := range b.store.Inspect().Tenants {
		for _, s := range t.Signals {
			if err := admin.CompactNow(ctx, t.Tenant, s.Signal); err != nil {
				if errors.Is(err, storage.ErrNotOwner) {
					continue
				}
				return errors.Wrapf(err, "compact %s/%s", t.Tenant, s.Signal)
			}
		}
	}
	return nil
}

// queryable builds a fresh Prometheus queryable over the engine's current data. A new
// fetcher is taken per query so reads observe the latest head and flushed parts, but the
// Backend-lifetime label cache (b.labels) is shared across queries so series label projections are
// interned once instead of rebuilt and GC-rescanned every query.
//
// The fetcher is scoped to b.tenant (a named tenant, "" ⇒ "default") rather than the no-arg
// cross-tenant form: in cluster mode a named tenant is served owner-aware (fanned out to the ring
// owners), whereas the no-arg form reads only tenants local to this node — so a query node that does
// not own the tenant would see nothing. The record signals already scope by b.tenant the same way.
func (b *Backend) queryable() *storagepromql.Queryable {
	return storagepromql.NewQueryableWithCache(b.src.Fetcher(b.tenant), b.tenant, b.labels)
}

// Querier implements storage.Queryable.
func (b *Backend) Querier(mint, maxt int64) (promstorage.Querier, error) {
	return b.queryable().Querier(clampQueryMs(mint), clampQueryMs(maxt))
}

// clampQueryMs clamps a Prometheus millisecond bound to the open-ended sentinels the storage
// querier recognizes. Unbounded label/metadata queries arrive with Prometheus' MinTime/MaxTime,
// whose millisecond magnitude overflows int64 when the storage querier multiplies by 1e6 to reach
// nanoseconds; that yielded a garbage window and empty results (e.g. /api/v1/labels with no range).
// math.MinInt64/MaxInt64 are passed through verbatim by the storage querier as "unbounded".
func clampQueryMs(ms int64) int64 {
	const maxMs = math.MaxInt64 / int64(time.Millisecond) // ms whose *1e6 still fits in int64.
	switch {
	case ms < -maxMs:
		return math.MinInt64
	case ms > maxMs:
		return math.MaxInt64
	default:
		return ms
	}
}

// MetricMetadata implements metricstorage.MetadataQuerier. The storage engine does not
// expose metric metadata yet, so this returns an empty set.
func (b *Backend) MetricMetadata(context.Context, metricstorage.MetadataParams) (metricstorage.Metadata, error) {
	return metricstorage.Metadata{}, nil
}

// ConsumeMetrics ingests an OTLP metrics batch into the storage engine. It is the metrics
// ingestion sink used by the oteldb collector exporter when the storage backend is selected.
//
// Histogram, exponential-histogram and summary points are stored by classic decomposition into
// float series; a value-less number point has nothing to store and is dropped, as is any exemplar
// the decomposition leaves without an unambiguous series. Both are counted on
// oteldb.storage.dropped_records, which is the only report this sink has — its signature returns
// only an error.
func (b *Backend) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if b.store == nil {
		return ErrNoEngine
	}

	// WriteMetrics owns stored identities and samples before returning, so the
	// conversion descriptors can be reused independently of their byte payloads.
	batch := metric.GetMetrics()
	defer metric.PutMetrics(batch)

	dropped := pdataconv.AppendMetrics(batch, md)
	b.countDropped(ctx, signal.Metric, reasonNoValue, dropped.Points)
	b.countDropped(ctx, signal.Metric, reasonExemplar, dropped.Exemplars)

	if _, err := b.store.WriteMetrics(ctx, *batch); err != nil {
		return errors.Wrap(err, "write metrics")
	}
	return nil
}
