// Package storagebackend adapts the embeddable github.com/oteldb/storage engine to oteldb's
// query and ingestion interfaces, so any signal can be served from the native Go storage
// engine instead of ClickHouse.
//
// All four signals are wired over a single shared *storage.Storage instance. [Backend]
// implements the metrics seam directly (Prometheus storage.Queryable + ExemplarQueryable, the
// PromQL engine's MetricsScanners, metricstorage.MetadataQuerier) and the ingestion sinks for
// every signal (ConsumeMetrics/ConsumeTraces/ConsumeLogs/ConsumeProfiles). Because the logs and
// profiles read interfaces declare colliding method names, each non-metric signal's query
// interface is implemented by a small wrapper obtained via [Backend.Logs], [Backend.Traces],
// and [Backend.Profiles] (see signals.go).
package storagebackend

import (
	"context"
	"time"

	"github.com/go-faster/errors"
	"github.com/oteldb/promql-engine/execution/model"
	"github.com/oteldb/promql-engine/logicalplan"
	"github.com/oteldb/promql-engine/query"
	enginestorage "github.com/oteldb/promql-engine/storage"
	promscanners "github.com/oteldb/promql-engine/storage/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"go.opentelemetry.io/collector/pdata/pmetric"

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
	store  *storage.Storage
	tenant signal.TenantID
	// logParallelism is the max number of workers used to materialize log query results across the
	// fetched record set. <= 1 keeps the sequential path (the default). See [WithLogParallelism].
	logParallelism int
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

// New returns a Backend over store. The query side reads across all tenants and the ingest
// side derives tenants from each batch's Resource/Scope (via the engine's tenant callback),
// so the empty tenant id here scopes reads to the federated cross-tenant view.
func New(store *storage.Storage, opts ...Option) *Backend {
	b := &Backend{store: store}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// queryable builds a fresh Prometheus queryable over the engine's current data. A new
// fetcher is taken per query so reads observe the latest head and flushed parts.
func (b *Backend) queryable() *storagepromql.Queryable {
	return storagepromql.NewQueryable(b.store.Fetcher(), b.tenant)
}

// Querier implements storage.Queryable.
func (b *Backend) Querier(mint, maxt int64) (promstorage.Querier, error) {
	return b.queryable().Querier(mint, maxt)
}

// ExemplarQuerier implements storage.ExemplarQueryable. The storage engine does not store
// exemplars yet, so this returns an empty querier.
func (b *Backend) ExemplarQuerier(context.Context) (promstorage.ExemplarQuerier, error) {
	return emptyExemplarQuerier{}, nil
}

type emptyExemplarQuerier struct{}

func (emptyExemplarQuerier) Select(int64, int64, ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	return nil, nil
}

// MetricsScanners implements the oteldb PromQL engine's scanner seam.
func (b *Backend) MetricsScanners() (enginestorage.Scanners, error) {
	return scanners{b: b}, nil
}

// MetricMetadata implements metricstorage.MetadataQuerier. The storage engine does not
// expose metric metadata yet, so this returns an empty set.
func (b *Backend) MetricMetadata(context.Context, metricstorage.MetadataParams) (metricstorage.Metadata, error) {
	return metricstorage.Metadata{}, nil
}

// ConsumeMetrics ingests an OTLP metrics batch into the storage engine. It is the metrics
// ingestion sink used by the oteldb collector exporter when the storage backend is selected.
// Histogram, exponential-histogram, summary, and value-less points are not representable in
// the storage engine yet and are silently dropped by the conversion.
func (b *Backend) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// A fresh batch is used (not pooled) because the engine may retain projected series
	// bytes; pdataconv already copies out of pdata, so this allocates regardless.
	var batch metric.Metrics
	pdataconv.AppendMetrics(&batch, md)

	if _, err := b.store.WriteMetrics(ctx, batch); err != nil {
		return errors.Wrap(err, "write metrics")
	}
	return nil
}

// scanners builds per-selector Prometheus scanners over the storage fetch seam. The storage
// querier scopes a read to its construction window and ignores SelectHints, so each selector
// gets a querier widened to its own [Start, End] (which already accounts for range and
// lookback) — mirroring how chstorage scopes its metric queriers per selector.
type scanners struct {
	b *Backend
}

var _ enginestorage.Scanners = scanners{}

func (scanners) Close() error { return nil }

func (s scanners) NewVectorSelector(
	ctx context.Context,
	opts *query.Options,
	hints promstorage.SelectHints,
	node logicalplan.VectorSelector,
) (model.VectorOperator, error) {
	inner, err := s.windowed(opts, hints)
	if err != nil {
		return nil, err
	}
	return inner.NewVectorSelector(ctx, opts, hints, node)
}

func (s scanners) NewMatrixSelector(
	ctx context.Context,
	opts *query.Options,
	hints promstorage.SelectHints,
	node logicalplan.MatrixSelector,
	call logicalplan.FunctionCall,
) (model.VectorOperator, error) {
	inner, err := s.windowed(opts, hints)
	if err != nil {
		return nil, err
	}
	return inner.NewMatrixSelector(ctx, opts, hints, node, call)
}

// windowed builds a Prometheus scanner set whose querier covers the selector window. opts is
// shallow-copied (the library's own idiom, see query.Options.WithEndTime) with the window
// overridden to the selector's hints so the storage querier reads the right range.
func (s scanners) windowed(opts *query.Options, hints promstorage.SelectHints) (*promscanners.Scanners, error) {
	o := *opts
	o.Start = time.UnixMilli(hints.Start)
	o.End = time.UnixMilli(hints.End)

	sc, err := promscanners.NewPrometheusScanners(s.b.queryable(), &o, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create prometheus scanners")
	}
	return sc, nil
}
