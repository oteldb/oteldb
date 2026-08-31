package storagebackend

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
	sigprofile "github.com/oteldb/storage/signal/profile"
)

// Source is the read seam [Backend] serves every query API from. *[storage.Storage] implements it,
// and so does a routed view of a storage cluster (see cmd/odbselect), which is what lets the same
// query engines run on a node that holds no data.
//
// It is deliberately read-only: ingestion, maintenance and the statistics views need engine-local
// state and are reached through the concrete engine instead (see [New] versus [NewQuery]).
type Source interface {
	// Fetcher returns the metrics read seam over the named tenants.
	Fetcher(tenants ...signal.TenantID) fetch.Fetcher
	// MetricSeries enumerates a tenant's matching metric series identities in [start, end] ns.
	MetricSeries(
		ctx context.Context, t signal.TenantID, matchers []fetch.Matcher, start, end int64,
	) ([]signal.Series, error)
	// AggregateMetricsNamed folds each matching series' samples over the whole request window.
	AggregateMetricsNamed(
		ctx context.Context, t signal.TenantID, r fetch.Request,
	) ([]storage.SeriesAggregate, error)
	// AggregateMetricsWindowNamed folds each matching series' samples into overlapping windows.
	AggregateMetricsWindowNamed(
		ctx context.Context, t signal.TenantID, r fetch.Request, spec engine.WindowSpec,
	) ([]engine.NamedWindowAgg, error)

	// LogFetcher returns the logs read seam over the named tenants.
	LogFetcher(tenants ...signal.TenantID) fetch.Fetcher
	// LogSeries enumerates a tenant's matching log stream identities in [start, end] ns.
	LogSeries(
		ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
	) ([]signal.Series, error)
	// LogKeys enumerates the distinct attribute keys of a tenant's log records in [start, end] ns.
	LogKeys(ctx context.Context, tenant signal.TenantID, start, end int64) ([]storage.KeyInfo, error)

	// ColumnValues enumerates the distinct values one record column, or one per-record attribute
	// key, takes for a tenant. It answers from the parts' column dictionaries, so tag/label value
	// autocomplete costs O(distinct values) instead of a window scan. The result is a superset:
	// a part overlapping the window contributes its whole dictionary.
	ColumnValues(ctx context.Context, tenant signal.TenantID, req storage.ValuesRequest) ([][]byte, error)

	// TraceFetcher returns the traces read seam over the named tenants.
	TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher
	// TraceSeries enumerates a tenant's matching span stream identities in [start, end] ns. It is the
	// traces twin of LogSeries, and it is what lets a resource- or instrumentation-scoped tag lookup
	// be answered from the stream identities instead of by materializing every span in the window.
	TraceSeries(
		ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
	) ([]signal.Series, error)
	// Trace returns every span of one trace.
	Trace(ctx context.Context, tenant signal.TenantID, traceID []byte) ([]*fetch.Batch, error)

	// ProfileFetcher returns the profiles read seam over the named tenants.
	ProfileFetcher(tenants ...signal.TenantID) fetch.Fetcher
	// ProfileSeries enumerates a tenant's matching profile stream identities in [start, end] ns.
	ProfileSeries(
		ctx context.Context, tenant signal.TenantID, matchers []fetch.Matcher, start, end int64,
	) ([]signal.Series, error)
	// ProfileResolver resolves a tenant's content-addressed stack ids to frames.
	ProfileResolver(ctx context.Context, tenant signal.TenantID) (*sigprofile.Resolver, error)
}

var _ Source = (*storage.Storage)(nil)

// ErrNoEngine is returned by the [Backend] methods that need engine-local state — ingestion,
// maintenance, and the statistics views — when the backend was built over a bare [Source] with
// [NewQuery] rather than over a local engine.
var ErrNoEngine = errors.New("storagebackend: operation requires a local storage engine")
