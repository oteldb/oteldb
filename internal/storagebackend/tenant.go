package storagebackend

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/multitenancy"
)

// ErrNoTenant is returned by every read when the backend requires per-request tenancy
// ([WithTenancy]) but the request context carries no tenant.
//
// It is a refusal rather than a fallback on purpose: falling back to the default tenant would mean
// a route that skipped the tenancy middleware silently reads whichever tenant the deployment calls
// default, which is the failure mode tenancy exists to prevent.
var ErrNoTenant = errors.New("storagebackend: request carries no tenant")

// WithTenancy makes every read resolve its tenant from the request context (see
// [multitenancy.WithTenant]) instead of serving the backend's single default tenant, and refuse a
// request that carries none with [ErrNoTenant].
//
// Without it a Backend behaves exactly as before: one tenant — the empty id, which the engine
// normalizes to "default" — for its whole lifetime.
func WithTenancy() Option {
	return func(b *Backend) { b.requireTenant = true }
}

// tenantFor returns the tenant ctx's request reads.
//
// A per-request tenant is threaded through the context rather than baked into a per-request
// Backend view because a Backend is not free to mint per request: it owns the label cache that
// interns series→labels projections for the engine's lifetime, and the query APIs are constructed
// once over one Backend at startup. The context already reaches every read, since each one is
// already a ctx-taking storage call.
//
// Sharing that label cache across tenants is safe: it is keyed by [signal.SeriesID], the
// content-addressed hash of the series' own attribute set, and its value is a pure projection of
// that same identity. Two tenants with an identical series share one entry holding the one label
// set both would have computed, and an entry is only ever looked up for a series the caller's own
// tenant-scoped fetch returned — so it can memoize work across tenants but cannot carry identities
// between them.
func (b *Backend) tenantFor(ctx context.Context) (signal.TenantID, error) {
	// A backend that has not opted in never looks at the context, so its behavior cannot be
	// changed by anything a request carries.
	if !b.requireTenant {
		return b.tenant, nil
	}

	t, ok := multitenancy.TenantFromContext(ctx)
	if !ok {
		return "", ErrNoTenant
	}

	return signal.TenantID(t), nil
}

// lazyQuerier defers building the tenant-scoped querier until a call that carries a context, since
// [promstorage.Queryable] resolves a querier without one. It is used only under [WithTenancy]; a
// single-tenant backend builds its querier eagerly, exactly as before.
//
// One querier serves one request, so the resolved inner querier is built at most once and needs no
// synchronization beyond the single-goroutine use Prometheus already assumes of a Querier.
type lazyQuerier struct {
	b          *Backend
	mint, maxt int64
	inner      promstorage.Querier
}

func (q *lazyQuerier) resolve(ctx context.Context) (promstorage.Querier, error) {
	if q.inner != nil {
		return q.inner, nil
	}

	tenant, err := q.b.tenantFor(ctx)
	if err != nil {
		return nil, err
	}

	inner, err := q.b.queryable(tenant).Querier(q.mint, q.maxt)
	if err != nil {
		return nil, err
	}

	q.inner = inner

	return inner, nil
}

func (q *lazyQuerier) Select(
	ctx context.Context, sortSeries bool, hints *promstorage.SelectHints, matchers ...*labels.Matcher,
) promstorage.SeriesSet {
	inner, err := q.resolve(ctx)
	if err != nil {
		return promstorage.ErrSeriesSet(err)
	}

	return inner.Select(ctx, sortSeries, hints, matchers...)
}

func (q *lazyQuerier) LabelValues(
	ctx context.Context, name string, hints *promstorage.LabelHints, matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	inner, err := q.resolve(ctx)
	if err != nil {
		return nil, nil, err
	}

	return inner.LabelValues(ctx, name, hints, matchers...)
}

func (q *lazyQuerier) LabelNames(
	ctx context.Context, hints *promstorage.LabelHints, matchers ...*labels.Matcher,
) ([]string, annotations.Annotations, error) {
	inner, err := q.resolve(ctx)
	if err != nil {
		return nil, nil, err
	}

	return inner.LabelNames(ctx, hints, matchers...)
}

func (q *lazyQuerier) Close() error {
	if q.inner == nil {
		return nil
	}

	return q.inner.Close()
}
