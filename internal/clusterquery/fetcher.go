package clusterquery

import (
	"context"

	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// Fetcher implements [storagebackend.Source] for metrics: it gathers the tenant's shards and merges
// them by series id.
//
// The merge is the metric-shaped one (samples of the same id combined in timestamp order) because a
// shard's owners are replicas of one another, and because a rebalance can briefly leave a series
// visible from two places. Under normal placement the shards partition the series space, so the
// merge is a concatenation that costs nothing.
//
// Only one tenant is served per call; the cross-tenant no-arg form of the engine's own Fetcher has
// no ring-side equivalent (there is no cluster-wide "which tenants exist" enumeration), so an empty
// tenant list reads the default tenant rather than every tenant.
func (s *Source) Fetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return seriesListerFetcher{
		Fetcher: fetch.Merge(s.shardFetchers(signal.Metric, tenants)...),
		list: func(ctx context.Context, r fetch.Request) ([]signal.Series, error) {
			series, err := s.MetricSeries(ctx, tenantOf(tenants), r.Matchers, r.Start, r.End)
			if err != nil {
				return nil, err
			}

			return fetch.SortSeries(series), nil
		},
	}
}

// LogFetcher implements [storagebackend.Source].
func (s *Source) LogFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return concat(s.shardFetchers(signal.Log, tenants))
}

// TraceFetcher implements [storagebackend.Source].
func (s *Source) TraceFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return concat(s.shardFetchers(signal.Trace, tenants))
}

// ProfileFetcher implements [storagebackend.Source].
func (s *Source) ProfileFetcher(tenants ...signal.TenantID) fetch.Fetcher {
	return concat(s.shardFetchers(signal.Profile, tenants))
}

// shardFetchers builds one fetcher per shard of the tenant, each stamping its shard key onto the
// request.
func (s *Source) shardFetchers(sig signal.Signal, tenants []signal.TenantID) []fetch.Fetcher {
	keys := s.shardKeys(tenantOf(tenants))

	out := make([]fetch.Fetcher, 0, len(keys))
	for _, sk := range keys {
		out = append(out, scopedFetcher{scope: sk, inner: s.rt.Fetcher(sig, sk)})
	}

	return out
}

// tenantOf picks the tenant a read is scoped to. The query APIs always pass exactly one.
func tenantOf(tenants []signal.TenantID) signal.TenantID {
	if len(tenants) == 0 {
		return ""
	}

	return tenants[0]
}

// scopedFetcher stamps a shard key as the request tenant, so the owner serves the addressed shard's
// engine rather than the tenant-wide one.
type scopedFetcher struct {
	inner fetch.Fetcher
	scope signal.TenantID
}

func (f scopedFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	r.Tenant = f.scope

	return f.inner.Fetch(ctx, r)
}

// seriesListerFetcher adds the [fetch.SeriesLister] capability to a shard fan-out.
//
// The merge underneath cannot carry it: a multi-child merge answers enumeration by draining every
// child, which for the label endpoints means decoding every sample of every matching series to keep
// the identities. Enumeration has its own RPC, and shards partition a tenant's series, so the
// gather is exact — this routes to it.
type seriesListerFetcher struct {
	fetch.Fetcher

	list func(context.Context, fetch.Request) ([]signal.Series, error)
}

func (f seriesListerFetcher) Series(ctx context.Context, r fetch.Request) ([]signal.Series, error) {
	return f.list(ctx, r)
}

// concat runs each shard in order and concatenates their batches. Unlike [fetch.Merge] it does not
// deduplicate by timestamp: records are append-only, several may share a timestamp, and the
// metric-shaped merge would drop their columns.
func concat(fetchers []fetch.Fetcher) fetch.Fetcher {
	if len(fetchers) == 1 {
		return fetchers[0]
	}

	return concatFetcher(fetchers)
}

type concatFetcher []fetch.Fetcher

func (c concatFetcher) Fetch(ctx context.Context, r fetch.Request) (fetch.Iterator, error) {
	var all []*fetch.Batch

	for _, f := range c {
		it, err := f.Fetch(ctx, r)
		if err != nil {
			return nil, err
		}

		batches, err := fetch.Drain(ctx, it)
		if err != nil {
			return nil, err
		}

		all = append(all, batches...)
	}

	return fetch.NewSliceIterator(all), nil
}
