package clusterquery

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/engine"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"
)

// AggregateMetricsNamed implements [storagebackend.Source]. Each shard's owner folds its own
// samples and ships one compact entry per series, so only aggregates cross the wire — the pushdown
// the `*_over_time` paths depend on survives being run from off the ring.
func (s *Source) AggregateMetricsNamed(
	ctx context.Context, t signal.TenantID, r fetch.Request,
) ([]storage.SeriesAggregate, error) {
	named, err := gatherShards(ctx, s, t, r.Matchers,
		func(ctx context.Context, agg *cluster.RemoteAggregator, sk signal.TenantID) ([]engine.NamedAgg, error) {
			// Step 0 asks for one whole-range bucket per series.
			return agg.Aggregate(ctx, string(sk), r.Start, r.End, 0, equalitySpecs(r.Matchers))
		},
		func(a *engine.NamedAgg) signal.Series { return a.Series })
	if err != nil {
		return nil, err
	}

	out := make([]storage.SeriesAggregate, 0, len(named))
	for i := range named {
		na := &named[i]
		if len(na.Buckets) == 0 {
			continue
		}

		out = append(out, storage.SeriesAggregate{Series: na.Series, SeriesAgg: na.Buckets[0].SeriesAgg})
	}

	return out, nil
}

// AggregateMetricsWindowNamed implements [storagebackend.Source]: the overlapping-window form of
// [Source.AggregateMetricsNamed], where each owner slides its own windows.
func (s *Source) AggregateMetricsWindowNamed(
	ctx context.Context, t signal.TenantID, r fetch.Request, spec engine.WindowSpec,
) ([]engine.NamedWindowAgg, error) {
	return gatherShards(ctx, s, t, r.Matchers,
		func(ctx context.Context, agg *cluster.RemoteAggregator, sk signal.TenantID) ([]engine.NamedWindowAgg, error) {
			return agg.AggregateWindow(ctx, string(sk), r.Start, r.End, spec, equalitySpecs(r.Matchers))
		},
		func(a *engine.NamedWindowAgg) signal.Series { return a.Series })
}

// gatherShards folds every shard of a tenant into one per-series list: it asks each shard's owners
// in turn, drops the series that fail the full matcher set (an owner applied only the equality
// subset), and concatenates. No cross-shard merge is needed — a metric series' shard is a function
// of its content-addressed id, so it is held by exactly one shard.
func gatherShards[T any](
	ctx context.Context, s *Source, tenant signal.TenantID, matchers []fetch.Matcher,
	call func(context.Context, *cluster.RemoteAggregator, signal.TenantID) ([]T, error),
	seriesOf func(*T) signal.Series,
) ([]T, error) {
	var out []T

	for _, sk := range s.shardKeys(tenant) {
		got, err := tryOwners(ctx, s.rt.Owners(sk), func(ctx context.Context, addr string) ([]T, error) {
			return call(ctx, cluster.NewRemoteAggregator(addr, s.httpc), sk)
		})
		if err != nil {
			return nil, errors.Wrapf(err, "aggregate shard %q", sk)
		}

		for i := range got {
			if matchesAll(seriesOf(&got[i]), matchers) {
				out = append(out, got[i])
			}
		}
	}

	return out, nil
}
