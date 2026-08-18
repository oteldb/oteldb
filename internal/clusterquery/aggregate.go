package clusterquery

import (
	"context"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage"
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
	named, err := gatherShards(ctx, s, t, func(ctx context.Context, sk signal.TenantID) ([]engine.NamedAgg, error) {
		// Step 0 asks for one whole-range bucket per series.
		return s.rt.Aggregate(ctx, sk, r.Start, r.End, 0, r.Matchers)
	})
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
	return gatherShards(ctx, s, t, func(ctx context.Context, sk signal.TenantID) ([]engine.NamedWindowAgg, error) {
		return s.rt.AggregateWindow(ctx, sk, r.Start, r.End, spec, r.Matchers)
	})
}

// gatherShards concatenates every shard of a tenant into one per-series list. No cross-shard merge
// is needed — a metric series' shard is a function of its content-addressed id, so it is held by
// exactly one shard.
func gatherShards[T any](
	ctx context.Context, s *Source, tenant signal.TenantID,
	call func(context.Context, signal.TenantID) ([]T, error),
) ([]T, error) {
	var out []T

	for _, sk := range s.shardKeys(tenant) {
		got, err := call(ctx, sk)
		if err != nil {
			return nil, errors.Wrapf(err, "aggregate shard %q", sk)
		}

		out = append(out, got...)
	}

	return out, nil
}
