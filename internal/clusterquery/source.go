// Package clusterquery reads an oteldb storage cluster through the ring without joining it.
//
// It is the read twin of what cmd/odbingest does for writes: [github.com/oteldb/storage/cluster/router.Router]
// resolves a shard's owners and carries the routed RPCs, and this package turns that into the
// [github.com/oteldb/oteldb/internal/storagebackend.Source] seam the query engines are built over —
// so PromQL, LogQL, TraceQL and the Pyroscope API run unchanged on a process that holds no data.
//
// What is left here is the part above a single shard: a tenant's data is split into ShardsPerTenant
// shards, each separately placed on the ring, so every read gathers across all of them and shapes
// the shards' answers into one. Serving a shard is the router's own: it hedges the shard's owners,
// treats an owner that disclaims the shard as a failover, and narrows the superset an owner returns
// back to the request's full matcher set.
package clusterquery

import (
	"context"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/readbudget"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// Source is a read-only view of a storage cluster, resolved through the ring.
type Source struct {
	rt *router.Router
	// maxQueryBytes bounds one query's read memory on this process. An aggregator needs its own
	// bound: it holds every owner's answer at once to merge them, so the owners' individual limits
	// do not add up to one here.
	maxQueryBytes int64
}

var _ storagebackend.Source = (*Source)(nil)

// New returns a Source over rt, bounding each query to maxQueryBytes of read memory. Zero sizes the
// bound from the detected process budget; negative leaves reads unbounded.
func New(rt *router.Router, maxQueryBytes int64) *Source {
	return &Source{rt: rt, maxQueryBytes: readbudget.ProcessShare(maxQueryBytes)}
}

// WithQueryBudget implements [storagebackend.Source].
//
// The allowance installed here is also what the fan-out declares to each owner, so a shard stops
// serializing an answer this process has no room to accept. Owners may only use it to tighten their
// own limit, never to raise it.
func (s *Source) WithQueryBudget(ctx context.Context) context.Context {
	if readbudget.From(ctx) != nil {
		return ctx
	}

	return readbudget.With(ctx, readbudget.New(s.maxQueryBytes))
}

// shardKeys returns the shard keys a tenant's reads fan out across, in index order.
func (s *Source) shardKeys(tenant signal.TenantID) []signal.TenantID {
	return s.rt.ShardKeys(normalize(tenant))
}

// normalize maps the empty tenant id onto the one the ingest path frames writes under, so a read
// resolves the same shard keys the write did.
func normalize(t signal.TenantID) signal.TenantID {
	if t == "" {
		return cluster.DefaultTenant
	}

	return t
}
