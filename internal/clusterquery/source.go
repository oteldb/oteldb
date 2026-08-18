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
	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// Source is a read-only view of a storage cluster, resolved through the ring.
type Source struct {
	rt *router.Router
}

var _ storagebackend.Source = (*Source)(nil)

// New returns a Source over rt.
func New(rt *router.Router) *Source {
	return &Source{rt: rt}
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
