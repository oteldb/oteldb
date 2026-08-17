// Package clusterquery reads an oteldb storage cluster through the ring without joining it.
//
// It is the read twin of what cmd/odbingest does for writes: [github.com/oteldb/storage/cluster/router.Router]
// resolves a shard's owners, and this package turns that into the [github.com/oteldb/oteldb/internal/storagebackend.Source]
// seam the query engines are built over — so PromQL, LogQL, TraceQL and the Pyroscope API run
// unchanged on a process that holds no data.
//
// It mirrors the storage node's own cluster read path rather than inventing one: a tenant's data is
// split into ShardsPerTenant shards, each separately placed on the ring, so every read gathers
// across all of them; a shard's owners are complete replicas, so a shard is served by whichever
// owner answers first; and a peer applies only the serializable (equality) subset of a request's
// matchers, so its answer is a superset the caller re-filters.
package clusterquery

import (
	"context"
	"net/http"

	"github.com/go-faster/errors"

	"github.com/oteldb/storage/cluster"
	"github.com/oteldb/storage/cluster/router"
	"github.com/oteldb/storage/query/fetch"
	"github.com/oteldb/storage/signal"

	"github.com/oteldb/oteldb/internal/storagebackend"
)

// Source is a read-only view of a storage cluster, resolved through the ring.
type Source struct {
	rt *router.Router
	// httpc carries the enumeration and aggregate RPCs. The router keeps its own copy for the
	// fetch path but does not expose it, so a caller that wants one client for both hands the same
	// one to [router.Config].HTTP and to [New].
	httpc *http.Client
}

var _ storagebackend.Source = (*Source)(nil)

// New returns a Source over rt. A nil client uses [http.DefaultClient]; pass the same client
// [router.Config].HTTP was given so every read shares one connection pool.
func New(rt *router.Router, httpc *http.Client) *Source {
	if httpc == nil {
		httpc = http.DefaultClient
	}

	return &Source{rt: rt, httpc: httpc}
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

// tryOwners calls fn against a shard's owners until one answers, mirroring the node's own
// enumeration failover.
//
// An owner answering [cluster.ErrShardAbsent] is not a result: the ring points at it but it holds
// no data for the shard (a fresh owner after a rebalance, a lagging membership view), so the call
// moves on. Only when every owner disclaims the shard is the empty answer real. A shard with no
// reachable owner is likewise empty rather than an error — that is how the ring reports a shard
// nothing holds.
func tryOwners[T any](
	ctx context.Context, owners []string, fn func(context.Context, string) (T, error),
) (T, error) {
	var (
		zero    T
		absent  int
		lastErr error
	)

	for _, addr := range owners {
		v, err := fn(ctx, addr)
		if err == nil {
			return v, nil
		}

		if errors.Is(err, cluster.ErrShardAbsent) {
			absent++
		}

		lastErr = err

		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
	}

	if lastErr == nil || absent == len(owners) {
		return zero, nil
	}

	return zero, lastErr
}

// equalitySpecs extracts the serializable subset of a matcher set — the only part a peer can apply.
func equalitySpecs(matchers []fetch.Matcher) []fetch.EqualMatcher {
	var eq []fetch.EqualMatcher

	for i := range matchers {
		if matchers[i].Spec != nil {
			eq = append(eq, *matchers[i].Spec)
		}
	}

	return eq
}
