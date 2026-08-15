package storagebackend

import (
	"context"

	"github.com/oteldb/storage/query/fetch"
)

// queryScope installs a [fetch.Scope] on ctx, so every read one logical query makes is admitted
// against the decode budget once instead of queueing against its own reservation.
//
// The budget is reserved per fetch and released when that fetch ends, which is deadlock-free only
// while a caller keeps one fetch open at a time. The record queriers do not: a TraceQL search runs
// one fetch per filter group and then scans the surviving spans, and a LogQL evaluation resolves
// stream labels before fetching records. Without a scope each of those is a separate query to
// admission control, so a single request queues behind itself and is metered several times over.
//
// The metrics path threads a Scope through [fetch.Request] directly; the record paths reach the
// fetcher through engine interfaces that cannot carry one, which is what [fetch.WithScope] is for.
// Install it at the request boundary only — a scope that outlives its query keeps its reservation
// forever, and the budget then bounds nothing.
//
// The boundary here is one engine call, not one HTTP request: a LogQL query evaluating several
// pipeline nodes still opens a scope per node, because the querier is wired once for the process
// and the LogQL/TraceQL engines own the per-request boundary above it.
func queryScope(ctx context.Context) context.Context {
	if fetch.ScopeFrom(ctx) != nil {
		return ctx
	}
	return fetch.WithScope(ctx, fetch.NewScope())
}
