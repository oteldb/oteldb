// Package multitenancy resolves, per request, which tenants a caller is authorized to read.
//
// The read path and the write path decide tenancy by different mechanisms and deliberately do not
// share one. Ingest resolves a tenant by *routing*: the sender is a trusted collector or gateway,
// and the question is which shard namespace its telemetry belongs to (see cmd/odbingest). Query
// resolves a tenant by *authorization*: the caller is untrusted, and the question is what it may
// see. Only the credential answers that, so a [Resolver] maps a credential to a [Decision] and a
// client-supplied header can never widen it.
//
// The types here are storage-agnostic: what a [Decision] means is applied by the backend serving
// the query — a tenant-scoped read on the storage engine, a predicate on ClickHouse.
package multitenancy

import (
	"context"
	"net/http"
	"time"
)

// Decision is the resolved authorization result for one request.
//
// The zero value grants nothing: with Enabled false it is the "tenancy is not configured" answer,
// which every caller reads as the deployment's single default tenant.
type Decision struct {
	// Enabled reports whether tenancy applies to this request. False bypasses tenant scoping
	// entirely, which is what an unconfigured deployment sees.
	Enabled bool
	// Username is informational, and the fallback quota key.
	Username string
	// TenantIDs are the tenants this credential may read. It is meaningful only when Enabled: an
	// empty set then permits nothing, whereas with Enabled false it means the scoping does not
	// apply at all.
	TenantIDs []string
	// ResourceSelectors are mandatory resource-attribute filters applied on top of tenant scoping.
	// They are enforced by backends that can inject predicates; the storage engine ignores them
	// (see [ErrUnsupportedSelectors]).
	ResourceSelectors []ResourceSelector
	// QuotaKey identifies the caller for backend-side quota accounting.
	QuotaKey string
	// Restrictions are per-query resource limits.
	Restrictions QueryRestrictions
}

// ResourceSelector is a mandatory filter on a resource attribute imposed by the authorization
// layer, applied in addition to tenant scoping and before any user-supplied matcher.
type ResourceSelector struct {
	// Key is the resource attribute name, e.g. "service.namespace".
	Key string
	// Op is the match operation.
	Op MatchOp
	// Value is the match value.
	Value string
}

// MatchOp is a [ResourceSelector] match operation.
type MatchOp uint8

// Supported [MatchOp] values.
const (
	// OpEq tests exact equality.
	OpEq MatchOp = iota
	// OpNotEq tests inequality.
	OpNotEq
	// OpRe tests a regex match.
	OpRe
	// OpNotRe tests a negated regex match.
	OpNotRe
)

// QueryRestrictions are per-query resource limits a backend may enforce.
type QueryRestrictions struct {
	// MaxMemoryUsageBytes bounds a query's memory.
	MaxMemoryUsageBytes uint64
	// MaxExecutionTime bounds a query's wall time.
	MaxExecutionTime time.Duration
	// MaxResultRows bounds a query's result size.
	MaxResultRows uint64
}

// Operation distinguishes the read and write trust domains, which resolve independently: the same
// credential may read one tenant set and write another.
type Operation uint8

// Supported [Operation] values.
const (
	// OperationRead is the query side: what may this caller see.
	OperationRead Operation = iota
	// OperationWrite is the ingest side: what may this sender claim.
	OperationWrite
)

// Resolver maps a request's credential to a [Decision].
//
// A Resolver is the only grant: it must derive the tenant set from the credential and must never
// read it from a caller-supplied tenant header.
type Resolver interface {
	Resolve(ctx context.Context, r *http.Request, op Operation) (Decision, error)
}

type (
	decisionKey struct{}
	tenantKey   struct{}
)

// WithDecision attaches d to ctx.
func WithDecision(ctx context.Context, d Decision) context.Context {
	return context.WithValue(ctx, decisionKey{}, d)
}

// DecisionFromContext returns the [Decision] attached to ctx, if any.
func DecisionFromContext(ctx context.Context) (Decision, bool) {
	d, ok := ctx.Value(decisionKey{}).(Decision)

	return d, ok
}

// WithTenant attaches the single tenant a request resolved to. It is set by [Middleware] after
// narrowing [Decision.TenantIDs], so a backend never has to re-derive it (and can never widen it).
func WithTenant(ctx context.Context, tenant string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenant)
}

// TenantFromContext returns the tenant a request resolved to, if any.
func TenantFromContext(ctx context.Context) (string, bool) {
	t, ok := ctx.Value(tenantKey{}).(string)

	return t, ok && t != ""
}
