// Package multitenancy provides tenant authorization and filtering for oteldb.
package multitenancy

import (
	"context"
	"net/http"
	"time"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
)

// Decision is the resolved authorization result for one HTTP request.
type Decision struct {
	// Enabled controls whether multi-tenancy is active for this request.
	// If false, all filtering is bypassed.
	Enabled bool
	// Username is informational; used as a fallback quota key.
	Username string
	// TenantIDs contains the allowed tenant identifiers.
	// Empty when Enabled is false (meaning all tenants).
	TenantIDs []string
	// ResourceSelectors contains mandatory resource-attribute filters
	// that must be applied in addition to tenant filtering.
	ResourceSelectors []ResourceSelector
	// QuotaKey is forwarded to ch.Query.QuotaKey for ClickHouse quota enforcement.
	QuotaKey string
	// Restrictions contains query-level resource limits.
	Restrictions QueryRestrictions
}

// ResourceSelector is a mandatory filter on a resource attribute enforced by
// the authorization layer, applied on top of the tenant_id filter.
type ResourceSelector struct {
	// Key is the resource attribute name, e.g. "service.namespace"
	Key string
	// Op is the match operation.
	Op MatchOp
	// Value is the match value.
	Value string
}

// MatchOp is a resource selector match operation.
type MatchOp uint8

const (
	// OpEq tests exact equality (=).
	OpEq MatchOp = iota
	// OpNotEq tests inequality (!=).
	OpNotEq
	// OpRe tests regex match.
	OpRe
	// OpNotRe tests negated regex match.
	OpNotRe
)

// QueryRestrictions contains per-query resource limits.
type QueryRestrictions struct {
	// MaxMemoryUsageBytes maps to ClickHouse max_memory_usage setting.
	MaxMemoryUsageBytes uint64
	// MaxExecutionTime maps to ClickHouse max_execution_time setting.
	MaxExecutionTime time.Duration
	// MaxResultRows maps to ClickHouse max_result_rows setting.
	MaxResultRows uint64
}

// Operation distinguishes read (query) and write (ingest) trust domains.
type Operation uint8

const (
	// OperationRead is the query-side operation: filter what the caller can see.
	OperationRead Operation = iota
	// OperationWrite is the ingest-side operation: validate what the sender can claim.
	OperationWrite
)

// Resolver maps an HTTP request to a Decision.
type Resolver interface {
	// Resolve returns the authorization Decision for the given request and operation.
	Resolve(ctx context.Context, r *http.Request, op Operation) (Decision, error)
}

// FilterProvider builds a PREWHERE/WHERE predicate restricting to the given tenant IDs.
type FilterProvider interface {
	// TenantFilter returns a ClickHouse expression filtering by tenant_id.
	// column is the ClickHouse column name (typically "tenant_id").
	TenantFilter(tenantIDs []string, column string) chsql.Expr
}

type decisionKey struct{}

// WithDecision attaches a Decision to the context.
func WithDecision(ctx context.Context, d Decision) context.Context {
	return context.WithValue(ctx, decisionKey{}, d)
}

// DecisionFromContext retrieves the Decision from the context.
// Returns (Decision{}, false) if no Decision is present.
func DecisionFromContext(ctx context.Context) (Decision, bool) {
	d, ok := ctx.Value(decisionKey{}).(Decision)
	return d, ok
}
