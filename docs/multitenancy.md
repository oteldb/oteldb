# Multi-tenancy

This document is a design proposal for adding request-scoped tenant
authorization and ClickHouse-side data isolation across PromQL, LogQL, and
TraceQL. It covers the storage model, authorization flow, query predicate
injection, and migration path.

## Current state

- No `tenant_id` column exists in any data table.
- Auth middleware returns `401`/`403` but carries no identity into query handlers.
- Metrics singleflight caches (`querier_metrics.go`, `querier_metrics_timeseries.go`)
  key on matcher/parameter hashes only — sharing across tenants would be a
  correctness bug once tenant filtering is introduced.
- Metric series hashes (`inserter_metrics.go:516`) are computed from
  name + resource/scope/attribute only; two tenants with identical series
  would share the same hash row in `metrics_points`.

## Package layout

```
internal/multitenancy/          # public types, Middleware constructor
internal/multitenancy/static/   # token→Decision map, O(1) lookup
internal/multitenancy/httpcb/   # HTTP callback resolver (generated from _oas/multitenancy.yml)
```

No new binary. The middleware is registered in `cmd/oteldb/app.go:addOgen()`.

## Core types

```go
// Decision is the resolved authorization result for one HTTP request.
type Decision struct {
    Enabled           bool
    Username          string             // informational; quota key fallback
    TenantIDs         []string           // allowed tenants; empty = all (only when !Enabled)
    ResourceSelectors []ResourceSelector // mandatory resource-attribute filters
    QuotaKey          string
    Restrictions      QueryRestrictions
}

type ResourceSelector struct {
    Key   string  // resource attribute name, e.g. "service.namespace"
    Op    MatchOp // Eq | NotEq | Re | NotRe
    Value string
}

type QueryRestrictions struct {
    MaxMemoryUsageBytes uint64
    MaxExecutionTime    time.Duration
    MaxResultRows       uint64
}

type Operation uint8
const (
    OperationRead  Operation = iota
    OperationWrite
)

type Resolver interface {
    Resolve(ctx context.Context, r *http.Request, op Operation) (Decision, error)
}

// FilterProvider builds a PREWHERE predicate restricting to the given tenant IDs.
type FilterProvider interface {
    TenantFilter(tenantIDs []string, column string) chsql.Expr
}
```

Context access: unexported `decisionKey`; `WithDecision` / `DecisionFromContext`
are the public API.

## Middleware

`multitenancy.Middleware(r Resolver, fp FilterProvider, cfg MiddlewareConfig)`
resolves the credential on each request, caches the decision in a bounded
TTL LRU keyed by credential value, injects it into context, and returns `401`
on resolver error when tenancy is enabled. `MiddlewareConfig` holds credential
extraction rules (header/cookie names), cache TTL, and whether to allow
anonymous requests.

## Resolver backends

**Static** (`internal/multitenancy/static`): a token→Decision map loaded from
config. Constant-time lookup. Suitable when no external auth service exists.

**HTTP callback** (`internal/multitenancy/httpcb`): `POST /v1/authorize` on an
external service. Client generated from `_oas/multitenancy.yml`.

```yaml
POST /v1/authorize
  request:  { credential, operation: "read"|"write", metadata? }
  response: { tenant_ids, username?, quota_key?,
              resource_selectors?, restrictions?, cache_ttl? }
  errors:   401
```

Both backends accept an `Operation` parameter so the same credential can
return different tenant sets for reads vs. writes.

## Config additions

```yaml
multitenancy:
  enabled: false
  tenants:
    - id: "fooapp-prod"
      key_attributes: { example.org/app: foo, example.org/env: prod }
  # tenant_id_template: "{example.org/app}-{example.org/env}"
  default_tenant: ""   # empty = drop + log
  backend:
    type: static        # or "http"
    static:
      tokens:
        - token: "..."
          read_tenant_ids: ["prod"]
          write_tenant_ids: ["prod"]
          read_resource_selectors:
            - { key: service.namespace, op: eq, value: backend }
          username: "alice"
          quota_key: "prod"
    http:
      url: "http://auth:8080"
      timeout: 5s
      cache_ttl: 60s
      credential_header: "Authorization"
  default_restrictions:
    max_memory_usage_bytes: 2147483648
    max_execution_time_ms: 60000
```

## Tenant identity model

`tenant_id LowCardinality(String)` — human-readable values like `"fooapp-prod"`.

Two forms for resolving `f(resource) → tenant_id` at ingest:

- **Explicit map**: `key_attributes` tuple → `id`. Preferred when the tenant
  set is enumerable. O(1) lookup, cached by attribute-value tuple.
- **Template**: `"{example.org/app}-{example.org/env}"`. Zero config; new
  attribute combinations auto-create tenant IDs.

If no mapping matches and `default_tenant` is empty, the record is dropped and
`multitenancy_unmapped_records_total` is incremented.

### Why not hash-based IDs

A hash-based model (xxh128 of key attributes, stored as `FixedString(16)`)
was considered. It was rejected because:

- Adding an attribute combination to a tenant's key set produces a new hash;
  queries spanning old and new data require an ever-growing `IN` set.
- Consolidating two tenants into one leaves incompatible hash lineages with
  no clean migration path.
- Read-side authorization requires a `tenant_registry` join to resolve
  attribute predicates to hash sets.

The named model handles both lifecycle operations with a bounded config change
and needs no registry at query time.

## Write vs. read trust domains

These are intentionally separate:

- **Write (ingest)**: "which tenant does this telemetry belong to?" — data
  classification via `f(resource) → tenant_id`.
- **Read (query)**: "which tenants can this caller see?" — enforced by
  `Resolver` with `OperationRead`.

After resolving `tenant_id` from the resource map, the write path checks that
the resolved ID appears in `Decision.TenantIDs` for the write credential. If
not, the record is dropped and `multitenancy_rejected_writes_total` is
incremented. When no write `Resolver` is configured, the resource map result
is trusted as-is.

## Schema changes

Add `tenant_id LowCardinality(String)` as the first column and first ORDER BY /
PRIMARY KEY prefix in all data tables. Schema changes follow the standard
checklist in `CLAUDE.md`.

| Table | ORDER BY before | ORDER BY after |
|---|---|---|
| `logs` | `(severity_number, service_namespace, service_name, resource)` | `(tenant_id, severity_number, ...)` |
| `traces_spans` | `(service_namespace, service_name, resource)` | `(tenant_id, service_namespace, ...)` |
| `metrics_timeseries` | `(name, resource, scope, attribute)` | `(tenant_id, name, ...)` |
| `metrics_points` / `metrics_exp_histograms` / `metrics_exemplars` | `(hash, timestamp)` | `(tenant_id, hash, timestamp)` |
| `metrics_labels` | `(name, value, scope)` | `(tenant_id, name, ...)` |
| `traces_tags` | `(value_type, name, value)` | `(tenant_id, value_type, ...)` |

`hashTimeseries()` at `inserter_metrics.go:515` must include `tenant_id` as the
first hash input so identical series for different tenants produce distinct rows
in `metrics_points`.

## Query predicate injection

Each query builder reads the `Decision` from context and attaches filters as
PREWHERE conditions before `query.Prepare()`:

```go
func decisionFilters(ctx context.Context) []chsql.Expr {
    d, ok := multitenancy.DecisionFromContext(ctx)
    if !ok || !d.Enabled {
        return nil
    }
    var exprs []chsql.Expr
    switch len(d.TenantIDs) {
    case 1:
        exprs = append(exprs, chsql.Eq(chsql.Ident("tenant_id"), chsql.String(d.TenantIDs[0])))
    default:
        args := make([]chsql.Expr, len(d.TenantIDs))
        for i, t := range d.TenantIDs {
            args[i] = chsql.String(t)
        }
        exprs = append(exprs, chsql.In(chsql.Ident("tenant_id"), chsql.Tuple(args...)))
    }
    for _, sel := range d.ResourceSelectors {
        col := resourceSelectorExpr(sel.Key)
        expr, _ := promQLLabelMatcher([]chsql.Expr{col}, sel.Op, sel.Value)
        exprs = append(exprs, expr)
    }
    return exprs
}
```

`resourceSelectorExpr` returns a materialized column expression for the common
service attributes (`service.namespace`, `service.name`, `service.instance.id`)
and falls back to JSON extraction for others. Both benefit from PREWHERE
evaluation before decompression.

Call sites (non-exhaustive):

- **Logs**: `LogsQuery.Execute()`, `SampleQuery.Execute()`, `LabelNames`,
  `LabelValues`, `Series`, `deduplicatedResource`.
- **Traces**: `buildSpansetsQuery()`, `SearchTags()`, `TraceByID()`,
  `TagNames()`, `TagValues()`, `attributeValues()`, `spanNames()`.
- **Metrics**: `queryTimeseries()`, `getLabelValues()`, `getLabelNames()`,
  `queryMetadata()`, `buildSeriesQuery()`, `queryPoints()`,
  `queryExpHistograms()`.

Subqueries (e.g. the `trace_id` subquery in `SelectSpansets`, hash subquery in
`queryPoints`) must also receive the filters.

Authorization-imposed resource selectors are mandatory and are attached before
any user-supplied label matchers.

## ClickHouse settings and quota key

`Querier.do()` at `querier.go:134` reads `Decision.Restrictions` from context
and merges the values into `ch.Query.Settings`; `Decision.QuotaKey` is set on
`ch.Query.QuotaKey`.

## Metrics cache correctness

`metricSelectParams` gains `TenantIDs []string` and
`ResourceSelectors []ResourceSelector` fields. `Hash()` sorts and hashes both
alongside existing fields. The same applies to `timeseriesQuerier.hashMatchers()`.
This scopes singleflight keys to the full authorization decision and prevents
cross-tenant cache sharing.

## Migration

MergeTree ORDER BY cannot be altered in-place. The migrator
(`internal/chstorage/migrate.go`) logs a warning when `tenant_id` is absent
from a live table and suggests recreation. `cmd/odbmigrate` can be extended
with a `migrate tenant-id` subcommand that performs
`CREATE → INSERT INTO SELECT → RENAME` for live data.

## Validation plan

- Unit tests: resolver backends, middleware context injection and TTL caching,
  `decisionFilters` output for 0/1/N tenants, `metricSelectParams.Hash()` with
  and without tenants, inserter `tenant_id` extraction.
- Schema tests: update `internal/chstorage/gold_test.go` golden SQL to include
  `tenant_id`.
- Query tests: assert generated SQL contains `PREWHERE tenant_id = ...` or
  `PREWHERE tenant_id IN (...)` when a `Decision` is in context; assert no
  filter when `Enabled = false`.
- Benchmark: run `cmd/otelbench` PromQL bench against a multi-tenant dataset;
  compare `EXPLAIN PLAN` and `system.query_log` profiles for no-tenancy
  baseline, single-tenant `=`, and 10-tenant `IN` to confirm primary-key
  pruning is effective.
