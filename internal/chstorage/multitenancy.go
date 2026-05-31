package chstorage

import (
	"context"

	"github.com/go-faster/errors"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/oteldb/oteldb/internal/chstorage/chsql"
	"github.com/oteldb/oteldb/internal/multitenancy"
)

// decisionFilters returns PREWHERE expressions derived from the authorization
// Decision: one for tenant_id and zero or more for resource attribute selectors.
func decisionFilters(ctx context.Context) ([]chsql.Expr, error) {
	d, ok := multitenancy.DecisionFromContext(ctx)
	if !ok || !d.Enabled {
		return nil, nil
	}
	var exprs []chsql.Expr
	// Tenant filter.
	if len(d.TenantIDs) == 1 {
		exprs = append(exprs, chsql.Eq(chsql.Ident("tenant_id"), chsql.String(d.TenantIDs[0])))
	} else if len(d.TenantIDs) > 1 {
		args := make([]chsql.Expr, len(d.TenantIDs))
		for i, t := range d.TenantIDs {
			args[i] = chsql.String(t)
		}
		exprs = append(exprs, chsql.In(chsql.Ident("tenant_id"), chsql.Tuple(args...)))
	}
	// Mandatory resource selectors.
	for _, sel := range d.ResourceSelectors {
		col := resourceSelectorExpr(sel.Key)

		var op labels.MatchType
		switch sel.Op {
		case multitenancy.OpEq:
			op = labels.MatchEqual
		case multitenancy.OpNotEq:
			op = labels.MatchNotEqual
		case multitenancy.OpRe:
			op = labels.MatchRegexp
		case multitenancy.OpNotRe:
			op = labels.MatchNotRegexp
		default:
			return nil, errors.Errorf("unsupported resource selector op %d", sel.Op)
		}

		expr, err := promQLLabelMatcher([]chsql.Expr{col}, op, sel.Value)
		if err != nil {
			return nil, errors.Wrap(err, "build resource selector filter")
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

func resourceSelectorExpr(key string) chsql.Expr {
	switch key {
	case "service.namespace":
		return chsql.Ident("service_namespace")
	case "service.name":
		return chsql.Ident("service_name")
	case "service.instance.id":
		return chsql.Ident("service_instance_id")
	default:
		return attrSelector(colResource, key)
	}
}
