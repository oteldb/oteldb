package chstorage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-faster/oteldb/internal/chstorage/chsql"
	"github.com/go-faster/oteldb/internal/multitenancy"
)

// renderExpr renders a chsql expression to a SQL string for assertion.
func renderExpr(t *testing.T, expr chsql.Expr) string {
	t.Helper()
	p := chsql.GetPrinter()
	defer chsql.PutPrinter(p)
	require.NoError(t, p.WriteExpr(expr))
	return p.String()
}

func TestDecisionFilters_NoDecision(t *testing.T) {
	ctx := context.Background()
	filters, err := decisionFilters(ctx)
	require.NoError(t, err)
	assert.Empty(t, filters)
}

func TestDecisionFilters_Disabled(t *testing.T) {
	ctx := multitenancy.WithDecision(context.Background(), multitenancy.Decision{
		Enabled: false,
	})
	filters, err := decisionFilters(ctx)
	require.NoError(t, err)
	assert.Empty(t, filters)
}

func TestDecisionFilters_SingleTenant(t *testing.T) {
	ctx := multitenancy.WithDecision(context.Background(), multitenancy.Decision{
		Enabled:   true,
		TenantIDs: []string{"acme"},
	})
	filters, err := decisionFilters(ctx)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	sql := renderExpr(t, filters[0])
	assert.Contains(t, sql, "tenant_id")
	assert.Contains(t, sql, "'acme'")
}

func TestDecisionFilters_MultiTenant(t *testing.T) {
	ctx := multitenancy.WithDecision(context.Background(), multitenancy.Decision{
		Enabled:   true,
		TenantIDs: []string{"acme", "beta"},
	})
	filters, err := decisionFilters(ctx)
	require.NoError(t, err)
	require.Len(t, filters, 1)
	sql := renderExpr(t, filters[0])
	assert.Contains(t, sql, "tenant_id")
	assert.Contains(t, sql, "'acme'")
	assert.Contains(t, sql, "'beta'")
}

func TestDecisionFilters_ResourceSelectors(t *testing.T) {
	cases := []struct {
		name        string
		selector    multitenancy.ResourceSelector
		wantContain string
	}{
		{
			name:        "eq on known column",
			selector:    multitenancy.ResourceSelector{Key: "service.name", Op: multitenancy.OpEq, Value: "myapp"},
			wantContain: "service_name = 'myapp'",
		},
		{
			name:        "neq on known column",
			selector:    multitenancy.ResourceSelector{Key: "service.namespace", Op: multitenancy.OpNotEq, Value: "infra"},
			wantContain: "service_namespace != 'infra'",
		},
		{
			name:        "regex on generic attr",
			selector:    multitenancy.ResourceSelector{Key: "k8s.cluster", Op: multitenancy.OpRe, Value: "prod-.*"},
			wantContain: "prod-.*",
		},
		{
			name:        "not-regex on generic attr",
			selector:    multitenancy.ResourceSelector{Key: "k8s.cluster", Op: multitenancy.OpNotRe, Value: "dev-.*"},
			wantContain: "dev-.*",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := multitenancy.WithDecision(context.Background(), multitenancy.Decision{
				Enabled:           true,
				TenantIDs:         []string{"t1"},
				ResourceSelectors: []multitenancy.ResourceSelector{tc.selector},
			})
			filters, err := decisionFilters(ctx)
			require.NoError(t, err)
			require.Len(t, filters, 2) // tenant_id filter + resource selector
			sql := renderExpr(t, filters[1])
			assert.True(t, strings.Contains(sql, tc.wantContain),
				"expected %q to contain %q", sql, tc.wantContain)
		})
	}
}

func TestDecisionFilters_InvalidOp(t *testing.T) {
	ctx := multitenancy.WithDecision(context.Background(), multitenancy.Decision{
		Enabled:   true,
		TenantIDs: []string{"t1"},
		ResourceSelectors: []multitenancy.ResourceSelector{
			{Key: "service.name", Op: multitenancy.MatchOp(255), Value: "x"},
		},
	})
	_, err := decisionFilters(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported resource selector op")
}
