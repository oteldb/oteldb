package chstorage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
)

func TestDecisionWriteValidator(t *testing.T) {
	ctx := context.Background()

	t.Run("NoDecisionInContext", func(t *testing.T) {
		validator := &decisionWriteValidator{}
		// Without a decision in context, all writes should be authorized
		require.True(t, validator.IsAuthorized(ctx, "tenant-a"))
		require.True(t, validator.IsAuthorized(ctx, "tenant-b"))
		require.True(t, validator.IsAuthorized(ctx, ""))
	})

	t.Run("WithDecisionInContext", func(t *testing.T) {
		validator := &decisionWriteValidator{}

		decision := multitenancy.Decision{
			Enabled:   true,
			TenantIDs: []string{"tenant-a", "tenant-b"},
		}
		ctx := multitenancy.WithDecision(ctx, decision)

		t.Run("AuthorizedTenant", func(t *testing.T) {
			require.True(t, validator.IsAuthorized(ctx, "tenant-a"))
			require.True(t, validator.IsAuthorized(ctx, "tenant-b"))
		})

		t.Run("UnauthorizedTenant", func(t *testing.T) {
			require.False(t, validator.IsAuthorized(ctx, "tenant-c"))
			require.False(t, validator.IsAuthorized(ctx, "tenant-unknown"))
		})

		t.Run("EmptyTenant", func(t *testing.T) {
			// Empty tenant_id should be unauthorized if decision has specific tenants
			require.False(t, validator.IsAuthorized(ctx, ""))
		})
	})

	t.Run("EmptyDecisionTenantList", func(t *testing.T) {
		validator := &decisionWriteValidator{}

		decision := multitenancy.Decision{
			Enabled:   true,
			TenantIDs: []string{},
		}
		ctx := multitenancy.WithDecision(ctx, decision)

		// Empty tenant list means no tenants are authorized
		require.False(t, validator.IsAuthorized(ctx, "tenant-a"))
		require.False(t, validator.IsAuthorized(ctx, ""))
	})
}
