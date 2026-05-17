package static

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
)

func TestResolver(t *testing.T) {
	ctx := context.Background()

	readDecisions := map[string]multitenancy.Decision{
		"token1": {
			Enabled:   true,
			TenantIDs: []string{"tenant-a", "tenant-b"},
		},
	}
	writeDecisions := map[string]multitenancy.Decision{
		"token1": {
			Enabled:   true,
			TenantIDs: []string{"tenant-a"},
		},
	}

	resolver := NewResolver(Config{
		ReadDecisions:  readDecisions,
		WriteDecisions: writeDecisions,
	})

	t.Run("ReadAuthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer token1")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.NoError(t, err)
		require.Equal(t, []string{"tenant-a", "tenant-b"}, decision.TenantIDs)
	})

	t.Run("WriteAuthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Authorization", "token1")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationWrite)
		require.NoError(t, err)
		require.Equal(t, []string{"tenant-a"}, decision.TenantIDs)
	})

	t.Run("Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "unknown")
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})

	t.Run("MissingCredential", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})
}
