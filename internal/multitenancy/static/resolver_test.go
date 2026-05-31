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
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer token1")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.NoError(t, err)
		require.Equal(t, []string{"tenant-a", "tenant-b"}, decision.TenantIDs)
	})

	t.Run("WriteAuthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
		req.Header.Set("Authorization", "token1")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationWrite)
		require.NoError(t, err)
		require.Equal(t, []string{"tenant-a"}, decision.TenantIDs)
	})

	t.Run("Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "unknown")
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})

	t.Run("MissingCredential", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})
}

func TestResolver_CustomCredentialHeader(t *testing.T) {
	ctx := context.Background()

	resolver := NewResolver(Config{
		CredentialHeader: "X-Tenant-Token",
		ReadDecisions: map[string]multitenancy.Decision{
			"secret": {Enabled: true, TenantIDs: []string{"acme"}},
		},
	})

	t.Run("CustomHeaderPresent", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("X-Tenant-Token", "secret")
		d, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.NoError(t, err)
		require.Equal(t, []string{"acme"}, d.TenantIDs)
	})

	t.Run("DefaultAuthorizationIgnored", func(t *testing.T) {
		// Setting Authorization should have no effect when a custom header is configured.
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer secret")
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err, "credential must come from the configured header, not Authorization")
	})
}
