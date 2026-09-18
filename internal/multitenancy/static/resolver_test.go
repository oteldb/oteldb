package static_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
	"github.com/oteldb/oteldb/internal/multitenancy/static"
)

func TestResolver(t *testing.T) {
	ctx := context.Background()

	r, err := static.NewResolver(static.Config{
		Read: map[string]multitenancy.Decision{
			"acme-token": {Enabled: true, TenantIDs: []string{"acme"}, Username: "alice"},
		},
		Write: map[string]multitenancy.Decision{
			"acme-token": {Enabled: true, TenantIDs: []string{"acme", "acme-staging"}},
		},
	})
	require.NoError(t, err)

	req := func(auth string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}

		return r
	}

	t.Run("BearerPrefixStripped", func(t *testing.T) {
		d, err := r.Resolve(ctx, req("Bearer acme-token"), multitenancy.OperationRead)
		require.NoError(t, err)
		require.Equal(t, []string{"acme"}, d.TenantIDs)
		require.Equal(t, "alice", d.Username)
	})

	t.Run("BareToken", func(t *testing.T) {
		d, err := r.Resolve(ctx, req("acme-token"), multitenancy.OperationRead)
		require.NoError(t, err)
		require.True(t, d.Enabled)
	})

	t.Run("ReadAndWriteResolveIndependently", func(t *testing.T) {
		d, err := r.Resolve(ctx, req("acme-token"), multitenancy.OperationWrite)
		require.NoError(t, err)
		require.Equal(t, []string{"acme", "acme-staging"}, d.TenantIDs)
	})

	t.Run("UnknownCredential", func(t *testing.T) {
		_, err := r.Resolve(ctx, req("nope"), multitenancy.OperationRead)
		require.Error(t, err)
	})

	t.Run("MissingCredential", func(t *testing.T) {
		_, err := r.Resolve(ctx, req(""), multitenancy.OperationRead)
		require.Error(t, err)
	})
}

// TestResolverValidatesTenants pins that a tenant id which is unusable as a shard key is a
// construction error, so it can never reach the storage layer.
func TestResolverValidatesTenants(t *testing.T) {
	_, err := static.NewResolver(static.Config{
		Read: map[string]multitenancy.Decision{
			"tok": {Enabled: true, TenantIDs: []string{"acme/_s0"}},
		},
	})
	require.Error(t, err)
}

func TestResolverCustomHeader(t *testing.T) {
	r, err := static.NewResolver(static.Config{
		Read:             map[string]multitenancy.Decision{"tok": {Enabled: true, TenantIDs: []string{"acme"}}},
		CredentialHeader: "X-API-Key",
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-API-Key", "tok")

	d, err := r.Resolve(context.Background(), req, multitenancy.OperationRead)
	require.NoError(t, err)
	require.Equal(t, []string{"acme"}, d.TenantIDs)
}
