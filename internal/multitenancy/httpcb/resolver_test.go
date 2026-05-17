package httpcb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
	"github.com/oteldb/oteldb/internal/multitenancyapi"
)

func TestResolver(t *testing.T) {
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req multitenancyapi.AuthorizeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if req.Credential != "valid-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		resp := multitenancyapi.AuthorizeResponse{
			TenantIds: []string{"tenant-1"},
			Username:  multitenancyapi.NewOptString("alice"),
		}
		data, err := resp.MarshalJSON()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	defer srv.Close()

	client, err := multitenancyapi.NewClient(srv.URL)
	require.NoError(t, err)

	resolver := NewResolver(ResolverConfig{
		Client: client,
	})

	t.Run("Authorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.NoError(t, err)
		assert.True(t, decision.Enabled)
		assert.Equal(t, []string{"tenant-1"}, decision.TenantIDs)
		assert.Equal(t, "alice", decision.Username)
	})

	t.Run("Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer invalid-token")
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})
}
