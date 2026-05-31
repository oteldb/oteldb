package httpcb

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
	"github.com/oteldb/oteldb/internal/multitenancyapi"
)

func makeTestServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
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
	t.Cleanup(srv.Close)
	return srv, &calls
}

func TestResolver(t *testing.T) {
	ctx := context.Background()

	srv, _ := makeTestServer(t)

	client, err := multitenancyapi.NewClient(srv.URL)
	require.NoError(t, err)

	resolver, err := NewResolver(ResolverConfig{
		Client: client,
	})
	require.NoError(t, err)

	t.Run("Authorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer valid-token")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.NoError(t, err)
		assert.True(t, decision.Enabled)
		assert.Equal(t, []string{"tenant-1"}, decision.TenantIDs)
		assert.Equal(t, "alice", decision.Username)
	})

	t.Run("Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer invalid-token")
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})
}

func TestResolver_Cache(t *testing.T) {
	ctx := context.Background()

	srv, calls := makeTestServer(t)

	client, err := multitenancyapi.NewClient(srv.URL)
	require.NoError(t, err)

	resolver, err := NewResolver(ResolverConfig{
		Client:   client,
		CacheTTL: time.Hour,
	})
	require.NoError(t, err)

	makeReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer valid-token")
		return req
	}

	// First call hits the server.
	d1, err := resolver.Resolve(ctx, makeReq(), multitenancy.OperationRead)
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load())

	// Second call must be served from cache without hitting the server.
	d2, err := resolver.Resolve(ctx, makeReq(), multitenancy.OperationRead)
	require.NoError(t, err)
	assert.Equal(t, int64(1), calls.Load(), "cache should absorb the second call")

	assert.Equal(t, d1.TenantIDs, d2.TenantIDs)
}

func TestResolver_Singleflight(t *testing.T) {
	ctx := context.Background()

	// Gate lets us hold all goroutines inside the server handler simultaneously.
	var gate sync.WaitGroup
	gate.Add(1)

	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		gate.Wait() // block until the test releases the gate
		var req multitenancyapi.AuthorizeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		resp := multitenancyapi.AuthorizeResponse{TenantIds: []string{"tenant-1"}}
		data, _ := resp.MarshalJSON()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	}))
	t.Cleanup(srv.Close)

	client, err := multitenancyapi.NewClient(srv.URL)
	require.NoError(t, err)

	resolver, err := NewResolver(ResolverConfig{Client: client})
	require.NoError(t, err)

	const n = 10
	results := make([]multitenancy.Decision, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
			req.Header.Set("Authorization", "Bearer valid-token")
			results[i], errs[i] = resolver.Resolve(ctx, req, multitenancy.OperationRead)
		}()
	}

	// Release all goroutines blocked in the handler.
	gate.Done()
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d", i)
		assert.Equal(t, []string{"tenant-1"}, results[i].TenantIDs)
	}
	assert.Equal(t, int64(1), calls.Load(), "singleflight should collapse concurrent requests into one")
}

func TestResolver_NoCaching(t *testing.T) {
	ctx := context.Background()

	srv, calls := makeTestServer(t)

	client, err := multitenancyapi.NewClient(srv.URL)
	require.NoError(t, err)

	// Zero CacheTTL disables caching; every call must reach the server.
	resolver, err := NewResolver(ResolverConfig{
		Client: client,
		// CacheTTL not set — caching disabled.
	})
	require.NoError(t, err)

	makeReq := func() *http.Request {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer valid-token")
		return req
	}

	_, err = resolver.Resolve(ctx, makeReq(), multitenancy.OperationRead)
	require.NoError(t, err)
	_, err = resolver.Resolve(ctx, makeReq(), multitenancy.OperationRead)
	require.NoError(t, err)
	assert.Equal(t, int64(2), calls.Load(), "each call should reach the server when caching is disabled")
}

func TestResolver_CustomHeader(t *testing.T) {
	ctx := context.Background()

	srv, _ := makeTestServer(t)

	client, err := multitenancyapi.NewClient(srv.URL)
	require.NoError(t, err)

	resolver, err := NewResolver(ResolverConfig{
		Client:           client,
		CredentialHeader: "X-API-Token",
	})
	require.NoError(t, err)

	t.Run("WithCustomHeader", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("X-API-Token", "valid-token")
		decision, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.NoError(t, err)
		assert.True(t, decision.Enabled)
	})

	t.Run("WrongHeader", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
		req.Header.Set("Authorization", "Bearer valid-token") // wrong header
		_, err := resolver.Resolve(ctx, req, multitenancy.OperationRead)
		require.Error(t, err)
	})
}
