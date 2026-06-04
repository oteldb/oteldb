package multitenancy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockResolver struct {
	resolve func(ctx context.Context, r *http.Request, op Operation) (Decision, error)
}

func (m *mockResolver) Resolve(ctx context.Context, r *http.Request, op Operation) (Decision, error) {
	return m.resolve(ctx, r, op)
}

func TestMiddleware(t *testing.T) {
	t.Run("Authorized", func(t *testing.T) {
		resolver := &mockResolver{
			resolve: func(ctx context.Context, r *http.Request, op Operation) (Decision, error) {
				return Decision{
					Enabled:   true,
					TenantIDs: []string{"tenant-1"},
				}, nil
			},
		}

		mw := NewMiddleware(MiddlewareConfig{
			Resolver: resolver,
		})

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d, ok := DecisionFromContext(r.Context())
			assert.True(t, ok)
			assert.True(t, d.Enabled)
			assert.Equal(t, []string{"tenant-1"}, d.TenantIDs)
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "http://localhost/query", http.NoBody)
		req.Header.Set("Authorization", "Bearer token-1")
		rec := httptest.NewRecorder()

		mw(next).ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("MissingCredential", func(t *testing.T) {
		mw := NewMiddleware(MiddlewareConfig{
			Resolver: &mockResolver{},
		})

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("should not reach next handler")
		})

		req := httptest.NewRequest(http.MethodGet, "http://localhost/query", http.NoBody)
		rec := httptest.NewRecorder()

		mw(next).ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
		assert.Contains(t, rec.Body.String(), "missing credential")
	})

	t.Run("AllowAnonymous", func(t *testing.T) {
		mw := NewMiddleware(MiddlewareConfig{
			Resolver:       &mockResolver{},
			AllowAnonymous: true,
		})

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, ok := DecisionFromContext(r.Context())
			assert.False(t, ok)
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "http://localhost/query", http.NoBody)
		rec := httptest.NewRecorder()

		mw(next).ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("ResolverError", func(t *testing.T) {
		resolver := &mockResolver{
			resolve: func(ctx context.Context, r *http.Request, op Operation) (Decision, error) {
				return Decision{}, assert.AnError
			},
		}

		mw := NewMiddleware(MiddlewareConfig{
			Resolver: resolver,
		})

		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("should not reach next handler")
		})

		req := httptest.NewRequest(http.MethodGet, "http://localhost/query", http.NoBody)
		req.Header.Set("Authorization", "Bearer invalid")
		rec := httptest.NewRecorder()

		mw(next).ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	})
}
