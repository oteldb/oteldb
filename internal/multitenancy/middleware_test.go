package multitenancy_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-faster/errors"
	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/multitenancy"
)

// resolverFunc adapts a function to [multitenancy.Resolver].
type resolverFunc func(*http.Request, multitenancy.Operation) (multitenancy.Decision, error)

func (f resolverFunc) Resolve(
	_ context.Context, r *http.Request, op multitenancy.Operation,
) (multitenancy.Decision, error) {
	return f(r, op)
}

// run drives the middleware over a request carrying the given selector header, returning the
// response and the tenant the handler observed ("" when none was attached).
func run(
	t *testing.T, d multitenancy.Decision, err error, selector string,
) (res *http.Response, tenant string) {
	t.Helper()

	var (
		seen   string
		called bool
	)

	mw := multitenancy.NewMiddleware(multitenancy.MiddlewareConfig{
		Resolver: resolverFunc(func(_ *http.Request, op multitenancy.Operation) (multitenancy.Decision, error) {
			require.Equal(t, multitenancy.OperationRead, op)

			return d, err
		}),
	})

	h := mw(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		called = true
		seen, _ = multitenancy.TenantFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/query", http.NoBody)
	if selector != "" {
		req.Header.Set(multitenancy.HeaderScopeOrgID, selector)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	res = rec.Result()
	t.Cleanup(func() { _ = res.Body.Close() })

	if res.StatusCode != http.StatusOK {
		require.False(t, called, "handler must not run when the request is refused")
	}

	return res, seen
}

func TestMiddlewareSelectsTenant(t *testing.T) {
	for _, tt := range []struct {
		name     string
		decision multitenancy.Decision
		selector string
		status   int
		tenant   string
	}{
		{
			name:     "DisabledPassesThroughUnscoped",
			decision: multitenancy.Decision{},
			status:   http.StatusOK,
			tenant:   "",
		},
		{
			name:     "SingleTenantNeedsNoSelector",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme"}},
			status:   http.StatusOK,
			tenant:   "acme",
		},
		{
			name:     "SelectorNarrowsPermittedSet",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme", "globex"}},
			selector: "globex",
			status:   http.StatusOK,
			tenant:   "globex",
		},
		{
			name:     "SelectorMatchingSoleTenant",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme"}},
			selector: "acme",
			status:   http.StatusOK,
			tenant:   "acme",
		},
		{
			name:     "AmbiguousWithoutSelector",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme", "globex"}},
			status:   http.StatusBadRequest,
		},
		{
			name:     "SelectorCannotWidenGrant",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme"}},
			selector: "globex",
			status:   http.StatusForbidden,
		},
		{
			name:     "EmptyGrantPermitsNothing",
			decision: multitenancy.Decision{Enabled: true},
			status:   http.StatusForbidden,
		},
		{
			name:     "MalformedSelectorRefused",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme"}},
			selector: "../etc",
			status:   http.StatusForbidden,
		},
		{
			name:     "SelectorEscapingShardKeyRefused",
			decision: multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme"}},
			selector: "acme/_s0",
			status:   http.StatusForbidden,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, tenant := run(t, tt.decision, nil, tt.selector)
			require.Equal(t, tt.status, res.StatusCode)
			require.Equal(t, tt.tenant, tenant)
		})
	}
}

// TestMiddlewareResolverErrorIs401 pins that an unresolvable credential is refused rather than
// falling through unscoped.
func TestMiddlewareResolverErrorIs401(t *testing.T) {
	res, tenant := run(t, multitenancy.Decision{}, errors.New("unauthorized"), "")
	require.Equal(t, http.StatusUnauthorized, res.StatusCode)
	require.Empty(t, tenant)
}

// TestMiddlewareAttachesDecision pins that the full decision reaches the handler, not just the
// narrowed tenant, so a backend can also enforce restrictions and quota keys.
func TestMiddlewareAttachesDecision(t *testing.T) {
	want := multitenancy.Decision{
		Enabled:   true,
		Username:  "alice",
		TenantIDs: []string{"acme"},
		QuotaKey:  "acme",
	}

	mw := multitenancy.NewMiddleware(multitenancy.MiddlewareConfig{
		Resolver: resolverFunc(func(*http.Request, multitenancy.Operation) (multitenancy.Decision, error) {
			return want, nil
		}),
	})

	var got multitenancy.Decision
	h := mw(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		var ok bool
		got, ok = multitenancy.DecisionFromContext(r.Context())
		require.True(t, ok)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", http.NoBody))

	require.Equal(t, want, got)
}

// TestMiddlewareCustomSelectorHeader pins that the selector header is configurable, and that the
// default one is then not honored.
func TestMiddlewareCustomSelectorHeader(t *testing.T) {
	mw := multitenancy.NewMiddleware(multitenancy.MiddlewareConfig{
		Resolver: resolverFunc(func(*http.Request, multitenancy.Operation) (multitenancy.Decision, error) {
			return multitenancy.Decision{Enabled: true, TenantIDs: []string{"acme", "globex"}}, nil
		}),
		SelectorHeader: "X-Tenant",
	})

	var seen string
	h := mw(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen, _ = multitenancy.TenantFromContext(r.Context())
	}))

	req := httptest.NewRequest(http.MethodGet, "/", http.NoBody)
	req.Header.Set("X-Tenant", "globex")
	req.Header.Set(multitenancy.HeaderScopeOrgID, "acme")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "globex", seen)
}
