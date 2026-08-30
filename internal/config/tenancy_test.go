package config_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/oteldb/oteldb/internal/config"
	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/multitenancy"
)

// TestTenancyMiddlewareDisabled pins that a deployment which has not opted in installs no
// middleware at all.
func TestTenancyMiddlewareDisabled(t *testing.T) {
	m, err := config.TenancyMiddleware(config.Tenancy{})
	require.NoError(t, err)
	require.Nil(t, m)

	m, err = config.TenancyMiddleware(config.Tenancy{
		Tokens: []config.TenancyToken{{
			Token:   httpmiddleware.Token{Token: "tok"},
			Tenants: []string{"acme"},
		}},
	})
	require.NoError(t, err)
	require.Nil(t, m, "tokens alone must not enable tenancy")
}

// TestTenancyMiddlewareRejectsUnusableConfig pins that enabling tenancy without a usable credential
// table is a startup error, so the easy misconfiguration fails loudly rather than permissively.
func TestTenancyMiddlewareRejectsUnusableConfig(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  config.Tenancy
	}{
		{
			name: "NoTokens",
			cfg:  config.Tenancy{Enabled: true},
		},
		{
			name: "TokenWithoutTenants",
			cfg: config.Tenancy{Enabled: true, Tokens: []config.TenancyToken{{
				Token: httpmiddleware.Token{Token: "tok"},
			}}},
		},
		{
			name: "EmptyToken",
			cfg: config.Tenancy{Enabled: true, Tokens: []config.TenancyToken{{
				Tenants: []string{"acme"},
			}}},
		},
		{
			name: "DuplicateToken",
			cfg: config.Tenancy{Enabled: true, Tokens: []config.TenancyToken{
				{Token: httpmiddleware.Token{Token: "tok"}, Tenants: []string{"acme"}},
				{Token: httpmiddleware.Token{Token: "tok"}, Tenants: []string{"globex"}},
			}},
		},
		{
			name: "InvalidTenantID",
			cfg: config.Tenancy{Enabled: true, Tokens: []config.TenancyToken{{
				Token:   httpmiddleware.Token{Token: "tok"},
				Tenants: []string{"acme/_s0"},
			}}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.TenancyMiddleware(tt.cfg)
			require.Error(t, err)
		})
	}
}

// TestTenancyMiddlewareEndToEnd drives a configured middleware, covering the whole path from a
// credential to the tenant a handler reads.
func TestTenancyMiddlewareEndToEnd(t *testing.T) {
	m, err := config.TenancyMiddleware(config.Tenancy{
		Enabled: true,
		Tokens: []config.TenancyToken{
			{Token: httpmiddleware.Token{Token: "acme-tok"}, Tenants: []string{"acme"}, Username: "alice"},
			{Token: httpmiddleware.Token{Token: "both-tok"}, Tenants: []string{"acme", "globex"}},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, m)

	var seen string
	h := m(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen, _ = multitenancy.TenantFromContext(r.Context())
	}))

	do := func(token, selector string) (int, string) {
		seen = ""

		req := httptest.NewRequest(http.MethodGet, "/api/v1/query", http.NoBody)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if selector != "" {
			req.Header.Set(multitenancy.HeaderScopeOrgID, selector)
		}

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		return rec.Code, seen
	}

	for _, tt := range []struct {
		name     string
		token    string
		selector string
		status   int
		tenant   string
	}{
		{"NoCredential", "", "", http.StatusUnauthorized, ""},
		{"UnknownCredential", "nope", "", http.StatusUnauthorized, ""},
		{"SingleTenant", "acme-tok", "", http.StatusOK, "acme"},
		{"HeaderCannotWidenGrant", "acme-tok", "globex", http.StatusForbidden, ""},
		{"AmbiguousGrantNeedsSelector", "both-tok", "", http.StatusBadRequest, ""},
		{"SelectorPicksPermittedTenant", "both-tok", "globex", http.StatusOK, "globex"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			status, tenant := do(tt.token, tt.selector)
			require.Equal(t, tt.status, status)
			require.Equal(t, tt.tenant, tenant)
		})
	}
}
