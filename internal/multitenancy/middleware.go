package multitenancy

import (
	"net/http"
	"slices"
	"strings"
)

// Middleware is a net/http middleware.
type Middleware = func(http.Handler) http.Handler

// HeaderScopeOrgID is the header Grafana-stack clients put a tenant in, and the default
// [MiddlewareConfig.SelectorHeader].
//
// On the read path it is a *selector*, never a grant: it may only narrow the tenant set the
// credential already resolved to. A request that names a tenant its credential does not permit is
// refused.
const HeaderScopeOrgID = "X-Scope-OrgID"

// MiddlewareConfig configures [NewMiddleware].
type MiddlewareConfig struct {
	// Resolver maps the request's credential to a [Decision]. Required.
	Resolver Resolver
	// SelectorHeader is the header a caller uses to pick one of the tenants its credential permits,
	// which it must do when the credential permits more than one. Empty ⇒ [HeaderScopeOrgID].
	SelectorHeader string
}

// NewMiddleware resolves each request's authorization and attaches both the [Decision] and the
// single tenant the request reads to the context.
//
// Narrowing a permitted set to one tenant happens here rather than in a backend, for two reasons.
// It is the only layer that can answer a caller — an ambiguous or refused selection is an HTTP
// status, not a query result. And it leaves every backend with the simple contract of one tenant
// per request, so a backend cannot accidentally read wider than it was authorized to.
func NewMiddleware(cfg MiddlewareConfig) Middleware {
	selector := cfg.SelectorHeader
	if selector == "" {
		selector = HeaderScopeOrgID
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			d, err := cfg.Resolver.Resolve(r.Context(), r, OperationRead)
			if err != nil {
				http.Error(w, "unauthorized", http.StatusUnauthorized)

				return
			}

			ctx := WithDecision(r.Context(), d)

			tenant, code, msg := selectTenant(d, r.Header.Get(selector), selector)
			if code != 0 {
				http.Error(w, msg, code)

				return
			}

			if tenant != "" {
				ctx = WithTenant(ctx, tenant)
			}

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// selectTenant narrows a decision to the one tenant a request reads, returning ("", 0, "") when
// tenancy does not apply and the backend should serve its default tenant. A non-zero status is the
// refusal to answer the caller with.
func selectTenant(d Decision, raw, header string) (tenant string, status int, msg string) {
	if !d.Enabled {
		return "", 0, ""
	}

	if len(d.TenantIDs) == 0 {
		return "", http.StatusForbidden, "credential permits no tenant"
	}

	raw = strings.TrimSpace(raw)
	if raw == "" {
		if len(d.TenantIDs) > 1 {
			return "", http.StatusBadRequest, "credential permits tenants " +
				strings.Join(d.TenantIDs, ", ") + "; select one with the " + header + " header"
		}
		raw = d.TenantIDs[0]
	}

	// The selector is checked against the permitted set before it is validated as an id, so a
	// malformed value cannot be distinguished from an unpermitted one — a caller probing for which
	// tenants exist learns nothing either way.
	if !slices.Contains(d.TenantIDs, raw) {
		return "", http.StatusForbidden, "tenant not permitted"
	}

	id, err := ParseTenantID(raw)
	if err != nil {
		return "", http.StatusForbidden, "tenant not permitted"
	}

	return id, 0, ""
}
