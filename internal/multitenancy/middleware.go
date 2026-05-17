package multitenancy

import (
	"net/http"
)

// Middleware is a net/http middleware.
type Middleware = func(http.Handler) http.Handler

// MiddlewareConfig configures the authorization middleware.
type MiddlewareConfig struct {
	// Resolver is the backend used to resolve credentials to Decisions.
	Resolver Resolver
	// CredentialHeader is the HTTP header to extract the credential from (e.g. "Authorization").
	CredentialHeader string
	// AllowAnonymous controls whether requests without credentials are allowed through.
	AllowAnonymous bool
}

// NewMiddleware creates a new HTTP middleware that resolves multi-tenant authorization
// and injects the Decision into the request context.
func NewMiddleware(cfg MiddlewareConfig) Middleware {
	headerName := cfg.CredentialHeader
	if headerName == "" {
		headerName = "Authorization"
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cred := r.Header.Get(headerName)
			if cred == "" {
				if cfg.AllowAnonymous {
					next.ServeHTTP(w, r)
					return
				}
				http.Error(w, "missing credential", http.StatusUnauthorized)
				return
			}

			// We use OperationRead as the default for API requests passing through this middleware.
			// Write operations (ingest) use the WriteValidator.
			decision, err := cfg.Resolver.Resolve(r.Context(), r, OperationRead)
			if err != nil {
				// E.g. 401 from callback or static config rejection
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}

			// Inject decision
			ctx := WithDecision(r.Context(), decision)
			r = r.WithContext(ctx)

			next.ServeHTTP(w, r)
		})
	}
}
