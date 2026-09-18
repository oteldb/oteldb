package config

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/httpmiddleware"
	"github.com/oteldb/oteldb/internal/multitenancy"
	"github.com/oteldb/oteldb/internal/multitenancy/static"
)

// Tenancy configures read-path multi-tenancy: which tenants a query credential may read.
//
// It is off by default, and a deployment that leaves it off behaves exactly as one with no notion
// of tenants — every read is served from the single default tenant. It is deliberately not a switch
// that can be turned on without also naming credentials: a tenant is granted by the credential, so
// enabling tenancy without a credential table is a config error rather than a permissive default.
type Tenancy struct {
	// Enabled turns read-path tenancy on. Off ⇒ no middleware is installed at all.
	Enabled bool `json:"enabled" yaml:"enabled"`
	// CredentialHeader is the header the credential is read from. Empty ⇒ "Authorization", whose
	// "Bearer " prefix is stripped.
	CredentialHeader string `json:"credential_header" yaml:"credential_header"`
	// SelectorHeader is the header a caller picks one of its permitted tenants with, which it must
	// do when its credential permits more than one. Empty ⇒ [multitenancy.HeaderScopeOrgID], the
	// header a Grafana datasource sends. It only ever narrows: a tenant the credential does not
	// permit is refused.
	SelectorHeader string `json:"selector_header" yaml:"selector_header"`
	// Tokens is the credential table. A credential absent from it reads nothing.
	Tokens []TenancyToken `json:"tokens" yaml:"tokens"`
}

// TenancyToken grants one credential read access to a set of tenants.
type TenancyToken struct {
	httpmiddleware.Token `json:",inline" yaml:",inline"`
	// Tenants are the tenant ids this credential may read. At least one is required — a token that
	// grants nothing is a config mistake, not a way to spell "no access".
	Tenants []string `json:"tenants" yaml:"tenants"`
	// Username is informational, and the fallback quota key.
	Username string `json:"username" yaml:"username"`
}

// TenancyMiddleware builds the read-path tenancy middleware, returning nil when tenancy is
// disabled.
//
// It must be installed inside any authentication middleware: it authenticates the credential itself
// (an unknown one is refused), so an outer authenticator only narrows who reaches it.
func TenancyMiddleware(cfg Tenancy) (httpmiddleware.Middleware, error) {
	if !cfg.Enabled {
		return nil, nil
	}

	if len(cfg.Tokens) == 0 {
		return nil, errors.New("tenancy.tokens is required when tenancy is enabled")
	}

	decisions := make(map[string]multitenancy.Decision, len(cfg.Tokens))
	for i, t := range cfg.Tokens {
		value, err := t.Token.Get()
		if err != nil {
			return nil, errors.Wrapf(err, "tenancy.tokens[%d]", i)
		}

		if len(t.Tenants) == 0 {
			return nil, errors.Errorf("tenancy.tokens[%d]: at least one tenant is required", i)
		}

		if _, ok := decisions[value]; ok {
			return nil, errors.Errorf("tenancy.tokens[%d]: duplicate token", i)
		}

		decisions[value] = multitenancy.Decision{
			Enabled:   true,
			Username:  t.Username,
			TenantIDs: t.Tenants,
			QuotaKey:  t.Username,
		}
	}

	resolver, err := static.NewResolver(static.Config{
		Read:             decisions,
		CredentialHeader: cfg.CredentialHeader,
	})
	if err != nil {
		return nil, errors.Wrap(err, "tenancy")
	}

	return multitenancy.NewMiddleware(multitenancy.MiddlewareConfig{
		Resolver:       resolver,
		SelectorHeader: cfg.SelectorHeader,
	}), nil
}
