package httpcb

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/multitenancy"
	"github.com/go-faster/oteldb/internal/multitenancyapi"
)

// Resolver implements multitenancy.Resolver using an external HTTP callback.
type Resolver struct {
	client           *multitenancyapi.Client
	credentialHeader string
	cacheTTL         time.Duration
}

// ResolverConfig configures the HTTP callback resolver.
type ResolverConfig struct {
	Client           *multitenancyapi.Client
	CredentialHeader string
	CacheTTL         time.Duration
}

// NewResolver creates a new HTTP callback resolver.
func NewResolver(cfg ResolverConfig) *Resolver {
	return &Resolver{
		client:           cfg.Client,
		credentialHeader: cfg.CredentialHeader,
		cacheTTL:         cfg.CacheTTL,
	}
}

// Resolve returns the authorization Decision for the given request and operation.
func (r *Resolver) Resolve(ctx context.Context, req *http.Request, op multitenancy.Operation) (multitenancy.Decision, error) {
	// Extract credential
	headerName := r.credentialHeader
	if headerName == "" {
		headerName = "Authorization"
	}
	
	cred := req.Header.Get(headerName)
	if cred == "" {
		return multitenancy.Decision{}, errors.New("missing credential")
	}

	// Strip "Bearer " if present
	if strings.HasPrefix(strings.ToLower(cred), "bearer ") {
		cred = cred[7:]
	}

	opStr := multitenancyapi.AuthorizeRequestOperationRead
	if op == multitenancy.OperationWrite {
		opStr = multitenancyapi.AuthorizeRequestOperationWrite
	}

	authReq := &multitenancyapi.AuthorizeRequest{
		Credential: cred,
		Operation:  opStr,
	}

	res, err := r.client.Authorize(ctx, authReq)
	if err != nil {
		return multitenancy.Decision{}, errors.Wrap(err, "authorize call")
	}

	switch v := res.(type) {
	case *multitenancyapi.AuthorizeResponse:
		d := multitenancy.Decision{
			Enabled:   true,
			TenantIDs: v.TenantIds,
		}
		if v.Username.IsSet() {
			d.Username = v.Username.Value
		}
		if v.QuotaKey.IsSet() {
			d.QuotaKey = v.QuotaKey.Value
		}

		if len(v.ResourceSelectors) > 0 {
			d.ResourceSelectors = make([]multitenancy.ResourceSelector, len(v.ResourceSelectors))
			for i, sel := range v.ResourceSelectors {
				var matchOp multitenancy.MatchOp
				switch sel.Op {
				case multitenancyapi.ResourceSelectorOpEq:
					matchOp = multitenancy.OpEq
				case multitenancyapi.ResourceSelectorOpNeq:
					matchOp = multitenancy.OpNotEq
				case multitenancyapi.ResourceSelectorOpRe:
					matchOp = multitenancy.OpRe
				case multitenancyapi.ResourceSelectorOpNre:
					matchOp = multitenancy.OpNotRe
				default:
					return d, errors.Errorf("unknown resource selector op %q", sel.Op)
				}
				d.ResourceSelectors[i] = multitenancy.ResourceSelector{
					Key:   sel.Key,
					Op:    matchOp,
					Value: sel.Value,
				}
			}
		}

		if restr, ok := v.Restrictions.Get(); ok {
			if maxMem, ok := restr.MaxMemoryUsageBytes.Get(); ok {
				d.Restrictions.MaxMemoryUsageBytes = maxMem
			}
			if maxTime, ok := restr.MaxExecutionTimeMs.Get(); ok {
				d.Restrictions.MaxExecutionTime = time.Duration(maxTime) * time.Millisecond
			}
			if maxRows, ok := restr.MaxResultRows.Get(); ok {
				d.Restrictions.MaxResultRows = maxRows
			}
		}

		return d, nil
	case *multitenancyapi.AuthorizeUnauthorized:
		return multitenancy.Decision{}, errors.New("authorize failed: HTTP 401 Unauthorized")
	default:
		return multitenancy.Decision{}, errors.Errorf("unexpected authorize response type %T", v)
	}
}
