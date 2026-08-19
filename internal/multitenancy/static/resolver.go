// Package static resolves tenancy from a token→[multitenancy.Decision] table held in the config,
// for a deployment with no external authorization service.
package static

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/multitenancy"
)

// Config configures a [Resolver].
type Config struct {
	// Read maps a credential to the decision it gets for a read. A credential absent here is
	// refused, so an empty table grants nothing.
	Read map[string]multitenancy.Decision
	// Write is [Config.Read] for the ingest side, resolved independently so one credential can read
	// and write different tenant sets.
	Write map[string]multitenancy.Decision
	// CredentialHeader is the header the credential is read from. Empty ⇒ "Authorization", whose
	// "Bearer " prefix is stripped.
	CredentialHeader string
}

// Resolver is a [multitenancy.Resolver] over a fixed credential table.
type Resolver struct {
	read   []entry
	write  []entry
	header string
}

type entry struct {
	credential string
	decision   multitenancy.Decision
}

// NewResolver builds a [Resolver] from cfg, validating every tenant id it grants so a typo is a
// startup error rather than a query that reaches the storage layer with an unusable shard key.
func NewResolver(cfg Config) (*Resolver, error) {
	read, err := entries(cfg.Read)
	if err != nil {
		return nil, errors.Wrap(err, "read")
	}

	write, err := entries(cfg.Write)
	if err != nil {
		return nil, errors.Wrap(err, "write")
	}

	header := cfg.CredentialHeader
	if header == "" {
		header = "Authorization"
	}

	return &Resolver{read: read, write: write, header: header}, nil
}

func entries(m map[string]multitenancy.Decision) ([]entry, error) {
	out := make([]entry, 0, len(m))
	for cred, d := range m {
		if cred == "" {
			return nil, errors.New("empty credential")
		}

		for i, t := range d.TenantIDs {
			id, err := multitenancy.ParseTenantID(t)
			if err != nil {
				return nil, errors.Wrap(err, "tenant")
			}

			d.TenantIDs[i] = id
		}

		out = append(out, entry{credential: cred, decision: d})
	}

	return out, nil
}

// Resolve implements [multitenancy.Resolver].
func (r *Resolver) Resolve(
	_ context.Context, req *http.Request, op multitenancy.Operation,
) (multitenancy.Decision, error) {
	cred := req.Header.Get(r.header)
	if cred == "" {
		return multitenancy.Decision{}, errors.New("missing credential")
	}

	if r.header == "Authorization" {
		if v, ok := strings.CutPrefix(cred, "Bearer "); ok {
			cred = strings.TrimSpace(v)
		}
	}

	var table []entry
	switch op {
	case multitenancy.OperationRead:
		table = r.read
	case multitenancy.OperationWrite:
		table = r.write
	default:
		return multitenancy.Decision{}, errors.Errorf("unknown operation %d", op)
	}

	// Scanned rather than looked up so a wrong credential costs the same time as a right one,
	// matching how [httpmiddleware.BearerToken] compares its tokens.
	found := false
	var d multitenancy.Decision
	for _, e := range table {
		if subtle.ConstantTimeCompare([]byte(cred), []byte(e.credential)) == 1 {
			d, found = e.decision, true
		}
	}

	if !found {
		return multitenancy.Decision{}, errors.New("unauthorized")
	}

	return d, nil
}
