package static

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/multitenancy"
)

// Resolver is a simple static resolver that maps tokens to pre-configured Decision objects.
type Resolver struct {
	readDecisions  map[string]multitenancy.Decision
	writeDecisions map[string]multitenancy.Decision
}

// Config defines the static resolver configuration.
type Config struct {
	ReadDecisions  map[string]multitenancy.Decision
	WriteDecisions map[string]multitenancy.Decision
}

// NewResolver creates a new static resolver with the given token-to-decision mapping.
func NewResolver(cfg Config) *Resolver {
	return &Resolver{
		readDecisions:  cfg.ReadDecisions,
		writeDecisions: cfg.WriteDecisions,
	}
}

// Resolve implements the multitenancy.Resolver interface.
func (r *Resolver) Resolve(ctx context.Context, req *http.Request, op multitenancy.Operation) (multitenancy.Decision, error) {
	// Extract credential
	// For simplicity, we assume Authorization header for now.
	cred := req.Header.Get("Authorization")
	if cred == "" {
		return multitenancy.Decision{}, errors.New("missing credential")
	}

	// Strip "Bearer " if present
	if strings.HasPrefix(strings.ToLower(cred), "bearer ") {
		cred = cred[7:]
	}

	var decisions map[string]multitenancy.Decision
	switch op {
	case multitenancy.OperationRead:
		decisions = r.readDecisions
	case multitenancy.OperationWrite:
		decisions = r.writeDecisions
	default:
		return multitenancy.Decision{}, errors.Errorf("unknown operation %v", op)
	}

	if decision, ok := decisions[cred]; ok {
		return decision, nil
	}
	// Return empty decision if token not found
	return multitenancy.Decision{}, errors.New("unauthorized")
}
