package chstorage

import (
	"context"

	"github.com/go-faster/oteldb/internal/multitenancy"
)

// writeValidator checks whether a resolved tenant_id is authorized for writing.
type writeValidator interface {
	// IsAuthorized returns true if the given tenant_id is allowed by the write Decision.
	IsAuthorized(ctx context.Context, tenantID string) bool
}

// decisionWriteValidator validates tenant_id against the write Decision from context.
type decisionWriteValidator struct{}

func (v *decisionWriteValidator) IsAuthorized(ctx context.Context, tenantID string) bool {
	d, ok := multitenancy.DecisionFromContext(ctx)
	if !ok || !d.Enabled {
		// No decision or multi-tenancy disabled → allow all
		return true
	}

	// Check if tenant_id is in the allowed list
	for _, allowed := range d.TenantIDs {
		if allowed == tenantID {
			return true
		}
	}
	return false
}

// NewDecisionWriteValidator creates a writeValidator that uses the Decision from context.
func NewDecisionWriteValidator() writeValidator {
	return &decisionWriteValidator{}
}
