// Package profilestorage defines the storage structure and query interface for
// the profiles signal.
//
// The actual storage backend (ClickHouse, object storage, etc.) is implemented
// elsewhere (e.g. oteldb/storage); this package only defines the domain types
// and the [Querier] interface consumed by the ProfileQL engine and the
// Pyroscope-compatible API handler.
package profilestorage

import (
	"context"
	"time"

	"github.com/oteldb/oteldb/internal/profileql"
)

// Querier is a profile storage query interface.
//
// Implementations are responsible for matching series by the profile-type and
// label matchers, resolving symbols, and merging samples into the requested
// representation.
type Querier interface {
	// ProfileTypes returns the profile types available in the given time range.
	ProfileTypes(ctx context.Context, opts ProfileTypesOptions) ([]profileql.ProfileType, error)

	// LabelNames returns all label names matching the given selector.
	LabelNames(ctx context.Context, opts LabelNamesOptions) ([]string, error)

	// LabelValues returns all values for the given label matching the selector.
	LabelValues(ctx context.Context, label string, opts LabelValuesOptions) ([]string, error)

	// SelectMergeProfile merges all matching profiles into a single flamegraph
	// tree, resolved to function names.
	SelectMergeProfile(ctx context.Context, params SelectProfileParams) (*FlameTree, error)
}

// ProfileTypesOptions defines options for [Querier.ProfileTypes].
type ProfileTypesOptions struct {
	// Start defines the time range for the lookup.
	//
	// Querier ignores the parameter, if it is zero.
	Start time.Time
	// End defines the time range for the lookup.
	//
	// Querier ignores the parameter, if it is zero.
	End time.Time
}

// LabelNamesOptions defines options for [Querier.LabelNames].
type LabelNamesOptions struct {
	// Type, when set, restricts the lookup to series of the given profile type.
	Type *profileql.ProfileType
	// Matchers restricts the lookup to series matching the given matchers.
	Matchers []profileql.LabelMatcher
	// Start defines the time range for the lookup.
	//
	// Querier ignores the parameter, if it is zero.
	Start time.Time
	// End defines the time range for the lookup.
	//
	// Querier ignores the parameter, if it is zero.
	End time.Time
}

// LabelValuesOptions defines options for [Querier.LabelValues].
type LabelValuesOptions struct {
	// Type, when set, restricts the lookup to series of the given profile type.
	Type *profileql.ProfileType
	// Matchers restricts the lookup to series matching the given matchers.
	Matchers []profileql.LabelMatcher
	// Start defines the time range for the lookup.
	//
	// Querier ignores the parameter, if it is zero.
	Start time.Time
	// End defines the time range for the lookup.
	//
	// Querier ignores the parameter, if it is zero.
	End time.Time
}

// SelectProfileParams defines parameters for [Querier.SelectMergeProfile].
type SelectProfileParams struct {
	// Type is the profile type to select.
	Type profileql.ProfileType
	// Matchers restricts the selection to series matching the given matchers.
	Matchers []profileql.LabelMatcher
	// Start defines the time range for the selection.
	Start time.Time
	// End defines the time range for the selection.
	End time.Time
	// MaxNodes, when positive, limits the number of nodes in the resulting
	// flamegraph; smaller nodes are folded into an "other" node.
	MaxNodes int
}
