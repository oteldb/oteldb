package scarecrow

import (
	"github.com/prometheus/prometheus/model/labels"
)

// SeriesRef indexes a [Schema]'s series. It is scoped to the operator that produced it: a
// column's Ref indexes its producing operator's schema, never a child's.
type SeriesRef uint32

// Schema is an operator's output series set, resolved once at plan time by a bottom-up pass
// over the whole tree, before any [Operator.Next] call.
//
// Resolving eagerly is deliberate. The prototype minted series IDs lazily during execution, so
// an operator's ID space depended on the order data happened to arrive, and it needed repeated
// fixes for cross-operator ID drift. Freezing identity before execution makes that class of
// defect unrepresentable.
type Schema struct {
	// Series are the output label sets, indexed by [SeriesRef].
	Series []labels.Labels
	// Hashes memoizes labels.Hash for each entry, for grouping and vector matching.
	Hashes []uint64
	// Scalar marks a scalar-typed operator: exactly one anonymous series carrying no labels.
	Scalar bool
}

// NewSchema returns a schema over series, memoizing their hashes.
func NewSchema(series []labels.Labels) *Schema {
	s := &Schema{
		Series: series,
		Hashes: make([]uint64, len(series)),
	}
	for i, ls := range series {
		s.Hashes[i] = ls.Hash()
	}

	return s
}

// ScalarSchema returns the schema of a scalar-typed operator.
func ScalarSchema() *Schema {
	return &Schema{
		Series: []labels.Labels{labels.EmptyLabels()},
		Hashes: []uint64{labels.EmptyLabels().Hash()},
		Scalar: true,
	}
}

// Len returns the number of output series.
func (s *Schema) Len() int { return len(s.Series) }

// At returns the label set for ref.
func (s *Schema) At(ref SeriesRef) labels.Labels { return s.Series[ref] }
