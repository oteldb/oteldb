package chstorage

import "github.com/oteldb/oteldb/internal/otelstorage"

// attrInterner keeps one [otelstorage.Attrs] per distinct attribute set, so a set decoded once per
// row can be replaced by a shared instance and the duplicates collected.
//
// Every row of metrics_timeseries decodes its own resource and scope map, but those sets are shared
// by construction: all series of one pod carry the same resource attributes. Retaining a map per
// series therefore holds hundreds of thousands of copies of a few hundred distinct sets.
type attrInterner struct {
	cache map[otelstorage.Hash]otelstorage.Attrs
}

func newAttrInterner() *attrInterner {
	return &attrInterner{cache: map[otelstorage.Hash]otelstorage.Attrs{}}
}

// intern returns a shared instance of an attribute set equal to a.
//
// Use it only for sets shared across many rows (resource, scope). Interning a set that is ~1:1 with
// rows costs a hash per row and saves nothing.
func (i *attrInterner) intern(a otelstorage.Attrs) otelstorage.Attrs {
	if a.Len() == 0 {
		return a
	}

	key := a.Hash()
	if got, ok := i.cache[key]; ok {
		return got
	}
	i.cache[key] = a
	return a
}
