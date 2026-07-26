package scarecrow

import "math/bits"

// Column is one series' values across every step of the current chunk.
//
// The step timestamps are shared by every operator in a chunk and live on the [EvalContext],
// not here, so a column is exactly one contiguous float64 run plus its validity bitset. This is
// the only value that crosses an [Operator] boundary.
//
// A column returned by [Operator.Next] is owned by that operator and is valid only until the
// next Next or Close call on it. Callers must not retain it.
type Column struct {
	// Ref indexes the producing operator's [Schema].
	Ref SeriesRef
	// V holds one value per step; len(V) == EvalContext.Steps.
	V []float64
	// Valid marks which steps carry a sample. A zero bit means absent, which PromQL
	// distinguishes from a NaN value, so absence needs its own channel. Keeping it as a bitset
	// rather than a branch per element is what lets kernels stay branch-free.
	Valid []uint64
}

// NewColumn returns a column sized for steps, with every step marked absent.
func NewColumn(ref SeriesRef, steps int) *Column {
	return &Column{
		Ref:   ref,
		V:     make([]float64, steps),
		Valid: make([]uint64, wordsFor(steps)),
	}
}

// Steps returns the number of steps this column spans.
func (c *Column) Steps() int { return len(c.V) }

// Reset clears every value and marks all steps absent, keeping the backing arrays. Operators
// reuse one column for a whole chunk, so this is the per-series entry point.
func (c *Column) Reset(ref SeriesRef) {
	c.Ref = ref
	clear(c.V)
	clear(c.Valid)
}

// Resize grows the column to hold steps, reusing capacity where possible, and resets it.
func (c *Column) Resize(ref SeriesRef, steps int) {
	if cap(c.V) < steps {
		c.V = make([]float64, steps)
	}
	c.V = c.V[:steps]

	if w := wordsFor(steps); cap(c.Valid) < w {
		c.Valid = make([]uint64, w)
	} else {
		c.Valid = c.Valid[:w]
	}

	c.Reset(ref)
}

// Set stores v at step i and marks it present.
func (c *Column) Set(i int, v float64) {
	c.V[i] = v
	c.Valid[i>>6] |= 1 << uint(i&63)
}

// IsSet reports whether step i carries a sample.
func (c *Column) IsSet(i int) bool {
	return c.Valid[i>>6]&(1<<uint(i&63)) != 0
}

// Clear marks step i absent. The value at i is left as-is; readers must consult the bitset.
func (c *Column) Clear(i int) {
	c.Valid[i>>6] &^= 1 << uint(i&63)
}

// Count returns how many steps carry a sample.
func (c *Column) Count() int {
	n := 0
	for _, w := range c.Valid {
		n += bits.OnesCount64(w)
	}

	return n
}

// Empty reports whether no step carries a sample. PromQL drops such series from results.
func (c *Column) Empty() bool {
	for _, w := range c.Valid {
		if w != 0 {
			return false
		}
	}

	return true
}

// CopyFrom overwrites c with the contents of src, growing c as needed. It is used where the
// borrow rule forbids passing a producer's column through — notably by the concurrency
// wrapper, which must not hand out a column its child will overwrite.
func (c *Column) CopyFrom(src *Column) {
	c.Resize(src.Ref, src.Steps())
	copy(c.V, src.V)
	copy(c.Valid, src.Valid)
}

// wordsFor returns the bitset word count covering n bits.
func wordsFor(n int) int { return (n + 63) / 64 }
