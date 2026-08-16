// Package xarena provides a chunked, resettable allocator for the zero-copy ingest converters.
package xarena

// Chunk is the element count a fresh arena chunk is sized to.
const Chunk = 512

// Arena hands out slices carved from chunks it retains across [Arena.Reset], so a steady-state
// converter allocates nothing. Chunks are never reallocated in place, so a slice handed out stays
// valid until the reset that recycles it — unlike appending to one growing slice, which would
// invalidate every slice handed out before a growth.
type Arena[T any] struct {
	chunks [][]T
	cur    int
}

// Alloc returns a zero-length slice with capacity n carved from the current chunk.
func (a *Arena[T]) Alloc(n int) []T {
	if n == 0 {
		return nil
	}

	for a.cur < len(a.chunks) {
		c := a.chunks[a.cur]
		if cap(c)-len(c) >= n {
			out := c[len(c) : len(c) : len(c)+n]
			a.chunks[a.cur] = c[:len(c)+n]
			return out
		}
		a.cur++
	}

	c := make([]T, n, max(n, Chunk))
	a.chunks = append(a.chunks, c)
	a.cur = len(a.chunks) - 1
	return c[:0:n]
}

// Reset makes every chunk available again. Slices handed out before it must not be used after.
func (a *Arena[T]) Reset() {
	for i := range a.chunks {
		a.chunks[i] = a.chunks[i][:0]
	}
	a.cur = 0
}

// Concat returns the concatenation of parts, carved from the arena.
func (a *Arena[T]) Concat(parts ...[]T) []T {
	var n int
	for _, p := range parts {
		n += len(p)
	}

	out := a.Alloc(n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
