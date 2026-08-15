package promrw

// arenaChunk is the element count a fresh arena chunk is sized to.
const arenaChunk = 512

// arena hands out slices carved from chunks it retains across [arena.reset], so a steady-state
// converter allocates nothing. Chunks are never reallocated in place, so a slice handed out stays
// valid until the reset that recycles it — unlike appending to one growing slice, which would
// invalidate every slice handed out before a growth.
type arena[T any] struct {
	chunks [][]T
	cur    int
}

// alloc returns a zero-length slice with capacity n carved from the current chunk.
func (a *arena[T]) alloc(n int) []T {
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

	c := make([]T, n, max(n, arenaChunk))
	a.chunks = append(a.chunks, c)
	a.cur = len(a.chunks) - 1
	return c[:0:n]
}

// reset makes every chunk available again. Slices handed out before it must not be used after.
func (a *arena[T]) reset() {
	for i := range a.chunks {
		a.chunks[i] = a.chunks[i][:0]
	}
	a.cur = 0
}

// concat returns the concatenation of parts, carved from the arena.
func (a *arena[T]) concat(parts ...[]T) []T {
	var n int
	for _, p := range parts {
		n += len(p)
	}

	out := a.alloc(n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
