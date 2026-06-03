package metricscache

// Key is a cache lookup key for one series at a given step and function.
type Key struct {
	Hash [16]byte
	Step int64  // Milliseconds. 0 means raw points.
	Fn   string // Aggregation function name; empty means raw/anyLast.
}
