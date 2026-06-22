package odbsafety

import (
	"sync/atomic"
)

// NewSampler builds a sampler func for ModeSample from cfg. It logs the
// first SampleFirst occurrences unconditionally, then 1-in-SampleThereafter
// after that.
func NewSampler(cfg Config) func() bool {
	first, thereafter := cfg.SampleFirst, cfg.SampleThereafter
	var count atomic.Uint64
	return func() bool {
		c := count.Add(1)
		if first > 0 && c <= uint64(first) {
			return true
		}
		if thereafter <= 0 {
			return false
		}
		return c%uint64(thereafter) == 0
	}
}
