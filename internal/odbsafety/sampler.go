package odbsafety

import (
	"math/rand/v2"
	"sync/atomic"
)

// NewSampler builds a sampler func for ModeSample from cfg.
//
// When SampleFirst or SampleThereafter is set, it logs the first N
// occurrences unconditionally, then 1-in-M after that. Otherwise it falls
// back to the deprecated SampleRate, sampling probabilistically — this
// keeps existing configs that only set sample_rate working.
func NewSampler(cfg Config) func() bool {
	if cfg.SampleFirst <= 0 && cfg.SampleThereafter <= 0 {
		rate := cfg.SampleRate
		return func() bool { return rand.Float64() < rate } //#nosec G404
	}

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
