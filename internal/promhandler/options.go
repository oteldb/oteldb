package promhandler

import "time"

// PromAPIOptions describes [PromAPI] options.
type PromAPIOptions struct {
	// LookbackDelta sets default lookback delta. Defaults to 5 * [time.Minute].
	LookbackDelta time.Duration
	// DefaultStep sets default step. Defaults to 5 * [time.Minute].
	DefaultStep time.Duration
}

func (opts *PromAPIOptions) setDefaults() {
	if opts.LookbackDelta == 0 {
		opts.LookbackDelta = 5 * time.Minute
	}
	if opts.DefaultStep == 0 {
		opts.DefaultStep = 5 * time.Minute
	}
}
