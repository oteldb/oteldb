package profilehandler

import "time"

// PyroscopeAPIOptions describes [PyroscopeAPI] options.
type PyroscopeAPIOptions struct {
	// DefaultSince sets the default lookback for the time range when only the
	// end (or nothing) is provided. Defaults to one [time.Hour].
	DefaultSince time.Duration
}

func (opts *PyroscopeAPIOptions) setDefaults() {
	if opts.DefaultSince == 0 {
		opts.DefaultSince = time.Hour
	}
}
