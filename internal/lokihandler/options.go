package lokihandler

import "time"

// LokiAPIOptions describes [LokiAPI] options.
type LokiAPIOptions struct {
	// DefaultSince sets default value of since parameter. Defaults to one [time.Hour].
	DefaultSince time.Duration
}

func (opts *LokiAPIOptions) setDefaults() {
	if opts.DefaultSince == 0 {
		opts.DefaultSince = time.Hour
	}
}
