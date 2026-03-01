package tempohandler

import "time"

// TempoAPIOptions describes [TempoAPI] options.
type TempoAPIOptions struct {
	// DefaultSince sets default value of since parameter. Defaults to one [time.Hour].
	DefaultSince time.Duration
	// EnableAutocompleteQuery whether if handler should parse
	// the `q` parameter in tag requests
	//
	// See https://grafana.com/docs/tempo/latest/api_docs/#filtered-tag-values.
	EnableAutocompleteQuery bool
}

func (opts *TempoAPIOptions) setDefaults() {
	if opts.DefaultSince == 0 {
		opts.DefaultSince = time.Hour
	}
}
