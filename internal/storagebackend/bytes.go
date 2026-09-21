package storagebackend

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// errNegativeBytes reports a byte setting that arrived negative. Every setting that takes one reads
// a negative as "off" — unlimited, disabled, or the library default — so it inverts the bound that
// was asked for rather than tightening it.
//
// Config can no longer produce one: [xbytes.Bytes] rejects a size above [xbytes.MaxBytes] instead
// of wrapping, and its parser rejects a leading "-". This guards the remaining source, a config
// built in Go rather than decoded (see internal/storagebackup), since past the checks below a
// negative is indistinguishable from the sentinel [resolveCacheSettings] writes on purpose.
var errNegativeBytes = errors.New("must not be negative")

// namedBytes pairs a byte setting with the config key an operator would fix.
type namedBytes struct {
	name  string
	value xbytes.Bytes
}

// optionalBytes resolves a pointer-valued setting for [checkBytes]. Unset reads as zero, which is
// always valid: it means "unset", never a wrapped size.
func optionalBytes(name string, value *xbytes.Bytes) namedBytes {
	if value == nil {
		return namedBytes{name: name}
	}
	return namedBytes{name: name, value: *value}
}

// checkBytes reports the first setting that overflowed, in the order given so the error is stable.
func checkBytes(fields ...namedBytes) error {
	for _, f := range fields {
		if f.value < 0 {
			return errors.Wrap(errNegativeBytes, f.name)
		}
	}
	return nil
}
