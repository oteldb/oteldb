package storagebackend

import (
	"github.com/go-faster/errors"

	"github.com/oteldb/oteldb/internal/xbytes"
)

// errNegativeBytes reports a byte setting that arrived negative. A size is parsed as uint64 and
// kept as int64, so any value at or above 8EiB wraps with no decode error, and every setting that
// takes one reads a negative as "off" — unlimited, disabled, or the library default. The size
// parser rejects a leading "-" in YAML and JSON alike, so a negative is always that overflow and
// never something an operator wrote.
var errNegativeBytes = errors.New("must not be negative; sizes at or above 8EiB overflow")

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
