// Package xbytes holds the byte-size config scalar shared by oteldb's config blocks.
package xbytes

import (
	"math"

	"github.com/dustin/go-humanize"
	"github.com/go-faster/errors"
)

// Bytes is a [humanize.Bytes] value.
type Bytes int64

// MaxBytes is the largest size [Bytes] can hold. Sizes are parsed as uint64 but kept as int64, so
// anything above it would wrap negative, and consumers read a negative as "off" — unlimited,
// disabled, or the library default — the opposite of the bound an operator asked for.
const MaxBytes uint64 = math.MaxInt64

// UnmarshalText implements [encoding.TextUnmarshaler].
func (b *Bytes) UnmarshalText(text []byte) error {
	v, err := humanize.ParseBytes(string(text))
	if err != nil {
		return err
	}
	if v > MaxBytes {
		return errors.Errorf("size %q is %d bytes, above the maximum of %d", text, v, MaxBytes)
	}
	*b = Bytes(v)
	return nil
}

// MarshalText implements [encoding.TextMarshaler].
func (b Bytes) MarshalText() ([]byte, error) {
	// humanize.Bytes takes a uint64, so a negative would render as a huge size. UnmarshalText
	// cannot produce one, so a negative here is a programming error, not a config the operator
	// wrote, and rendering it as ~18EB would hide that.
	if b < 0 {
		return nil, errors.Errorf("negative size %d is not a byte size", int64(b))
	}
	return []byte(humanize.Bytes(uint64(b))), nil
}
