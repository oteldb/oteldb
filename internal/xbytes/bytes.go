package xbytes

import (
	"github.com/dustin/go-humanize"
)

// Bytes is a [humanize.Bytes] value.
type Bytes int64

// UnmarshalText implements [encoding.TextUnmarshaler].
func (b *Bytes) UnmarshalText(text []byte) error {
	v, err := humanize.ParseBytes(string(text))
	if err != nil {
		return err
	}
	*b = Bytes(v)
	return nil
}

// MarshalText implements [encoding.TextMarshaler].
func (b Bytes) MarshalText() ([]byte, error) {
	return []byte(humanize.Bytes(uint64(b))), nil
}
