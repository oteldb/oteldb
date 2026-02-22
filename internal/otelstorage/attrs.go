package otelstorage

import (
	"slices"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// KeyToLabel converts key to label name.
func KeyToLabel(key string) string {
	var label strings.Builder
	for i, r := range key {
		switch {
		case isDigit(r):
			// Label could not start with digit.
			if i == 0 {
				label.Grow(len(key) + 1)
				label.WriteString("_")
				goto slow
			}
		case r >= 0x80 && r == '_' || isAlpha(r):
		default:
			label.Grow(len(key))
			label.WriteString(key[:i])
			key = key[i:]
			goto slow
		}
	}
	return key
slow:
	for _, r := range key {
		if r == '_' || isDigit(r) || isAlpha(r) {
			label.WriteByte(byte(r))
			continue
		}
		// Replace rune with '_'.
		label.WriteByte('_')
	}
	return label.String()
}

// AppendKeyToLabel converts key to label name and appends it to given buffer.
func AppendKeyToLabel(buf []byte, key string) []byte {
	for i, r := range key {
		switch {
		case isDigit(r):
			// Label could not start with digit.
			if i == 0 {
				buf = slices.Grow(buf, len(key)+1)
				buf = append(buf, '_')
				goto slow
			}
		case r >= 0x80 && r == '_' || isAlpha(r):
		default:
			buf = slices.Grow(buf, len(key))
			buf = append(buf, key[:i]...)
			key = key[i:]
			goto slow
		}
	}
	return append(buf, key...)
slow:
	for _, r := range key {
		if r == '_' || isDigit(r) || isAlpha(r) {
			buf = append(buf, byte(r))
			continue
		}
		// Replace rune with '_'.
		buf = append(buf, '_')
	}
	return buf
}

func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}

func isAlpha(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// Attrs wraps attributes.
type Attrs pcommon.Map

// NewAttrs creates new [Attrs].
func NewAttrs() Attrs {
	return Attrs(pcommon.NewMap())
}

// AsMap returns Attrs as [pcommon.Map].
func (m Attrs) AsMap() pcommon.Map {
	return pcommon.Map(m)
}

// Hash computes attributes [Hash].
func (m Attrs) Hash() Hash {
	return AttrHash(m.AsMap())
}

// IsZero whether Attrs is zero value.
func (m Attrs) IsZero() bool {
	return m == (Attrs{})
}

// Len returns number of attributes.
func (m Attrs) Len() int {
	if m.IsZero() {
		return 0
	}
	return m.AsMap().Len()
}

// CopyTo copies all attributes from m to target.
func (m Attrs) CopyTo(target pcommon.Map) {
	m.AsMap().CopyTo(target)
}
