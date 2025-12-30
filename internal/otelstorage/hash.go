package otelstorage

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"slices"
	"strings"

	"github.com/zeebo/xxh3"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Hash is an attribute hash.
type Hash [16]byte

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

// MapStackThreshold defines common constant for on-stack [Attrs] sorting.
const MapStackThreshold = 32

// AttrHash computes attributes hash.
func AttrHash(m pcommon.Map) Hash {
	h := xxh3.New()
	hashMap(h, m)
	return h.Sum128().Bytes()
}

// WriteAttrHash adds attributes hash to the [xxh3.Hasher].
func WriteAttrHash(h *xxh3.Hasher, m pcommon.Map) {
	hashMap(h, m)
}

func hashValue(h *xxh3.Hasher, val pcommon.Value) {
	var buf [8]byte

	switch val.Type() {
	case pcommon.ValueTypeEmpty:
	case pcommon.ValueTypeStr:
		_, _ = h.WriteString(val.Str())
	case pcommon.ValueTypeInt:
		bits := val.Int()
		binary.LittleEndian.PutUint64(buf[:], uint64(bits))
		_, _ = h.Write(buf[:])
	case pcommon.ValueTypeDouble:
		bits := math.Float64bits(val.Double())
		binary.LittleEndian.PutUint64(buf[:], bits)
		_, _ = h.Write(buf[:])
	case pcommon.ValueTypeBool:
		if val.Bool() {
			_, _ = h.Write([]byte{1})
		} else {
			_, _ = h.Write([]byte{0})
		}
	case pcommon.ValueTypeBytes:
		// FIXME(tdakkota): AsRaw allocates a new byte slice (opentelemetry moment).
		_, _ = h.Write(val.Bytes().AsRaw())
	case pcommon.ValueTypeSlice:
		slice := val.Slice()

		bits := slice.Len()
		binary.LittleEndian.PutUint64(buf[:], uint64(bits))
		_, _ = h.Write(buf[:])

		for i := 0; i < slice.Len(); i++ {
			hashValue(h, slice.At(i))
		}
	case pcommon.ValueTypeMap:
		hashMap(h, val.Map())
	}
}

func hashMap(h *xxh3.Hasher, m pcommon.Map) {
	if Attrs(m).IsZero() {
		return
	}

	type pair struct {
		key   string
		value pcommon.Value
	}
	var pairs []pair
	if l := m.Len(); l < MapStackThreshold {
		pairs = make([]pair, 0, MapStackThreshold)
	} else {
		pairs = make([]pair, 0, m.Len())
	}
	m.Range(func(k string, v pcommon.Value) bool {
		pairs = append(pairs, pair{
			key:   k,
			value: v,
		})
		return true
	})
	slices.SortFunc(pairs, func(a, b pair) int {
		// [cmp.Compare] has significantly worse performance
		// than [strings.Compare] due to NaN checks.
		//
		// Note that [strings.Compare] is not intrinsified, although
		// SIMD-based implementation would not give any performance
		// gains, since attribute names tend to be relatively small (<64 bytes).
		//
		// See https://go.dev/issue/61725.
		return strings.Compare(a.key, b.key)
	})

	for _, pair := range pairs {
		_, _ = h.WriteString(pair.key)
		hashValue(h, pair.value)
	}
}
