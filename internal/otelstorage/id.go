package otelstorage

import (
	"encoding/binary"
	"errors"
	"strings"
	"unicode"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// TraceID is OpenTelemetry trace ID.
type TraceID [16]byte

// ParseTraceID parses trace ID from given string.
//
// Deals with missing leading zeroes.
func ParseTraceID(input string) (_ TraceID, err error) {
	var id uuid.UUID

	switch {
	case input == "":
		return TraceID{}, errors.New("empty traceID")
	case len(input) >= 32:
		id, err = uuid.Parse(input)
		return TraceID(id), err
	default:
		// UUID without leading zeroes.
		var hex [32]byte
		// FIXME(tdakkota): probably, it's faster to define some variable with zeroes
		// 	and just copy it.
		// #nosec G602 -- some bullshit
		for i := range hex {
			hex[i] = '0'
		}
		// Copy input to the end of hex array.
		//
		//  00000000000000000000000000000000 <- hex, len = 32
		//    b78e08df6f20dc3ad29d3915beab75 <- input, len = 30
		//  00b78e08df6f20dc3ad29d3915beab75 <- copy(hex[32-30:], input)
		//
		copy(hex[len(hex)-len(input):], input)
		for i, r := range hex {
			if r <= unicode.MaxASCII && ('A' <= r && r <= 'Z') {
				hex[i] += 'a' - 'A' // #nosec G602 -- some bullshit
			}
		}
		id, err = uuid.ParseBytes(hex[:])
		return TraceID(id), err
	}
}

// IsEmpty returns true if span ID is empty.
func (id TraceID) IsEmpty() bool {
	return pcommon.TraceID(id).IsEmpty()
}

// Hex returns a hex representation of TraceID.
func (id TraceID) Hex() string {
	const hextable = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(len(id) * 2)
	for _, c := range id {
		sb.WriteByte(hextable[c>>4])
		sb.WriteByte(hextable[c&0x0f])
	}
	return sb.String()
}

// SpanID is OpenTelemetry span ID.
type SpanID [8]byte

// SpanIDFromUint64 creates new SpanID from uint64.
func SpanIDFromUint64(v uint64) (r SpanID) {
	binary.LittleEndian.PutUint64(r[:], v)
	return r
}

// IsEmpty returns true if span ID is empty.
func (id SpanID) IsEmpty() bool {
	return pcommon.SpanID(id).IsEmpty()
}

// AsUint64 returns span ID as LittleEndian uint64.
func (id SpanID) AsUint64() uint64 {
	return binary.LittleEndian.Uint64(id[:])
}

// Hex returns a hex representation of SpanID.
func (id SpanID) Hex() string {
	const hextable = "0123456789abcdef"
	var sb strings.Builder
	sb.Grow(len(id) * 2)
	for _, c := range id {
		sb.WriteByte(hextable[c>>4])
		sb.WriteByte(hextable[c&0x0f])
	}
	return sb.String()
}
