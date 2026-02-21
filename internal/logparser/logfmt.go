package logparser

import (
	"encoding/hex"
	"math/bits"
	"strings"
	"time"

	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"github.com/kr/logfmt"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LogFmtParser parses logfmt lines.
type LogFmtParser struct{}

func init() {
	p := &LogFmtParser{}
	formatRegistry.Store(p.String(), p)
}

// Parse line.
func (LogFmtParser) Parse(data []byte) (*Line, error) {
	line := &Line{}
	attrs := pcommon.NewMap()
	hf := logfmt.HandlerFunc(func(key, val []byte) error {
		ftyp, _ := deduceFieldType(key)
		switch ftyp {
		case messageField:
			line.Body = string(val)
		case levelField:
			if len(val) == 0 {
				attrs.PutStr(string(key), string(val))
				return nil
			}
			line.SeverityText = string(val)
			line.SeverityNumber = DeduceSeverity(line.SeverityText)
		case spanIDField:
			// TODO(tdakkota): move parser into separate function.
			decodedLen := hex.DecodedLen(len(val))
			if decodedLen != 8 {
				attrs.PutStr(string(key), string(val))
				return nil
			}

			var spanID otelstorage.SpanID
			_, err := hex.Decode(spanID[:], val)
			if err != nil {
				attrs.PutStr(string(key), string(val))
				return nil
			}
			line.SpanID = spanID
		case traceIDField:
			val := string(val)
			// TODO(tdakkota): improve acceptance of ParseTraceID.
			traceID, err := otelstorage.ParseTraceID(strings.ToLower(val))
			if err != nil {
				// Trying to parse as UUID.
				id, err := uuid.Parse(val)
				if err != nil {
					attrs.PutStr(string(key), val)
					return nil
				}
				traceID = otelstorage.TraceID(id)
			}
			line.TraceID = traceID
		case timestampField:
			val := string(val)
			for _, layout := range []string{
				time.RFC3339Nano,
				time.RFC3339,
				ISO8601Millis,
			} {
				ts, err := time.Parse(layout, val)
				if err != nil {
					continue
				}
				line.Timestamp = otelstorage.Timestamp(ts.UnixNano())
			}
			if line.Timestamp == 0 {
				attrs.PutStr(string(key), val)
			}
		default:
			k, v := string(key), string(val)
			// Try to deduce a type.
			if v == "" {
				attrs.PutBool(k, true)
				return nil
			}
			dec := jx.DecodeBytes(val)
			switch dec.Next() {
			case jx.Number:
				n, err := dec.Num()
				if err == nil && n.IsInt() {
					i, err := n.Int64()
					if err == nil {
						attrs.PutInt(k, i)
						return nil
					}
				} else if err == nil {
					f, err := n.Float64()
					if err == nil {
						attrs.PutDouble(k, f)
						return nil
					}
				}
			case jx.Bool:
				v, err := dec.Bool()
				if err == nil {
					attrs.PutBool(k, v)
					return nil
				}
			}
			// Fallback.
			attrs.PutStr(k, v)
		}
		return nil
	})
	if err := logfmt.Unmarshal(data, hf); err != nil {
		return nil, err
	}
	if attrs.Len() > 0 {
		line.Attrs = otelstorage.Attrs(attrs)
	}
	return line, nil
}

func (LogFmtParser) String() string {
	return "logfmt"
}

// Detect if line is parsable by this parser.
func (LogFmtParser) Detect(line string) bool {
	if line == "" {
		return false
	}
	if line[0] == '{' {
		return false
	}

	// Collect a bitset of detected field types.
	var detectedFields uint64
	noop := logfmt.HandlerFunc(func(k, _ []byte) error {
		ftyp, ok := deduceFieldType(k)
		if ok {
			detectedFields |= uint64(ftyp)
		}
		return nil
	})
	if err := logfmt.Unmarshal([]byte(line), noop); err != nil {
		return false
	}

	// A bit of bit magic: ensure we met at least two of useful fields.
	detectedFields &= uint64(levelField | timestampField | messageField)
	return bits.OnesCount64(detectedFields) >= 2
}
