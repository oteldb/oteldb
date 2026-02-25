package logparser

import (
	"math/bits"

	"github.com/go-faster/jx"
	"github.com/kr/logfmt"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// LogFmtParser parses logfmt lines.
type LogFmtParser struct{}

var _ Parser = (*LogFmtParser)(nil)

func init() {
	p := &LogFmtParser{}
	formatRegistry.Store(p.String(), p)
}

// Parse line.
func (LogFmtParser) Parse(data string, target *Record) error {
	return logfmt.Unmarshal([]byte(data), target)
}

func (r *Record) HandleLogfmt(key, val []byte) error {
	if r.Attrs.IsZero() {
		r.Attrs = otelstorage.NewAttrs()
	}
	attrs := r.Attrs.AsMap()

	ftyp, _ := deduceFieldType(key)
	switch ftyp {
	case messageField:
		r.Body = string(val)
	case levelField:
		if len(val) == 0 {
			attrs.PutStr(string(key), string(val))
			return nil
		}
		r.SeverityText = string(val)
		r.SeverityNumber = DeduceSeverity(r.SeverityText)
	case spanIDField:
		spanID, ok := ParseSpanID(val)
		if !ok {
			attrs.PutStr(string(key), string(val))
			return nil
		}
		r.SpanID = spanID
	case traceIDField:
		val := string(val)
		traceID, ok := ParseTraceID(val)
		if !ok {
			attrs.PutStr(string(key), val)
			return nil
		}
		r.TraceID = traceID
	case timestampField:
		ts, ok := ParseTimestamp(val)
		if ok {
			r.Timestamp = ts
			return nil
		}
		attrs.PutStr(string(key), string(val))
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
