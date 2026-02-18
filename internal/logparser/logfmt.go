package logparser

import (
	"encoding/hex"
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
		k := string(key)
		v := string(val)
		switch k {
		case "msg":
			line.Body = v
		case "level", "lvl", "levelStr", "severity_text", "severity", "levelname":
			if v == "" {
				attrs.PutStr(k, v)
				return nil
			}
			line.SeverityText = v
			line.SeverityNumber = DeduceSeverity(v)
		case "span_id", "spanid", "spanID", "spanId":
			raw, _ := hex.DecodeString(v)
			if len(raw) != 8 {
				attrs.PutStr(k, v)
				return nil
			}
			var spanID otelstorage.SpanID
			copy(spanID[:], raw)
			line.SpanID = spanID
		case "trace_id", "traceid", "traceID", "traceId":
			traceID, err := otelstorage.ParseTraceID(strings.ToLower(v))
			if err != nil {
				// Trying to parse as UUID.
				id, err := uuid.Parse(v)
				if err != nil {
					attrs.PutStr(k, v)
					return nil
				}
				traceID = otelstorage.TraceID(id)
			}
			line.TraceID = traceID
		case "t", "ts", "time", "@timestamp", "timestamp":
			for _, layout := range []string{
				time.RFC3339Nano,
				time.RFC3339,
				ISO8601Millis,
			} {
				ts, err := time.Parse(layout, v)
				if err != nil {
					continue
				}
				line.Timestamp = otelstorage.Timestamp(ts.UnixNano())
			}
			if line.Timestamp == 0 {
				attrs.PutStr(k, v)
			}
		default:
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
	noop := logfmt.HandlerFunc(func(_, _ []byte) error {
		return nil
	})
	return logfmt.Unmarshal([]byte(line), noop) == nil
}
