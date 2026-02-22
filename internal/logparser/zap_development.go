package logparser

import (
	"encoding/hex"
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap/zapcore"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// ZapDevelopmentParser parses zap's development mode lines.
type ZapDevelopmentParser struct{}

var _ Parser = (*ZapDevelopmentParser)(nil)

func init() {
	p := &ZapDevelopmentParser{}
	formatRegistry.Store(p.String(), p)
}

// Parse line.
func (ZapDevelopmentParser) Parse(data string) (*Line, error) {
	var (
		line  = &Line{}
		attrs = pcommon.NewMap()

		consoleSep = "\t"
	)

	// Cut timestamp.
	rawTimestamp, data, ok := strings.Cut(data, consoleSep)
	if !ok {
		return nil, errors.New("expected a timestamp")
	}
	ts, err := time.Parse(ISO8601Millis, string(rawTimestamp))
	if err != nil {
		return nil, errors.Wrap(err, "parse timestamp")
	}
	line.Timestamp = otelstorage.NewTimestampFromTime(ts)

	// Cut level.
	rawLevel, data, ok := strings.Cut(data, consoleSep)
	if !ok {
		return nil, errors.New("expected level")
	}
	var zapLevel zapcore.Level
	if err := zapLevel.Set(rawLevel); err != nil {
		return nil, errors.Wrap(err, "parse level")
	}
	line.SeverityText = string(rawLevel)
	switch zapLevel {
	case zapcore.DebugLevel:
		line.SeverityNumber = plog.SeverityNumberDebug
	case zapcore.InfoLevel:
		line.SeverityNumber = plog.SeverityNumberInfo
	case zapcore.WarnLevel:
		line.SeverityNumber = plog.SeverityNumberWarn
	case zapcore.ErrorLevel:
		line.SeverityNumber = plog.SeverityNumberError
	case zapcore.DPanicLevel:
		line.SeverityNumber = plog.SeverityNumberFatal
	case zapcore.PanicLevel:
		line.SeverityNumber = plog.SeverityNumberFatal
	case zapcore.FatalLevel:
		line.SeverityNumber = plog.SeverityNumberFatal
	default:
		return nil, errors.Errorf("unexpected level %v", zapLevel)
	}

	// Next might be a logger name or filename.
	name, data, ok := strings.Cut(data, consoleSep)
	if !ok {
		return nil, errors.New("expected filename or logger name")
	}
	if strings.Contains(name, ".go:") {
		attrs.PutStr("filename", string(name))
	} else {
		// That's a logger name.
		attrs.PutStr("logger", string(name))

		// Cut filename now.
		var filename string
		filename, data, ok = strings.Cut(data, consoleSep)
		if !ok {
			return nil, errors.New("expected filename")
		}
		attrs.PutStr("filename", string(filename))
	}

	// Cut message.
	msg, data, ok := strings.Cut(data, consoleSep)
	line.Body = string(msg)
	if ok {
		if err := jx.DecodeStr(data).ObjBytes(func(d *jx.Decoder, k []byte) error {
			switch string(k) {
			case "trace_id", "traceid", "traceID", "traceId":
				if d.Next() != jx.String {
					return addJSONMapKey(attrs, string(k), d)
				}
				v, err := d.Str()
				if err != nil {
					return err
				}
				traceID, err := otelstorage.ParseTraceID(strings.ToLower(v))
				if err != nil {
					// Trying to parse as UUID.
					id, err := uuid.Parse(v)
					if err != nil {
						attrs.PutStr(string(k), v)
						return nil
					}
					traceID = otelstorage.TraceID(id)
				}
				line.TraceID = traceID
			case "span_id", "spanid", "spanID", "spanId":
				if d.Next() != jx.String {
					// TODO: handle integers
					return addJSONMapKey(attrs, string(k), d)
				}
				v, err := d.Str()
				if err != nil {
					return err
				}
				raw, _ := hex.DecodeString(v)
				if len(raw) != 8 {
					attrs.PutStr(string(k), v)
					return nil
				}
				var spanID otelstorage.SpanID
				copy(spanID[:], raw)
				line.SpanID = spanID
			default:
				return addJSONMapKey(attrs, string(k), d)
			}
			return nil
		}); err != nil {
			return nil, errors.Wrap(err, "read object")
		}
	}

	line.Attrs = otelstorage.Attrs(attrs)
	return line, nil
}

func (ZapDevelopmentParser) String() string {
	return "zap-development"
}

// Detect if line is parsable by this parser.
func (ZapDevelopmentParser) Detect(line string) bool {
	const consoleSep = "\t"

	// Cut timestamp.
	ts, line, ok := strings.Cut(line, consoleSep)
	if !ok {
		return false
	}
	if _, err := time.Parse(ISO8601Millis, ts); err != nil {
		return false
	}

	// Cut level.
	lvl, line, ok := strings.Cut(line, consoleSep)
	if !ok {
		return false
	}
	if err := new(zapcore.Level).UnmarshalText([]byte(lvl)); err != nil {
		return false
	}

	// Next might be a logger name or filename.
	name, line, ok := strings.Cut(line, consoleSep)
	if !ok {
		return false
	}
	if !strings.Contains(name, ".go:") {
		// That's a logger name. Cut filename now.
		_, line, ok = strings.Cut(line, consoleSep)
		if !ok {
			return false
		}
	}

	// Cut message.
	_, line, ok = strings.Cut(line, consoleSep)
	if ok {
		return jx.DecodeStr(line).Next() == jx.Object
	}
	return true
}
