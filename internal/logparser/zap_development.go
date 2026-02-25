package logparser

import (
	"strings"
	"time"

	"github.com/go-faster/errors"
	"github.com/go-faster/jx"
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
func (ZapDevelopmentParser) Parse(data string, target *Record) error {
	if target.Attrs.IsZero() {
		target.Attrs = otelstorage.NewAttrs()
	}
	var (
		attrs      = target.Attrs.AsMap()
		consoleSep = "\t"
	)

	// Cut timestamp.
	rawTimestamp, data, ok := strings.Cut(data, consoleSep)
	if !ok {
		return errors.New("expected a timestamp")
	}
	ts, err := time.Parse(ISO8601Millis, rawTimestamp)
	if err != nil {
		return errors.Wrap(err, "parse timestamp")
	}

	// Cut level.
	rawLevel, data, ok := strings.Cut(data, consoleSep)
	if !ok {
		return errors.New("expected level")
	}
	var zapLevel zapcore.Level
	if err := zapLevel.Set(rawLevel); err != nil {
		return errors.Wrap(err, "parse level")
	}

	target.Timestamp = otelstorage.NewTimestampFromTime(ts)
	target.SeverityText = rawLevel
	switch zapLevel {
	case zapcore.DebugLevel:
		target.SeverityNumber = plog.SeverityNumberDebug
	case zapcore.InfoLevel:
		target.SeverityNumber = plog.SeverityNumberInfo
	case zapcore.WarnLevel:
		target.SeverityNumber = plog.SeverityNumberWarn
	case zapcore.ErrorLevel:
		target.SeverityNumber = plog.SeverityNumberError
	case zapcore.DPanicLevel:
		target.SeverityNumber = plog.SeverityNumberFatal
	case zapcore.PanicLevel:
		target.SeverityNumber = plog.SeverityNumberFatal
	case zapcore.FatalLevel:
		target.SeverityNumber = plog.SeverityNumberFatal
	default:
		return errors.Errorf("unexpected level %v", zapLevel)
	}

	// Next might be a logger name or filename.
	name, data, ok := strings.Cut(data, consoleSep)
	if !ok {
		return errors.New("expected filename or logger name")
	}
	if strings.Contains(name, ".go:") {
		// Gracefully handle invalid source.
		src, err := ParseSource(name)
		if err == nil {
			src.Add(attrs)
		} else {
			attrs.PutStr("code.file.path", name)
		}
	} else {
		// That's a logger name.
		attrs.PutStr("logger", name)

		// Cut filename now.
		var filename string
		filename, data, ok = strings.Cut(data, consoleSep)
		if !ok {
			return errors.New("expected filename")
		}

		// Gracefully handle invalid source.
		src, err := ParseSource(filename)
		if err == nil {
			src.Add(attrs)
		} else {
			attrs.PutStr("code.file.path", filename)
		}
	}

	// Cut message.
	msg, data, ok := strings.Cut(data, consoleSep)
	target.Body = msg
	if ok {
		if err := jx.DecodeStr(data).ObjBytes(func(d *jx.Decoder, k []byte) error {
			ftyp, _ := deduceFieldType(k)
			switch ftyp {
			case traceIDField:
				if d.Next() != jx.String {
					return addJSONMapKey(attrs, string(k), d)
				}
				v, err := d.Str()
				if err != nil {
					return err
				}
				traceID, ok := ParseTraceID(v)
				if !ok {
					attrs.PutStr(string(k), v)
					return nil
				}
				target.TraceID = traceID
			case spanIDField:
				if d.Next() != jx.String {
					// TODO: handle integers
					return addJSONMapKey(attrs, string(k), d)
				}
				v, err := d.StrBytes()
				if err != nil {
					return err
				}
				spanID, ok := ParseSpanID(v)
				if !ok {
					attrs.PutStr(string(k), string(v))
					return nil
				}
				target.SpanID = spanID
			default:
				return addJSONMapKey(attrs, string(k), d)
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, "read object")
		}
	}
	return nil
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
