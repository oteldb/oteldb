package logparser

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/google/uuid"

	"github.com/go-faster/errors"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// ISO8601Millis time format with millisecond precision.
const ISO8601Millis = "2006-01-02T15:04:05.000Z0700"

// we are not expecting logs from past.
var deduceStart = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// DeduceNanos returns unix nano from arbitrary time integer, deducing resolution by range.
func DeduceNanos(n int64) (int64, bool) {
	if n > deduceStart.UnixNano() {
		return n, true
	}
	if n > deduceStart.UnixMicro() {
		return n * 1e3, true
	}
	if n > deduceStart.UnixMilli() {
		return n * 1e6, true
	}
	if n > deduceStart.Unix() {
		return n * 1e9, true
	}
	return 0, false
}

// DeduceSeverity deduces severity from given level.
func DeduceSeverity(text string) plog.SeverityNumber {
	normalized := false
retry:
	if len(text) == 1 {
		first, _ := utf8.DecodeRuneInString(text)
		r := unicode.ToLower(first)
		switch r {
		case 'i':
			return plog.SeverityNumberInfo
		case 't':
			return plog.SeverityNumberTrace
		case 'd':
			return plog.SeverityNumberDebug
		case 'w':
			return plog.SeverityNumberWarn
		case 'e':
			return plog.SeverityNumberError
		case 'f':
			return plog.SeverityNumberFatal
		}
	}
	switch text {
	case "":
		return plog.SeverityNumberUnspecified
	case "trace", "TRACE":
		return plog.SeverityNumberTrace
	case "debug", "DEBUG":
		return plog.SeverityNumberDebug
	case "info", "INFO":
		return plog.SeverityNumberInfo
	case "warn", "WARN", "warning", "WARNING":
		return plog.SeverityNumberWarn
	case "error", "ERROR":
		return plog.SeverityNumberError
	case "fatal", "FATAL", "crit", "CRIT", "critical", "CRITICAL":
		return plog.SeverityNumberFatal
	default:
		if normalized {
			return plog.SeverityNumberUnspecified
		}
		text = strings.TrimSpace(text)
		text = strings.ToLower(text)
		normalized = true
		goto retry
	}
}

// ParseTraceID tries its best to parse trace ID.
func ParseTraceID(s string) (otelstorage.TraceID, bool) {
	traceID, err := otelstorage.ParseTraceID(s)
	if err != nil {
		// Trying to parse as UUID.
		id, err := uuid.Parse(strings.ToLower(s))
		if err != nil {
			return traceID, false
		}
		traceID = otelstorage.TraceID(id)
	}
	return traceID, true
}

// ParseSpanID tries its best to parse trace ID.
func ParseSpanID[S ~string | ~[]byte](s S) (spanID otelstorage.SpanID, _ bool) {
	decodedLen := hex.DecodedLen(len(s))
	if decodedLen != 8 {
		return spanID, false
	}

	var buf [32]byte
	n := copy(buf[:], s)
	_, err := hex.Decode(spanID[:], buf[:n])
	if err != nil {
		return spanID, false
	}
	return spanID, true
}

// Source locates source code produced the log record.
type Source struct {
	Filename string
	Line     int
	Column   int
}

// ParseSource parses source location.
func ParseSource(s string) (src Source, _ error) {
	s = strings.TrimSpace(s)
	idx := strings.LastIndex(s, ":")
	if idx < 0 {
		return Source{}, errors.New("a ':' expected")
	}
	var (
		rawLineNumber   = s[idx+1:]
		rawColumnNumber = ""
	)
	s = s[:idx]
	src.Filename = s

	idx = strings.LastIndex(s, ":")
	if idx >= 0 {
		rawColumnNumber = rawLineNumber
		rawLineNumber = s[idx+1:]
		s = s[:idx]
		src.Filename = s
	}
	if src.Filename == "" {
		return Source{}, errors.New("filename is empty")
	}

	var err error
	src.Line, err = strconv.Atoi(rawLineNumber)
	if err != nil {
		return Source{}, errors.Wrap(err, "parse line number")
	}
	if rawColumnNumber != "" {
		src.Column, err = strconv.Atoi(rawColumnNumber)
		if err != nil {
			return Source{}, errors.Wrap(err, "parse column number")
		}
	}
	return src, nil
}

// Add adds fields as attributes to given set.
func (s *Source) Add(m pcommon.Map) bool {
	if s.Filename == "" {
		return false
	}
	m.PutStr("code.file.path", s.Filename)
	if s.Line != 0 {
		m.PutInt("code.line.number", int64(s.Line))
	}
	if s.Column != 0 {
		m.PutInt("code.column.number", int64(s.Column))
	}
	return true
}

type fieldKind uint8

const (
	unknownField = fieldKind(iota + 1)
	levelField
	timestampField
	messageField
	traceIDField
	spanIDField
)

var wellKnownFieldNames = map[string]fieldKind{
	"level": levelField, "lvl": levelField, "levelStr": levelField, "severity_text": levelField, "severity": levelField, "levelname": levelField,
	"t": timestampField, "ts": timestampField, "time": timestampField, "@timestamp": timestampField, "timestamp": timestampField,
	"message": messageField, "msg": messageField,
	"trace_id": traceIDField, "traceid": traceIDField, "traceID": traceIDField, "traceId": traceIDField,
	"span_id": spanIDField, "spanid": spanIDField, "spanID": spanIDField, "spanId": spanIDField,
}

func deduceFieldType[S ~string | ~[]byte](name S) (fieldKind, bool) {
	v, ok := wellKnownFieldNames[string(name)]
	return v, ok
}
