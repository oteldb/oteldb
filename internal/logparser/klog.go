package logparser

import (
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/go-faster/errors"
	"github.com/kr/logfmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/go-faster/oteldb/internal/otelstorage"
)

// KLogParser can parse klog record into [Line].
type KLogParser struct{}

var _ Parser = (*KLogParser)(nil)

func init() {
	p := &KLogParser{}
	formatRegistry.Store(p.String(), p)
}

// Parse klog line into [Line].
func (KLogParser) Parse(data string, target *Record) error {
	if target.Attrs.IsZero() {
		target.Attrs = otelstorage.NewAttrs()
	}
	m := target.Attrs.AsMap()

	t := target.ObservedTimestamp.AsTime()
	if target.ObservedTimestamp == 0 {
		t = target.Timestamp.AsTime()
		if target.Timestamp == 0 {
			t = time.Now()
		}
	}

	rest, h, ok := parseKLogHeader(t, data)
	if !ok {
		return errors.New("invalid klog header")
	}
	target.Timestamp = pcommon.NewTimestampFromTime(h.Timestamp)
	target.SeverityNumber = h.Level
	target.Body = rest
	h.Source.Add(m)
	if h.ThreadID != 0 {
		m.PutInt("thread.id", h.ThreadID)
	}

	if err := parseKLogMessage(rest, target); err != nil {
		return errors.Wrap(err, "parse message")
	}
	return nil
}

// Detect if line is parsable by this parser.
func (KLogParser) Detect(line string) bool {
	_, _, ok := parseKLogHeader(time.Now(), line)
	return ok
}

func (KLogParser) String() string {
	return "klog"
}

type klogHeader struct {
	Level     plog.SeverityNumber
	Timestamp time.Time
	ThreadID  int64
	Source    Source
}

func parseKLogHeader(year time.Time, s string) (rest string, h klogHeader, _ bool) {
	// See https://github.com/kubernetes/klog/blob/v1.0.0/klog.go#L599-L639.
	if len(s) < 21 {
		return rest, h, false
	}

	switch level := unicode.ToUpper(rune(s[0])); level {
	case 'D':
		h.Level = plog.SeverityNumberDebug
	case 'I':
		h.Level = plog.SeverityNumberInfo
	case 'W':
		h.Level = plog.SeverityNumberWarn
	case 'E':
		h.Level = plog.SeverityNumberError
	case 'F':
		h.Level = plog.SeverityNumberFatal
	default:
		return rest, h, false
	}
	s = s[1:]

	// <month><day> <hour>:<minute>:<second>.<microseconds padded to 6 digits>
	const timeFormat = `0102 15:04:05.999999`
	if len(s) < len(timeFormat) {
		return rest, h, false
	}
	rawTimestamp := s[:len(timeFormat)]
	s = s[len(timeFormat):]

	t, err := time.Parse(timeFormat, rawTimestamp)
	if err != nil {
		return rest, h, false
	}
	t = time.Date(year.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), t.Location()).UTC()
	h.Timestamp = t

	additional, rest, ok := strings.Cut(s, "]")
	if !ok {
		return rest, h, false
	}

	if v := strings.TrimSpace(additional); v != "" {
		rawThreadID, source, ok := strings.Cut(v, " ")
		if ok {
			h.Source, _ = ParseSource(source)
			h.ThreadID, _ = strconv.ParseInt(rawThreadID, 10, 64)
		}
	}

	return rest, h, true
}

func parseKLogMessage(s string, target *Record) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("message is empty")
	}
	if s[0] != '"' {
		// The rest of the line is just message as-is.
		target.Body = s
		return nil
	}

	message, err := strconv.QuotedPrefix(s)
	if err != nil {
		return errors.Wrap(err, "get message")
	}
	target.Body, err = strconv.Unquote(message)
	if err != nil {
		return errors.Wrap(err, "parse message")
	}

	s = s[len(message):]

	// Parse labels.
	return logfmt.Unmarshal([]byte(s), target)
}
