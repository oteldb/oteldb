package logparser

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/collector/pdata/plog"
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
