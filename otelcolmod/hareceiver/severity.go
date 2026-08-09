package hareceiver

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"
)

// severityScanTokens is how many leading whitespace-separated tokens of a
// message are examined for a level. Home Assistant Core and Supervisor both
// prefix their messages with a timestamp before the level.
const severityScanTokens = 4

var severityLevels = map[string]plog.SeverityNumber{
	"CRITICAL": plog.SeverityNumberFatal,
	"FATAL":    plog.SeverityNumberFatal,
	"ERROR":    plog.SeverityNumberError,
	"ERR":      plog.SeverityNumberError,
	"WARNING":  plog.SeverityNumberWarn,
	"WARN":     plog.SeverityNumberWarn,
	"NOTICE":   plog.SeverityNumberInfo2,
	"INFO":     plog.SeverityNumberInfo,
	"DEBUG":    plog.SeverityNumberDebug,
	"TRACE":    plog.SeverityNumberTrace,
}

// detectSeverity looks for a level in the leading tokens of msg.
//
// Home Assistant does not expose the journal PRIORITY field through its API, so
// severity can only be recovered from the message text. The match is
// deliberately narrow — an exact, upper-case, whole token — to keep add-on logs
// that do not follow the convention from being mislabelled.
func detectSeverity(msg string) (plog.SeverityNumber, string, bool) {
	for range severityScanTokens {
		var token string
		token, msg, _ = strings.Cut(strings.TrimLeft(msg, " \t"), " ")
		if token == "" {
			return plog.SeverityNumberUnspecified, "", false
		}
		if sev, ok := severityLevels[token]; ok {
			return sev, token, true
		}
	}
	return plog.SeverityNumberUnspecified, "", false
}
