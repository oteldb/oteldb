package hareceiver

import (
	"strings"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/oteldb/oteldb/internal/logparser"
)

// severityScanTokens is how many leading whitespace-separated tokens of a
// message are examined for a level. Home Assistant Core and Supervisor both
// prefix their messages with a timestamp before the level.
const severityScanTokens = 4

// isLevel reports whether token is a level, requiring upper case so that
// ordinary prose is not mistaken for one. [logparser.DeduceSeverity] itself is
// case-insensitive and accepts single letters, which is too permissive here.
func isLevel(token string) (plog.SeverityNumber, bool) {
	if len(token) < 3 || token != strings.ToUpper(token) {
		return plog.SeverityNumberUnspecified, false
	}
	sev := logparser.DeduceSeverity(token)
	return sev, sev != plog.SeverityNumberUnspecified
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
		if sev, ok := isLevel(token); ok {
			return sev, token, true
		}
	}
	return plog.SeverityNumberUnspecified, "", false
}
