package hareceiver

import "regexp"

// appMessage is the structure Home Assistant Core and Supervisor put inside a
// journal MESSAGE, as produced by their Python logging formatter:
//
//	2026-08-09 15:26:04.893 INFO (SyncWorker_2) [supervisor.backups.backup] Backing up folder ssl
//
// The leading timestamp is the application's own, rendered in the instance's
// local timezone, and duplicates the journal timestamp — it is dropped in
// favor of __REALTIME_TIMESTAMP, which is unambiguous.
type appMessage struct {
	Level   string
	Thread  string
	Logger  string
	Message string
}

// (?s) so that a multi-line message, such as a traceback, stays in Message.
var appMessageLine = regexp.MustCompile(
	`(?s)^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} ([A-Z]+) \(([^)\n]*)\) \[([^\]\n]*)\] (.*)$`,
)

// parseAppMessage splits a Home Assistant application log line into its parts.
//
// Unlike [detectSeverity], this is an exact match on a fixed format rather than
// a guess, so the level it yields is authoritative.
func parseAppMessage(msg string) (appMessage, bool) {
	m := appMessageLine.FindStringSubmatch(msg)
	if m == nil {
		return appMessage{}, false
	}
	return appMessage{
		Level:   m[1],
		Thread:  m[2],
		Logger:  m[3],
		Message: m[4],
	}, true
}
