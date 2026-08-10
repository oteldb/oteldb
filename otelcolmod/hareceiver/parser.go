package hareceiver

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Entry is a journal entry recovered from the Home Assistant log API.
//
// Home Assistant renders journal entries to text before returning them, so only
// the fields of the verbose formatter survive. See the package README.
type Entry struct {
	Timestamp  time.Time
	Hostname   string
	Identifier string
	PID        int64
	HasPID     bool
	Message    string
}

// unknownIdentifier is what Home Assistant emits for entries without a
// SYSLOG_IDENTIFIER.
const unknownIdentifier = "_UNKNOWN_"

// entryLine matches a line produced by Home Assistant's verbose journal
// formatter: "<ts> <hostname> <identifier>[<pid>]: <message>", where the
// timestamp is UTC with millisecond precision.
var entryLine = regexp.MustCompile(
	`^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) (\S*) (.*?)(?:\[(\d+)\])?: (.*)$`,
)

const entryTimeLayout = "2006-01-02 15:04:05.000"

var epoch = time.Unix(0, 0).UTC()

// ParseEntries parses a verbose log response body into journal entries.
//
// A line that does not start a new entry is a continuation of the previous
// entry's message, since a journal MESSAGE may span multiple lines. Leading
// continuation lines have no entry to attach to and are dropped: the response
// may begin in the middle of a multi-line message.
func ParseEntries(body string) []Entry {
	var entries []Entry
	for line := range strings.SplitSeq(strings.TrimSuffix(body, "\n"), "\n") {
		m := entryLine.FindStringSubmatch(line)
		if m == nil {
			if n := len(entries); n > 0 {
				entries[n-1].Message += "\n" + line
			}
			continue
		}

		// A journal __REALTIME_TIMESTAMP is an unsigned microsecond count, so a
		// pre-epoch timestamp cannot start an entry, only look like one.
		ts, err := time.ParseInLocation(entryTimeLayout, m[1], time.UTC)
		if err != nil || ts.Before(epoch) {
			if n := len(entries); n > 0 {
				entries[n-1].Message += "\n" + line
			}
			continue
		}

		e := Entry{
			Timestamp:  ts,
			Hostname:   m[2],
			Identifier: m[3],
			Message:    m[5],
		}
		if e.Identifier == unknownIdentifier {
			e.Identifier = ""
		}
		if m[4] != "" {
			// Regexp guarantees digits, overflow is the only failure mode.
			if pid, err := strconv.ParseInt(m[4], 10, 64); err == nil {
				e.PID, e.HasPID = pid, true
			}
		}
		entries = append(entries, e)
	}
	return entries
}
